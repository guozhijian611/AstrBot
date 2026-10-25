import asyncio
import copy
import json
import os
import re
import shutil
import uuid
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote

from astrbot.api.message_components import Image, Plain, Record, Reply
from astrbot.api.platform import Group, MessageMember
from astrbot.core.utils.astrbot_path import get_astrbot_temp_path
from astrbot.core.utils.tencent_record_helper import tencent_silk_to_wav

if TYPE_CHECKING:
    from .wechat_use_adapter import WechatUseAdapter


class WechatUseOperationError(RuntimeError):
    """A stable failure code without private raw CLI diagnostics."""


class WechatUseClient:
    """Read native Bridge APIs and use the account-bound local CLI for media."""

    def __init__(self, adapter: "WechatUseAdapter") -> None:
        self.adapter = adapter
        self.media_dir = Path(get_astrbot_temp_path()) / "wechat_use"
        self.cli_path = str(adapter.config.get("wechat_use_cli_path", "auto"))
        self.audio_mode = adapter.config.get("wechat_use_audio_mode", "wechat_use")
        if self.audio_mode not in {"wechat_use", "astrbot", "off"}:
            raise ValueError("Invalid wechat-use audio mode")

    def validate_chat(self, chat: str) -> None:
        """Require an exact ID within the configured messaging scope.

        Args:
            chat: Exact WeChat username or chatroom ID.

        Raises:
            ValueError: The ID is invalid or outside the configured scope.
        """
        if (
            not isinstance(chat, str)
            or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.@-]*", chat)
            or (chat.endswith("@chatroom") and chat not in self.adapter.group_ids)
            or (not chat.endswith("@chatroom") and not self.adapter.private_enabled)
        ):
            raise ValueError("wechat-use recipient is outside the configured scope")

    async def request(self, path: str, params: dict | None = None) -> dict | list:
        """Perform one authenticated read without redirects or hidden retries.

        Args:
            path: Fixed native Bridge endpoint.
            params: Endpoint query parameters.

        Returns:
            Original JSON, including coverage and completeness metadata.

        Raises:
            RuntimeError: The Bridge refuses the query or returns invalid data.
        """
        session = self.adapter._http
        if session is None:
            raise RuntimeError("wechat-use adapter is not running")
        async with session.get(
            self.adapter.base_url + path, params=params, allow_redirects=False
        ) as response:
            if response.status != 200:
                raise RuntimeError(f"wechat-use query HTTP {response.status}")
            payload = await response.json()
        if not isinstance(payload, dict | list):
            raise RuntimeError("Invalid wechat-use query response")
        return payload

    def cli_executable(self) -> str:
        """Validate local media/account binding before any part of a chain is sent.

        Returns:
            Absolute executable path for the currently bound managed account.

        Raises:
            RuntimeError: The local CLI cannot safely target the configured account.
        """
        from yarl import URL

        if URL(self.adapter.base_url).host not in {"127.0.0.1", "localhost", "::1"}:
            raise RuntimeError("Local CLI media requires a loopback Bridge")
        # Do not redirect the managed client to another account or process.
        base = Path.home() / ".wx-rs"
        try:
            managed = json.loads((base / "managed-wechat.json").read_text())
            bundle = managed["bundle_id"]
            if not re.fullmatch(r"[A-Za-z0-9_.-]+", bundle):
                raise ValueError("Invalid managed bundle")
            config = json.loads(
                (base / bundle.replace(".", "_") / "config.json").read_text()
            )
            account = config.get("account_wxid", "")
            if account != self.adapter.self_id and not re.fullmatch(
                re.escape(self.adapter.self_id) + r"_[0-9a-fA-F]{4,}", account
            ):
                raise ValueError("Account mismatch")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise RuntimeError(
                "wechat-use local CLI account binding is unavailable"
            ) from exc
        executable = self.cli_path
        if executable == "auto":
            executable = shutil.which("wechat-use") or str(
                Path.home() / ".local" / "bin" / "wechat-use"
            )
        if not Path(executable).is_file() or not os.access(executable, os.X_OK):
            raise RuntimeError("wechat-use CLI is not installed at the configured path")
        return executable

    async def run_cli(
        self, *args: str, timeout: float = 120, check: bool = True
    ) -> tuple[int, dict | list]:
        """Run a fixed CLI operation against the same managed local account.

        Args:
            *args: Explicit argument vector; no shell is involved.
            timeout: Maximum operation duration in seconds.
            check: Whether nonzero exit codes should raise an exception.

        Returns:
            Exit code and parsed JSON. Send exit code 3 can be unconfirmed.

        Raises:
            RuntimeError: The CLI, account binding or JSON result is unavailable.
            TimeoutError: The operation timed out; it is never retried.
        """
        if not args or args[0] not in {
            "send",
            "send-receipt",
            "audio",
            "members",
            "search",
            "stats",
            "export",
        }:
            raise ValueError("Unsupported wechat-use CLI operation")
        if (
            args[0] == "audio"
            and (len(args) < 2 or args[1] not in {"get", "transcribe"})
        ) or any(arg in {"--account", "--bundle-id", "--pid"} for arg in args):
            raise ValueError(
                "Account overrides and administrative CLI operations are forbidden"
            )
        executable = self.cli_executable()
        process = await asyncio.create_subprocess_exec(
            executable,
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
        except (TimeoutError, asyncio.CancelledError):
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            await process.communicate()
            raise
        # Diagnostics may contain private XML, local paths or media keys.
        try:
            payload = json.loads(stdout)
        except (ValueError, UnicodeError) as exc:
            raise RuntimeError("wechat-use CLI returned no structured result") from exc
        if not isinstance(payload, dict | list):
            raise RuntimeError("Invalid wechat-use CLI response")
        if check and process.returncode:
            raw_error = (
                str(payload.get("error", "")) if isinstance(payload, dict) else ""
            )
            reason = next(
                (
                    code
                    for code in (
                        "source_database_access_paused",
                        "db_scope_unsupported",
                        "not_supported",
                        "no_deps",
                    )
                    if code in raw_error
                ),
                f"cli_exit_{process.returncode}",
            )
            raise WechatUseOperationError(reason)
        return int(process.returncode or 0), payload

    async def get_sessions(self, limit: int = 50) -> dict | list:
        return await self.request("/chats", {"limit": str(max(1, min(limit, 1000)))})

    async def get_unread(self) -> dict | list:
        return await self.request("/unread")

    async def get_contacts(self, query: str = "", limit: int = 50) -> dict | list:
        return await self.request(
            "/contacts", {"query": query, "limit": str(max(1, min(limit, 1000)))}
        )

    async def resolve_recipient(self, hint: str) -> dict | list:
        """Return all candidates; never select a fuzzy match automatically.

        Args:
            hint: Contact or group hint.

        Returns:
            The complete resolver response.
        """
        return await self.request("/resolve", {"hint": hint})

    async def get_history(
        self,
        chat: str,
        limit: int = 50,
        since: int | None = None,
        until: int | None = None,
    ) -> dict | list:
        self.validate_chat(chat)
        params = {"limit": str(max(1, min(limit, 1000)))}
        if since is not None:
            params["since"] = str(int(since))
        if until is not None:
            params["until"] = str(int(until))
        return await self.request(f"/chat/{quote(chat, safe='')}/history", params)

    async def search_messages(
        self, query: str, chat: str, limit: int = 50
    ) -> dict | list:
        self.validate_chat(chat)
        _, payload = await self.run_cli(
            "search",
            "--in",
            chat,
            "-n",
            str(max(1, min(limit, 1000))),
            "--json",
            "--no-images",
            "--",
            query,
        )
        return payload

    async def get_stats(self, chat: str) -> dict | list:
        self.validate_chat(chat)
        _, payload = await self.run_cli("stats", chat, "--json")
        return payload

    async def export_history(self, chat: str, format: str = "json") -> dict | list:
        self.validate_chat(chat)
        if format not in {"json", "markdown", "csv", "html"}:
            raise ValueError("Unsupported export format")
        self.media_dir.mkdir(parents=True, exist_ok=True)
        destination = self.media_dir / f"export-{uuid.uuid4().hex}.{format}"
        _, payload = await self.run_cli(
            "export",
            chat,
            "--format",
            format,
            "--output",
            str(destination),
            "--no-images",
            "--json",
            timeout=300,
        )
        return payload

    async def get_group(self, chat: str, name: str | None = None) -> Group:
        """Read group members when supported by the current runtime.

        Args:
            chat: Exact allowed chatroom ID.
            name: Optional group name from the inbound event.

        Returns:
            Group information enriched with available members.

        Raises:
            RuntimeError: The current runtime does not provide group members.
        """
        self.validate_chat(chat)
        if not chat.endswith("@chatroom"):
            raise ValueError("Expected a group ID")
        _, payload = await self.run_cli("members", chat, "--json")
        members = (
            payload
            if isinstance(payload, list)
            else payload.get("members", payload.get("rows"))
        )
        if not isinstance(members, list):
            raise RuntimeError("Invalid group member response")
        group = Group(chat, name, members=[])
        for member in members:
            if not isinstance(member, dict):
                continue
            user_id = (
                member.get("wxid")
                or member.get("username")
                or member.get("member_wxid")
            )
            if isinstance(user_id, str) and user_id:
                group.members.append(
                    MessageMember(
                        user_id,
                        member.get("display_name") or member.get("nickname") or user_id,
                    )
                )
        group.member_count = len(group.members)
        return group

    async def enrich_message(self, message) -> list[str]:
        """Materialize media only after message scope and deduplication checks.

        Args:
            message: Converted, claimed AstrBot message to enrich in place.

        Returns:
            Event-owned temporary paths for lifecycle cleanup.
        """
        row = message.raw_message
        kind = row.get("message_kind")
        media = row.get("media") if isinstance(row.get("media"), dict) else {}
        paths: list[str] = []
        if kind not in {"image", "emoticon", "audio", "voice"}:
            # A fresh @-question may quote an older image/audio message. Resolve
            # only that exact ID from the same allowed conversation, never the
            # whole history or an untrusted download URL from quoted XML.
            quoted_reply = next(
                (p for p in message.message if isinstance(p, Reply)), None
            )
            if (
                quoted_reply is not None
                and quoted_reply.message_str == "[引用媒体消息]"
            ):
                try:
                    history = await self.get_history(message.session_id, limit=100)
                    rows = (
                        history.get("rows", [])
                        if isinstance(history, dict)
                        else history
                    )
                    matched = next(
                        (
                            r
                            for r in rows
                            if isinstance(r, dict)
                            and str(r.get("server_id_str") or r.get("server_id"))
                            == str(quoted_reply.id)
                            and r.get("chat_username", message.session_id)
                            == message.session_id
                            and r.get("message_kind")
                            in {"image", "emoticon", "audio", "voice"}
                        ),
                        None,
                    )
                    if matched is None:
                        raise RuntimeError(
                            "Quoted media is outside the available history window"
                        )
                    quoted = copy.copy(message)
                    quoted.raw_message = matched
                    quoted.message = []
                    quoted.message_str = ""
                    paths.extend(await self.enrich_message(quoted))
                    message.message.append(Plain("\n[所引用的媒体]\n"))
                    message.message.extend(quoted.message)
                except Exception as exc:
                    message.message.append(
                        Plain(f"\n[引用媒体不可用：{type(exc).__name__}；未重试]")
                    )
                message.message_str = "".join(
                    p.text for p in message.message if isinstance(p, Plain)
                )
            return paths
        if media.get("local_path_status") in {"unavailable", "pending", "unknown"}:
            message.message.append(Plain("[媒体当前不可用，未重新下载]"))
            message.message_str += "\n[媒体当前不可用，未重新下载]"
            return paths
        self.media_dir.mkdir(parents=True, exist_ok=True)
        self.media_dir.chmod(0o700)
        try:
            if kind in {"image", "emoticon"}:
                ref = media.get("image_ref")
                ref = ref if isinstance(ref, dict) else {}
                local_id = (
                    ref.get("localId") or row.get("local_id_str") or row.get("local_id")
                )
                server_id = (
                    ref.get("serverId")
                    or row.get("server_id_str")
                    or row.get("server_id")
                )
                if ref.get("chatId", message.session_id) != message.session_id:
                    raise ValueError("Image reference chat mismatch")
                if not str(local_id).isdigit() or not str(server_id).isdigit():
                    raise ValueError("Image reference lacks exact IDs")
                params = {
                    "chatId": message.session_id,
                    "localId": str(local_id),
                    "serverId": str(server_id),
                }
                if md5 := ref.get("md5") or media.get("md5"):
                    params["md5"] = str(md5)
                session = self.adapter._http
                if session is None:
                    raise RuntimeError("Adapter is not running")
                async with session.get(
                    self.adapter.base_url + "/image",
                    params=params,
                    allow_redirects=False,
                ) as response:
                    if response.status != 200:
                        raise RuntimeError("Verified image is unavailable")
                    mime = response.headers.get("Content-Type", "").split(";")[0]
                    suffix = {
                        "image/png": ".png",
                        "image/jpeg": ".jpg",
                        "image/gif": ".gif",
                        "image/webp": ".webp",
                    }.get(mime)
                    if suffix is None:
                        raise ValueError("Bridge returned unsupported image bytes")
                    content = bytearray()
                    async for chunk in response.content.iter_chunked(65536):
                        content.extend(chunk)
                        if len(content) > 32 * 1024 * 1024:
                            raise ValueError("Image exceeds the adapter's 32 MiB limit")
                destination = self.media_dir / (uuid.uuid4().hex + suffix)
                destination.write_bytes(content)
                destination.chmod(0o600)
                paths.append(str(destination))
                message.message.append(Image.fromFileSystem(destination))
            elif self.audio_mode != "off":
                server_id = row.get("server_id_str") or row.get("server_id")
                if not str(server_id).isdigit() or int(server_id) <= 0:
                    raise ValueError("Audio requires a server-backed message ID")
                if self.audio_mode == "wechat_use":
                    transcript = media.get("transcript")
                    if not isinstance(transcript, str) or not transcript.strip():
                        _, result = await self.run_cli(
                            "audio",
                            "transcribe",
                            str(server_id),
                            "--json",
                            "--include-transcript",
                            timeout=300,
                        )
                        transcript = (
                            result.get("transcript")
                            if isinstance(result, dict)
                            else None
                        )
                    if not isinstance(transcript, str) or not transcript.strip():
                        raise RuntimeError("No audio transcript is available")
                    message.message.append(Plain("[语音转写] " + transcript))
                    message.message_str += "\n[语音转写] " + transcript
                else:
                    silk = self.media_dir / (uuid.uuid4().hex + ".silk")
                    wav = silk.with_suffix(".wav")
                    paths.extend([str(silk), str(wav)])
                    await self.run_cli(
                        "audio", "get", str(server_id), "--out", str(silk), "--json"
                    )
                    await tencent_silk_to_wav(str(silk), str(wav))
                    wav.chmod(0o600)
                    message.message.append(Record.fromFileSystem(wav))
        except Exception as exc:
            # A decoder/backend failure must preserve the text event. Cancellation
            # is a BaseException and intentionally propagates instead.
            reason = (
                str(exc)
                if isinstance(exc, WechatUseOperationError)
                else type(exc).__name__
            )
            message.message.append(Plain(f"[媒体不可用：{reason}；未重试]"))
        message.message_str = "".join(
            component.text
            for component in message.message
            if isinstance(component, Plain)
        )
        return paths
