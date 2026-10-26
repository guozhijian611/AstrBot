import asyncio
import hashlib
import json
import shutil
import time
import uuid
from pathlib import Path

import aiohttp
import aiosqlite
from yarl import URL

from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import At, Node, Plain, Reply, Share
from astrbot.api.platform import (
    AstrBotMessage,
    Group,
    MessageMember,
    MessageType,
    Platform,
    PlatformMetadata,
    register_platform_adapter,
)
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

from .client import WechatUseClient
from .outbound import prepare_outbound
from .wechat_use_event import WechatUseMessageEvent


class WechatUseHealthError(RuntimeError):
    """Expose only locally constructed, safe Bridge health diagnostics."""


@register_platform_adapter(
    "wechat_use",
    "Local macOS WeChat via wechat-use HTTP Bridge",
    adapter_display_name="WeChat (wechat-use)",
    support_streaming_message=False,
)
class WechatUseAdapter(Platform):
    def __init__(
        self,
        platform_config: dict,
        platform_settings: dict,
        event_queue: asyncio.Queue,
    ) -> None:
        super().__init__(platform_config, event_queue)
        self.settings = platform_settings
        self.base_url = str(
            platform_config.get("wechat_use_bridge_url", "http://127.0.0.1:18400")
        ).rstrip("/")
        url = URL(self.base_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.host
            or url.user is not None
            or url.path not in {"", "/"}
            or url.query_string
            or url.fragment
        ):
            raise ValueError("wechat-use Bridge URL must be an HTTP(S) origin")
        self.self_id = str(platform_config.get("wechat_use_self_id", "")).strip()
        if not self.self_id or self.self_id.endswith("@chatroom"):
            raise ValueError("wechat-use requires the logged-in account's exact wxid")
        group_ids = platform_config.get("wechat_use_group_ids", [])
        if not isinstance(group_ids, list) or any(
            not isinstance(group_id, str) or not group_id.endswith("@chatroom")
            for group_id in group_ids
        ):
            raise ValueError(
                "wechat-use group IDs must be a list of exact @chatroom IDs"
            )
        self.group_ids = set(group_ids)
        self.private_enabled = bool(
            platform_config.get("wechat_use_private_enabled", False)
        )
        self.mention_only = bool(
            platform_config.get("wechat_use_require_mention", True)
        )
        self.metadata = PlatformMetadata(
            name="wechat_use",
            description="Local macOS WeChat via wechat-use HTTP Bridge",
            id=str(platform_config["id"]),
            adapter_display_name="WeChat (wechat-use)",
            support_streaming_message=False,
        )
        # Store only adapter-owned deduplication and submission metadata here.
        digest = hashlib.sha256(self.metadata.id.encode()).hexdigest()[:24]
        self.state_path = (
            Path(get_astrbot_data_path()) / "wechat_use" / f"{digest}.sqlite3"
        )
        self._db: aiosqlite.Connection | None = None
        self._http: aiohttp.ClientSession | None = None
        self._running = False
        self._receive_started_at = int(time.time())
        self._cursor = self._receive_started_at
        self.client = WechatUseClient(self)

    def meta(self) -> PlatformMetadata:
        return self.metadata

    def get_client(self) -> WechatUseClient:
        return self.client

    async def initialize(self) -> None:
        """Open the adapter's own journal and HTTP connection.

        Raises:
            OSError: The adapter data directory cannot be created.
        """
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.parent.chmod(0o700)
        self._db = await aiosqlite.connect(self.state_path)
        self.state_path.chmod(0o600)
        await self._db.executescript(
            "CREATE TABLE IF NOT EXISTS inbound ("
            "account TEXT, chat TEXT, message TEXT, "
            "PRIMARY KEY(account, chat, message));"
            "CREATE TABLE IF NOT EXISTS submissions ("
            "request_id TEXT PRIMARY KEY, recipient TEXT, status TEXT, "
            "bridge_request_id TEXT, created_at INTEGER);"
        )
        # Never assign today's account to legacy attempts with unknown ownership.
        async with self._db.execute("PRAGMA table_info(submissions)") as cursor:
            columns = {row[1] for row in await cursor.fetchall()}
        for name in ("account", "kind", "reason"):
            if name not in columns:
                await self._db.execute(
                    f"ALTER TABLE submissions ADD COLUMN {name} TEXT"
                )
        await self._db.commit()
        headers = {}
        if token := self.config.get("wechat_use_bridge_token"):
            headers["Authorization"] = f"Bearer {token}"
        self._http = aiohttp.ClientSession(
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=120, connect=10),
            trust_env=False,
        )

    async def check_health(self) -> dict:
        """Validate the native Bridge and its current receive readiness.

        Returns:
            Bridge health metadata.

        Raises:
            RuntimeError: The Bridge is incompatible or WeChat is not ready.
        """
        if self._http is None:
            raise WechatUseHealthError("wechat-use adapter is not running")
        async with self._http.get(
            f"{self.base_url}/health", allow_redirects=False
        ) as response:
            if response.status != 200:
                raise WechatUseHealthError(f"wechat-use health HTTP {response.status}")
            health = await response.json()
        if not isinstance(health, dict) or "bridge_version" not in health:
            raise WechatUseHealthError("wechat-use requires the native Bridge shape")
        if not all(
            isinstance(health.get(field), dict)
            for field in ("daemon", "wechat", "receive_readiness")
        ):
            raise WechatUseHealthError("wechat-use health metadata is invalid")
        if health["daemon"].get("alive") is not True:
            raise WechatUseHealthError("wechat-use daemon is not ready")
        if health["wechat"].get("account_state") != "ready":
            raise WechatUseHealthError("wechat-use account is not ready")
        if health["receive_readiness"].get("ready") is not True:
            source = health["receive_readiness"].get("source")
            refusal = (
                source.get("last_capture_refusal") if isinstance(source, dict) else None
            )
            stage = refusal.get("capture_step") if isinstance(refusal, dict) else None
            # Never include arbitrary upstream text, paths or credentials in errors.
            if not isinstance(stage, str) or stage not in {
                "profile_fetch",
                "profile_verify",
                "capture",
                "metadata_load",
                "adaptation",
            }:
                stage = "unknown"
            raise WechatUseHealthError(
                f"wechat-use receiver is not ready (stage={stage})"
            )
        return health

    async def run(self) -> None:
        self._running = True
        self._receive_started_at = self._cursor = int(time.time())
        try:
            await self.initialize()
            while self._running:
                try:
                    await self.check_health()
                    if self.last_error is not None:
                        logger.info(
                            "wechat-use receiver recovered; reconnecting event stream"
                        )
                    self.clear_errors()
                    assert self._http is not None
                    async with self._http.get(
                        f"{self.base_url}/messages/stream",
                        params={"since": str(self._cursor)},
                        timeout=aiohttp.ClientTimeout(
                            total=None, connect=10, sock_read=90
                        ),
                        read_bufsize=1024 * 1024,
                        allow_redirects=False,
                    ) as response:
                        if response.status != 200:
                            raise RuntimeError(
                                f"wechat-use event stream HTTP {response.status}"
                            )
                        # SSE events can span multiple data lines or TCP chunks.
                        data_lines: list[str] = []
                        async for raw_line in response.content:
                            line = raw_line.decode("utf-8").rstrip("\r\n")
                            if not line:
                                if data_lines:
                                    await self.handle_batch(
                                        json.loads("\n".join(data_lines))
                                    )
                                    data_lines.clear()
                            elif line.startswith("data:"):
                                data_lines.append(line[5:].lstrip(" "))
                        raise RuntimeError("wechat-use event stream closed")
                except asyncio.CancelledError:
                    raise
                except (
                    aiohttp.ClientError,
                    TimeoutError,
                    ValueError,
                    RuntimeError,
                ) as exc:
                    # Avoid logging raw responses, chat content or credentials.
                    error = (
                        str(exc)
                        if isinstance(exc, WechatUseHealthError)
                        else f"wechat-use receive unavailable ({type(exc).__name__})"
                    )
                    if self.last_error is None or self.last_error.message != error:
                        self.record_error(error)
                        logger.warning(error)
                    await asyncio.sleep(5)
        finally:
            await self.terminate()

    async def handle_batch(self, batch: dict) -> None:
        """Dispatch complete native Bridge batches once per message.

        Args:
            batch: Native SSE payload with rows and source-completeness metadata.

        Raises:
            RuntimeError: Source coverage or account identity is inconsistent.
        """
        if not isinstance(batch, dict):
            raise ValueError("Invalid wechat-use batch")
        meta = batch.get("meta", {})
        if (
            not isinstance(meta, dict)
            or meta.get("status") not in ("ok", "windowed")
            or meta.get("unknown_shards")
            or meta.get("unreadable_shards")
        ):
            raise RuntimeError("wechat-use source coverage is unavailable")
        rows = batch.get("rows")
        if not isinstance(rows, list):
            raise ValueError("Invalid wechat-use rows")
        cursor = self._cursor
        for row in rows:
            if not isinstance(row, dict):
                continue
            message = self.convert_message(row)
            if message is not None:
                assert self._db is not None
                result = await self._db.execute(
                    "INSERT OR IGNORE INTO inbound VALUES (?, ?, ?)",
                    (self.self_id, message.session_id, message.message_id),
                )
                # Claim before enqueueing: crashes cannot replay a possible reply.
                await self._db.commit()
                if result.rowcount:
                    paths = await self.client.enrich_message(message)
                    event = self.create_event(message)
                    for path in paths:
                        event.track_temporary_local_file(path)
                    self.commit_event(event)
                await result.close()
            timestamp = row.get("create_time")
            if isinstance(timestamp, int) and timestamp <= time.time() + 30:
                cursor = max(cursor, timestamp)
        # Overlap the boundary second; durable message IDs remove duplicates.
        self._cursor = cursor

    def convert_message(self, row: dict) -> AstrBotMessage | None:
        """Convert fresh, authorized messages using structured message metadata.

        Args:
            row: One native Bridge message row.

        Returns:
            An AstrBot message, or None when the message is outside the policy.

        Raises:
            RuntimeError: A self-authored row belongs to another account.
        """
        sender = row.get("sender_wxid") or row.get("real_sender_id")
        if row.get("sender_is_self") is True:
            if sender and sender != self.self_id:
                raise RuntimeError("wechat-use account does not match configured wxid")
            return None
        if not isinstance(sender, str) or not sender or sender == self.self_id:
            return None
        chat = row.get("chat_username") or row.get("wxid")
        if not isinstance(chat, str) or not chat:
            return None
        timestamp = row.get("create_time")
        if (
            not isinstance(timestamp, int)
            or timestamp < max(self._receive_started_at, time.time() - 300)
            or timestamp > time.time() + 30
        ):
            return None
        group = chat.endswith("@chatroom")
        mentioned = row.get("is_mentioned") is True
        if group:
            if chat not in self.group_ids or (self.mention_only and not mentioned):
                return None
        elif not self.private_enabled:
            return None
        kind = row.get("message_kind", "unknown")
        if not isinstance(kind, str):
            return None
        text = row.get("display_text")
        if not isinstance(text, str) or not text.strip():
            text = row.get("message_content") if kind == "text" else f"[{kind}]"
        if not isinstance(text, str) or not text.strip():
            return None
        # Never feed media XML, encrypted CDN parameters or AES keys to a model.
        if kind != "text" and text.lstrip().startswith("<"):
            text = f"[{kind}]"
        # Python retains all digits of native JSON integer IDs, unlike JS Number.
        server_id = row.get("server_id_str") or row.get("server_id")
        if server_id and str(server_id).isdigit() and int(server_id) > 0:
            message_id = str(server_id)
        elif row.get("local_id") is not None and row.get("_db"):
            source = f"{row['_db']}:{row.get('_table', '')}:{row['local_id']}"
            message_id = "local-" + hashlib.sha256(source.encode()).hexdigest()
        else:
            return None
        message = AstrBotMessage()
        message.type = (
            MessageType.GROUP_MESSAGE if group else MessageType.FRIEND_MESSAGE
        )
        message.self_id = self.self_id
        message.session_id = chat
        message.message_id = message_id
        message.sender = MessageMember(sender, row.get("sender_display_name") or sender)
        message.timestamp = timestamp
        message.group = Group(chat, row.get("chat_display_name")) if group else None
        message.message = [Plain(text)]
        refer = row.get("refer")
        if isinstance(refer, dict):
            quoted_text = str(refer.get("content") or "")
            if quoted_text.lstrip().startswith("<"):
                quoted_text = "[引用媒体消息]"
            quote_id = refer.get("svrId") or refer.get("svr_id")
            if quote_id is not None:
                message.message.insert(
                    0,
                    Reply(
                        id=str(quote_id),
                        sender_id=refer.get("fromUser") or refer.get("from_user") or "",
                        sender_nickname=refer.get("displayName")
                        or refer.get("display_name")
                        or "",
                        message_str=quoted_text,
                        chain=[Plain(quoted_text)],
                    ),
                )
        link = row.get("url_link")
        if isinstance(link, dict) and isinstance(link.get("url"), str):
            if link["url"].startswith(("http://", "https://")):
                message.message.append(
                    Share(
                        url=link["url"],
                        title=str(link.get("title") or "链接"),
                        content=str(link.get("description") or ""),
                    )
                )
                message.message.append(
                    Plain(f"\n{link.get('title') or '链接'} {link['url']}")
                )
        mini = row.get("mini_program")
        if isinstance(mini, dict):
            message.message.append(
                Plain(
                    f"\n[小程序] {mini.get('title') or ''} "
                    f"appId={mini.get('app_id') or mini.get('appId') or ''} "
                    f"path={mini.get('page_path') or mini.get('pagePath') or ''}"
                )
            )
        forwarded = row.get("forwarded_messages")
        if isinstance(forwarded, list):
            for item in forwarded[:50]:
                if isinstance(item, dict):
                    content = str(
                        item.get("display_text") or item.get("content") or "[媒体]"
                    )
                    if content.lstrip().startswith("<"):
                        content = "[转发媒体消息]"
                    message.message.append(
                        Node(
                            name=str(
                                item.get("sender_display_name")
                                or item.get("display_name")
                                or ""
                            ),
                            content=[Plain(content)],
                        )
                    )
        media = row.get("media")
        if kind in {"file", "attachment", "video"} and isinstance(media, dict):
            name = (
                media.get("filename")
                or media.get("file_name")
                or media.get("name")
                or kind
            )
            message.message.append(
                Plain(f"\n[附件] {name}（当前 runtime 未提供文件下载接口）")
            )
        if kind in {"system", "recalled", "recall", "transfer", "red_envelope"}:
            # Deliver notifications to plugins without treating them as private chat prompts.
            message.type = MessageType.OTHER_MESSAGE
            recall = row.get("recall")
            if isinstance(recall, dict):
                message.message.append(
                    Plain(
                        f"\n[撤回消息 ID] {recall.get('replacedMsgId') or recall.get('replaced_msg_id') or ''}"
                    )
                )
        if group and mentioned:
            message.message.insert(0, At(qq=self.self_id))
        message.message_str = "".join(
            part.text for part in message.message if isinstance(part, Plain)
        )
        message.raw_message = row
        return message

    def create_event(self, message: AstrBotMessage) -> WechatUseMessageEvent:
        return WechatUseMessageEvent(
            message_str=message.message_str,
            message_obj=message,
            platform_meta=self.meta(),
            session_id=message.session_id,
            adapter=self,
        )

    async def send_message(self, recipient: str, message: MessageChain) -> dict:
        """Preflight a complete response, then submit its ordered parts once.

        Args:
            recipient: Exact group or contact wxid, never a display name.
            message: Framework response containing text, mentions or media.

        Returns:
            Submission metadata; an uncertain outcome is not a delivery receipt.

        Raises:
            ValueError: The recipient or message components are unsupported.
            RuntimeError: The Bridge is unavailable or definitively refused the send.
        """
        plan = await prepare_outbound(
            message,
            recipient,
            self.client,
            bool(self.config.get("wechat_use_quote_fallback", True)),
        )
        if any(part["kind"] != "text" or part.get("mentions") for part in plan):
            self.client.cli_executable()
        receipts = []
        for part in plan:
            receipt = await self._submit_part(recipient, part)
            receipts.append(receipt)
            if receipt["status"] == "status_unknown":
                break  # Do not continue a chain after an indeterminate submission.
        if len(receipts) == 1 and len(plan) == 1:
            return receipts[0]
        return {
            "status": "status_unknown"
            if len(receipts) != len(plan)
            or any(r["status"] == "status_unknown" for r in receipts)
            else "submitted_unconfirmed"
            if any(r["status"] == "submitted_unconfirmed" for r in receipts)
            else "delivered",
            "parts": receipts,
            "planned_parts": len(plan),
            "submitted_parts": len(receipts),
        }

    async def _submit_part(self, recipient: str, part: dict) -> dict:
        """Journal a single submission before crossing either transport boundary.

        Args:
            recipient: Exact allowed recipient.
            part: Validated text or local media operation.

        Returns:
            Original request identifiers, outcome and explicit fallback metadata.

        Raises:
            RuntimeError: The account is not ready or submission is refused.
        """
        await self.check_health()
        assert self._db is not None and self._http is not None
        request_id = f"astrbot-{uuid.uuid4().hex}"
        path = None
        if part["kind"] in {"image", "file"}:
            directory = self.state_path.parent / "outbox" / request_id
            directory.mkdir(parents=True, mode=0o700)
            directory.chmod(0o700)
            path = directory / part["name"]
            await asyncio.to_thread(shutil.copyfile, part["path"], path)
            path.chmod(0o600)
            # Keep snapshots for unconfirmed/unknown receipts; native uploads can
            # outlive their CLI invocation and the caller's temporary source file.
        await self._db.execute(
            "INSERT INTO submissions (request_id, recipient, status, created_at, account, kind) VALUES (?, ?, ?, ?, ?, ?)",
            (
                request_id,
                recipient,
                "status_unknown",
                int(time.time()),
                self.self_id,
                part["kind"],
            ),
        )
        await self._db.commit()
        status, bridge_id, reason = "status_unknown", None, None
        try:
            http_status = 200
            if part["kind"] == "text" and not part.get("mentions"):
                async with self._http.post(
                    f"{self.base_url}/send",
                    json={
                        "wxid": recipient,
                        "text": part["text"],
                        "request_id": request_id,
                    },
                    allow_redirects=False,
                ) as response:
                    http_status = response.status
                    receipt = await response.json()
            else:
                args = [
                    "send",
                    "--recipient",
                    recipient,
                    "--request-id",
                    request_id,
                    "--json",
                ]
                if path is not None:
                    args.extend(
                        ["--image" if part["kind"] == "image" else "--file", str(path)]
                    )
                else:
                    args.extend(["--text", part["text"]])
                    for mention in part.get("mentions", []):
                        args.extend(["--mention", mention])
                _, receipt = await self.client.run_cli(*args, timeout=150, check=False)
            if isinstance(receipt, dict):
                layers = [receipt]
                if isinstance(receipt.get("receipt"), dict):
                    layers.append(receipt["receipt"])
                statuses = set()
                bound = True
                for layer in layers:
                    if layer.get("request_id"):
                        bridge_id = layer["request_id"]
                    reported = (
                        layer.get("status")
                        or layer.get("submission_state")
                        or layer.get("receipt_state")
                    )
                    if isinstance(reported, str) and reported in {
                        "delivered",
                        "delivered_verified",
                        "submitted_unconfirmed",
                        "status_unknown",
                        "failed",
                        "not_submitted",
                    }:
                        statuses.add(reported)
                    bound &= all(
                        layer.get(k) in (None, self.self_id)
                        for k in ("expected_account", "account_id")
                    )
                    bound &= all(
                        layer.get(k) in (None, recipient)
                        for k in ("expected_recipient", "wxid")
                    )
                    if layer.get("reason") in (
                        "native_send_source_unavailable",
                        "wechat_in_foreground",
                        "background_navigation_unavailable",
                    ):
                        reason = layer["reason"]
                if bound and len(statuses) == 1:
                    status = statuses.pop()
                else:
                    reason = None
            if http_status >= 400 and status not in {"failed", "not_submitted"}:
                status = "status_unknown"
        except (aiohttp.ClientError, TimeoutError, ValueError, RuntimeError, OSError):
            # An interrupted POST may already have reached WeChat. Never retry it.
            pass
        finally:
            await self._db.execute(
                "UPDATE submissions SET status=?, bridge_request_id=?, reason=? WHERE request_id=?",
                (status, str(bridge_id) if bridge_id else None, reason, request_id),
            )
            await self._db.commit()
        logger.info(
            "wechat-use submission %s: %s (reason=%s)",
            request_id,
            status,
            reason or "unspecified",
        )
        if status in {"failed", "not_submitted"}:
            raise RuntimeError(
                f"wechat-use submission refused ({request_id}; reason={reason or 'unspecified'})"
            )
        return {
            "request_id": request_id,
            "bridge_request_id": bridge_id,
            "status": status,
            "kind": part["kind"],
            "reason": reason,
            "fallback": part.get("fallback"),
        }

    async def reconcile_submission(self, request_id: str) -> dict:
        """Check an original account-bound receipt without ever resubmitting.

        Args:
            request_id: Adapter request ID recorded in the submission journal.

        Returns:
            Original CLI receipt, preserving uncertain outcomes.

        Raises:
            ValueError: The attempt is unknown or has no trustworthy account binding.
        """
        if self._db is None:
            raise RuntimeError("wechat-use adapter is not running")
        async with self._db.execute(
            "SELECT bridge_request_id, account, recipient FROM submissions WHERE request_id=?",
            (request_id,),
        ) as cursor:
            attempt = await cursor.fetchone()
        if attempt is None or attempt[1] != self.self_id:
            raise ValueError("Unknown or unbound wechat-use submission")
        _, receipt = await self.client.run_cli(
            "send-receipt",
            "--request-id",
            attempt[0] or request_id,
            "--expected-account",
            attempt[1],
            "--json",
            check=False,
        )
        if isinstance(receipt, dict):
            layers = [receipt]
            if isinstance(receipt.get("receipt"), dict):
                layers.append(receipt["receipt"])
            statuses = set()
            reason = None
            for layer in layers:
                if any(
                    layer.get(k) not in (None, attempt[1])
                    for k in ("expected_account", "account_id")
                ) or any(
                    layer.get(k) not in (None, attempt[2])
                    for k in ("expected_recipient", "wxid")
                ):
                    raise ValueError("Receipt account or recipient binding mismatch")
                if layer.get("request_id") not in (None, request_id, attempt[0]):
                    raise ValueError("Receipt request binding mismatch")
                reported = (
                    layer.get("status")
                    or layer.get("submission_state")
                    or layer.get("receipt_state")
                )
                if isinstance(reported, str) and reported in {
                    "delivered",
                    "delivered_verified",
                    "submitted_unconfirmed",
                    "status_unknown",
                    "failed",
                    "not_submitted",
                }:
                    statuses.add(reported)
                if layer.get("reason") in (
                    "native_send_source_unavailable",
                    "wechat_in_foreground",
                    "background_navigation_unavailable",
                ):
                    reason = layer["reason"]
            if statuses:
                status = statuses.pop() if len(statuses) == 1 else "status_unknown"
                await self._db.execute(
                    "UPDATE submissions SET status=?, reason=? WHERE request_id=?",
                    (status, reason, request_id),
                )
                await self._db.commit()
                receipt = {**receipt, "status": status, "reason": reason}
        return receipt

    async def send_by_session(
        self, session: MessageSession, message_chain: MessageChain
    ) -> None:
        await self.send_message(session.session_id, message_chain)
        await super().send_by_session(session, message_chain)

    async def terminate(self) -> None:
        self._running = False
        if self._http is not None:
            await self._http.close()
            self._http = None
        if self._db is not None:
            await self._db.close()
            self._db = None
