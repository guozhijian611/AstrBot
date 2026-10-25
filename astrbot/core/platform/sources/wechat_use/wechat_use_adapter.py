import asyncio
import hashlib
import json
import time
import uuid
from pathlib import Path

import aiohttp
import aiosqlite
from yarl import URL

from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import At, Plain
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

from .wechat_use_event import WechatUseMessageEvent


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
        self._started_at = int(time.time())
        self._cursor = self._started_at

    def meta(self) -> PlatformMetadata:
        return self.metadata

    def get_client(self) -> "WechatUseAdapter":
        return self

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
            raise RuntimeError("wechat-use adapter is not running")
        async with self._http.get(
            f"{self.base_url}/health", allow_redirects=False
        ) as response:
            if response.status != 200:
                raise RuntimeError(f"wechat-use health HTTP {response.status}")
            health = await response.json()
        if not isinstance(health, dict) or "bridge_version" not in health:
            raise RuntimeError("wechat-use requires the native Bridge shape")
        if (
            not isinstance(health.get("daemon"), dict)
            or not isinstance(health.get("wechat"), dict)
            or not isinstance(health.get("receive_readiness"), dict)
            or health["daemon"].get("alive") is not True
            or health["wechat"].get("account_state") != "ready"
            or health["receive_readiness"].get("ready") is not True
        ):
            raise RuntimeError("wechat-use daemon, account or receiver is not ready")
        return health

    async def run(self) -> None:
        self._running = True
        self._started_at = self._cursor = int(time.time())
        try:
            await self.initialize()
            while self._running:
                try:
                    await self.check_health()
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
                    error = f"wechat-use receive unavailable ({type(exc).__name__})"
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
                    self.commit_event(self.create_event(message))
                await result.close()
            timestamp = row.get("create_time")
            if isinstance(timestamp, int) and timestamp <= time.time() + 30:
                cursor = max(cursor, timestamp)
        # Overlap the boundary second; durable message IDs remove duplicates.
        self._cursor = cursor

    def convert_message(self, row: dict) -> AstrBotMessage | None:
        """Convert fresh, authorized text messages using structured mention data.

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
            or timestamp < max(self._started_at, time.time() - 300)
            or timestamp > time.time() + 30
            or row.get("message_kind") != "text"
        ):
            return None
        group = chat.endswith("@chatroom")
        mentioned = row.get("is_mentioned") is True
        if group:
            if chat not in self.group_ids or (self.mention_only and not mentioned):
                return None
        elif not self.private_enabled:
            return None
        text = row.get("display_text") or row.get("message_content")
        if not isinstance(text, str) or not text.strip():
            return None
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
        if group and mentioned:
            message.message.insert(0, At(qq=self.self_id))
        message.message_str = text
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
        """Submit one text message and journal its outcome without retries.

        Args:
            recipient: Exact group or contact wxid, never a display name.
            message: A plain-text message chain.

        Returns:
            Submission metadata; an uncertain outcome is not a delivery receipt.

        Raises:
            ValueError: The recipient or message components are unsupported.
            RuntimeError: The Bridge is unavailable or definitively refused the send.
        """
        if (
            not recipient
            or (recipient.endswith("@chatroom") and recipient not in self.group_ids)
            or (not recipient.endswith("@chatroom") and not self.private_enabled)
        ):
            raise ValueError("wechat-use recipient is outside the configured scope")
        if any(not isinstance(part, Plain) for part in message.chain):
            raise ValueError("wechat-use currently sends plain text only")
        text = "".join(part.text for part in message.chain)
        if not text.strip():
            raise ValueError("wechat-use cannot send empty text")
        await self.check_health()
        assert self._db is not None and self._http is not None
        request_id = f"astrbot-{uuid.uuid4().hex}"
        await self._db.execute(
            "INSERT INTO submissions VALUES (?, ?, ?, NULL, ?)",
            (request_id, recipient, "status_unknown", int(time.time())),
        )
        await self._db.commit()
        status, bridge_id = "status_unknown", None
        try:
            async with self._http.post(
                f"{self.base_url}/send",
                json={"wxid": recipient, "text": text, "request_id": request_id},
                allow_redirects=False,
            ) as response:
                receipt = await response.json()
                if isinstance(receipt, dict):
                    bridge_id = receipt.get("request_id")
                    reported = receipt.get("status")
                    if isinstance(reported, str) and reported in {
                        "delivered",
                        "delivered_verified",
                        "submitted_unconfirmed",
                        "status_unknown",
                        "failed",
                        "not_submitted",
                    }:
                        status = reported
                if response.status >= 400 and status not in {"failed", "not_submitted"}:
                    status = "status_unknown"
        except (aiohttp.ClientError, TimeoutError, ValueError):
            # An interrupted POST may already have reached WeChat. Never retry it.
            pass
        finally:
            await self._db.execute(
                "UPDATE submissions SET status=?, bridge_request_id=? WHERE request_id=?",
                (status, str(bridge_id) if bridge_id else None, request_id),
            )
            await self._db.commit()
        logger.info("wechat-use submission %s: %s", request_id, status)
        if status in {"failed", "not_submitted"}:
            raise RuntimeError(f"wechat-use submission refused ({request_id})")
        return {
            "request_id": request_id,
            "bridge_request_id": bridge_id,
            "status": status,
        }

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
