"""Exercise the native Bridge contract without sending any real WeChat messages."""

import asyncio
import copy
import json
import time
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web

from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import At, Image, Plain
from astrbot.core.config.default import CONFIG_METADATA_2
from astrbot.core.pipeline.waking_check.stage import WakingCheckStage
from astrbot.core.platform.manager import PlatformManager
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.platform.message_type import MessageType
from astrbot.core.platform.platform import Platform
from astrbot.core.platform.sources.wechat_use.wechat_use_adapter import WechatUseAdapter
from astrbot.core.star.session_plugin_manager import SessionPluginManager
from astrbot.core.star.star_handler import star_handlers_registry
from astrbot.dashboard.services.config_service import ConfigDisplayService

SELF = "wxid_fixture_bot"
ROOM = "fixture-room@chatroom"
OTHER = "wxid_fixture_sender"
HEALTH = {
    "bridge_version": "1.19.9",
    "daemon": {"alive": True},
    "wechat": {"account_state": "ready"},
    "receive_readiness": {"ready": True},
}
CONFIG = {
    "id": "wechat_fixture",
    "type": "wechat_use",
    "enable": True,
    "wechat_use_self_id": SELF,
    "wechat_use_group_ids": [ROOM],
}


def row(**overrides):
    return {
        "server_id": 9007199254740993123,
        "local_id": 1,
        "chat_username": ROOM,
        "sender_wxid": OTHER,
        "sender_display_name": "Fixture Sender",
        "create_time": int(time.time()),
        "message_kind": "text",
        "display_text": "@Fixture Bot explain the fixture",
        "is_mentioned": True,
        "sender_is_self": False,
        **overrides,
    }


def batch(*rows, **meta):
    return {
        "rows": list(rows),
        "meta": {
            "status": "ok",
            "unknown_shards": [],
            "unreadable_shards": [],
            **meta,
        },
    }


@pytest_asyncio.fixture
async def bridge(unused_tcp_port):
    state = {
        "health": copy.deepcopy(HEALTH),
        "requests": [],
        "streams": [],
        "stream_batches": [],
        "receipt": {"status": "submitted_unconfirmed", "request_id": "bridge-fixture"},
        "status": 200,
        "disconnect": False,
        "malformed": False,
        "send_started": asyncio.Event(),
        "block_send": asyncio.Event(),
        "hold_send": False,
        "headers": [],
    }

    async def health(request):
        state["headers"].append(request.headers.get("Authorization"))
        return web.json_response(state["health"])

    async def send(request):
        state["requests"].append(await request.json())
        state["send_started"].set()
        if state["hold_send"]:
            await state["block_send"].wait()
        if state["disconnect"]:
            request.transport.close()
            return web.Response()
        if state["malformed"]:
            return web.Response(text="invalid JSON", status=500)
        return web.json_response(state["receipt"], status=state["status"])

    async def stream(request):
        state["streams"].append(int(request.query["since"]))
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        batches = state["stream_batches"]
        index = min(len(state["streams"]) - 1, len(batches) - 1)
        if batches:
            # Test multiline SSE, comments, CRLF and arbitrary TCP boundaries.
            lines = json.dumps(batches[index], indent=2).splitlines()
            wire = ": heartbeat\r\nevent: messages\r\n"
            wire += "\r\n".join("data: " + line for line in lines) + "\r\n\r\n"
            encoded = wire.encode()
            for offset in range(0, len(encoded), 7):
                await response.write(encoded[offset : offset + 7])
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_post("/send", send)
    app.router.add_get("/messages/stream", stream)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", unused_tcp_port).start()
    state["url"] = f"http://127.0.0.1:{unused_tcp_port}"
    try:
        yield state
    finally:
        state["block_send"].set()
        await runner.cleanup()


@pytest_asyncio.fixture
async def adapter(bridge, tmp_path):
    instance = WechatUseAdapter(
        {**CONFIG, "wechat_use_bridge_url": bridge["url"]}, {}, asyncio.Queue()
    )
    instance.state_path = tmp_path / "adapter.sqlite3"
    await instance.initialize()
    try:
        yield instance
    finally:
        await instance.terminate()


def test_dashboard_template_and_secret_metadata():
    metadata = CONFIG_METADATA_2["platform_group"]["metadata"]["platform"]
    template = metadata["config_template"]["WeChat (wechat-use)"]
    assert template["type"] == "wechat_use"
    assert template["enable"] is False
    assert template["wechat_use_group_ids"] == []
    assert template["wechat_use_private_enabled"] is False
    assert template["wechat_use_require_mention"] is True
    for field in template:
        if field.startswith("wechat_use_"):
            assert field in metadata["items"]
    assert metadata["items"]["wechat_use_bridge_token"]["secret"] is True


@pytest.mark.parametrize(
    "url",
    [
        "file:///tmp/db",
        "http://user:secret@localhost",
        "http://localhost/api",
        "http://localhost?token=secret",
    ],
)
def test_invalid_bridge_origins(url):
    with pytest.raises(ValueError):
        WechatUseAdapter({**CONFIG, "wechat_use_bridge_url": url}, {}, asyncio.Queue())


def test_account_is_required():
    with pytest.raises(ValueError, match="exact wxid"):
        WechatUseAdapter({"id": "fixture"}, {}, asyncio.Queue())


@pytest.mark.parametrize("ids", [None, ROOM, ["Group nickname"], [123]])
def test_group_scope_requires_exact_ids(ids):
    with pytest.raises(ValueError, match="group IDs"):
        WechatUseAdapter({**CONFIG, "wechat_use_group_ids": ids}, {}, asyncio.Queue())


@pytest.mark.asyncio
async def test_dashboard_returns_translatable_wechat_use_fields():
    service = ConfigDisplayService(SimpleNamespace(astrbot_config={}))
    result = await service.get_astrbot_config()
    metadata = result["metadata"]["platform_group"]["metadata"]["platform"]
    assert "WeChat (wechat-use)" in metadata["config_template"]
    assert metadata["items"]["wechat_use_self_id"]["description"] == (
        "platform_group.platform.wechat_use_self_id.description"
    )


@pytest.mark.asyncio
async def test_group_conversion_preserves_ids_and_wakes_astrbot(adapter):
    await adapter.handle_batch(batch(row()))
    event = adapter._event_queue.get_nowait()
    assert event.get_group_id() == ROOM
    assert event.get_session_id() == ROOM
    assert event.get_sender_id() == OTHER
    assert event.message_obj.message_id == "9007199254740993123"
    assert isinstance(event.get_messages()[0], At)
    assert str(event.get_messages()[0].qq) == SELF
    assert event.get_platform_name() == "wechat_use"
    assert event.get_platform_id() == CONFIG["id"]


@pytest.mark.asyncio
async def test_structured_mention_passes_real_waking_stage(adapter, monkeypatch):
    stage = WakingCheckStage()
    stage.ctx = SimpleNamespace(
        astrbot_config={"admins_id": [], "wake_prefix": ["/"], "plugin_set": ["*"]}
    )
    stage.ignore_bot_self_message = True
    stage.friend_message_needs_wake_prefix = False
    stage.ignore_at_all = True
    stage.disable_builtin_commands = False
    stage.no_permission_reply = False
    stage._umo_auto_name_recorder = MagicMock()
    monkeypatch.setattr(
        star_handlers_registry, "get_handlers_by_event_type", lambda *a, **kw: []
    )
    monkeypatch.setattr(
        SessionPluginManager, "filter_handlers_by_session", AsyncMock(return_value=[])
    )
    event = adapter.create_event(
        adapter.convert_message(
            row(display_text="A plain question without nickname text")
        )
    )
    await stage.process(event)
    assert event.is_wake is True
    assert event.is_at_or_wake_command is True
    assert event.is_stopped() is False


@pytest.mark.parametrize(
    "changes",
    [
        {"is_mentioned": False},
        {"is_mentioned": None},
        {"chat_username": "different@chatroom"},
        {"chat_username": OTHER},
        {"sender_wxid": SELF},
        {"sender_wxid": ""},
        {"sender_is_self": True, "sender_wxid": SELF},
        {"create_time": 1},
        {"create_time": int(time.time()) + 3600},
        {"message_kind": "image"},
        {"display_text": "", "message_content": ""},
        {"server_id": 0},
    ],
)
@pytest.mark.asyncio
async def test_inbound_policy_drops_unsupported_messages(adapter, changes):
    await adapter.handle_batch(batch(row(**changes)))
    assert adapter._event_queue.empty()


@pytest.mark.asyncio
async def test_explicit_private_and_group_policy(adapter):
    adapter.private_enabled = True
    await adapter.handle_batch(batch(row(chat_username=OTHER, is_mentioned=False)))
    event = adapter._event_queue.get_nowait()
    assert event.is_private_chat()
    assert all(not isinstance(c, At) for c in event.get_messages())
    adapter.mention_only = False
    await adapter.handle_batch(batch(row(is_mentioned=False, server_id=2)))
    assert not adapter._event_queue.empty()


@pytest.mark.asyncio
async def test_self_row_from_different_account_is_rejected(adapter):
    with pytest.raises(RuntimeError, match="account"):
        await adapter.handle_batch(batch(row(sender_is_self=True)))
    assert adapter._event_queue.empty()


@pytest.mark.parametrize(
    "meta",
    [
        {"status": "possibly_stale_unavailable_shards"},
        {"unreadable_shards": ["fixture"]},
        {"unknown_shards": ["fixture"]},
    ],
)
@pytest.mark.asyncio
async def test_incomplete_source_does_not_enqueue_or_advance(adapter, meta):
    cursor = adapter._cursor
    with pytest.raises(RuntimeError, match="coverage"):
        await adapter.handle_batch(batch(row(create_time=cursor + 1), **meta))
    assert adapter._event_queue.empty()
    assert adapter._cursor == cursor


@pytest.mark.asyncio
async def test_dedup_survives_adapter_restart(adapter):
    await adapter.handle_batch(batch(row(), row(), status="windowed"))
    assert adapter._event_queue.qsize() == 1
    config, path = adapter.config, adapter.state_path
    await adapter.terminate()
    restarted = WechatUseAdapter(config, {}, asyncio.Queue())
    restarted.state_path = path
    await restarted.initialize()
    try:
        await restarted.handle_batch(batch(row()))
        assert restarted._event_queue.empty()
    finally:
        await restarted.terminate()


@pytest.mark.asyncio
async def test_local_id_fallback_is_stable_and_database_scoped(adapter):
    message = row(server_id=0, _db="fixture-a", _table="Msg")
    await adapter.handle_batch(batch(message, message, {**message, "_db": "fixture-b"}))
    assert adapter._event_queue.qsize() == 2


@pytest.mark.asyncio
async def test_sse_reconnect_uses_cursor_and_deduplicates(bridge, tmp_path):
    original = row()
    bridge["stream_batches"] = [batch(original), batch(original, row(server_id=2))]
    adapter = WechatUseAdapter(
        {**CONFIG, "wechat_use_bridge_url": bridge["url"]}, {}, asyncio.Queue()
    )
    adapter.state_path = tmp_path / "stream.sqlite3"
    task = asyncio.create_task(adapter.run())
    try:
        first = await asyncio.wait_for(adapter._event_queue.get(), timeout=5)
        second = await asyncio.wait_for(adapter._event_queue.get(), timeout=8)
        assert first.message_obj.message_id != second.message_obj.message_id
        assert adapter._event_queue.empty()
        assert bridge["streams"][:2] == [adapter._started_at, original["create_time"]]
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    assert adapter._db is None
    assert adapter._http is None


@pytest.mark.asyncio
async def test_health_readiness_and_auth(adapter, bridge):
    await adapter.terminate()
    adapter.config["wechat_use_bridge_token"] = "fixture-token"
    await adapter.initialize()
    assert await adapter.check_health() == HEALTH
    assert bridge["headers"][-1] == "Bearer fixture-token"
    bridge["health"]["receive_readiness"]["ready"] = False
    with pytest.raises(RuntimeError, match="not ready"):
        await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    assert bridge["requests"] == []


@pytest.mark.asyncio
async def test_wrong_bridge_shape_is_rejected(adapter, bridge):
    bridge["health"] = {"status": "connected"}
    with pytest.raises(RuntimeError, match="native"):
        await adapter.check_health()


@pytest.mark.parametrize(
    "status", ["submitted_unconfirmed", "status_unknown", "delivered"]
)
@pytest.mark.asyncio
async def test_send_once_and_journal_actual_receipt(adapter, bridge, status):
    bridge["receipt"]["status"] = status
    receipt = await adapter.send_message(
        ROOM, MessageChain([Plain("one"), Plain("two")])
    )
    assert receipt["status"] == status
    assert receipt["bridge_request_id"] == "bridge-fixture"
    assert len(bridge["requests"]) == 1
    assert bridge["requests"][0] == {
        "wxid": ROOM,
        "text": "onetwo",
        "request_id": receipt["request_id"],
    }
    async with adapter._db.execute(
        "SELECT status, bridge_request_id FROM submissions"
    ) as cursor:
        assert await cursor.fetchone() == (status, "bridge-fixture")


@pytest.mark.parametrize("mode", ["disconnect", "malformed"])
@pytest.mark.asyncio
async def test_uncertain_send_is_not_retried(adapter, bridge, mode):
    bridge[mode] = True
    receipt = await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    assert receipt["status"] == "status_unknown"
    assert len(bridge["requests"]) == 1


@pytest.mark.asyncio
async def test_post_timeout_keeps_original_unknown_request(adapter, bridge):
    bridge["hold_send"] = True
    await adapter._http.close()
    adapter._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.05))
    receipt = await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    assert receipt["status"] == "status_unknown"
    assert len(bridge["requests"]) == 1
    async with adapter._db.execute(
        "SELECT request_id, status FROM submissions"
    ) as cursor:
        assert await cursor.fetchone() == (receipt["request_id"], "status_unknown")
    bridge["block_send"].set()


@pytest.mark.asyncio
async def test_cancelled_post_keeps_unknown_submission(adapter, bridge):
    bridge["hold_send"] = True
    task = asyncio.create_task(
        adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    )
    await asyncio.wait_for(bridge["send_started"].wait(), timeout=3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with adapter._db.execute("SELECT status FROM submissions") as cursor:
        assert await cursor.fetchone() == ("status_unknown",)
    assert len(bridge["requests"]) == 1
    bridge["block_send"].set()


@pytest.mark.asyncio
async def test_definitive_refusal_is_recorded(adapter, bridge):
    bridge["receipt"] = {"status": "not_submitted"}
    bridge["status"] = 409
    with pytest.raises(RuntimeError, match="refused"):
        await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    async with adapter._db.execute("SELECT status FROM submissions") as cursor:
        assert await cursor.fetchone() == ("not_submitted",)
    assert len(bridge["requests"]) == 1


@pytest.mark.asyncio
async def test_outbound_scope_and_components(adapter, bridge):
    for recipient in [OTHER, "different@chatroom", ""]:
        with pytest.raises(ValueError, match="scope"):
            await adapter.send_message(recipient, MessageChain([Plain("fixture")]))
    for chain in [
        MessageChain([]),
        MessageChain(
            [Plain("fixture"), Image.fromURL("https://example.invalid/image.png")]
        ),
    ]:
        with pytest.raises(ValueError):
            await adapter.send_message(ROOM, chain)
    assert bridge["requests"] == []


@pytest.mark.asyncio
async def test_event_reply_and_streaming_use_same_client(adapter, bridge, monkeypatch):
    monkeypatch.setattr(AstrMessageEvent, "send", AsyncMock())
    event = adapter.create_event(adapter.convert_message(row()))
    await event.send(MessageChain([Plain("fixture reply")]))
    assert event.get_extra("wechat_use_receipt")["status"] == "submitted_unconfirmed"

    async def fragments():
        yield MessageChain([Plain("first ")])
        yield MessageChain([Plain("second")])

    await event.send_streaming(fragments())
    assert [r["text"] for r in bridge["requests"]] == ["fixture reply", "first second"]


@pytest.mark.asyncio
async def test_proactive_send_and_manager_registration(adapter, bridge, monkeypatch):
    monkeypatch.setattr(Platform, "send_by_session", AsyncMock())
    await adapter.send_by_session(
        MessageSession(CONFIG["id"], MessageType.GROUP_MESSAGE, ROOM),
        MessageChain([Plain("fixture proactive")]),
    )
    assert len(bridge["requests"]) == 1
    config = MagicMock()
    config.__getitem__.side_effect = {
        "platform": [],
        "platform_settings": {},
    }.__getitem__
    manager = PlatformManager(config, asyncio.Queue())
    monkeypatch.setattr(manager, "_start_platform_task", MagicMock())
    await manager.load_platform(CONFIG.copy())
    assert isinstance(manager.platform_insts[0], WechatUseAdapter)
    manager._start_platform_task.assert_called_once()
