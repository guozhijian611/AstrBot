"""Exercise the native Bridge contract without sending any real WeChat messages."""

import asyncio
import base64
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
from astrbot.api.message_components import (
    At,
    AtAll,
    File,
    Image,
    Location,
    Node,
    Nodes,
    Plain,
    Record,
    Reply,
    Share,
    Video,
)
from astrbot.core.config.default import CONFIG_METADATA_2
from astrbot.core.pipeline.waking_check.stage import WakingCheckStage
from astrbot.core.platform.manager import PlatformManager
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.platform.message_type import MessageType
from astrbot.core.platform.platform import Platform, PlatformStatus
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
        "image_requests": [],
        "query_requests": [],
        "image_status": 200,
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

    async def image(request):
        state["image_requests"].append(dict(request.query))
        return web.Response(
            body=base64.b64decode(
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jK4sAAAAASUVORK5CYII="
            ),
            content_type="image/png",
            status=state["image_status"],
        )

    async def query(request):
        state["query_requests"].append((request.path, dict(request.query)))
        return web.json_response({"rows": [], "meta": {"status": "windowed"}})

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_post("/send", send)
    app.router.add_get("/messages/stream", stream)
    app.router.add_get("/image", image)
    for path in ("/chats", "/contacts", "/unread", "/resolve", "/chat/{chat}/history"):
        app.router.add_get(path, query)
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
        assert bridge["streams"][:2] == [
            adapter._receive_started_at,
            original["create_time"],
        ]
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    assert adapter._db is None
    assert adapter._http is None


def test_receive_boundary_preserves_platform_statistics():
    adapter = WechatUseAdapter(CONFIG, {}, asyncio.Queue())
    assert adapter.get_stats()["started_at"] is None
    adapter.status = PlatformStatus.RUNNING
    started_at = adapter.get_stats()["started_at"]
    assert isinstance(started_at, str)
    adapter.record_error("Fixture receiver unavailable")
    stats = adapter.get_stats()
    assert stats["status"] == "error"
    assert stats["started_at"] == started_at
    assert stats["last_error"]["message"] == "Fixture receiver unavailable"
    adapter.clear_errors()
    assert adapter.get_stats()["status"] == "running"


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


@pytest.mark.parametrize(
    ("stage", "expected"),
    [
        ("profile_fetch", "profile_fetch"),
        ("profile_verify", "profile_verify"),
        ("private-account-token", "unknown"),
        ({"private": "content"}, "unknown"),
    ],
)
@pytest.mark.asyncio
async def test_health_diagnostics_expose_only_known_stages(
    adapter, bridge, stage, expected
):
    bridge["health"]["receive_readiness"] = {
        "ready": False,
        "source": {
            "last_capture_refusal": {"capture_step": stage, "reason": "private-token"}
        },
    }
    with pytest.raises(RuntimeError) as error:
        await adapter.check_health()
    assert str(error.value) == f"wechat-use receiver is not ready (stage={expected})"
    assert adapter._event_queue.empty()
    assert bridge["requests"] == []


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("daemon", {"alive": False}, "wechat-use daemon is not ready"),
        (
            "wechat",
            {"account_state": "private-account"},
            "wechat-use account is not ready",
        ),
        ("receive_readiness", None, "wechat-use health metadata is invalid"),
    ],
)
@pytest.mark.asyncio
async def test_health_diagnostics_distinguish_service_failures(
    adapter, bridge, field, value, message
):
    bridge["health"][field] = value
    with pytest.raises(RuntimeError) as error:
        await adapter.check_health()
    assert str(error.value) == message


@pytest.mark.asyncio
async def test_receive_failure_stage_reaches_dashboard_statistics(bridge, tmp_path):
    bridge["health"]["receive_readiness"] = {
        "ready": False,
        "source": {"last_capture_refusal": {"capture_step": "profile_fetch"}},
    }
    adapter = WechatUseAdapter(
        {**CONFIG, "wechat_use_bridge_url": bridge["url"]}, {}, asyncio.Queue()
    )
    adapter.state_path = tmp_path / "health.sqlite3"
    task = asyncio.create_task(adapter.run())
    try:
        for _ in range(100):
            if adapter.last_error is not None:
                break
            await asyncio.sleep(0.01)
        assert adapter.get_stats()["last_error"]["message"] == (
            "wechat-use receiver is not ready (stage=profile_fetch)"
        )
        assert bridge["streams"] == []
        assert bridge["requests"] == []
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


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
async def test_nested_native_refusal_is_recorded_with_safe_reason(adapter, bridge):
    bridge["receipt"] = {
        "account_id": SELF,
        "receipt": {
            "request_id": "native-fixture",
            "account_id": SELF,
            "wxid": ROOM,
            "submission_state": "not_submitted",
            "receipt_state": "not_submitted",
            "reason": "native_send_source_unavailable",
        },
    }
    bridge["status"] = 503
    with pytest.raises(RuntimeError, match="native_send_source_unavailable"):
        await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    async with adapter._db.execute(
        "SELECT status, reason, bridge_request_id FROM submissions"
    ) as cursor:
        assert await cursor.fetchone() == (
            "not_submitted",
            "native_send_source_unavailable",
            "native-fixture",
        )
    assert len(bridge["requests"]) == 1


@pytest.mark.asyncio
async def test_unrecognized_refusal_reason_does_not_leak(adapter, bridge):
    bridge["receipt"] = {
        "receipt": {
            "submission_state": "not_submitted",
            "reason": "private-token-or-path",
        }
    }
    with pytest.raises(RuntimeError) as error:
        await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    assert "private-token-or-path" not in str(error.value)
    async with adapter._db.execute("SELECT reason FROM submissions") as cursor:
        assert await cursor.fetchone() == (None,)


@pytest.mark.parametrize(
    ("field", "value"), [("account_id", "wxid_other"), ("wxid", "other@chatroom")]
)
@pytest.mark.asyncio
async def test_nested_submission_binding_is_checked(adapter, bridge, field, value):
    bridge["receipt"] = {"receipt": {"submission_state": "delivered", field: value}}
    result = await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    assert result["status"] == "status_unknown"


@pytest.mark.asyncio
async def test_nested_original_receipt_reconciles_without_resending(
    adapter, bridge, monkeypatch
):
    original = await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    cli = AsyncMock(
        return_value=(
            0,
            {
                "account_id": SELF,
                "request_id": "bridge-fixture",
                "receipt": {
                    "request_id": "bridge-fixture",
                    "account_id": SELF,
                    "wxid": ROOM,
                    "submission_state": "not_submitted",
                    "reason": "native_send_source_unavailable",
                },
            },
        )
    )
    monkeypatch.setattr(adapter.client, "run_cli", cli)
    receipt = await adapter.reconcile_submission(original["request_id"])
    assert receipt["status"] == "not_submitted"
    async with adapter._db.execute("SELECT status, reason FROM submissions") as cursor:
        assert await cursor.fetchone() == (
            "not_submitted",
            "native_send_source_unavailable",
        )
    assert cli.call_args.args[0] == "send-receipt"
    assert len(bridge["requests"]) == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("account_id", "wxid_other"),
        ("wxid", "other@chatroom"),
        ("request_id", "different-request"),
    ],
)
@pytest.mark.asyncio
async def test_nested_original_receipt_binding_cannot_update_journal(
    adapter, bridge, monkeypatch, field, value
):
    original = await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    monkeypatch.setattr(
        adapter.client,
        "run_cli",
        AsyncMock(
            return_value=(
                0,
                {"receipt": {"submission_state": "delivered", field: value}},
            )
        ),
    )
    with pytest.raises(ValueError, match="binding mismatch"):
        await adapter.reconcile_submission(original["request_id"])
    async with adapter._db.execute("SELECT status FROM submissions") as cursor:
        assert await cursor.fetchone() == ("submitted_unconfirmed",)
    assert len(bridge["requests"]) == 1


@pytest.mark.asyncio
async def test_ambiguous_post_checks_original_receipt_once_without_resend(
    adapter, bridge, monkeypatch
):
    bridge["malformed"] = True
    cli = AsyncMock(
        return_value=(
            0,
            {
                "account_id": SELF,
                "receipt": {
                    "account_id": SELF,
                    "wxid": ROOM,
                    "submission_state": "not_submitted",
                    "reason": "native_send_source_unavailable",
                },
            },
        )
    )
    monkeypatch.setattr(adapter.client, "run_cli", cli)
    with pytest.raises(RuntimeError, match="native_send_source_unavailable"):
        await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    assert len(bridge["requests"]) == 1
    assert cli.await_count == 1
    assert cli.call_args.args[0] == "send-receipt"
    async with adapter._db.execute(
        "SELECT request_id, status FROM submissions"
    ) as cursor:
        request_id, status = await cursor.fetchone()
    assert status == "not_submitted"
    assert (
        cli.call_args.args[cli.call_args.args.index("--request-id") + 1] == request_id
    )


@pytest.mark.asyncio
async def test_native_send_failed_envelope_checks_original_receipt(
    adapter, bridge, monkeypatch
):
    bridge["status"] = 503
    bridge["receipt"] = {
        "ok": False,
        "status": "send_failed",
        "reason": "native_send_source_unavailable",
        "error": "private explanation",
    }
    cli = AsyncMock(
        return_value=(
            0,
            {
                "receipt": {
                    "account_id": SELF,
                    "wxid": ROOM,
                    "submission_state": "not_submitted",
                    "reason": "native_send_source_unavailable",
                }
            },
        )
    )
    monkeypatch.setattr(adapter.client, "run_cli", cli)
    with pytest.raises(RuntimeError, match="native_send_source_unavailable"):
        await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    assert len(bridge["requests"]) == 1
    assert cli.await_count == 1
    assert cli.call_args.args[0] == "send-receipt"


@pytest.mark.asyncio
async def test_original_receipt_timeout_leaves_unknown_without_resend(
    adapter, bridge, monkeypatch
):
    bridge["malformed"] = True
    cli = AsyncMock(side_effect=TimeoutError)
    monkeypatch.setattr(adapter.client, "run_cli", cli)
    result = await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    assert result["status"] == "status_unknown"
    assert len(bridge["requests"]) == 1
    assert cli.await_count == 1
    assert cli.call_args.args[0] == "send-receipt"


@pytest.mark.asyncio
async def test_outbound_scope_and_components(adapter, bridge):
    for recipient in [OTHER, "different@chatroom", ""]:
        with pytest.raises(ValueError, match="scope"):
            await adapter.send_message(recipient, MessageChain([Plain("fixture")]))
    for chain in [
        MessageChain([]),
        MessageChain([Plain("fixture"), AtAll()]),
    ]:
        with pytest.raises((ValueError, NotImplementedError)):
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


@pytest.mark.asyncio
async def test_exact_image_reference_is_fetched_once_and_tracked(adapter, bridge):
    source = row(
        message_kind="image",
        display_text="[图片]",
        media={
            "image_ref": {
                "chatId": ROOM,
                "localId": "12",
                "serverId": "9007199254740993123",
                "md5": "fixture-md5",
            },
            "aes_key": "private-key-never-sent-to-model",
            "cdn_url": "https://example.invalid/encrypted",
        },
    )
    await adapter.handle_batch(batch(source, source))
    event = adapter._event_queue.get_nowait()
    images = [p for p in event.get_messages() if isinstance(p, Image)]
    assert len(images) == 1
    image_path = await images[0].convert_to_file_path()
    assert image_path in event._temporary_local_files
    assert bridge["image_requests"] == [
        {
            "chatId": ROOM,
            "localId": "12",
            "serverId": "9007199254740993123",
            "md5": "fixture-md5",
        }
    ]
    assert "private-key" not in event.message_str
    assert "encrypted" not in event.message_str


@pytest.mark.parametrize(
    "media",
    [
        {"local_path_status": "pending"},
        {"local_path_status": "unknown"},
        {"local_path_status": "unavailable"},
        {
            "image_ref": {
                "chatId": "different@chatroom",
                "localId": "1",
                "serverId": "2",
            }
        },
    ],
)
@pytest.mark.asyncio
async def test_unavailable_or_cross_chat_image_never_downloads(adapter, bridge, media):
    await adapter.handle_batch(batch(row(message_kind="image", media=media)))
    assert bridge["image_requests"] == []
    event = adapter._event_queue.get_nowait()
    assert not any(isinstance(p, Image) for p in event.get_messages())


@pytest.mark.asyncio
async def test_failed_image_keeps_text_without_retry(adapter, bridge):
    bridge["image_status"] = 409
    source = row(message_kind="image", display_text="[图片]")
    await adapter.handle_batch(batch(source, source))
    event = adapter._event_queue.get_nowait()
    assert "未重试" in event.message_str
    assert len(bridge["image_requests"]) == 1


@pytest.mark.asyncio
async def test_media_resolution_is_after_scope_and_age_filter(adapter, bridge):
    await adapter.handle_batch(
        batch(
            row(message_kind="image", chat_username="different@chatroom"),
            row(message_kind="image", create_time=1),
            row(message_kind="image", is_mentioned=False),
        )
    )
    assert bridge["image_requests"] == []
    assert adapter._event_queue.empty()


@pytest.mark.asyncio
async def test_native_audio_transcript_enters_model_text(adapter, monkeypatch):
    cli = AsyncMock(return_value=(0, {"transcript": "A synthetic shipping question"}))
    monkeypatch.setattr(adapter.client, "run_cli", cli)
    await adapter.handle_batch(batch(row(message_kind="audio", display_text="[语音]")))
    event = adapter._event_queue.get_nowait()
    assert "A synthetic shipping question" in event.message_str
    assert not any(isinstance(p, Record) for p in event.get_messages())
    assert "--include-transcript" in cli.call_args.args
    assert cli.await_count == 1


@pytest.mark.asyncio
async def test_cached_audio_and_off_mode_do_not_invoke_cli(adapter, monkeypatch):
    cli = AsyncMock()
    monkeypatch.setattr(adapter.client, "run_cli", cli)
    await adapter.handle_batch(
        batch(row(message_kind="audio", media={"transcript": "cached fixture"}))
    )
    assert "cached fixture" in adapter._event_queue.get_nowait().message_str
    adapter.client.audio_mode = "off"
    await adapter.handle_batch(batch(row(message_kind="audio", server_id=2)))
    assert not any(
        isinstance(c, Record) for c in adapter._event_queue.get_nowait().get_messages()
    )
    cli.assert_not_awaited()


@pytest.mark.asyncio
async def test_audio_is_decoded_to_wav_for_astrbot_stt(adapter, monkeypatch):
    import io
    from pathlib import Path

    import pysilk

    encoded = io.BytesIO()
    pysilk.encode(io.BytesIO(b"\x00\x00" * 2400), encoded, 24000, 24000, tencent=True)

    async def cli(*args, **kwargs):
        assert args[:2] == ("audio", "get")
        Path(args[args.index("--out") + 1]).write_bytes(encoded.getvalue())
        return 0, {"ok": True}

    monkeypatch.setattr(adapter.client, "run_cli", cli)
    adapter.client.audio_mode = "astrbot"
    await adapter.handle_batch(batch(row(message_kind="voice")))
    event = adapter._event_queue.get_nowait()
    record = next(c for c in event.get_messages() if isinstance(c, Record))
    wav = Path(await record.convert_to_file_path())
    assert wav.read_bytes().startswith(b"RIFF")
    assert str(wav) in event._temporary_local_files


@pytest.mark.asyncio
async def test_card_quote_forward_and_attachment_metadata_are_preserved(adapter):
    await adapter.handle_batch(
        batch(
            row(
                display_text="<msg aeskey='never-render-this' />",
                message_kind="appmsg",
                refer={
                    "svrId": "9007199254740993124",
                    "fromUser": OTHER,
                    "displayName": "Fixture",
                    "content": "quoted fixture",
                },
                url_link={
                    "title": "Fixture link",
                    "url": "https://example.com/fixture",
                },
                mini_program={
                    "title": "Fixture mini",
                    "appId": "fixture-app",
                    "pagePath": "pages/fixture",
                },
                forwarded_messages=[
                    {"display_name": "Fixture", "content": "Forwarded fixture"}
                ],
            )
        )
    )
    event = adapter._event_queue.get_nowait()
    assert any(
        isinstance(c, Reply) and str(c.id) == "9007199254740993124"
        for c in event.get_messages()
    )
    assert any(isinstance(c, Share) for c in event.get_messages())
    assert any(isinstance(c, Node) for c in event.get_messages())
    assert "Fixture mini" in event.message_str
    assert "never-render-this" not in event.message_str
    await adapter.handle_batch(
        batch(
            row(
                server_id=2,
                message_kind="attachment",
                media={"filename": "fixture.pdf", "local_path": "/etc/passwd"},
            )
        )
    )
    attachment = adapter._event_queue.get_nowait()
    assert "fixture.pdf" in attachment.message_str
    assert not any(isinstance(c, File) for c in attachment.get_messages())


@pytest.mark.asyncio
async def test_recall_is_notification_not_a_private_chat_prompt(adapter):
    adapter.private_enabled = True
    await adapter.handle_batch(
        batch(
            row(
                chat_username=OTHER,
                message_kind="recalled",
                recall={"replacedMsgId": "fixture-id"},
            )
        )
    )
    event = adapter._event_queue.get_nowait()
    assert event.message_obj.type == MessageType.OTHER_MESSAGE
    assert not event.is_private_chat()
    assert "fixture-id" in event.message_str


@pytest.mark.asyncio
async def test_rich_chain_preflight_sends_ordered_parts_and_freezes_files(
    adapter, bridge, monkeypatch, tmp_path
):
    from pathlib import Path

    image = tmp_path / "fixture.png"
    image.write_bytes(
        base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jK4sAAAAASUVORK5CYII="
        )
    )
    file = tmp_path / "fixture.txt"
    file.write_text("immutable fixture")
    cli = AsyncMock(return_value=(3, {"status": "submitted_unconfirmed"}))
    monkeypatch.setattr(adapter.client, "cli_executable", lambda: "/fixture/cli")
    monkeypatch.setattr(adapter.client, "run_cli", cli)
    result = await adapter.send_message(
        ROOM,
        MessageChain(
            [
                Plain("first"),
                Image.fromFileSystem(image),
                File(name="report.txt", file=str(file)),
                Plain("last"),
            ]
        ),
    )
    assert result["planned_parts"] == result["submitted_parts"] == 4
    assert result["status"] == "submitted_unconfirmed"
    assert [r["kind"] for r in result["parts"]] == ["text", "image", "file", "text"]
    assert [r["text"] for r in bridge["requests"]] == ["first", "last"]
    assert cli.await_count == 2
    args = cli.call_args.args
    snapshot = Path(args[args.index("--file") + 1])
    file.write_text("changed after submission")
    assert snapshot.name == "report.txt"
    assert snapshot.read_text() == "immutable fixture"


@pytest.mark.asyncio
async def test_native_mentions_use_exact_wxids_and_stable_attempt_id(
    adapter, monkeypatch
):
    cli = AsyncMock(return_value=(3, {"submission_state": "submitted_unconfirmed"}))
    monkeypatch.setattr(adapter.client, "cli_executable", lambda: "/fixture/cli")
    monkeypatch.setattr(adapter.client, "run_cli", cli)
    result = await adapter.send_message(
        ROOM, MessageChain([At(qq=OTHER), Plain("fixture")])
    )
    args = cli.call_args.args
    assert args[args.index("--mention") + 1] == OTHER
    assert args[args.index("--request-id") + 1] == result["request_id"]
    assert result["status"] == "submitted_unconfirmed"


@pytest.mark.asyncio
async def test_cli_unavailable_prevents_partial_text_send(
    adapter, bridge, monkeypatch, tmp_path
):
    path = tmp_path / "fixture.txt"
    path.write_text("fixture")

    def unavailable():
        raise RuntimeError("CLI unavailable")

    monkeypatch.setattr(adapter.client, "cli_executable", unavailable)
    with pytest.raises(RuntimeError, match="unavailable"):
        await adapter.send_message(
            ROOM,
            MessageChain(
                [Plain("must not send"), File(name="fixture.txt", file=str(path))]
            ),
        )
    assert bridge["requests"] == []


@pytest.mark.asyncio
async def test_uncertain_media_stops_remaining_parts_without_retry(
    adapter, bridge, monkeypatch, tmp_path
):
    path = tmp_path / "fixture.txt"
    path.write_text("fixture")
    cli = AsyncMock(side_effect=TimeoutError)
    monkeypatch.setattr(adapter.client, "cli_executable", lambda: "/fixture/cli")
    monkeypatch.setattr(adapter.client, "run_cli", cli)
    result = await adapter.send_message(
        ROOM,
        MessageChain(
            [
                Plain("first"),
                File(name="fixture.txt", file=str(path)),
                Plain("must not send"),
            ]
        ),
    )
    assert result["status"] == "status_unknown"
    assert result["submitted_parts"] == 2
    assert [call.args[0] for call in cli.call_args_list] == ["send", "send-receipt"]
    assert [r["text"] for r in bridge["requests"]] == ["first"]


@pytest.mark.asyncio
async def test_quote_fallback_is_explicit_and_can_be_disabled(adapter, bridge):
    chain = MessageChain(
        [
            Reply(
                id="fixture", message_str="quoted fixture", sender_nickname="Fixture"
            ),
            Plain("reply"),
        ]
    )
    await adapter.send_message(ROOM, chain)
    assert bridge["requests"][0]["text"] == "[引用 Fixture：quoted fixture]\nreply"
    adapter.config["wechat_use_quote_fallback"] = False
    with pytest.raises(NotImplementedError, match="Native quoted"):
        await adapter.send_message(ROOM, chain)
    assert len(bridge["requests"]) == 1


@pytest.mark.asyncio
async def test_forward_share_and_location_use_honest_text_fallbacks(adapter, bridge):
    await adapter.send_message(
        ROOM,
        MessageChain(
            [
                Nodes(
                    nodes=[Node(name="Fixture", content=[Plain("forwarded fixture")])]
                ),
                Share(title="Fixture link", url="https://example.com"),
                Location(lat=1, lon=2, title="Fixture location"),
            ]
        ),
    )
    text = bridge["requests"][0]["text"]
    assert "[转发内容 Fixture]" in text
    assert "https://example.com" in text
    assert "[位置]" in text


@pytest.mark.asyncio
async def test_queries_keep_coverage_metadata_and_enforce_scope(adapter, bridge):
    client = adapter.get_client()
    result = await client.get_sessions()
    await client.get_unread()
    await client.get_contacts("fixture")
    await client.resolve_recipient("ambiguous fixture")
    await client.get_history(ROOM, limit=10, since=1, until=2)
    assert result["meta"]["status"] == "windowed"
    assert bridge["query_requests"][-1] == (
        f"/chat/{ROOM}/history",
        {"limit": "10", "since": "1", "until": "2"},
    )
    with pytest.raises(ValueError, match="scope"):
        await client.get_history("different@chatroom")


@pytest.mark.asyncio
async def test_search_stats_export_and_group_members_use_bound_cli(
    adapter, monkeypatch
):
    cli = AsyncMock(
        return_value=(0, {"members": [{"wxid": OTHER, "nickname": "Fixture"}]})
    )
    monkeypatch.setattr(adapter.client, "run_cli", cli)
    group = await adapter.client.get_group(ROOM)
    assert group.members[0].user_id == OTHER
    assert group.member_count == 1
    await adapter.client.search_messages("fixture $(never execute)", ROOM)
    args = cli.call_args.args
    assert args[-2:] == ("--", "fixture $(never execute)")
    await adapter.client.get_stats(ROOM)
    await adapter.client.export_history(ROOM, "json")
    assert cli.call_args.args[0] == "export"
    with pytest.raises(ValueError):
        await adapter.client.export_history(ROOM, "executable")


@pytest.mark.asyncio
async def test_receipt_reconciliation_keeps_saved_account_and_request(
    adapter, bridge, monkeypatch
):
    original = await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    cli = AsyncMock(return_value=(0, {"status": "submitted_unconfirmed"}))
    monkeypatch.setattr(adapter.client, "run_cli", cli)
    result = await adapter.reconcile_submission(original["request_id"])
    assert result["status"] == "submitted_unconfirmed"
    args = cli.call_args.args
    assert args[0] == "send-receipt"
    assert args[args.index("--request-id") + 1] == "bridge-fixture"
    assert args[args.index("--expected-account") + 1] == SELF
    assert len(bridge["requests"]) == 1
    with pytest.raises(ValueError):
        await adapter.reconcile_submission("unknown-attempt")


@pytest.mark.asyncio
async def test_group_event_falls_back_without_fabricating_members(adapter, monkeypatch):
    monkeypatch.setattr(
        adapter.client, "get_group", AsyncMock(side_effect=RuntimeError("unsupported"))
    )
    event = adapter.create_event(adapter.convert_message(row()))
    group = await event.get_group()
    assert group.group_id == ROOM
    assert group.members is None
    assert event.get_extra("wechat_use_group_members_unavailable") is True


@pytest.mark.asyncio
async def test_active_question_can_resolve_an_exact_older_quoted_image(
    adapter, bridge, monkeypatch
):
    history = AsyncMock(
        return_value={
            "rows": [
                row(
                    server_id=123,
                    create_time=1,
                    message_kind="image",
                    media={
                        "image_ref": {"chatId": ROOM, "localId": "7", "serverId": "123"}
                    },
                )
            ]
        }
    )
    monkeypatch.setattr(adapter.client, "get_history", history)
    await adapter.handle_batch(
        batch(row(refer={"svrId": "123", "content": "<img aeskey='private'/>"}))
    )
    event = adapter._event_queue.get_nowait()
    assert any(isinstance(c, Image) for c in event.get_messages())
    assert "private" not in event.message_str
    assert bridge["image_requests"][0]["serverId"] == "123"
    history.assert_awaited_once_with(ROOM, limit=100)


@pytest.mark.asyncio
async def test_quoted_media_never_crosses_conversation_boundary(
    adapter, bridge, monkeypatch
):
    monkeypatch.setattr(
        adapter.client,
        "get_history",
        AsyncMock(
            return_value={
                "rows": [
                    row(
                        server_id=123,
                        chat_username="different@chatroom",
                        message_kind="image",
                    )
                ]
            }
        ),
    )
    await adapter.handle_batch(batch(row(refer={"svrId": "123", "content": "<img/>"})))
    event = adapter._event_queue.get_nowait()
    assert "引用媒体不可用" in event.message_str
    assert bridge["image_requests"] == []


@pytest.mark.asyncio
async def test_cli_binding_and_argument_vector_without_shell(
    adapter, monkeypatch, tmp_path
):
    import sys
    from pathlib import Path

    fake_home = tmp_path / "home"
    base = fake_home / ".wx-rs"
    (base / "com_fixture_WeChat").mkdir(parents=True)
    (base / "managed-wechat.json").write_text(
        json.dumps({"bundle_id": "com.fixture.WeChat"})
    )
    config = base / "com_fixture_WeChat" / "config.json"
    config.write_text(json.dumps({"account_wxid": SELF + "_abcd"}))
    executable = tmp_path / "fixture-cli"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import json,sys,time\n"
        "if 'timeout-fixture' in sys.argv: time.sleep(10)\n"
        "if 'error-fixture' in sys.argv:\n"
        " print(json.dumps({'error':'source_database_access_paused /private/fixture secret-canary'}));sys.exit(2)\n"
        "print(json.dumps({'args':sys.argv[1:]}))\n"
    )
    executable.chmod(0o700)
    adapter.client.cli_path = str(executable)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: fake_home))
    code, output = await adapter.client.run_cli(
        "search", "--in", ROOM, "--json", "--", "$(never execute); fixture"
    )
    assert code == 0
    assert output["args"][-1] == "$(never execute); fixture"
    with pytest.raises(RuntimeError) as unavailable:
        await adapter.client.run_cli("stats", "error-fixture", "--json")
    assert str(unavailable.value) == "source_database_access_paused"
    with pytest.raises(TimeoutError):
        await adapter.client.run_cli("stats", "timeout-fixture", "--json", timeout=0.02)
    for args in [("init",), ("audio", "setup"), ("send", "--account", OTHER)]:
        with pytest.raises(ValueError):
            await adapter.client.run_cli(*args)
    config.write_text(json.dumps({"account_wxid": "wxid_different_abcd"}))
    with pytest.raises(RuntimeError, match="account binding"):
        adapter.client.cli_executable()


def test_remote_bridge_never_uses_local_account_cli(adapter):
    adapter.base_url = "https://bridge.example.com"
    with pytest.raises(RuntimeError, match="loopback"):
        adapter.client.cli_executable()


@pytest.mark.asyncio
async def test_audio_and_video_send_as_explicit_file_fallbacks(
    adapter, monkeypatch, tmp_path
):
    path = tmp_path / "fixture.media"
    path.write_bytes(b"fixture")
    monkeypatch.setattr(adapter.client, "cli_executable", lambda: "/fixture/cli")
    monkeypatch.setattr(
        adapter.client,
        "run_cli",
        AsyncMock(return_value=(3, {"status": "submitted_unconfirmed"})),
    )
    monkeypatch.setattr(
        Record, "convert_to_file_path", AsyncMock(return_value=str(path))
    )
    monkeypatch.setattr(
        Video, "convert_to_file_path", AsyncMock(return_value=str(path))
    )
    result = await adapter.send_message(
        ROOM, MessageChain([Record(file=str(path)), Video(file=str(path))])
    )
    assert [r["fallback"] for r in result["parts"]] == [
        "audio_as_file",
        "video_as_file",
    ]
    assert all(r["kind"] == "file" for r in result["parts"])


@pytest.mark.asyncio
async def test_mismatched_receipt_binding_is_never_reported_as_delivery(
    adapter, bridge
):
    bridge["receipt"].update(
        {"status": "delivered", "expected_account": "wxid_different"}
    )
    result = await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    assert result["status"] == "status_unknown"


@pytest.mark.asyncio
async def test_legacy_submission_migration_does_not_invent_an_account(adapter):
    await adapter._db.execute(
        "INSERT INTO submissions (request_id, recipient, status, created_at) VALUES (?, ?, ?, ?)",
        ("legacy-fixture", ROOM, "status_unknown", 1),
    )
    await adapter._db.commit()
    with pytest.raises(ValueError, match="unbound"):
        await adapter.reconcile_submission("legacy-fixture")


@pytest.mark.asyncio
async def test_upstream_audio_refusal_is_visible_without_private_diagnostics(
    adapter, monkeypatch
):
    from astrbot.core.platform.sources.wechat_use.client import WechatUseOperationError

    monkeypatch.setattr(
        adapter.client,
        "run_cli",
        AsyncMock(side_effect=WechatUseOperationError("source_database_access_paused")),
    )
    await adapter.handle_batch(batch(row(message_kind="audio")))
    event = adapter._event_queue.get_nowait()
    assert "source_database_access_paused" in event.message_str
    assert "未重试" in event.message_str


def test_literal_xml_in_real_text_is_preserved_and_invalid_kinds_are_skipped(adapter):
    message = adapter.convert_message(
        row(display_text="<html>synthetic coding question</html>")
    )
    assert message.message_str == "<html>synthetic coding question</html>"
    assert adapter.convert_message(row(message_kind={"invalid": True})) is None


@pytest.mark.asyncio
async def test_reconciliation_does_not_accept_other_account_receipts(
    adapter, monkeypatch
):
    original = await adapter.send_message(ROOM, MessageChain([Plain("fixture")]))
    monkeypatch.setattr(
        adapter.client,
        "run_cli",
        AsyncMock(
            return_value=(0, {"status": "delivered", "expected_account": "wxid_other"})
        ),
    )
    with pytest.raises(ValueError, match="binding mismatch"):
        await adapter.reconcile_submission(original["request_id"])
