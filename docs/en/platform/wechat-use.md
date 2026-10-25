# Local WeChat via wechat-use

This adapter reuses an already logged-in [wechat-use](https://github.com/leeguooooo/wechat-use) macOS WeChat instance through its **native HTTP Bridge and SSE** interface. It does not use Tencent's iLink bot channel or require another QR login. The WeChat host requires Apple Silicon macOS and the dedicated WeChat version supported by wechat-use; AstrBot can run on any host with access to that Bridge.

## Setup

1. Complete wechat-use installation, activation and dedicated-client login.
2. Check `wechat-use doctor --json` and `http://127.0.0.1:18400/health`. The daemon, account and receiver must be ready; HTTP 200 alone is insufficient.
3. Reuse the existing native Bridge. Start `wechat-bridge` only if it is not running; do **not** use `--shape hermes` or start a second Bridge on the same port.
4. Find exact group IDs with `wechat-use sessions --json` and identify the logged-in account's exact wxid.

In **Bots → Create Bot**, select **WeChat (wechat-use)**. Set the Bridge origin (default `http://127.0.0.1:18400`), the account wxid and allowed group IDs (`xxx@chatroom`). Enter a Bridge Bearer Token only if `WECHAT_BRIDGE_BEARER` is configured. An empty group list denies all groups; private messages are disabled and group mentions are required by default. Enable and save the bot.

Choose the model, persona and knowledge base under the bot's **Configuration File → AI** settings. Magpie can be used as an OpenAI-compatible provider at `http://127.0.0.1:3425/v1`; use its exact model ID. Provider API keys are separate from Bridge tokens.

AstrBot access controls, wake rules and session isolation remain in effect. Run only one responder per target group and pause any previous auto-reply script before enabling this adapter. Existing bots are not modified automatically.

## Behavior and limits

- Text-only inbound and outbound. Images, audio, files, outgoing mentions and quoted replies are unsupported. Disable mention/quote reply decorations. Unsupported chains are rejected as a whole.
- Starts at the current time without replying to old messages. Reconnects overlap the timestamp boundary and deduplicate using durable IDs. Messages older than five minutes are ignored.
- Claims are committed before enqueueing. A crash may drop a reply; the adapter does not replay possibly processed messages.
- Each outgoing text is submitted once. Timeouts, `submitted_unconfirmed` and `status_unknown` never trigger retries or imply delivery. Local request IDs and any Bridge receipt IDs are kept separately in the adapter-owned `data/wechat_use/` SQLite journal. No original WeChat database is opened.
- The Bridge health response does not expose a complete account ID. Keep the configured wxid aligned with its account. Self-authored rows from another account stop batch processing.
- Keep the Bridge on loopback. Container loopback points to the container itself; remote access needs a protected network and Bearer authentication. Do not expose the Bridge publicly.

## Tests

```bash
uv sync
.venv/bin/python -m pytest tests/test_wechat_use_adapter.py -q
```

Tests use a local fake Bridge for receive/send behavior, reconnection, durable deduplication, access controls and uncertain submission outcomes. They send no messages to real contacts. For a real group test, first confirm the target and pause the previous responder, then ask a group member to send a new mention.
