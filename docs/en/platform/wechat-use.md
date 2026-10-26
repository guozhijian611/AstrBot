# Local WeChat via wechat-use

This adapter reuses an already logged-in [wechat-use](https://github.com/leeguooooo/wechat-use) macOS WeChat instance through its **native HTTP Bridge and SSE** interface, with the same-account local CLI for native images, files and mentions. It does not use Tencent's iLink bot channel or require another QR login. The WeChat host requires Apple Silicon macOS and the dedicated WeChat version supported by wechat-use.

## Capabilities

| Capability | Integration and limits |
| --- | --- |
| Text, group mentions, DMs | Standard events, group scope, structured mention gating, self-echo filtering and durable deduplication |
| Inbound images / image stickers | Exact-ID verified `/image` downloads after authorization and claim; event-owned files are cleaned up. Availability depends on the verified upstream image path |
| Quoted older media | A fresh authorized question may resolve its exact quoted image/audio ID in the same chat's latest 100 history rows; unrelated historical media is not materialized |
| Inbound audio | Installed local Whisper, decoded WAV for AstrBot STT, or notifications only. Upstream audio-access refusals remain unavailable |
| Outbound images / files | Same-host CLI `send --image/--file`, immutable attachment snapshots; 100 MiB per file |
| Outbound mentions | Exact member wxids via CLI `--mention`, subject to current runtime support; native @all is unavailable |
| Quotes | Inbound `Reply` components; outbound native quotes are unavailable. Use an explicit labeled text fallback or reject quotes |
| Links, mini programs, forwards | Structured metadata and readable content; outbound links/forwards/locations are text representations rather than native cards |
| Inbound files / videos | Available metadata; no invented download URLs or arbitrary local-path reads when upstream lacks a general attachment download API |
| Outbound audio / video | File attachments with `audio_as_file` / `video_as_file` receipt metadata, not native voice bubbles or video messages |
| Recall / system / financial notifications | Available metadata as `OtherMessage`, without treating notifications as DM prompts or performing financial actions |
| Queries | Sessions, unread, contacts, candidates, history, search, statistics, export, members and original receipts. Coverage metadata is preserved |
| Models, personas, reasoning, knowledge bases | AstrBot's native configuration, vision processing and STT pipelines |

Unavailable upstream interfaces (Moments, favorites, adding friends, native quoted/voice sending, etc.) are not implemented by bypassing validation. The experimental Windows CLI does not offer the required Bridge.

## Setup

1. Complete wechat-use installation, activation and dedicated-client login.
2. Check `wechat-use doctor --json` and `http://127.0.0.1:18400/health`. The daemon, account and receiver must be ready; HTTP 200 alone is insufficient.
3. Reuse the existing native Bridge. Start `wechat-bridge` only if it is not running; do **not** use `--shape hermes` or start a second Bridge on the same port.
4. Find exact group IDs with `wechat-use sessions --json` and identify the logged-in account's exact wxid.

In **Bots → Create Bot**, select **WeChat (wechat-use)**. Set the Bridge origin (default `http://127.0.0.1:18400`), the account wxid and allowed group IDs (`xxx@chatroom`). Enter a Bridge Bearer Token only if `WECHAT_BRIDGE_BEARER` is configured. An empty group list denies all groups; private messages are disabled and group mentions are required by default. Enable and save the bot.

Choose the model, persona and knowledge base under the bot's **Configuration File → AI** settings. Magpie can be used as an OpenAI-compatible provider at `http://127.0.0.1:3425/v1`; use its exact model ID. Provider API keys are separate from Bridge tokens.

Additional options:

- **Local CLI path**: `auto` discovers `wechat-use`, or set an absolute executable path.
- **Voice processing**: `wechat_use` uses installed Whisper, `astrbot` supplies decoded WAV to AstrBot STT, and `off` keeps only notifications.
- **Quote text fallback**: enabled by default; disabling it rejects unavailable native quoted sending instead of silently dropping the quote.

AstrBot access controls, wake rules and session isolation remain in effect. Run only one responder per target group and pause any previous auto-reply script before enabling this adapter. Existing bots are not modified automatically.

## Behavior and limits

- Receiving requires a ready snapshot. Sending an existing response checks the Bridge, daemon and account, then relies on the native send endpoint to validate its sending configuration and current conditions. Snapshot capture or catch-up alone does not discard a response; native refusals and uncertain outcomes are still never retried.
- Images use the verified endpoint, never encrypted CDN URLs or media XML. Images are fetched only for in-scope triggered messages and are limited to 32 MiB.
- Mixed chains are fully preflighted, including local account binding, before ordered submission. Refusal stops sending; an unknown outcome stops remaining parts; accepted unconfirmed parts are not retried. Each part retains its original request ID.
- Audio/video sending is explicitly represented as file attachments. Unsupported components are rejected before submission. Labeled quote text is not a native quote bubble.
- Starts at the current time without replying to old messages. Reconnects overlap the timestamp boundary and deduplicate using durable IDs. Messages older than five minutes are ignored.
- Claims are committed before enqueueing. A crash may drop a reply; the adapter does not replay possibly processed messages.
- Each outgoing text is submitted once. Timeouts, `submitted_unconfirmed` and `status_unknown` never trigger retries or imply delivery. Local request IDs and any Bridge receipt IDs are kept separately in the adapter-owned `data/wechat_use/` SQLite journal. No original WeChat database is opened.
- The Bridge health response does not expose a complete account ID. Keep the configured wxid aligned with its account. Self-authored rows from another account stop batch processing.
- Frozen outgoing attachments remain under `data/wechat_use/outbox/` so background uploads do not depend on a caller's temporary file. Do not remove snapshots belonging to unresolved tasks.
- CLI operations verify the managed account and do not accept account, bundle or process overrides. Deploy CLI-dependent operations on the WeChat Mac; remote Bridges still support HTTP text and verified image reads.
- Keep the Bridge on loopback. Container loopback points to the container itself; remote access needs a protected network and Bearer authentication. Do not expose the Bridge publicly.

## Plugin client

`event.adapter.get_client()` exposes `get_sessions`, `get_unread`, `get_contacts`, `resolve_recipient`, `get_history`, `search_messages`, `get_stats`, `export_history`, and `get_group`. The resolver returns candidates without selecting a recipient. History, search, statistics and export enforce messaging scope. Member queries and other advanced CLI operations remain runtime-dependent; unavailable group members preserve inbound metadata rather than returning a fabricated empty group.

`await event.adapter.reconcile_submission(original_adapter_request_id)` queries the original Bridge/CLI request with its saved account without resubmitting. Legacy journal rows with unknown account ownership cannot be reconciled automatically.

If audio retrieval reports `voice_data not found` / `source_database_access_paused`, upstream has no usable verified audio source for that message. Existing transcripts remain usable, otherwise media is marked unavailable. Switching STT providers cannot repair missing source audio. Do not open the original WeChat databases directly.

When receiving is unavailable, dashboard errors and AstrBot logs distinguish daemon, account, and receiver failures. `stage=profile_fetch` identifies upstream adaptation retrieval; `stage=profile_verify` identifies adaptation verification. Stages come from a fixed allowlist; arbitrary upstream text, account identifiers, and credentials are omitted. Use the official `wechat-use` commands and local `~/.wx-rs/wechatd.log` for underlying diagnostics. The adapter reconnects after recovery without replaying old messages.

Submission logs and the adapter journal retain fixed refusal codes such as `native_send_source_unavailable`. Nested CLI `receipt` responses are checked against the saved account, recipient, and original request before updating status. An explicit `not_submitted` is preserved and never automatically resubmitted.

For an ambiguous submission response, the adapter reads the original receipt at most once with a five-second timeout. An unavailable receipt preserves the unknown outcome; it never resends or starts a new send task.

## Tests

```bash
uv sync
.venv/bin/python -m pytest tests/test_wechat_use_adapter.py -q
```

Tests use a fake Bridge, synthetic media, real SILK decoding and a temporary CLI to verify media conversion, reconnects, persistent deduplication, authorization, isolated arguments, timeouts and uncertain outcomes. They send no real messages. CLI `--dry-run` and read-only live Bridge checks can be run separately. For a real group test, confirm the target and pause the previous responder, then ask a group member to send a new mention.
