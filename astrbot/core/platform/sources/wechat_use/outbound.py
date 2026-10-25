import re
from collections import deque
from pathlib import Path

from astrbot.api.event import MessageChain
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

from .client import WechatUseClient


async def prepare_outbound(
    message: MessageChain,
    recipient: str,
    client: WechatUseClient,
    quote_fallback: bool,
) -> list[dict]:
    """Validate the entire chain before submitting any component.

    Args:
        message: Framework response, including generated media or decorations.
        recipient: Exact authorized WeChat username.
        client: Account-bound transport client.
        quote_fallback: Allow explicitly labeled text in place of native quotes.

    Returns:
        Ordered text/image/file submissions. Native-unsupported conversions are labeled.

    Raises:
        ValueError: A component, mention or media source is invalid.
        NotImplementedError: A requested native feature has no safe representation.
    """
    client.validate_chat(recipient)
    flattened = []
    pending = deque((part, 0) for part in message.chain)
    while pending:
        part, depth = pending.popleft()
        if depth > 5 or len(flattened) + len(pending) > 200:
            raise ValueError("wechat-use message chain is too large")
        if isinstance(part, Nodes):
            pending.extendleft((node, depth + 1) for node in reversed(part.nodes))
        elif isinstance(part, Node):
            pending.extendleft((item, depth + 1) for item in reversed(part.content))
            pending.appendleft((Plain(f"\n[转发内容 {part.name or ''}]\n"), depth + 1))
        else:
            flattened.append(part)

    plan: list[dict] = []
    text, mentions = "", []
    for part in flattened:
        if isinstance(part, Plain):
            text += part.text
        elif isinstance(part, At):
            if isinstance(part, AtAll) or str(part.qq) == "all":
                raise NotImplementedError("wechat-use does not expose native @all")
            if not recipient.endswith("@chatroom"):
                raise ValueError("wechat-use mentions require a group")
            member = str(part.qq)
            if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", member):
                raise ValueError("A mention requires an exact member wxid")
            if member not in mentions:
                mentions.append(member)
        elif isinstance(part, Reply):
            if not quote_fallback:
                raise NotImplementedError(
                    "Native quoted replies are not exposed by wechat-use"
                )
            excerpt = part.message_str or "".join(
                item.text for item in (part.chain or []) if isinstance(item, Plain)
            )
            if not excerpt:
                history = await client.get_history(recipient, limit=100)
                rows = history.get("rows", []) if isinstance(history, dict) else history
                for row in rows:
                    if isinstance(row, dict) and str(
                        row.get("server_id_str") or row.get("server_id")
                    ) == str(part.id):
                        excerpt = row.get("display_text") or ""
                        break
            if not excerpt or excerpt.lstrip().startswith("<"):
                raise NotImplementedError(
                    "Quoted content is unavailable for an honest text fallback"
                )
            text += f"[引用 {part.sender_nickname or part.sender_id or ''}：{excerpt[:1000]}]\n"
        elif isinstance(part, Share):
            text += f"{part.title}\n{part.url}\n{part.content or ''}"
        elif isinstance(part, Location):
            text += (
                f"[位置] {part.title or ''} {part.lat},{part.lon} {part.content or ''}"
            )
        elif isinstance(part, Image | File | Record | Video):
            # Resolve every source before the first send so a bad later component
            # cannot cause a partially submitted chain.
            if isinstance(part, File):
                source = await part.get_file()
            else:
                source = await part.convert_to_file_path()
            if not source:
                raise ValueError("wechat-use media has no local file")
            path = Path(source).resolve(strict=True)
            if not path.is_file() or path.stat().st_size > 100 * 1024 * 1024:
                raise ValueError(
                    "wechat-use media must be a regular file up to 100 MiB"
                )
            if text.strip() or mentions:
                plan.append(
                    {
                        "kind": "text",
                        "text": text
                        if text.strip()
                        else " ".join("@" + m for m in mentions),
                        "mentions": mentions,
                    }
                )
                text, mentions = "", []
            name = path.name
            if isinstance(part, File) and part.name:
                name = Path(part.name.replace("\\", "/")).name
            if name in {"", ".", ".."}:
                raise ValueError("Invalid attachment name")
            fallback = (
                "audio_as_file"
                if isinstance(part, Record)
                else "video_as_file"
                if isinstance(part, Video)
                else None
            )
            plan.append(
                {
                    "kind": "image" if isinstance(part, Image) else "file",
                    "path": str(path),
                    "name": name,
                    "fallback": fallback,
                }
            )
        else:
            raise NotImplementedError(f"wechat-use cannot send component {part.type}")
    if text.strip() or mentions:
        plan.append(
            {
                "kind": "text",
                "text": text if text.strip() else " ".join("@" + m for m in mentions),
                "mentions": mentions,
            }
        )
    if not plan:
        raise ValueError("wechat-use cannot send empty text")
    return plan
