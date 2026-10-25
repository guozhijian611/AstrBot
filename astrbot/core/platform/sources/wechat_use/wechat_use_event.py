from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.platform import Group

if TYPE_CHECKING:
    from .wechat_use_adapter import WechatUseAdapter


class WechatUseMessageEvent(AstrMessageEvent):
    def __init__(self, *args, adapter: "WechatUseAdapter", **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.adapter = adapter

    async def get_group(self, group_id: str | None = None, **kwargs) -> Group | None:
        """Enrich the current group when this runtime provides member queries.

        Args:
            group_id: Optional exact allowed group ID.
            **kwargs: Reserved framework compatibility arguments.

        Returns:
            Enriched group, or inbound metadata when member lookup is unavailable.
        """
        group = await super().get_group(group_id, **kwargs)
        if group is None:
            return None
        self.adapter.client.validate_chat(group.group_id)
        try:
            return await self.adapter.client.get_group(group.group_id, group.group_name)
        except (OSError, RuntimeError, TimeoutError, ValueError):
            self.set_extra("wechat_use_group_members_unavailable", True)
            return group

    async def send(self, message: MessageChain) -> None:
        receipt = await self.adapter.send_message(self.get_session_id(), message)
        self.set_extra("wechat_use_receipt", receipt)
        await super().send(message)

    async def send_streaming(
        self,
        generator: AsyncGenerator[MessageChain, None],
        use_fallback: bool = False,
    ) -> None:
        """Buffer a generated response into a single non-streaming submission.

        Args:
            generator: Incoming response fragments.
            use_fallback: Reserved framework compatibility flag.
        """
        buffered = MessageChain([])
        async for chain in generator:
            buffered.chain.extend(chain.chain)
        if buffered.chain:
            buffered.squash_plain()
            await self.send(buffered)
