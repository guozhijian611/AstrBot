from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

from astrbot.api.event import AstrMessageEvent, MessageChain

if TYPE_CHECKING:
    from .wechat_use_adapter import WechatUseAdapter


class WechatUseMessageEvent(AstrMessageEvent):
    def __init__(self, *args, adapter: "WechatUseAdapter", **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.adapter = adapter

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
