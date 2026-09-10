from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.send import PromiseSearchResult
from resonate.types import PromiseCreateReq, PromiseSettleReq, Value

if TYPE_CHECKING:
    from resonate.codec import Codec
    from resonate.core import SettleState
    from resonate.send import Sender
    from resonate.types import PromiseRecord


class Promises:
    """Sub-client for durable promise operations."""

    def __init__(self, sender: Sender, codec: Codec) -> None:
        self.sender = sender
        self.codec = codec

    async def get(self, id: str) -> PromiseRecord:
        """Get a promise by ID."""
        record = await self.sender.promise_get(id)
        return self.codec.decode_promise(record)

    async def create(
        self,
        id: str,
        timeout_at: int,
        param: Value,
        tags: dict[str, str],
    ) -> PromiseRecord:
        """Create a promise."""
        record = await self.sender.promise_create(
            PromiseCreateReq(
                id=id,
                timeout_at=timeout_at,
                param=self.codec.encode(param.data),
                tags=tags,
            )
        )
        return self.codec.decode_promise(record)

    async def resolve(self, id: str, value: Value) -> PromiseRecord:
        """Resolve a promise."""
        return await self._settle(id, "resolved", value)

    async def reject(self, id: str, value: Value) -> PromiseRecord:
        """Reject a promise."""
        return await self._settle(id, "rejected", value)

    async def cancel(self, id: str, value: Value) -> PromiseRecord:
        """Cancel a promise (settles as ``rejected_canceled``)."""
        return await self._settle(id, "rejected_canceled", value)

    async def _settle(
        self,
        id: str,
        state: SettleState,
        value: Value,
    ) -> PromiseRecord:
        record = await self.sender.promise_settle(
            PromiseSettleReq(id=id, state=state, value=self.codec.encode(value.data))
        )
        return self.codec.decode_promise(record)

    async def search(
        self,
        state: str | None,
        tags: dict[str, str] | None,
        limit: int | None,
        cursor: str | None,
    ) -> PromiseSearchResult:
        """Search for promises matching optional state/tags filters."""
        result = await self.sender.promise_search(state, tags, limit, cursor)
        promises = [self.codec.decode_promise(p) for p in result.promises]
        return PromiseSearchResult(promises=promises, cursor=result.cursor)
