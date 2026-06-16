from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.send import ScheduleCreateReq

if TYPE_CHECKING:
    from resonate.codec import Codec
    from resonate.send import ScheduleSearchResult, Sender
    from resonate.types import ScheduleRecord, Value


class Schedules:
    """Sub-client for schedule operations."""

    def __init__(self, sender: Sender, codec: Codec) -> None:
        self.sender = sender
        self.codec = codec

    async def create(
        self,
        id: str,
        cron: str,
        promise_id: str,
        promise_timeout: int,
        promise_param: Value,
    ) -> ScheduleRecord:
        """Create a schedule."""
        return await self.sender.schedule_create(
            ScheduleCreateReq(
                id=id,
                cron=cron,
                promise_id=promise_id,
                promise_timeout=promise_timeout,
                promise_param=self.codec.encode(promise_param.data),
                promise_tags={},
            )
        )

    async def get(self, id: str) -> ScheduleRecord:
        """Get a schedule by ID."""
        return await self.sender.schedule_get(id)

    async def delete(self, id: str) -> None:
        """Delete a schedule."""
        await self.sender.schedule_delete(id)

    async def search(
        self,
        tags: dict[str, str] | None,
        limit: int | None,
        cursor: str | None,
    ) -> ScheduleSearchResult:
        """Search for schedules matching optional tag filter."""
        return await self.sender.schedule_search(tags, limit, cursor)
