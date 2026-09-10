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
        promise_tags: dict[str, str] | None = None,
    ) -> ScheduleRecord:
        """Create a schedule.

        ``promise_tags`` are stamped onto every promise the schedule fires.
        The server requires a ``resonate:target`` tag (the routing target for
        the fired promise) and rejects the create without one.
        """
        return await self.sender.schedule_create(
            ScheduleCreateReq(
                id=id,
                cron=cron,
                promise_id=promise_id,
                promise_timeout=promise_timeout,
                promise_param=self.codec.encode(promise_param.data),
                promise_tags=promise_tags if promise_tags is not None else {},
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
