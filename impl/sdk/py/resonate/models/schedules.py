from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from resonate.models.durable_promise import DurablePromiseValue

if TYPE_CHECKING:
    from collections.abc import Mapping

    from resonate.models.store import Store


@dataclass
class Schedule:
    id: str
    description: str | None
    cron: str
    tags: dict[str, str] | None
    promise_id: str
    promise_timeout: int
    promise_param: DurablePromiseValue
    promise_tags: dict[str, str] | None
    last_runtime: int | None
    next_runtime: int
    ikey: str | None
    created_on: int

    store: Store = field(repr=False)

    def delete(self) -> None:
        self.store.schedules.delete(self.id)

    @classmethod
    def from_dict(cls, store: Store, data: Mapping[str, Any]) -> Schedule:
        return cls(
            id=data["id"],
            description=data.get("description"),
            cron=data["cron"],
            tags=data.get("tags", {}),
            promise_id=data["promiseId"],
            promise_timeout=data["promiseTimeout"],
            promise_param=DurablePromiseValue.from_dict(store, data.get("param", {})),
            promise_tags=data.get("promiseTags", {}),
            last_runtime=data.get("lastRunTime"),
            next_runtime=data["nextRunTime"],
            ikey=data.get("idempotencyKey"),
            created_on=data["createdOn"],
            store=store,
        )
