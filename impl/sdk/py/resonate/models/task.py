from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

    from resonate.models.durable_promise import DurablePromise
    from resonate.models.store import Store


@dataclass
class Task:
    id: str
    counter: int
    store: Store = field(repr=False)

    def claim(self, pid: str, ttl: int) -> tuple[DurablePromise, DurablePromise | None]:
        return self.store.tasks.claim(id=self.id, counter=self.counter, pid=pid, ttl=ttl)

    def complete(self) -> None:
        self._completed = self.store.tasks.complete(id=self.id, counter=self.counter)

    @classmethod
    def from_dict(cls, store: Store, data: Mapping[str, Any]) -> Task:
        return cls(
            id=data["id"],
            counter=data["counter"],
            store=store,
        )
