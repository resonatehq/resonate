from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

if TYPE_CHECKING:
    from resonate.typing import Data, Headers, IdempotencyKey, State, Tags


@dataclass(frozen=True)
class Param:
    headers: Headers
    data: Data


@dataclass(frozen=True)
class Value:
    headers: Headers
    data: Data


@dataclass(frozen=True)
class DurablePromiseRecord:
    state: State
    promise_id: str
    timeout: int
    param: Param
    value: Value
    created_on: int
    completed_on: int | None
    idempotency_key_for_create: IdempotencyKey
    idempotency_key_for_complete: IdempotencyKey
    tags: Tags

    def is_completed(self) -> bool:
        return not self.is_pending()

    def is_timeout(self) -> bool:
        return self.state == "REJECTED_TIMEDOUT"

    def is_canceled(self) -> bool:
        return self.state == "REJECTED_CANCELED"

    def is_rejected(self) -> bool:
        return self.state == "REJECTED"

    def is_resolved(self) -> bool:
        return self.state == "RESOLVED"

    def is_pending(self) -> bool:
        return self.state == "PENDING"

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Self:
        return cls(
            state=data["state"],
            promise_id=data["id"],
            timeout=data["timeout"],
            param=data["param"],
            value=data["value"],
            created_on=data["createdOn"],
            completed_on=data.get("completedOn"),
            idempotency_key_for_complete=data.get("idempotencyKeyForComplete"),
            idempotency_key_for_create=data.get("idempotencyKeyForCreate"),
            tags=data.get("tags"),
        )
