from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Self


@dataclass
class Callback:
    id: str
    promise_id: str
    timeout: int
    created_on: int

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        return cls(
            id=data["id"],
            promise_id=data["promiseId"],
            timeout=data["timeout"],
            created_on=data["createdOn"],
        )
