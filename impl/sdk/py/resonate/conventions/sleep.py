from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class Sleep:
    id: str
    secs: float

    @property
    def idempotency_key(self) -> str:
        return self.id

    @property
    def data(self) -> Any:
        return None

    @property
    def timeout(self) -> float:
        return self.secs

    @property
    def tags(self) -> dict[str, str]:
        return {"resonate:timeout": "true"}

    def options(
        self,
        id: str | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        tags: dict[str, str] | None = None,
        target: str | None = None,
        timeout: float | None = None,
        version: int | None = None,
    ) -> Sleep:
        self.id = id or self.id
        return self
