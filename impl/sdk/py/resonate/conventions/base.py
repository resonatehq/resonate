from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class Base:
    id: str
    idempotency_key: str | None
    headers: dict[str, str] | None
    data: Any
    timeout: int
    tags: dict[str, str] | None

    def options(
        self,
        id: str | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        send_to: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Base:
        self.id = id or self.id
        self.idempotency_key = idempotency_key(self.id) if callable(idempotency_key) else (idempotency_key or self.idempotency_key)
        self.timeout = timeout or self.timeout
        self.tags = tags or self.tags

        return self
