from __future__ import annotations

from typing import Any


class Command: ...


class DurablePromise:
    def __init__(
        self,
        id: str | None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        self.id = id
        self.data = data
        self.headers = headers
        self.tags = tags
