from __future__ import annotations

from typing import Any


class Dependencies:
    def __init__(self) -> None:
        self._deps: dict[str, Any] = {}

    def add(self, key: str, obj: Any) -> None:
        self._deps[key] = obj

    def get[T](self, key: str, default: T) -> Any | T:
        return self._deps.get(key, default)
