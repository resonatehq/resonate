from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from resonate.options import Options


class FunctionRegistry:
    def __init__(self) -> None:
        self._store: dict[str, tuple[Callable[[Any], Any], Options]] = {}
        self._index: dict[Callable[[Any], Any], str] = {}

    def add(self, key: str, value: tuple[Callable[[Any], Any], Options]) -> None:
        assert key not in self._store
        assert value[0] not in self._index
        self._store[key] = value
        self._index[value[0]] = key

    def get(self, key: str) -> tuple[Callable[[Any], Any], Options] | None:
        return self._store.get(key)

    def get_from_value(self, v: Callable[[Any], Any]) -> str | None:
        return self._index.get(v)
