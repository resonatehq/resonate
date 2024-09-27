from __future__ import annotations

from typing import Generic, TypeVar

V = TypeVar("V")
K = TypeVar("K")


class DoubleDict(Generic[K, V]):
    def __init__(self, data: dict[K, V] | None = None) -> None:
        self.data = data if data is not None else {}
        self._index = self._data_to_index()

    def _data_to_index(self) -> dict[V, K]:
        index = {}
        for k, v in self.data.items():
            assert v not in index, "Value cannot be duplicated."
            index[v] = k
        return index

    def add(self, key: K, value: V) -> None:
        assert key not in self.data
        assert value not in self._index
        self.data[key] = value
        self._index[value] = key

    def pop(self, key: K) -> V:
        assert key in self.data
        assert self.data[key] in self._index
        value = self.data.pop(key)
        self._index.pop(value)
        return value

    def get(self, key: K) -> V | None:
        return self.data.get(key)

    def get_from_value(self, value: V) -> K | None:
        return self._index.get(value)
