from __future__ import annotations

from typing import Generic, TypeVar

V = TypeVar("V")
K = TypeVar("K")


class EphemeralMemo(Generic[K, V]):
    def __init__(self) -> None:
        self._memo: dict[K, V] = {}
        self.roots: set[V] = set()

    def add(self, key: K, value: V, *, as_root: bool) -> None:
        if as_root:
            assert key not in self.roots, f"There's alredy a root for key={key}"
            self.roots.add(value)

        assert key not in self._memo, f"There's already a value for key={key}"
        self._memo[key] = value

    def get(self, key: K) -> V | None:
        return self._memo.get(key)

    def pop(self, key: K) -> V:
        assert key in self._memo, f"There's not value for key={key}"
        value = self._memo.pop(key)
        self.roots.discard(value)
        return value

    def has(self, key: K) -> bool:
        return key in self._memo

    def clear(self) -> None:
        self._memo.clear()
        self.roots.clear()

    def is_a_root(self, key: K) -> bool:
        assert key in self._memo, f"There's not value for key={key}"
        return key in self.roots


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
