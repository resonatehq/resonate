from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import assert_never

if TYPE_CHECKING:
    from resonate.dataclasses import ResonateCoro
    from resonate.promise import Promise
    from resonate.typing import AwaitingFor

V = TypeVar("V")
K = TypeVar("K")


class Awaiting:
    def __init__(self) -> None:
        self._local: dict[Promise[Any], list[ResonateCoro[Any]]] = {}
        self._remote: dict[Promise[Any], list[ResonateCoro[Any]]] = {}

    def is_empty(self, awaiting_for: AwaitingFor) -> bool:
        if awaiting_for == "local":
            return len(self._local) == 0
        if awaiting_for == "remote":
            return len(self._remote) == 0
        assert_never(awaiting_for)

    def clear(self) -> None:
        self._local.clear()
        self._remote.clear()

    def waiting_for(
        self, key: Promise[Any], awaiting_for: AwaitingFor | None = None
    ) -> bool:
        if awaiting_for is None:
            return key in self._local or key in self._remote
        if awaiting_for == "local":
            return key in self._local
        if awaiting_for == "remote":
            return key in self._remote

        assert_never(awaiting_for)

    def append(
        self,
        key: Promise[Any],
        value: ResonateCoro[Any] | None,
        awaiting_for: AwaitingFor,
    ) -> None:
        awaiting: dict[Promise[Any], list[ResonateCoro[Any]]]
        if awaiting_for == "local":
            awaiting = self._local
        elif awaiting_for == "remote":
            awaiting = self._remote
        else:
            assert_never(awaiting_for)
        if value is not None:
            awaiting.setdefault(key, []).append(value)
        else:
            assert key not in awaiting
            awaiting[key] = []

    def get(
        self, key: Promise[Any], awaiting_for: AwaitingFor
    ) -> list[ResonateCoro[Any]] | None:
        awaiting: dict[Promise[Any], list[ResonateCoro[Any]]]
        if awaiting_for == "local":
            awaiting = self._local
        elif awaiting_for == "remote":
            awaiting = self._remote
        else:
            assert_never(awaiting_for)
        return awaiting.get(key)

    def pop(
        self, key: Promise[Any], awaiting_for: AwaitingFor
    ) -> list[ResonateCoro[Any]]:
        awaiting: dict[Promise[Any], list[ResonateCoro[Any]]]
        if awaiting_for == "local":
            awaiting = self._local
        elif awaiting_for == "remote":
            awaiting = self._remote
        else:
            assert_never(awaiting_for)
        return awaiting.pop(key)


class EphemeralMemo(Generic[K, V]):
    def __init__(self) -> None:
        self._memo: dict[K, V] = {}

    def add(self, key: K, value: V) -> None:
        assert key not in self._memo, f"There's already a value for key={key}"
        self._memo[key] = value

    def get(self, key: K) -> V | None:
        return self._memo.get(key)

    def pop(self, key: K) -> None:
        assert key in self._memo, f"There's not value for key={key}"
        self._memo.pop(key)

    def has(self, key: K) -> bool:
        return key in self._memo

    def clear(self) -> None:
        self._memo.clear()

    def is_empty(self) -> bool:
        return len(self._memo) == 0


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
