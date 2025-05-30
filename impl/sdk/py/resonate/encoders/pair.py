from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resonate.models.encoder import Encoder


class PairEncoder[T, U, V]:
    def __init__(self, l: Encoder[T, U], r: Encoder[T, V]) -> None:
        self._l = l
        self._r = r

    def encode(self, obj: T) -> tuple[U, V]:
        return self._l.encode(obj), self._r.encode(obj)

    def decode(self, obj: tuple[U, V]) -> T | None:
        u, v = obj

        if (t := self._l.decode(u)) is not None:
            return t

        if (t := self._r.decode(v)) is not None:
            return t

        return None
