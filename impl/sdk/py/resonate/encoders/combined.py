from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resonate.models.encoder import Encoder


class CombinedEncoder[T, U, V]:
    def __init__(self, l: Encoder[T, U], r: Encoder[U, V]) -> None:
        self._l = l
        self._r = r

    def encode(self, obj: T) -> V:
        return self._r.encode(self._l.encode(obj))

    def decode(self, obj: V) -> T:
        return self._l.decode(self._r.decode(obj))
