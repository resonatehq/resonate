from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resonate.models.encoder import Encoder


class ChainEncoder[I, O, R]:
    def __init__(self, first: Encoder[I, O], last: Encoder[O, R]) -> None:
        self._first = first
        self._last = last

    def encode(self, obj: I) -> R:
        return self._last.encode(self._first.encode(obj))

    def decode(self, obj: R) -> I:
        return self._first.decode(self._last.decode(obj))
