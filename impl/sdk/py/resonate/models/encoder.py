from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Encoder[I, O](Protocol):
    def encode(self, obj: I, /) -> O: ...
    def decode(self, obj: O, /) -> I: ...
