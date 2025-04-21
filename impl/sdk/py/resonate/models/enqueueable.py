from __future__ import annotations

from typing import Protocol


class Enqueueable[T](Protocol):
    def enqueue(self, item: T, /) -> None: ...
