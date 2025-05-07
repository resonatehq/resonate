from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    def time(self) -> float: ...
    def strftime(self, format: str, /) -> str: ...
