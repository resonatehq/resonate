from __future__ import annotations

from typing import Protocol


class Clock(Protocol):
    def time(self) -> int: ...
