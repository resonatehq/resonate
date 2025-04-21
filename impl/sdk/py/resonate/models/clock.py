from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def time(self) -> int: ...


class WallClock:
    def time(self) -> int:
        return int(time.time() * 1000)
