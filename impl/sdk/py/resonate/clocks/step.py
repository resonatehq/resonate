from __future__ import annotations

import time


class StepClock:
    def __init__(self) -> None:
        self._time = 0.0

    def step(self, time: float) -> None:
        assert time >= self._time, "The arrow of time only flows forward."
        self._time = time

    def time(self) -> float:
        """Return the current time in seconds."""
        return self._time

    def strftime(self, format: str, /) -> str:
        return time.strftime(format, time.gmtime(self._time))
