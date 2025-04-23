from __future__ import annotations


class StepClock:
    def __init__(self) -> None:
        self._time = 0

    def time(self) -> int:
        return self._time

    def set_time(self, time: int) -> None:
        assert time >= self._time, "The arrow of time only flows forward."
        self._time = time
