from __future__ import annotations

import heapq


class DelayQ[T]:
    def __init__(self) -> None:
        self._delayed: list[tuple[float, T]] = []

    def add(self, item: T, delay: float) -> None:
        heapq.heappush(self._delayed, (delay, item))

    def get(self, time: float) -> tuple[list[T], float]:
        items: list[T] = []
        while self._delayed and self._delayed[0][0] <= time:
            _, item = heapq.heappop(self._delayed)
            items.append(item)

        next_time = self._delayed[0][0] if self._delayed else 0
        return items, next_time

    def empty(self) -> bool:
        return bool(self._delayed)
