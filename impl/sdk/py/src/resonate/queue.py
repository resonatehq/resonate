from __future__ import annotations

import contextlib
import queue
from typing import Generic, TypeVar, final

T = TypeVar("T")


@final
class Queue(Generic[T]):
    def __init__(self, maxsize: int = 0) -> None:
        self._q = queue.Queue[T](maxsize=maxsize)

    def dequeue_batch(self, batch_size: int) -> list[T]:
        elements: list[T] = []
        with contextlib.suppress(queue.Empty):
            for _ in range(batch_size):
                elements.append(self._q.get_nowait())
                self._q.task_done()
        return elements

    def dequeue(self, timeout: float | None = None) -> T:
        qe = self._q.get(timeout=timeout)
        self._q.task_done()
        return qe

    def put_nowait(self, item: T) -> None:
        self._q.put_nowait(item)

    def put(self, item: T, *, block: bool = True, timeout: float | None = None) -> None:
        self._q.put(item, block, timeout)

    def qsize(self) -> int:
        return self._q.qsize()
