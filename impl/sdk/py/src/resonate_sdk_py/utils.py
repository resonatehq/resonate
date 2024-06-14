from __future__ import annotations

import contextlib
import queue
from typing import TypeVar

T = TypeVar("T")


def dequeue_batch(q: queue.Queue[T], batch_size: int) -> list[T] | None:
    elements: list[T] = []
    with contextlib.suppress(queue.Empty):
        for _ in range(batch_size):
            e = q.get_nowait()
            elements.append(e)
            q.task_done()
    if len(elements) == 0:
        return None
    return elements


def dequeue(q: queue.Queue[T], timeout: float | None = None) -> T:
    qe = q.get(timeout=timeout)
    q.task_done()
    return qe
