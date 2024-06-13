from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    import queue

T = TypeVar("T")


def dequeue(q: queue.Queue[T], timeout: float | None = None) -> T:
    qe = q.get(timeout=timeout)
    q.task_done()
    return qe
