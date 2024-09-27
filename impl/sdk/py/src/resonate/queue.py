from __future__ import annotations

import contextlib
import heapq
import queue
from threading import Event, Thread
from typing import Generic, TypeVar, final

from resonate.time import now, ns_to_secs, secs_to_ns

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

    def dequeue_all(self) -> list[T]:
        return self.dequeue_batch(self.qsize())

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

    def get_nowait(self) -> T:
        return self._q.get_nowait()

    def task_done(self) -> None:
        self._q.task_done()

    def get(self, *, block: bool = True, timeout: float | None = None) -> T:
        return self._q.get(block, timeout)


@final
class DelayQueue(Generic[T]):
    def __init__(self, caller_event: Event | None, maxsize: int = 0) -> None:
        self._inq = Queue[tuple[T, float]](maxsize=maxsize)
        self._outq = Queue[T](maxsize=maxsize)
        self._delayed: list[tuple[float, int, T]] = []
        self._caller_event = caller_event
        self._continue_event = Event()

        self._worker_thread = Thread(target=self._run, daemon=True)
        self._worker_thread.start()

    def _next_release_time(self) -> float:
        return self._delayed[0][0]

    def _run(self) -> None:
        """Worker thread that processes the delayed queue."""
        while True:
            current_time = now()

            sqes = self._inq.dequeue_batch(self._inq.qsize())
            for idx, (item, delay) in enumerate(sqes):
                heapq.heappush(
                    self._delayed, (current_time + secs_to_ns(delay), idx, item)
                )

            # Release any items whose delay has expired
            while self._delayed and self._next_release_time() <= current_time:
                _, _, item = heapq.heappop(self._delayed)
                self._outq.put_nowait(item)  # Put the item in the consumer queue
                if self._caller_event:
                    self._caller_event.set()

            # Calculate the time to wait until the next item is ready

            wait_time: None | float = None
            if self._delayed:
                next_item_time = self._next_release_time()
                wait_time = max(0, next_item_time - current_time)

            self._continue_event.wait(
                ns_to_secs(wait_time) if wait_time is not None else wait_time
            )
            self._continue_event.clear()

    def dequeue_batch(self, batch_size: int) -> list[T]:
        return self._outq.dequeue_batch(batch_size)

    def dequeue(self, timeout: float | None = None) -> T:
        return self._outq.dequeue(timeout)

    def put_nowait(self, item: T, delay: float) -> None:
        self._inq.put_nowait((item, delay))
        self._continue_event.set()

    def items_in_delay(self) -> int:
        return len(self._delayed)

    def qsize(self) -> int:
        return self._outq.qsize()

    def dequeue_all(self) -> list[T]:
        return self._outq.dequeue_all()
