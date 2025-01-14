from __future__ import annotations

import contextlib
import heapq
import time
from queue import Empty, Queue
from threading import Thread
from typing import Optional, TypeVar, final

from resonate import utils
from resonate.cmd_queue import CommandQ, Invoke
from resonate.traits import Subsystem

T = TypeVar("T")


def _ns_to_secs(ns: float) -> float:
    return ns / 1e9


def _secs_to_ns(secs: float) -> float:
    return secs * 1e9


@final
class DelayQueue(Subsystem):
    def __init__(self) -> None:
        self._inq = Queue[Optional[tuple[Invoke, float]]]()
        self._delayed: list[tuple[float, Invoke]] = []
        self._worker_thread: Thread | None = None

    def start(self, cmd_queue: CommandQ, pid: str) -> None:
        assert self._worker_thread is None, "Already been started."
        self._worker_thread = Thread(target=self._run, args=(cmd_queue,), daemon=True)
        self._worker_thread.start()

    def stop(self) -> None:
        assert self._worker_thread is not None, "Never started."
        self._inq.put(None)
        self._worker_thread.join()

    def _next_release_time(self) -> float:
        return self._delayed[0][0]

    @utils.exit_on_exception
    def _run(self, cmd_queue: CommandQ) -> None:
        """Worker thread that processes the delayed queue."""
        wait_time: float | None = None
        while True:
            current_time = time.time_ns()

            with contextlib.suppress(Empty):
                item_to_delay = self._inq.get(timeout=wait_time)
                if item_to_delay is None:
                    break
                item, delay = item_to_delay
                heapq.heappush(self._delayed, (current_time + _secs_to_ns(delay), item))
                self._inq.task_done()

            # Release any items whose delay has expired
            while self._delayed and self._next_release_time() <= current_time:
                _, item = heapq.heappop(self._delayed)
                cmd_queue.put(item)

            # Calculate the time to wait until the next item is ready
            if self._delayed:
                next_item_time = self._next_release_time()
                wait_time = _ns_to_secs(max(0, next_item_time - current_time))
            else:
                wait_time = None

    def enqueue(self, item: Invoke, delay: float) -> None:
        self._inq.put((item, delay))
