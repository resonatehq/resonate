from __future__ import annotations

import os
from dataclasses import dataclass
from threading import Thread
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, final

from resonate.result import Err, Ok

if TYPE_CHECKING:
    from resonate.queue import Queue
    from resonate.result import Result
    from resonate.scheduler.scheduler import Scheduler

T = TypeVar("T")


@final
@dataclass(frozen=True)
class SQE(Generic[T]):
    thunk: Callable[[], T]
    callback: Callable[[Result[T, Exception]], None]


@final
@dataclass(frozen=True)
class CQE(Generic[T]):
    thunk_result: Result[T, Exception]
    callback: Callable[[Result[T, Exception]], None]


class Processor:
    def __init__(
        self,
        max_workers: int | None,
        sq: Queue[SQE[Any]],
        scheduler: Scheduler,
    ) -> None:
        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        assert max_workers > 0, "`max_workers` must be positive."
        self._max_workers = max_workers
        self._sq = sq
        self._scheduler = scheduler
        self._threads = set[Thread]()

    def enqueue(self, sqe: SQE[Any]) -> None:
        self._sq.put_nowait(sqe)
        self._adjust_thread_count()

    def _run(self) -> None:
        while True:
            sqe = self._sq.dequeue()
            result: Result[Any, Exception]
            try:
                result = Ok(sqe.thunk())
            except Exception as e:  # noqa: BLE001
                result = Err(e)
            self._scheduler.add_cqe(
                CQE[Any](thunk_result=result, callback=sqe.callback)
            )

    def _adjust_thread_count(self) -> None:
        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            t = Thread(target=self._run, daemon=True)
            t.start()
            self._threads.add(t)
