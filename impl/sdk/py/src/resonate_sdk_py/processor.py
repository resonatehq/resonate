from __future__ import annotations

import asyncio
import os
import queue
from abc import ABC, abstractmethod
from asyncio import iscoroutinefunction
from dataclasses import dataclass
from threading import Thread
from time import perf_counter
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class IAsyncCommand(ABC, Generic[T]):
    @abstractmethod
    async def run(self) -> T: ...


class ICommand(ABC, Generic[T]):
    @abstractmethod
    def run(self) -> T: ...


@dataclass(frozen=True)
class SQE(Generic[T]):
    cmd: ICommand[T] | IAsyncCommand[T]
    callback: Callable[[T], None]


@dataclass(frozen=True)
class CQE(Generic[T]):
    cmd_result: T
    callback: Callable[[T], None]
    processing_time: float


def _worker(sq: queue.Queue[SQE[Any]], cq: queue.Queue[CQE[Any]]) -> CQE[Any]:
    loop = asyncio.new_event_loop()
    while True:
        sqe = sq.get()
        start = perf_counter()
        if iscoroutinefunction(sqe.cmd.run):
            cmd_result = loop.run_until_complete(sqe.cmd.run())
        else:
            cmd_result = sqe.cmd.run()
        end = perf_counter()
        cq.put(
            CQE(
                cmd_result=cmd_result,
                callback=sqe.callback,
                processing_time=end - start,
            )
        )


class Processor:
    def __init__(self, max_workers: int | None) -> None:
        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        assert max_workers > 0, "max_workers must be greater than 0"

        self._max_workers = max_workers
        self._submission_queue = queue.Queue[SQE[Any]]()
        self._completion_queue = queue.Queue[CQE[Any]]()
        self._threads = set[Thread]()

    def enqueue(self, sqe: SQE[Any]) -> None:
        self._submission_queue.put(sqe)
        self._adjust_thread_count()

    def _adjust_thread_count(self) -> None:
        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            t = Thread(
                target=_worker,
                args=(
                    self._submission_queue,
                    self._completion_queue,
                ),
                daemon=True,
            )
            t.start()
            self._threads.add(t)

    def dequeue(self) -> CQE[Any]:
        return self._completion_queue.get()

    def close(self) -> None:
        for t in self._threads:
            t.join()
