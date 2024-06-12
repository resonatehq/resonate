from __future__ import annotations

import asyncio
import os
import queue
from abc import ABC, abstractmethod
from asyncio import iscoroutinefunction
from collections.abc import Coroutine
from dataclasses import dataclass
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar

from result import Err, Ok

if TYPE_CHECKING:
    from result import Result

T = TypeVar("T")


class IAsyncCommand(ABC, Generic[T]):
    @abstractmethod
    async def run(self) -> Result[T, Exception]: ...


class ICommand(ABC, Generic[T]):
    @abstractmethod
    def run(self) -> Result[T, Exception]: ...


@dataclass(frozen=True)
class SQE(Generic[T]):
    cmd: ICommand[T] | IAsyncCommand[T]
    callback: Callable[[T], None]


@dataclass(frozen=True)
class CQE(Generic[T]):
    cmd_result: Result[T, Exception]
    callback: Callable[[Result[T, Exception]], None]


def _worker(
    kill_threads: Event, sq: queue.Queue[SQE[Any]], cq: queue.Queue[CQE[Any]]
) -> None:
    loop = asyncio.new_event_loop()
    while not kill_threads.is_set():
        try:
            sqe = sq.get(timeout=0.1)
        except queue.Empty:
            continue

        if iscoroutinefunction(sqe.cmd.run):
            cmd_result = loop.run_until_complete(sqe.cmd.run())
        else:
            cmd_result = sqe.cmd.run()
            assert not isinstance(
                cmd_result, Coroutine
            ), "cmd result cannot be a Coroutine at this point."

        assert isinstance(
            cmd_result, (Ok, Err)
        ), "Command result must be a Result variant."
        cq.put(
            CQE(
                cmd_result=cmd_result,
                callback=sqe.callback,
            )
        )
    loop.close()


class Processor:
    def __init__(self, max_workers: int | None) -> None:
        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        assert max_workers > 0, "max_workers must be greater than 0"

        self._max_workers = max_workers
        self._submission_queue = queue.Queue[SQE[Any]]()
        self._completion_queue = queue.Queue[CQE[Any]]()
        self._threads = set[Thread]()
        self._kill_threads = Event()

    def enqueue(self, sqe: SQE[Any]) -> None:
        self._submission_queue.put(sqe)
        self._adjust_thread_count()

    def _adjust_thread_count(self) -> None:
        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            t = Thread(
                target=_worker,
                args=(
                    self._kill_threads,
                    self._submission_queue,
                    self._completion_queue,
                ),
                daemon=True,
            )
            t.start()
            self._threads.add(t)

    def dequeue(self) -> CQE[Any]:
        return self._completion_queue.get()

    def cq_qsize(self) -> int:
        return self._completion_queue.qsize()

    def close(self) -> None:
        self._kill_threads.set()
        for t in self._threads:
            t.join()
            assert not t.is_alive(), "Thread should be dead."
