from __future__ import annotations

import asyncio
import os
import queue
from abc import ABC, abstractmethod
from asyncio import iscoroutinefunction
from collections.abc import Coroutine
from dataclasses import dataclass
from threading import Thread
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar

from resonate import utils
from resonate.logging import logger
from resonate.result import Err, Ok

if TYPE_CHECKING:
    from resonate.result import Result
    from resonate.scheduler import Scheduler

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


class Processor:
    def __init__(
        self, max_workers: int | None, scheduler: Scheduler | None = None
    ) -> None:
        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        assert max_workers > 0, "max_workers must be greater than 0"
        logger.debug("Processor setup with %s max workers", max_workers)
        self._max_workers = max_workers
        self._submission_queue = queue.Queue[SQE[Any]]()
        self._completion_queue = queue.Queue[CQE[Any]]()
        self._threads = set[Thread]()
        self._scheduler = scheduler

    def enqueue(self, sqe: SQE[Any]) -> None:
        self._submission_queue.put(sqe)
        self._adjust_thread_count()

    def _run(
        self,
    ) -> None:
        logger.debug("Processor starting")
        loop = asyncio.new_event_loop()
        while True:
            sqe = utils.dequeue(q=self._submission_queue)

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
            self._completion_queue.put(
                CQE(
                    cmd_result=cmd_result,
                    callback=sqe.callback,
                )
            )
            if self._scheduler is not None:
                self._scheduler.signal()

    def _adjust_thread_count(self) -> None:
        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            t = Thread(
                target=self._run,
                daemon=True,
            )
            t.start()
            self._threads.add(t)

    def dequeue(self) -> CQE[Any]:
        return utils.dequeue(q=self._completion_queue)

    def dequeue_batch(self, batch_size: int) -> list[CQE[Any]] | None:
        return utils.dequeue_batch(q=self._completion_queue, batch_size=batch_size)

    def cq_qsize(self) -> int:
        return self._completion_queue.qsize()
