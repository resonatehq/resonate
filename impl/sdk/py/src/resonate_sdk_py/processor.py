from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class ICommand(ABC, Generic[T]):
    @abstractmethod
    async def run(self) -> T: ...


@dataclass(frozen=True)
class SQE(Generic[T]):
    cmd: ICommand[T]
    callback: Callable[[T], None]


@dataclass(frozen=True)
class CQE(Generic[T]):
    cmd_result: T
    callback: Callable[[T], None]
    processing_time: float


class Processor:
    def __init__(self, workers: int) -> None:
        self.submission_queue = asyncio.Queue[SQE[Any]]()
        self.completion_queue = asyncio.Queue[CQE[Any]]()
        self._tasks = [asyncio.create_task(self._do_work()) for _ in range(workers)]

    async def _do_work(self) -> None:
        while True:
            sqe = await self.submission_queue.get()
            start = perf_counter()
            cmd_result = await sqe.cmd.run()
            end = perf_counter()
            await self.completion_queue.put(
                CQE(
                    cmd_result=cmd_result,
                    callback=sqe.callback,
                    processing_time=end - start,
                )
            )
            self.submission_queue.task_done()

    def enqueue(self, sqe: SQE[Any]) -> None:
        self.submission_queue.put_nowait(sqe)

    async def dequeue(self) -> CQE[Any]:
        cqe = await self.completion_queue.get()
        self.completion_queue.task_done()
        return cqe

    async def close(self) -> None:
        assert (
            self.completion_queue.empty()
        ), "Completion queue must be empty before closing the processor"
        assert (
            self.completion_queue.empty()
        ), "Submission queue must be empty before closing the processor"
        await self.submission_queue.join()
        await self.completion_queue.join()
        for task in self._tasks:
            task.cancel()
