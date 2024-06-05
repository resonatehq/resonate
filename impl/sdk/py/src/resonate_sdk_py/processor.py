from __future__ import annotations

import asyncio
import contextlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from time import perf_counter
from typing import Callable, Generic, TypeVar

T = TypeVar("T")


class ICommand(ABC, Generic[T]):
    @abstractmethod
    def run(self) -> T: ...


@dataclass(frozen=True)
class SQE(Generic[T]):
    cmd: ICommand[T]
    callback: Callable[[T], None]


@dataclass(frozen=True)
class CQE(Generic[T]):
    cmd_result: T
    callback: Callable[[T], None]


class Processor:
    def __init__(self, workers: int) -> None:
        self.submission_queue = asyncio.Queue[SQE]()
        self.completion_queue = asyncio.Queue[CQE]()
        self._tasks = [asyncio.create_task(self._do_work()) for _ in range(workers)]

    async def _do_work(self) -> None:
        while True:
            sqe = await self.submission_queue.get()
            cmd_result = await sqe.cmd.run()
            await self.completion_queue.put(
                CQE(cmd_result=cmd_result, callback=sqe.callback)
            )
            self.submission_queue.task_done()

    async def enqueue(self, sqes: list[SQE]) -> None:
        for sqe in sqes:
            await self.submission_queue.put(sqe)

    async def dequeue(self, batch_size: int, timeout: float) -> list[CQE]:
        items: list[CQE] = []
        with contextlib.suppress(asyncio.TimeoutError):
            for _ in range(batch_size):
                cqe = await asyncio.wait_for(
                    self.completion_queue.get(), timeout=timeout
                )
                self.completion_queue.task_done()
                items.append(cqe)
        return items

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


async def do_work(
    sqe: asyncio.Queue[ICommand], cqe: asyncio.Queue[tuple[T, float]]
) -> None:
    while True:
        cmd = await sqe.get()
        start = perf_counter()
        cmd_result = await cmd.run()
        end = perf_counter()
        await cqe.put((cmd_result, end - start))
        sqe.task_done()


async def handle_result(
    cqe: asyncio.Queue[tuple[T, float]], callback: Callable[[tuple[T, float]], None]
) -> None:
    while True:
        result = await cqe.get()
        callback(result)
        cqe.task_done()


async def produce_work(
    cmds: list[ICommand],
    sqe: asyncio.Queue[ICommand],
    producer_completed: asyncio.Event,
) -> None:
    for cmd in cmds:
        await sqe.put(cmd)
    producer_completed.set()


async def _controller(  # noqa: PLR0913
    cmds: list[ICommand],
    num_workers: int,
    num_result_handlers: int,
    task_completed_callback: Callable[[tuple[T, float]], None],
    sqe_size: int = 0,
    cqe_size: int = 0,
) -> None:
    start = perf_counter()
    sqe_queue = asyncio.Queue[ICommand](sqe_size)
    cqe_queue = asyncio.Queue(cqe_size)

    producer_completed = asyncio.Event()
    producer_completed.clear()

    tasks = [
        asyncio.create_task(
            produce_work(
                cmds=cmds, sqe=sqe_queue, producer_completed=producer_completed
            )
        )
    ]

    tasks.extend(
        asyncio.create_task(do_work(sqe=sqe_queue, cqe=cqe_queue))
        for _ in range(num_workers)
    )
    tasks.extend(
        asyncio.create_task(
            handle_result(cqe=cqe_queue, callback=task_completed_callback)
        )
        for _ in range(num_result_handlers)
    )

    await producer_completed.wait()
    await sqe_queue.join()
    await cqe_queue.join()

    for task in tasks:
        task.cancel()

    end = perf_counter()
    print("In total it took {}", end - start)  # noqa: T201


def run(
    cmds: list[ICommand],
    task_completed_callback: Callable[[tuple[T, float]], None],
) -> None:
    asyncio.run(
        _controller(
            cmds=cmds,
            num_workers=5,
            num_result_handlers=5,
            task_completed_callback=task_completed_callback,
        )
    )
