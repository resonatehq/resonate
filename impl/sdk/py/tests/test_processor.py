from __future__ import annotations

import asyncio
from dataclasses import dataclass
from functools import partial
from time import perf_counter, sleep
from typing import TypeVar

from resonate_sdk_py.processor import SQE, IAsyncCommand, ICommand, Processor

T = TypeVar("T")


@dataclass(frozen=True)
class AsyncGreetingCommand(IAsyncCommand[str]):
    name: str
    sleep_time: float

    async def run(self) -> str:
        await asyncio.sleep(self.sleep_time)
        return f"Hi {self.name}"


@dataclass(frozen=True)
class GreetingCommand(ICommand[str]):
    name: str
    sleep_time: float

    def run(self) -> str:
        sleep(self.sleep_time)
        return f"Hi {self.name}"


def _callback_that_asserts(actual: str, expected: str) -> None:
    assert expected == actual


def test_processor() -> None:
    processor = Processor(max_workers=3)

    start = perf_counter()
    processor.enqueue(
        SQE(
            GreetingCommand(name="A", sleep_time=2),
            partial(_callback_that_asserts, "Hi A"),
        )
    )
    processor.enqueue(
        SQE(
            AsyncGreetingCommand(name="B", sleep_time=0.4),
            partial(_callback_that_asserts, "Hi B"),
        )
    )
    processor.enqueue(
        SQE(
            GreetingCommand(name="C", sleep_time=5),
            partial(_callback_that_asserts, "Hi C"),
        )
    )
    cqe = processor.dequeue()
    cqe.callback(cqe.cmd_result)
    cqe = processor.dequeue()
    cqe.callback(cqe.cmd_result)
    cqe = processor.dequeue()
    cqe.callback(cqe.cmd_result)

    processor.enqueue(
        SQE(
            GreetingCommand(name="D", sleep_time=0.1),
            partial(_callback_that_asserts, "Hi D"),
        )
    )
    cqe = processor.dequeue()
    cqe.callback(cqe.cmd_result)

    end = perf_counter()

    print(end - start)  # noqa: T201
