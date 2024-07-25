from __future__ import annotations

import asyncio
from dataclasses import dataclass
from functools import partial
from time import sleep
from typing import TypeVar

from resonate.processor import SQE, IAsyncCommand, ICommand, Processor
from resonate.result import Ok, Result

T = TypeVar("T")


@dataclass(frozen=True)
class AsyncGreetingCommand(IAsyncCommand[str]):
    name: str
    sleep_time: float

    async def run(self) -> Result[str, Exception]:
        await asyncio.sleep(self.sleep_time)
        return Ok(f"Hi {self.name}")


@dataclass(frozen=True)
class GreetingCommand(ICommand[str]):
    name: str
    sleep_time: float

    def run(self) -> Result[str, Exception]:
        sleep(self.sleep_time)
        return Ok(f"Hi {self.name}")


def _callback_that_asserts(expected: str, actual: Result[str, Exception]) -> None:
    assert expected == actual.unwrap()


def test_processor() -> None:
    p = Processor(max_workers=3)
    p.enqueue(
        SQE(
            GreetingCommand(name="A", sleep_time=0.2),
            partial(_callback_that_asserts, "Hi A"),
        )
    )
    p.enqueue(
        SQE(
            AsyncGreetingCommand(name="B", sleep_time=0.4),
            partial(_callback_that_asserts, "Hi B"),
        )
    )
    p.enqueue(
        SQE(
            GreetingCommand(name="C", sleep_time=0.5),
            partial(_callback_that_asserts, "Hi C"),
        )
    )
    cqe = p.dequeue()
    cqe.callback(cqe.cmd_result)
    cqe = p.dequeue()
    cqe.callback(cqe.cmd_result)
    cqe = p.dequeue()
    cqe.callback(cqe.cmd_result)

    p.enqueue(
        SQE(
            GreetingCommand(name="D", sleep_time=0.1),
            partial(_callback_that_asserts, "Hi D"),
        )
    )
    cqe = p.dequeue()
    cqe.callback(cqe.cmd_result)
