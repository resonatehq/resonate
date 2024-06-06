from __future__ import annotations

import asyncio
from dataclasses import dataclass
from functools import partial
from typing import TypeVar

from resonate_sdk_py.processor import SQE, ICommand, Processor

T = TypeVar("T")


@dataclass(frozen=True)
class GreetingCommand(ICommand[str]):
    name: str
    sleep_time: float

    async def run(self) -> str:
        greeting_msg = f"Hi {self.name}"
        await asyncio.sleep(self.sleep_time)
        return greeting_msg


def _callback_that_asserts(expected: str, actual: str) -> None:
    assert expected == actual


async def test_processor() -> None:
    # Pass event-loop as param
    processor = Processor(workers=1)

    cmds = [
        GreetingCommand(name="A", sleep_time=2),
        GreetingCommand(name="C", sleep_time=5),
        GreetingCommand(name="B", sleep_time=0.2),
    ]
    for cmd in cmds:
        processor.enqueue(
            sqe=SQE(
                cmd=cmd,
                callback=partial(_callback_that_asserts, actual=f"Hi {cmd.name}"),
            )
        )

    for _ in range(len(cmds)):
        cqe = await processor.dequeue()
        cqe.callback(cqe.cmd_result)

    await processor.close()
