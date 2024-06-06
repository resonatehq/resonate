from __future__ import annotations

import asyncio
from dataclasses import dataclass
from functools import partial
from typing import TypeVar

from resonate_sdk_py.processor import CQE, SQE, ICommand, Processor

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
    processor = Processor(workers=1)

    cmds = [
        GreetingCommand(name="A", sleep_time=2),
        GreetingCommand(name="C", sleep_time=5),
        GreetingCommand(name="B", sleep_time=0.2),
    ]
    await processor.enqueue(
        sqes=[
            SQE(
                cmd,
                partial(_callback_that_asserts, actual=f"Hi {cmd.name}"),
            )
            for cmd in cmds
        ]
    )
    batch_size: int = 3
    all_cqes: list[CQE] = []
    cqes = await processor.dequeue(
        batch_size=batch_size,
        timeout=2.5,
    )
    expected_items: int = 1
    assert len(cqes) == expected_items
    all_cqes.extend(cqes)

    cqes = await processor.dequeue(batch_size=batch_size, timeout=10)

    assert len(cqes) == 2  # noqa: PLR2004

    all_cqes.extend(cqes)
    for cqe in all_cqes:
        cqe.callback(cqe.cmd_result)
    await processor.close()
