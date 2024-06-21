from __future__ import annotations

import random
from typing import TYPE_CHECKING, Any

import pytest
from resonate_sdk_py.scheduler.dst import DSTScheduler
from resonate_sdk_py.scheduler.shared import Call, Invoke, Promise, Yieldable
from typing_extensions import TypeVar

if TYPE_CHECKING:
    from collections.abc import Generator

T = TypeVar("T")


def _number(n: int) -> int:
    return n


def only_call() -> Generator[Yieldable, Any, int]:
    x: int = yield Call(_number, n=1)
    return x


def only_invocation() -> Generator[Yieldable, Any, int]:
    xp: Promise[int] = yield Invoke(_number, n=3)
    x: int = yield xp
    return x


def _promise_result(promises: list[Promise[T]]) -> list[T]:
    return [x.result() for x in promises]


@pytest.mark.dst()
def test_dst_scheduler() -> None:
    for _ in range(100):
        seed = random.randint(0, 1000000)  # noqa: S311
        s = DSTScheduler(seed=seed)

        promises = s.add(
            coros=[
                only_call(),  # 3
                only_call(),  # 3
                only_call(),
                only_call(),
                only_call(),
                only_call(),
                only_call(),
                only_call(),
                only_call(),
                only_invocation(),  # 4
            ]
        )
        values = _promise_result(promises=promises)
        assert values == [
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            3,
        ], f"Test fails when seed it {seed}"
