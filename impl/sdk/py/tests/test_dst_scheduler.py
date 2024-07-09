from __future__ import annotations

import random
from functools import partial
from typing import TYPE_CHECKING, Any

import pytest
import resonate
from resonate.scheduler.dst import DSTScheduler
from typing_extensions import TypeVar

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.scheduler.shared import Promise
    from resonate.typing import Yieldable

T = TypeVar("T")


def _number(ctx: Context, n: int) -> int:  # noqa: ARG001
    return n


def only_call(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
    x: int = yield ctx.call(_number, n=n)
    return x


def only_invocation(ctx: Context) -> Generator[Yieldable, Any, int]:
    xp: Promise[int] = yield ctx.invoke(_number, n=3)
    x: int = yield xp
    return x


def failing_asserting(ctx: Context) -> Generator[Yieldable, Any, int]:
    x: int = yield ctx.call(only_invocation)
    ctx.assert_statement(x < 0, f"{x} should be negative")
    return x


def _promise_result(promises: list[Promise[T]]) -> list[T]:
    return [x.result() for x in promises]


@pytest.mark.dst()
def test_dst_scheduler() -> None:
    for _ in range(100):
        seed = random.randint(0, 1000000)  # noqa: S311
        s = DSTScheduler(seed=seed)

        promises = s.run(
            [
                partial(only_call, n=1),
                partial(only_call, n=2),
                partial(only_call, n=3),
                partial(only_call, n=4),
                partial(only_call, n=5),
            ]
        )
        values = _promise_result(promises=promises)
        assert values == [
            1,
            2,
            3,
            4,
            5,
        ], f"Test fails when seed it {seed}"


@pytest.mark.dst()
def test_dst_determinitic() -> None:
    seed = random.randint(1, 100)  # noqa: S311
    s = DSTScheduler(seed=seed)
    promises = s.run(
        [
            partial(only_call, n=1),
            partial(only_call, n=2),
            partial(only_call, n=3),
            partial(only_call, n=4),
            partial(only_call, n=5),
        ]
    )
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004
    expected_events = s.get_events()

    same_seed_s = DSTScheduler(seed=seed)
    promises = same_seed_s.run(
        [
            partial(only_call, n=1),
            partial(only_call, n=2),
            partial(only_call, n=3),
            partial(only_call, n=4),
            partial(only_call, n=5),
        ]
    )
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004
    assert expected_events == same_seed_s.get_events()

    different_seed_s = DSTScheduler(seed=seed + 10)
    promises = different_seed_s.run(
        [
            partial(only_call, n=1),
            partial(only_call, n=2),
            partial(only_call, n=3),
            partial(only_call, n=4),
            partial(only_call, n=5),
        ]
    )
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004
    assert expected_events != different_seed_s.get_events()


@pytest.mark.dst()
def test_failing_asserting() -> None:
    s = DSTScheduler(seed=1)
    p = s.run([partial(failing_asserting)])
    with pytest.raises(AssertionError):
        p[0].result()


@pytest.mark.dst()
@pytest.mark.parametrize("scheduler", resonate.testing.dst([range(10)]))
def test_dst_framework(scheduler: DSTScheduler) -> None:
    promises = scheduler.run(
        [
            partial(only_call, n=1),
            partial(only_call, n=2),
            partial(only_call, n=3),
            partial(only_call, n=4),
            partial(only_call, n=5),
        ]
    )
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004
