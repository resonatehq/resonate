from __future__ import annotations

import os
import random
from typing import TYPE_CHECKING, Any

import pytest
import resonate
from resonate.contants import ENV_VARIABLE_PIN_SEED
from resonate.scheduler.dst import DSTScheduler
from resonate.testing import dst
from typing_extensions import TypeVar

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.scheduler.shared import Promise
    from resonate.typing import Yieldable

T = TypeVar("T")


def number(ctx: Context, n: int) -> int:  # noqa: ARG001
    return n


def only_call(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
    x: int = yield ctx.call(number, n=n)
    return x


def only_invocation(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
    xp: Promise[int] = yield ctx.invoke(number, n=n)
    x: int = yield xp
    return x


def failing_asserting(ctx: Context) -> Generator[Yieldable, Any, int]:
    x: int = yield ctx.call(only_invocation, n=3)
    ctx.assert_statement(x < 0, f"{x} should be negative")
    return x


def _promise_result(promises: list[Promise[T]]) -> list[T]:
    return [x.result() for x in promises]


def mocked_number() -> int:
    return 23


@pytest.mark.dst()
def test_pin_seed() -> None:
    s = dst(seeds=[1])[0]
    assert s.seed == 1

    os.environ[ENV_VARIABLE_PIN_SEED] = "32"
    s = dst(seeds=[1])[0]

    assert s.seed == int(os.environ.pop(ENV_VARIABLE_PIN_SEED))


@pytest.mark.dst()
def test_mock_function() -> None:
    s = DSTScheduler(seed=1)
    s.add(only_call, n=3)
    s.add(only_invocation, n=3)
    promises = s.run()
    assert all(p.result() == 3 for p in promises)  # noqa: PLR2004
    s = DSTScheduler(seed=1, mocks={number: mocked_number})
    promises = s.run()
    assert all(p.result() == 23 for p in promises)  # noqa: PLR2004


@pytest.mark.dst()
def test_dst_scheduler() -> None:
    for _ in range(100):
        seed = random.randint(0, 1000000)  # noqa: S311
        s = DSTScheduler(seed=seed)
        s.add(only_call, n=1)
        s.add(only_call, n=2)
        s.add(only_call, n=3)
        s.add(only_call, n=4)
        s.add(only_call, n=5)

        promises = s.run()
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
    s.add(only_call, n=1)
    s.add(only_call, n=2)
    s.add(only_call, n=3)
    s.add(only_call, n=4)
    s.add(only_call, n=5)
    promises = s.run()
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004
    expected_events = s.get_events()

    same_seed_s = DSTScheduler(seed=seed)
    same_seed_s.add(only_call, n=1)
    same_seed_s.add(only_call, n=2)
    same_seed_s.add(only_call, n=3)
    same_seed_s.add(only_call, n=4)
    same_seed_s.add(only_call, n=5)
    promises = same_seed_s.run()
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004
    assert expected_events == same_seed_s.get_events()

    different_seed_s = DSTScheduler(seed=seed + 10)
    different_seed_s.add(only_call, n=1)
    different_seed_s.add(only_call, n=2)
    different_seed_s.add(only_call, n=3)
    different_seed_s.add(only_call, n=4)
    different_seed_s.add(only_call, n=5)
    promises = different_seed_s.run()
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004
    assert expected_events != different_seed_s.get_events()


@pytest.mark.dst()
def test_failing_asserting() -> None:
    s = DSTScheduler(seed=1)
    s.add(failing_asserting)
    p = s.run()
    with pytest.raises(AssertionError):
        p[0].result()


@pytest.mark.dst()
@pytest.mark.parametrize("scheduler", resonate.testing.dst([range(10)]))
def test_dst_framework(scheduler: DSTScheduler) -> None:
    scheduler.add(only_call, n=1)
    scheduler.add(only_call, n=2)
    scheduler.add(only_call, n=3)
    scheduler.add(only_call, n=4)
    scheduler.add(only_call, n=5)
    promises = scheduler.run()
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004


@pytest.mark.dst()
def test_failure() -> None:
    scheduler = DSTScheduler(seed=1, max_failures=2, failure_chance=100)
    scheduler.add(only_call, n=1)
    p = scheduler.run()
    assert p[0].done()
    assert p[0].result() == 1
    assert scheduler.tick == 5  # noqa: PLR2004
    assert scheduler.current_failures == 2  # noqa: PLR2004

    scheduler = DSTScheduler(seed=1, max_failures=2, failure_chance=0)
    scheduler.add(only_call, n=1)
    p = scheduler.run()
    assert p[0].done()
    assert p[0].result() == 1
    assert scheduler.tick == 5  # noqa: PLR2004
    assert scheduler.current_failures == 0


@pytest.mark.dst()
def test_sequential() -> None:
    seq_scheduler = DSTScheduler(seed=1, mode="sequential")
    seq_scheduler.add(only_call, n=1)
    seq_scheduler.add(only_call, n=2)
    seq_scheduler.add(only_call, n=3)
    seq_scheduler.add(only_call, n=4)
    seq_scheduler.add(only_call, n=5)
    promises = seq_scheduler.run()
    assert [p.result() for p in promises] == [1, 2, 3, 4, 5]
    assert seq_scheduler.get_events() == [
        "Call number with params args=() kwargs={'n': 5} handled",
        "Promise resolved with value Ok(5)",
        "Call number with params args=() kwargs={'n': 4} handled",
        "Promise resolved with value Ok(4)",
        "Call number with params args=() kwargs={'n': 3} handled",
        "Promise resolved with value Ok(3)",
        "Call number with params args=() kwargs={'n': 2} handled",
        "Promise resolved with value Ok(2)",
        "Call number with params args=() kwargs={'n': 1} handled",
        "Promise resolved with value Ok(1)",
    ]

    con_scheduler = DSTScheduler(seed=1, mode="concurrent")
    con_scheduler.add(only_call, n=1)
    con_scheduler.add(only_call, n=2)
    con_scheduler.add(only_call, n=3)
    con_scheduler.add(only_call, n=4)
    con_scheduler.add(only_call, n=5)
    promises = con_scheduler.run()
    assert [p.result() for p in promises] == [1, 2, 3, 4, 5]
    assert con_scheduler.get_events() == [
        "Call number with params args=() kwargs={'n': 4} handled",
        "Call number with params args=() kwargs={'n': 5} handled",
        "Call number with params args=() kwargs={'n': 2} handled",
        "Call number with params args=() kwargs={'n': 1} handled",
        "Promise resolved with value Ok(4)",
        "Call number with params args=() kwargs={'n': 3} handled",
        "Promise resolved with value Ok(2)",
        "Promise resolved with value Ok(1)",
        "Promise resolved with value Ok(3)",
        "Promise resolved with value Ok(5)",
    ]
