from __future__ import annotations

import random
from typing import TYPE_CHECKING, Any

import pytest
from resonate_sdk_py.context import Context
from resonate_sdk_py.scheduler.dst import DSTScheduler
from resonate_sdk_py.scheduler.shared import Call, Invoke, Promise, Yieldable
from typing_extensions import TypeVar

if TYPE_CHECKING:
    from collections.abc import Generator

T = TypeVar("T")


def _add_many(s: DSTScheduler, ctx: Context) -> list[Promise[Any]]:
    promises: list[Promise[Any]] = []
    promises.append(s.add(only_call(ctx)))
    promises.append(s.add(only_call(ctx)))
    promises.append(s.add(only_call(ctx)))
    promises.append(s.add(only_call(ctx)))
    promises.append(s.add(only_call(ctx)))
    promises.append(s.add(only_call(ctx)))
    promises.append(s.add(only_call(ctx)))
    promises.append(s.add(only_call(ctx)))
    promises.append(s.add(only_call(ctx)))
    promises.append(s.add(only_invocation(ctx)))
    return promises


def _number(ctx: Context, n: int) -> int:  # noqa: ARG001
    return n


def only_call(ctx: Context) -> Generator[Yieldable, Any, int]:
    x: int = yield Call(ctx, _number, n=1)
    return x


def only_invocation(ctx: Context) -> Generator[Yieldable, Any, int]:
    xp: Promise[int] = yield Invoke(ctx, _number, n=3)
    x: int = yield xp
    return x


def _promise_result(promises: list[Promise[T]]) -> list[T]:
    return [x.result() for x in promises]


@pytest.mark.dst()
def test_dst_scheduler() -> None:
    for _ in range(100):
        seed = random.randint(0, 1000000)  # noqa: S311
        s = DSTScheduler(seed=seed)
        ctx = Context()

        promises = _add_many(s, ctx)
        s.run()
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


@pytest.mark.dst()
def test_dst_determinitic() -> None:
    seed = random.randint(1, 100)  # noqa: S311
    s = DSTScheduler(seed=seed)
    ctx = Context()
    _add_many(s, ctx)
    s.run()
    expected_events = s.get_events()

    s = DSTScheduler(seed=seed)
    ctx = Context()
    _add_many(s, ctx)
    s.run()
    reproduced_events = s.get_events()

    assert expected_events == reproduced_events

    s = DSTScheduler(seed=seed + 10)
    ctx = Context()
    _add_many(s, ctx)
    s.run()
    other_events = s.get_events()
    assert expected_events != other_events
