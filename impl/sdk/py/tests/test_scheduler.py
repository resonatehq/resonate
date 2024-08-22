from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import pytest
from resonate import scheduler

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.promise import Promise
    from resonate.typing import Yieldable


def foo(ctx: Context, name: str, sleep_time: float) -> str:  # noqa: ARG001
    time.sleep(sleep_time)
    return name


def baz(ctx: Context, name: str, sleep_time: float) -> Generator[Yieldable, Any, str]:
    p = yield ctx.invoke(foo, name, sleep_time)
    return (yield p)


def bar(
    ctx: Context, name: str, sleep_time: float
) -> Generator[Yieldable, Any, Promise[str]]:
    p: Promise[str] = yield ctx.invoke(foo, name=name, sleep_time=sleep_time)
    return p


@pytest.mark.skip()
def test_coro_return_promise() -> None:
    s = scheduler.Scheduler(processor_threads=1)
    p: Promise[Promise[str]] = s.run("bar", bar, "A", 0.1)
    assert p.result(timeout=2) == "A"


def test_scheduler() -> None:
    p = scheduler.Scheduler()

    promise: Promise[str] = p.run("baz", baz, name="A", sleep_time=0.2)
    assert promise.result(timeout=4) == "A"

    promise = p.run("foo", foo, name="B", sleep_time=0.2)
    assert promise.result(timeout=4) == "B"


def test_multithreading_capabilities() -> None:
    s = scheduler.Scheduler(processor_threads=3)
    time_per_process: int = 5
    start = time.time()
    p1: Promise[str] = s.run("1", baz, name="A", sleep_time=time_per_process)
    p2: Promise[str] = s.run("2", baz, name="B", sleep_time=time_per_process)
    p3: Promise[str] = s.run("3", baz, name="C", sleep_time=time_per_process)

    assert p1.result() == "A"
    assert p2.result() == "B"
    assert p3.result() == "C"
    total_time = time.time() - start
    assert total_time == pytest.approx(
        time_per_process, rel=1e-1
    ), f"I should have taken about {time_per_process} seconds to process all coroutines"


def sleep_coroutine(
    ctx: Context, sleep_time: int, name: str
) -> Generator[Yieldable, Any, str]:
    yield ctx.sleep(sleep_time)
    return name


def test_sleep_on_coroutines() -> None:
    s = scheduler.Scheduler(processor_threads=1)
    start = time.time()
    sleep_time = 4
    p1: Promise[str] = s.run("1", sleep_coroutine, sleep_time, "A")
    p2: Promise[str] = s.run("2", sleep_coroutine, sleep_time, "B")
    p3: Promise[str] = s.run("3", sleep_coroutine, sleep_time, "C")
    assert p1.result() == "A"
    assert p2.result() == "B"
    assert p3.result() == "C"
    assert time.time() - start == pytest.approx(
        sleep_time, rel=1e-1
    ), f"I should have taken about {sleep_time} seconds to process all coroutines"
