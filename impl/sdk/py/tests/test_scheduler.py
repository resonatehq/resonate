from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest
from resonate_sdk_py.scheduler import Call, Invocation, Promise, Scheduler, Yieldable

if TYPE_CHECKING:
    from collections.abc import Generator


def bar(name: str) -> str:
    return f"Hi {name}"


def buzz(a: int, b: int) -> int:
    return a // b


def gen_buzz() -> Generator[Yieldable, Any, int]:
    p: Promise[int] = yield Invocation(buzz, a=15, b=1)
    v: int = yield p
    return v


def foo() -> Generator[Yieldable, Any, str]:
    p1: Promise[str] = yield Invocation(bar, name="tomas")
    p2: Promise[int] = yield Invocation(buzz, a=10, b=1)
    v1: str = yield p1
    v2: int = yield p2
    p3: Promise[int] = yield Invocation(gen_buzz)
    v3: int = yield p3
    v4: int = yield Call(buzz, a=10, b=1)
    v5: int = yield Call(gen_buzz)
    return f"{v1} {v2} {v3} {v4} {v5}"


def foo_promise() -> Generator[Yieldable, Any, Promise[str]]:
    p1: Promise[str] = yield Invocation(bar, name="tomas")
    return p1


def call_that_errors() -> Generator[Yieldable, Any, None]:
    yield Call(buzz, a=10, b=0)
    return


def invocation_that_errors() -> Generator[Yieldable, Any, None]:
    yield Call(buzz, a=10, b=0)
    return


async def test_exploration() -> None:
    scheduler = Scheduler(event_loop=asyncio.get_running_loop())
    scheduler.add(foo)
    assert scheduler.run() == "Hi tomas 10 15 10 15"
    await scheduler.close()


async def test_func_that_return_promise() -> None:
    scheduler = Scheduler(event_loop=asyncio.get_running_loop())
    scheduler.add(foo_promise)
    assert scheduler.run() == "Hi tomas"
    await scheduler.close()


async def test_function_with_errors() -> None:
    scheduler1 = Scheduler(event_loop=asyncio.get_running_loop())
    scheduler1.add(invocation_that_errors)
    with pytest.raises(ZeroDivisionError):
        scheduler1.run()

    scheduler2 = Scheduler(event_loop=asyncio.get_running_loop())
    scheduler2.add(call_that_errors)
    with pytest.raises(ZeroDivisionError):
        scheduler2.run()

    await scheduler1.close()
    await scheduler2.close()
