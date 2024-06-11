from __future__ import annotations

from typing import TYPE_CHECKING, Any

from resonate_sdk_py.other_scheduler import Call, Invoke, Scheduler, Yieldable
from result import Ok

if TYPE_CHECKING:
    from collections.abc import Generator
    from concurrent.futures import Future


def divide(a: int, b: int) -> int:
    return a // b


async def async_divide(a: int, b: int) -> int:
    return a // b


def foo(b: int) -> Generator[Yieldable, Any, int]:
    x: int = yield Call(divide, a=3, b=b)
    y: int = yield Call(async_divide, a=4, b=1)
    return x + y


def bar() -> Generator[Yieldable, Any, int]:
    x: Future[int] = yield Invoke(divide, a=3, b=1)
    return x.result()


def foo_with_error_handling(b: int) -> Generator[Yieldable, Any, int]:
    try:
        x: int = yield Call(divide, a=3, b=b)
    except ZeroDivisionError:
        x = 3
    y: int = yield Call(async_divide, a=4, b=1)
    return x + y


def test_scheduler_failing() -> None:
    s = Scheduler()
    foo_1 = s.add(fn=foo, b=0)
    assert isinstance(foo_1.result().err(), ZeroDivisionError)


def test_scheduler_with_error_handling() -> None:
    s = Scheduler()
    foo_1 = s.add(fn=foo_with_error_handling, b=0)
    assert foo_1.result() == Ok(7)


def test_scheduler() -> None:
    s = Scheduler()
    foo_1 = s.add(fn=foo, b=1)
    foo_2 = s.add(fn=foo, b=2)
    assert foo_1.result() == Ok(7)
    assert foo_2.result() == Ok(5)
