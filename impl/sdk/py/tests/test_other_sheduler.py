from __future__ import annotations

from typing import TYPE_CHECKING, Any

from resonate_sdk_py.other_scheduler import Call, Invoke, Promise, Scheduler, Yieldable
from result import Ok

if TYPE_CHECKING:
    from collections.abc import Generator


def divide(a: int, b: int) -> int:
    return a // b


async def async_divide(a: int, b: int) -> int:
    return a // b


def foo(b: int) -> Generator[Yieldable, Any, int]:
    x: int = yield Call(divide, a=3, b=b)
    y: int = yield Call(async_divide, a=4, b=1)
    return x + y


def bar() -> Generator[Yieldable, Any, int]:
    x: Promise[int] = yield Invoke(divide, a=3, b=1)
    y: int = yield x
    return y


def bar_with_errors() -> Generator[Yieldable, Any, int]:
    x: Promise[int] = yield Invoke(divide, a=3, b=0)
    try:
        y: int = yield x
    except ZeroDivisionError:
        y = 3
    return y


def _abc(value: Promise[int]) -> Generator[Yieldable, int, int]:
    x = yield value
    return x


def whatever() -> Generator[Yieldable, Any, int]:
    af: Promise[int] = yield Invoke(divide, a=3, b=1)
    xf: Promise[int] = yield Invoke(_abc, value=af)
    yf: Promise[int] = yield Invoke(_abc, value=af)
    try:
        x: int = yield xf
    except Exception:  # noqa: BLE001
        x = 3
    y: int = yield yf
    return x + y


def foo_with_error_handling(b: int) -> Generator[Yieldable, Any, int]:
    try:
        x: int = yield Call(divide, a=3, b=b)
    except ZeroDivisionError:
        x = 3
    y: int = yield Call(async_divide, a=4, b=1)
    return x + y


def test_bar() -> None:
    s = Scheduler()
    bar_1 = s.add(bar)
    assert bar_1.result() == Ok(3)
    s.close()


# @pytest.mark.dev()
# def test_bar_with_errors() -> None:
#     s = Scheduler()
#     bar_1 = s.add(bar_with_errors)
#     assert bar_1.result() == Ok(3)


def test_scheduler_failing() -> None:
    s = Scheduler()
    foo_1 = s.add(fn=foo, b=0)
    assert isinstance(foo_1.result().err(), ZeroDivisionError)
    s.close()


def test_scheduler_with_error_handling() -> None:
    s = Scheduler()
    foo_1 = s.add(fn=foo_with_error_handling, b=0)
    assert foo_1.result() == Ok(7)
    s.close()


def test_scheduler() -> None:
    s = Scheduler()
    foo_1 = s.add(fn=foo, b=1)
    foo_2 = s.add(fn=foo, b=2)
    assert foo_1.result() == Ok(7)
    assert foo_2.result() == Ok(5)
    s.close()
