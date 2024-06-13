from __future__ import annotations

from typing import TYPE_CHECKING, Any

from resonate_sdk_py.scheduler import Call, Invoke, Promise, Scheduler, Yieldable

if TYPE_CHECKING:
    from collections.abc import Generator


def _divide(a: int, b: int) -> int:
    return a // b


def only_call() -> Generator[Yieldable, Any, int]:
    x: int = yield Call(_divide, a=3, b=1)
    return x


def call_with_errors() -> Generator[Yieldable, Any, int]:
    x: int
    try:
        x = yield Call(_divide, a=100, b=0)
    except ZeroDivisionError:
        x = 3
    return x


def double_call() -> Generator[Yieldable, Any, int]:
    x: int = yield Call(_divide, a=3, b=1)
    y: int = yield Call(_divide, a=5, b=1)
    return x + y


def test_calls() -> None:
    s = Scheduler()
    p = s.add(only_call)
    assert p.result(timeout=30) == 3  # noqa: PLR2004
    p = s.add(call_with_errors)
    assert p.result(timeout=30) == 3  # noqa: PLR2004
    p = s.add(double_call)
    assert p.result(timeout=30) == 8  # noqa: PLR2004
    s.close()


def only_invocation() -> Generator[Yieldable, Any, int]:
    xp: Promise[int] = yield Invoke(_divide, a=3, b=1)
    x: int = yield xp
    return x


def invocation_with_error() -> Generator[Yieldable, Any, int]:
    xp: Promise[int] = yield Invoke(_divide, a=3, b=0)
    try:
        x: int = yield xp
    except ZeroDivisionError:
        x = 4
    return x


def test_invocation() -> None:
    s = Scheduler()
    p = s.add(only_invocation)
    assert p.result(timeout=30) == 3

    s.close()


def test_invocation_with_error() -> None:
    s = Scheduler()
    p = s.add(invocation_with_error)
    assert p.result(timeout=30) == 4
    s.close()
