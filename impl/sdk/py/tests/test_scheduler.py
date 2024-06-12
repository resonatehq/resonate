from __future__ import annotations

from typing import TYPE_CHECKING, Any

from resonate_sdk_py.scheduler import Call, Invoke, Promise, Scheduler, Yieldable

if TYPE_CHECKING:
    from collections.abc import Generator


def _divide(a: int, b: int) -> int:
    return a // b


def invocation_that_fails() -> Generator[Yieldable, Any, int]:
    xp: Promise[int] = yield Invoke(_divide, a=3, b=0)
    x: int = yield xp
    return x


def one_invocation() -> Generator[Yieldable, Any, int]:
    xp: Promise[int] = yield Invoke(_divide, a=3, b=1)
    x: int = yield xp
    return x


def only_call() -> Generator[Yieldable, Any, int]:
    x: int = yield Call(_divide, a=3, b=1)
    return x


def call_with_errors() -> Generator[Yieldable, Any, int]:
    try:
        x: int = yield Call(_divide, a=100, b=0)
    except ZeroDivisionError:
        x: int = 3
    return x


def double_call() -> Generator[Yieldable, Any, int]:
    x: int = yield Call(_divide, a=3, b=1)
    y: int = yield Call(_divide, a=5, b=1)
    return x + y


def test_calls() -> None:
    s = Scheduler()
    p = s.add(only_call)
    assert p.result() == 3  # noqa: PLR2004
    p = s.add(call_with_errors)
    assert p.result() == 3  # noqa: PLR2004
    p = s.add(double_call)
    assert p.result() == 8  # noqa: PLR2004
    s.close()
