from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from resonate_sdk_py.scheduler import Call, Invoke, Promise, Scheduler, Yieldable

if TYPE_CHECKING:
    from collections.abc import Generator


def _nested_gen(a: Promise[int]) -> Generator[Yieldable, Any, int]:
    x = yield a
    return x


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


def gen_call() -> Generator[Yieldable, Any, int]:
    x: Promise[int] = yield Invoke(_divide, a=3, b=1)
    y: int = yield Call(_nested_gen, x)
    return y


def gen_invoke() -> Generator[Yieldable, Any, int]:
    x: Promise[int] = yield Invoke(_divide, a=3, b=1)
    y: Promise[int] = yield Invoke(_nested_gen, x)
    z: int = yield y
    return z


def double_call() -> Generator[Yieldable, Any, int]:
    x: int = yield Call(_divide, a=3, b=1)
    y: int = yield Call(_divide, a=5, b=1)
    return x + y


def _abc(value: Promise[int]) -> Generator[Yieldable, int, int]:
    x = yield value
    return x


def whatever() -> Generator[Yieldable, Any, int]:
    af: Promise[int] = yield Invoke(_divide, a=3, b=1)
    xf: Promise[int] = yield Invoke(_abc, value=af)
    yf: Promise[int] = yield Invoke(_abc, value=af)
    try:
        x: int = yield xf
    except Exception:  # noqa: BLE001
        x = 3
    y: int = yield yf
    return x + y


def whatever_with_error() -> Generator[Yieldable, Any, int]:
    af: Promise[int] = yield Invoke(_divide, a=3, b=0)
    xf: Promise[int] = yield Invoke(_abc, value=af)
    yf: Promise[int] = yield Invoke(_abc, value=af)
    try:
        x: int = yield xf
    except Exception:  # noqa: BLE001
        x = 3
    y: int = yield yf
    return x + y


def test_whatever() -> None:
    s = Scheduler()
    p = s.add(whatever())
    assert p.result(timeout=4) == 6  # noqa: PLR2004


def test_whatever_with_error() -> None:
    s = Scheduler()
    p = s.add(whatever_with_error())
    with pytest.raises(ZeroDivisionError):
        p.result(timeout=4)


def test_calls() -> None:
    s = Scheduler()
    p = s.add(only_call())
    assert p.result(timeout=30) == 3  # noqa: PLR2004
    p = s.add(call_with_errors())
    assert p.result(timeout=30) == 3  # noqa: PLR2004
    p = s.add(double_call())
    assert p.result(timeout=30) == 8  # noqa: PLR2004


@pytest.mark.dev()
def test_call_gen() -> None:
    s = Scheduler()
    p = s.add(gen_call())
    assert p.result() == 3  # noqa: PLR2004


@pytest.mark.dev()
def test_invoke_gen() -> None:
    s = Scheduler()
    p = s.add(gen_invoke())
    assert p.result() == 3  # noqa: PLR2004


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
    p = s.add(only_invocation())
    assert p.result(timeout=30) == 3  # noqa: PLR2004


def test_invocation_with_error() -> None:
    s = Scheduler()
    p = s.add(invocation_with_error())
    assert p.result(timeout=30) == 4  # noqa: PLR2004


def test_add_multiple() -> None:
    s = Scheduler()
    promises = s.add_multiple([invocation_with_error(), only_invocation()])
    assert promises[0].result(timeout=3) == 4  # noqa: PLR2004
    assert promises[1].result(timeout=3) == 3  # noqa: PLR2004
