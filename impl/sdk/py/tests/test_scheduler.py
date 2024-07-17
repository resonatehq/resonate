from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from resonate.scheduler import (
    Promise,
    Scheduler,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.typing import Yieldable


def _nested_gen(ctx: Context, a: Promise[int]) -> Generator[Yieldable, Any, int]:  # noqa: ARG001
    x = yield a
    return x


def _divide(ctx: Context, a: int, b: int) -> int:  # noqa: ARG001
    return a // b


def only_call(ctx: Context) -> Generator[Yieldable, Any, int]:
    x: int = yield ctx.call(_divide, a=3, b=1)
    return x


def call_with_errors(
    ctx: Context,
) -> Generator[Yieldable, Any, int]:
    x: int
    try:
        x = yield ctx.call(_divide, a=100, b=0)
    except ZeroDivisionError:
        x = 3
    return x


def gen_call(
    ctx: Context,
) -> Generator[Yieldable, Any, int]:
    x: Promise[int] = yield ctx.invoke(_divide, a=3, b=1)
    y: int = yield ctx.call(_nested_gen, x)
    return y


def gen_invoke(ctx: Context) -> Generator[Yieldable, Any, int]:
    x: Promise[int] = yield ctx.invoke(_divide, a=3, b=1)
    y: Promise[int] = yield ctx.invoke(_nested_gen, x)
    z: int = yield y
    return z


def double_call(ctx: Context) -> Generator[Yieldable, Any, int]:
    x: int = yield ctx.call(_divide, a=3, b=1)
    y: int = yield ctx.call(_divide, a=5, b=1)
    return x + y


def _abc(ctx: Context, value: Promise[int]) -> Generator[Yieldable, int, int]:  # noqa: ARG001
    x = yield value
    return x


def whatever(ctx: Context) -> Generator[Yieldable, Any, int]:
    af: Promise[int] = yield ctx.invoke(_divide, a=3, b=1)
    xf: Promise[int] = yield ctx.invoke(_abc, value=af)
    yf: Promise[int] = yield ctx.invoke(_abc, value=af)
    try:
        x: int = yield xf
    except Exception:  # noqa: BLE001
        x = 3
    y: int = yield yf
    z = x + y
    ctx.assert_statement(z > 0, f"{z} should be positive")
    return z


def whatever_with_error(ctx: Context) -> Generator[Yieldable, Any, int]:
    af: Promise[int] = yield ctx.invoke(_divide, a=3, b=0)
    xf: Promise[int] = yield ctx.invoke(_abc, value=af)
    yf: Promise[int] = yield ctx.invoke(_abc, value=af)
    try:
        x: int = yield xf
    except Exception:  # noqa: BLE001
        x = 3
    y: int = yield yf
    return x + y


def test_whatever() -> None:
    s = Scheduler()
    s.run()
    p = s.add(whatever)
    assert p.result(timeout=4) == 6  # noqa: PLR2004


def test_whatever_with_error() -> None:
    s = Scheduler()
    s.run()
    p = s.add(whatever_with_error)
    with pytest.raises(ZeroDivisionError):
        p.result(timeout=4)


def test_calls() -> None:
    s = Scheduler()
    s.run()
    p = s.add(only_call)
    assert p.result(timeout=30) == 3  # noqa: PLR2004
    p = s.add(call_with_errors)
    assert p.result(timeout=30) == 3  # noqa: PLR2004
    p = s.add(double_call)
    assert p.result(timeout=30) == 8  # noqa: PLR2004


def test_call_gen() -> None:
    s = Scheduler()
    s.run()
    p = s.add(gen_call)
    assert p.result() == 3  # noqa: PLR2004
    assert p.success()
    assert not p.failure()


def test_invoke_gen() -> None:
    s = Scheduler()
    s.run()
    p = s.add(gen_invoke)
    assert p.result() == 3  # noqa: PLR2004


def only_invocation(ctx: Context) -> Generator[Yieldable, Any, int]:
    xp: Promise[int] = yield ctx.invoke(_divide, a=3, b=1)
    x: int = yield xp
    return x


def invocation_with_error(ctx: Context) -> Generator[Yieldable, Any, int]:
    xp: Promise[int] = yield ctx.invoke(_divide, a=3, b=0)
    try:
        x: int = yield xp
    except ZeroDivisionError:
        x = 4
    return x


def test_invocation() -> None:
    s = Scheduler()
    s.run()
    p = s.add(only_invocation)
    assert p.result(timeout=30) == 3  # noqa: PLR2004


def test_invocation_with_error() -> None:
    s = Scheduler()
    s.run()
    p = s.add(invocation_with_error)
    assert p.result(timeout=30) == 4  # noqa: PLR2004


def test_add_multiple() -> None:
    s = Scheduler()
    s.run()
    p1 = s.add(invocation_with_error)
    p2 = s.add(only_invocation)

    assert p1.result(timeout=3) == 4  # noqa: PLR2004
    assert p2.result(timeout=3) == 3  # noqa: PLR2004
