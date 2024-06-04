from __future__ import annotations

from typing import TYPE_CHECKING, Any

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


def test_exploration() -> None:
    scheduler = Scheduler()
    scheduler.add(foo)
    assert scheduler.run() == "Hi tomas 10 15 10 15"
