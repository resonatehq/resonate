from __future__ import annotations

from typing import TYPE_CHECKING, Any

from resonate_sdk_py.other_scheduler import Call, Scheduler, Yieldable

if TYPE_CHECKING:
    from collections.abc import Generator


def divide(a: int, b: int) -> int:
    return a // b


async def async_divide(a: int, b: int) -> int:
    return a // b


def foo() -> Generator[Yieldable, Any, int]:
    x: int = yield Call(divide, a=3, b=1)
    y: int = yield Call(async_divide, a=4, b=1)
    return x + y


def test_scheduler() -> None:
    s = Scheduler()
    foo_1 = s.add(fn=foo)
    assert foo_1.result() == 7  # noqa: PLR2004
