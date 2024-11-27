from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.record import Promise
    from resonate.typing import Yieldable

logging.basicConfig(level="DEBUG")
resonate = Resonate()


def foo(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
    p: Promise[str] = yield ctx.lfi(bar, n)
    v: str = yield p
    return v


def bar(ctx: Context, n: str) -> str:  # noqa: ARG001
    return n


resonate.register(foo)
p = resonate.lfi("foo", foo, "hi")

print(p.result())  # noqa: T201
assert p.result() == "hi"
