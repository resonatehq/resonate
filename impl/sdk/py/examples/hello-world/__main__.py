"""hello is a minimal example of using the Resonate SDK.

It registers a function, invokes it durably against a Resonate server, and
prints the result.

Start a Resonate server on localhost:8001 first (``resonate dev``), then::

    uv run python examples/hello
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


async def foo(ctx: Context, name: str) -> str:
    return await ctx.run(bar, name)


async def bar(ctx: Context, name: str) -> str:
    return await ctx.rpc("baz", name)


async def baz(ctx: Context, name: str) -> str:
    print("foo")
    return f"hello, {name}!"


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)
    r.register(foo)
    r.register(baz)
    try:
        id = f"hello-{time.time_ns()}"
        handle = r.run(id, foo, "world")
        print(await handle.result())
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
