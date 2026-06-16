"""fibonacci shows three ways to compose recursive durable invocations with
the Resonate SDK:

    --mode rpc   every recursive call goes through ctx.rpc (server-dispatched,
                 may execute on any worker in the group)
    --mode run   every recursive call goes through ctx.run (local task,
                 same worker)
    --mode mix   one branch via rpc, the other via run

Start a Resonate server on localhost:8001 first (``resonate dev``), then
e.g.::

    uv run python examples/fibonacci --mode rpc --n 10
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import os
from typing import TYPE_CHECKING

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


@functools.cache
def fib(n: int) -> int:
    if n <= 1:
        return n
    return fib(n - 1) + fib(n - 2)


async def fib_rpc(ctx: Context, n: int) -> int:
    if n < 2:
        return n
    f1 = ctx.rpc("fib_rpc", n - 1)
    f2 = ctx.rpc("fib_rpc", n - 2)
    return await f1 + await f2


async def fib_run(ctx: Context, n: int) -> int:
    if n < 2:
        return n
    f1 = ctx.run(fib_run, n - 1)
    f2 = ctx.run(fib_run, n - 2)
    return await f1 + await f2


async def fib_mix(ctx: Context, n: int) -> int:
    if n < 2:
        return n
    f1 = ctx.rpc("fib_mix", n - 1)
    f2 = ctx.run(fib_mix, n - 2)
    return await f1 + await f2


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("rpc", "run", "mix"), default="run")
    parser.add_argument("--n", type=int, default=10)
    args = parser.parse_args()

    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)
    fns = {"rpc": fib_rpc, "run": fib_run, "mix": fib_mix}
    for fn in fns.values():
        r.register(fn)

    try:
        id = f"fib-{args.mode}-{args.n}"
        handle = r.run(id, fns[args.mode], args.n)
        out = await handle.result()
        assert out == fib(args.n)
        print(f"fib({args.n}) = {out}  [mode={args.mode}]")
    except Exception as e:
        print("oops", e)
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
