"""versioning shows how to run several versions of one function side by side.

The registry is keyed on ``(name, version)``, so the *same* function name can
have multiple implementations registered at once -- the bread and butter of a
rolling deploy, where in-flight work must keep running the version it started
on while new work picks up the new code.

The version is **explicit, never "latest"**. That is what makes replay
deterministic: a durable promise records its version at create time, so when an
orchestrator suspends and replays -- or another worker picks the task up -- it
resolves the *same* implementation every time, no matter what has been
registered since.

Two ways to pick the version, depending on how you dispatch:

* ``run(id, fn, ...)`` takes a function **object**, so the version is whatever
  ``fn`` was registered as -- recovered by identity. ``with_opts(version=)``
  does NOT apply here; the object already implies the version.
* ``rpc(id, "name", ...)`` dispatches by **name string**, so the version comes
  from ``with_opts(version=)`` (default ``1``). This is also the path that runs
  a function that need not be registered in *this* process.

Start a Resonate server on localhost:8001 first (``resonate dev``), then::

    uv run python examples/versioning
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


async def charge_v1(ctx: Context, amount: float) -> float:
    # v1: charge the amount as-is.
    return amount


async def charge_v2(ctx: Context, amount: float) -> float:
    # v2: the billing rules changed -- add a 3% processing fee.
    return round(amount * 1.03, 2)


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)

    # Same name "charge", two coexisting implementations.
    r.register(charge_v1, name="charge", version=1)
    r.register(charge_v2, name="charge", version=2)

    ts = time.time_ns()
    try:
        # run() -- version is taken from the function OBJECT you hand it.
        v1 = await r.run(f"charge-run-v1-{ts}", charge_v1, 100.0).result()
        v2 = await r.run(f"charge-run-v2-{ts}", charge_v2, 100.0).result()
        assert v1 == 100
        assert v2 == 103
        print(f"run  charge_v1(100) = {v1}")  # 100.0
        print(f"run  charge_v2(100) = {v2}")  # 103.0

        # rpc() -- dispatched by NAME; version comes from with_opts (default 1).
        rpc_v1 = await r.rpc(f"charge-rpc-v1-{ts}", "charge", 100.0).result()
        rpc_v2 = await (
            r.options(version=2).rpc(f"charge-rpc-v2-{ts}", "charge", 100.0).result()
        )
        assert rpc_v1 == 100
        assert rpc_v2 == 103
        print(f"rpc  charge v1 (100) = {rpc_v1}")  # 100.0 -- default version 1
        print(f"rpc  charge v2 (100) = {rpc_v2}")  # 103.0 -- selected via with_opts
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
