"""retries shows Resonate retrying a flaky function until it succeeds.

The rule: Resonate retries **leaf** functions -- those that perform no durable
op (``ctx.run`` / ``ctx.rpc`` / ``ctx.sleep`` / ...). A function that *does*
perform one is a workflow, recovered by replay, and is never retried.

``charge`` is a leaf that fails twice before it succeeds. This example invokes it
through every entrypoint, and Resonate retries it every time:

* ``resonate.run(id, charge, ...)``  -- top-level, as a root task.
* ``resonate.rpc(id, "charge", ...)``-- top-level, dispatched by name.
* ``ctx.run(charge, ...)``           -- from a workflow, locally.
* ``ctx.rpc("charge", ...)``         -- from a workflow, by name.

Each path runs ``charge`` as a leaf, so each is retried. The ``run`` paths use
the policy ``charge`` was registered with, except ``ctx.run`` here, which sets a
per-call policy via ``with_opts``. The ``checkout`` workflow itself performs
durable ops, so it is never retried -- only the leaves it calls are.

Start a Resonate server on localhost:8001 first (``resonate dev``), then::

    uv run python examples/retries
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from resonate.resonate import Resonate
from resonate.retry import Constant

if TYPE_CHECKING:
    from resonate.context import Context

# Retry up to 5 times with no delay between attempts (keeps the demo instant).
POLICY = Constant(max_retries=5, delay=0)


@dataclass
class Gateway:
    """A flaky service: fails the first two calls *per key*, then succeeds."""

    attempts: dict[str, int] = field(default_factory=dict)

    def charge(self, key: str, amount: int) -> str:
        n = self.attempts[key] = self.attempts.get(key, 0) + 1
        print(f"  [{key}] attempt {n}...")
        if n <= 2:
            msg = f"timeout (attempt {n})"
            raise ConnectionError(msg)
        return f"charged ${amount}"


async def charge(ctx: Context, key: str, amount: int) -> str:
    # A leaf -- no ctx.* durable op -- so Resonate retries it on failure.
    return ctx.get_dependency(Gateway).charge(key, amount)


async def checkout(ctx: Context) -> str:
    # checkout is a workflow (it calls ctx.run / ctx.rpc), so it is never
    # retried -- but each flaky leaf it invokes is.
    via_run = await ctx.options(retry_policy=POLICY).run(charge, "ctx.run", 300)
    via_rpc = await ctx.rpc("charge", "ctx.rpc", 400)
    return f"{via_run} | {via_rpc}"


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)
    r.with_dependency(Gateway())
    r.register(checkout)
    # Registering with a policy is what makes the run-as-root-task paths
    # (resonate.run, resonate.rpc, ctx.rpc) retry: the executing worker reads it.
    r.register(charge, retry_policy=POLICY)
    ts = time.time_ns()
    try:
        # Top-level: run / rpc the registered leaf directly. Each runs as a root
        # task, retried via the policy charge was registered with.
        print("resonate.run:")
        a = await r.run(f"retries-run-{ts}", charge, "resonate.run", 100).result()
        print(f"  -> {a}")

        print("resonate.rpc:")
        b = await r.rpc(f"retries-rpc-{ts}", "charge", "resonate.rpc", 200).result()
        print(f"  -> {b}")

        # From inside a workflow: ctx.run / ctx.rpc the same leaf.
        print("ctx.run + ctx.rpc (inside the checkout workflow):")
        c = await r.run(f"retries-checkout-{ts}", checkout).result()
        print(f"  -> {c}")
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
