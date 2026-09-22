"""polling shows non-blocking progress tracking over many concurrent
durable workflows using :meth:`ResonateHandle.done`.

Top-level :meth:`Resonate.run` returns *immediately* with a handle -- the
durable invocation runs in the background. That makes it trivial to fan
several workflows out at once: hand each its own id, collect the handles,
and you have N independent executions in flight.

The usual next step is ``await handle.result()``, which blocks until that
one workflow settles. For a progress dashboard, a "wait for any" race, or
any other "tick without committing to a single workflow" pattern, you want
the *non-blocking* observation: :meth:`ResonateHandle.done`. It returns a
plain ``bool`` -- synchronous, no ``await``, no preference among the
handles in flight -- so you can scan every handle on every tick and
decide what to render or whom to harvest first.

This example dispatches three ``render_frame`` workflows of different
weights, then loops printing a dashboard every 250ms. Each tick reads
``handle.done()`` per workflow; when all three flip to ``True`` the loop
exits and the results are harvested with the usual ``await
handle.result()``.

Why not just ``asyncio.gather`` the results? ``gather`` is fine when all
you need is "wait for everything." It cannot tell you *which* workflow
finished first, or render a live UI, or implement "kick off ten, harvest
the first three to finish, cancel the rest." Polling ``done()`` is the
primitive those patterns are built from.

Start a Resonate server on localhost:8001 first (``resonate dev``), then::

    uv run python examples/polling

Note on replay: each render's body is a durable orchestrator and re-executes
from the top whenever it awaits a not-yet-settled future, so every side
effect lives in a leaf step that settles once -- never in the orchestrator
or in this top-level polling loop.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


# -- Leaf steps (each prints once, settles once) ---------------------------


async def shade(ctx: Context, frame: str, ms: int) -> str:
    # Pretend this is GPU work. ``asyncio.sleep`` stands in for any I/O or
    # compute that takes a while -- the point is that different frames take
    # different amounts of wall time, so the handles settle out of order.
    await asyncio.sleep(ms / 1000)
    print(f"  [shade]    frame={frame} ms={ms}")
    return f"shaded-{frame}"


async def encode(ctx: Context, shaded: str) -> str:
    await asyncio.sleep(0.05)
    print(f"  [encode]   {shaded}")
    return shaded.replace("shaded-", "encoded-")


# -- Orchestrator ----------------------------------------------------------


async def render_frame(ctx: Context, frame: str, ms: int) -> str:
    # Two-step durable workflow per frame. The polling loop watches the root
    # handle's ``done()`` -- it does not care which step the workflow is on.
    shaded = await ctx.run(shade, frame, ms)
    return await ctx.run(encode, shaded)


# -- main ------------------------------------------------------------------


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)
    r.register(render_frame)

    try:
        # Fan out: three workflows of different weights, all dispatched up
        # front. ``r.run`` returns a handle synchronously; the durable
        # invocation runs in the background, so by the time the third call
        # returns all three are already in flight.
        batch = time.time_ns()
        jobs = [
            ("frame-1", 200),
            ("frame-2", 600),
            ("frame-3", 400),
        ]
        handles = {
            frame: r.run(f"render-{batch}-{frame}", render_frame, frame, ms)
            for frame, ms in jobs
        }
        print(f"[polling] dispatched {len(handles)} render workflows")

        # Non-blocking progress dashboard. ``handle.done()`` is synchronous --
        # no ``await``, no scheduling point -- so a single tick can scan every
        # handle without committing to wait on any one of them. The loop ends
        # the first tick on which every handle is ``done``; at that point all
        # the results are already settled and ``await handle.result()`` is a
        # no-op fetch from the local subscription.
        tick = 0
        while True:
            states = {frame: handles[frame].done() for frame in handles}
            done_count = sum(states.values())
            bar = " ".join(
                f"{frame}={'✓' if s else '…'}" for frame, s in states.items()
            )
            print(f"[polling] tick={tick:>2}  {done_count}/{len(handles)}  {bar}")
            if done_count == len(handles):
                break
            await asyncio.sleep(0.25)
            tick += 1

        # Every handle is done: harvest the results. Order is unchanged from
        # dispatch order -- ``done()`` does not reshuffle anything; it only
        # answers "settled yet?" per handle.
        for frame, handle in handles.items():
            result = await handle.result()
            print(f"[polling] {frame} -> {result}")
            assert result == f"encoded-{frame}"
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
