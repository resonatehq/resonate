"""structured-concurrency shows that the runtime never leaks an unawaited
durable child.

    foo:
        f1 = ctx.run(bar, 1)   # spawned, never awaited
        f2 = ctx.run(bar, 2)   # spawned, never awaited
        return 5               # returns without touching f1 / f2

``foo`` fires off two ``ctx.run(bar)`` children and returns ``5`` immediately,
never awaiting either future. A naive runtime would resolve ``foo`` and orphan
the two children. Resonate does not: structured concurrency guarantees a
parent cannot settle while any child it spawned is still in flight. Before
``foo``'s promise resolves, the runtime joins every eagerly-spawned local
child, so both ``bar`` invocations run to completion regardless of whether
``foo`` awaited them.

We prove it durably. ``ctx.run`` children get deterministic ids
``{foo_id}.1`` and ``{foo_id}.2``. After ``foo`` returns ``5`` we attach to
those two promises by id and assert each one resolved -- evidence the
never-awaited work was awaited *by the runtime* on our behalf.

Start a Resonate server on localhost:8001 first (``resonate dev``), then::

    uv run python examples/structured-concurrency

Note on replay: a durable orchestrator re-executes from the top each time it
awaits a not-yet-settled future, so any side effect (a ``print``) belongs in a
leaf function -- which settles once and never re-runs -- not in ``foo``
itself. That is why the log line below lives in ``bar``.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


async def bar(ctx: Context, n: int) -> int:
    # Leaf: prints once, settles once. If structured concurrency holds, both
    # of foo's never-awaited children land here even though foo returned first.
    print(f"  [bar] running child n={n}")
    return n * 10


async def foo(ctx: Context) -> int:
    # Spawn two local children and walk away -- neither future is awaited.
    ctx.run(bar, 1)
    ctx.run(bar, 2)
    return 5


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)
    r.register(foo)
    r.register(bar)

    try:
        foo_id = f"structured-concurrency-{time.time_ns()}"
        print(f"[foo] starting workflow id={foo_id}")
        handle = r.run(foo_id, foo)
        out = await handle.result()

        # foo returned 5 without awaiting either child.
        assert out == 5, f"expected 5, got {out!r}"
        print(f"[foo] OK: returned {out} (never awaited its two children)")

        # Structured concurrency: the runtime awaited the two never-awaited
        # ctx.run children before resolving foo. ctx.run assigns child ids in
        # call order as ``{parent_id}.{seq}`` (seq starts at 1), so foo's two
        # children are ``{foo_id}.1`` and ``{foo_id}.2``. Attach to each
        # durable promise and confirm it resolved with bar's result.
        for seq, n in ((1, 1), (2, 2)):
            child_id = f"{foo_id}.{seq}"
            child_handle = await r.get(child_id)
            child_out = await child_handle.result()
            assert child_out == n * 10, (
                f"child {child_id} resolved {child_out!r}, expected {n * 10}"
            )
            print(f"[child] {child_id} resolved {child_out} -- the runtime awaited it")

        print(
            "[ok] both never-awaited children completed: structured concurrency holds"
        )
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
