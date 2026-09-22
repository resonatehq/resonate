"""recovery shows typed serialize/deserialize across the durability boundary.

Every value a durable function exchanges -- its arguments and its return -- is
written to a durable promise as JSON. The interesting claim is that **recovery
needs no special path**: the same (de)serialization runs on every invocation, so
a value that is rebuilt after a crash is rebuilt by the exact steps that ran the
first time.

:class:`~resonate.durable.DurableFunction` is where that happens. On every call
it coerces each argument to its declared parameter type, and the top-level
:class:`~resonate.handle.ResonateHandle` coerces the settled value to the
function's declared return type (both ``msgspec.convert``). Whether the input is
a freshly-packed in-memory object (live) or the JSON builtins it round-tripped to
(replay / re-run / a different worker), the coercion is identical -- that is what
keeps the live and recovery paths in lock-step. A function with **no** annotation
opts out and gets raw builtins, exactly like ``rpc``.

This example is a plain typed checkout -- no crash, no forced suspend. It then:

  * re-runs with the same id, so the result is served from the durable promise
    (genuine recovery) and arrives as the *same* rebuilt struct -- same code, no
    recovery-only branch; and
  * dispatches the same function by name via ``rpc`` (which is untyped) to show
    the other side of the coin: identical stored bytes, but a raw ``dict`` back,
    because nothing told the runtime how to rebuild it.

Start a Resonate server on localhost:8001 first (``resonate dev``), then::

    uv run python examples/recovery

Note on replay: a durable orchestrator re-executes from the top each time it
awaits a not-yet-settled future, so any side effect (a ``print``, an external
call) belongs in a leaf step -- which settles once and never re-runs -- not in
``checkout`` itself. That is why every log line below lives in a step.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING

import msgspec

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


# -- Domain types: what crosses the durability boundary --------------------


class Cart(msgspec.Struct, frozen=True):
    items: list[str]
    total: int


class Receipt(msgspec.Struct, frozen=True):
    cart: Cart  # a nested struct -- the whole tree is rebuilt on the way out
    paid: bool


# -- Leaf steps (each prints once, settles once) ---------------------------


async def summarize(ctx: Context, items: list[str]) -> Cart:
    cart = Cart(items=items, total=len(items) * 10)
    print(f"  [summarize] {items} -> {cart}")
    return cart


async def pay(ctx: Context, cart: Cart) -> Receipt:
    # ``cart`` is declared ``Cart``, so DurableFunction.invoke coerces the
    # argument to a Cart before this body runs -- on every call. Passed a live
    # Cart it is a no-op; handed the JSON dict a recovery would deliver it
    # rebuilds the Cart. Either way the attribute access below is valid.
    print(f"  [pay] charging {cart.total} for {len(cart.items)} items")
    return Receipt(cart=cart, paid=True)


# -- Orchestrator ----------------------------------------------------------


async def checkout(ctx: Context, items: list[str]) -> Receipt:
    cart = await ctx.run(summarize, items)
    # The Cart is handed straight back across the boundary as an argument to the
    # next step; ``pay`` coerces it back to a Cart on the way in, and its Receipt
    # return is coerced to a Receipt for whoever awaits this workflow.
    return await ctx.run(pay, cart)


# -- main ------------------------------------------------------------------


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)
    r.register(checkout)

    try:
        id = f"recovery-{time.time_ns()}"
        print(f"[checkout] run id={id}")
        receipt = await r.run(id, checkout, ["apple", "pear", "plum"]).result()

        # ``run`` is typed: the handle coerces the settled JSON to checkout's
        # declared ``Receipt`` -- a real struct, nested Cart and all -- not a dict.
        assert isinstance(receipt, Receipt)
        assert isinstance(receipt.cart, Cart)
        assert receipt.cart.total == 30
        assert receipt.paid is True
        print(f"[checkout] typed result: {receipt}")

        # Re-run with the SAME id. Nothing re-executes (no leaf lines print); the
        # value is served from the durable promise -- genuine recovery -- and
        # comes back through the very same deserialize, yielding an equal struct.
        print(f"[checkout] re-run id={id} (served from the durable promise)")
        again = await r.run(id, checkout, ["apple", "pear", "plum"]).result()
        assert isinstance(again, Receipt)
        assert again == receipt
        print(f"[checkout] recovered result equals the original: {again == receipt}")

        # Same function, dispatched by NAME via rpc -- which is untyped (its
        # handle decodes to ``Any``). Identical bytes on the wire, but no
        # annotation to rebuild from, so the value comes back as a raw dict. This
        # is the opt-out the docstring warns about: annotate to keep the type.
        rpc_id = f"recovery-rpc-{time.time_ns()}"
        print(f"[checkout] rpc  id={rpc_id} (untyped dispatch by name)")
        untyped = await r.rpc(rpc_id, "checkout", ["apple", "pear", "plum"]).result()
        assert not isinstance(untyped, Receipt)
        assert isinstance(untyped, dict)
        assert untyped["paid"] is True  # value survived; only the *type* was lost
        print(
            f"[checkout] untyped (rpc) result is a {type(untyped).__name__}: {untyped}"
        )

        # Re-run the rpc with the SAME id: like the run re-run, the promise is
        # already settled, so it is served from durable storage (no leaf lines)
        # and decodes to the same raw dict -- recovery is the same path here too,
        # untyped on both the first call and the re-run.
        print(f"[checkout] re-run rpc id={rpc_id} (served from the durable promise)")
        untyped_again = await r.rpc(
            rpc_id, "checkout", ["apple", "pear", "plum"]
        ).result()
        assert untyped_again == untyped
        print(
            f"[checkout] recovered untyped result equals the original: {untyped_again == untyped}"
        )
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
