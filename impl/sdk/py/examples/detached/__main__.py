"""detached shows fire-and-forget durable invocations whose lifetime is
**decoupled** from the parent.

    place_order:
        1. reserve_stock        (ctx.run -- parent waits)
        2. charge_card          (ctx.run -- parent waits)
        3. write_audit_log      (ctx.detached -- parent does NOT wait)

The first two steps are ordinary durable children: their result feeds the
orchestrator, so the orchestrator awaits them. The audit log is different --
it is a side-effect workflow that must *survive* the order completing. The
order should not block on the audit, and an audit that takes longer than the
order's remaining timeout must still finish.

``ctx.detached`` is the tool. Unlike :meth:`~resonate.context.Context.run` /
:meth:`~resonate.context.Context.rpc`, it returns a future of the **promise
id**, not of the result. The parent never awaits the body, so the audit's
lifetime, timeout, and execution worker are independent of ``place_order``.
Internally the audit lives under a ``Det`` node, which is exempt from the
suspension frontier -- a pending detached child never holds the parent in
suspended state.

This is the building block for: post-commit hooks, async notifications,
metric emission, cleanup jobs, fan-out of background workflows from inside
another workflow. Anything where "kick this off durably, don't make me wait."

Start a Resonate server on localhost:8001 first (``resonate dev``), then::

    uv run python examples/detached

Note on replay: a durable orchestrator re-executes from the top each time it
awaits a not-yet-settled future, so any side effect (a ``print``, an external
call) belongs in a leaf step function -- which settles once and never re-runs
-- not in ``place_order`` itself. That is why every log line below lives in a
step.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


# -- Order steps (leaves: each prints once, settles once) ------------------


async def reserve_stock(ctx: Context, sku: str, qty: int) -> str:
    ref = f"STK-{sku}-{qty}"
    print(f"  [reserve_stock] reserved {ref}")
    return ref


async def charge_card(ctx: Context, customer: str, amount: int) -> str:
    ref = f"CH-{customer}-{amount}"
    print(f"  [charge_card] charged {ref}")
    return ref


# -- Audit workflow (a real, multi-step durable workflow on its own) -------
#
# Has nothing to do with the order's lifetime: lives under its own root id,
# its own timeout, and -- crucially -- a Det subtree exempt from
# ``place_order``'s suspension frontier.


async def hash_payload(ctx: Context, customer: str, sku: str, amount: int) -> str:
    # Pretend this hashes the order for tamper-evident logging.
    digest = f"{customer}:{sku}:{amount}".encode().hex()
    print(f"  [hash_payload] {digest}")
    return digest


async def ship_to_warehouse(ctx: Context, digest: str) -> str:
    # Pretend this writes to a remote audit warehouse.
    print(f"  [ship_to_warehouse] persisted {digest}")
    return f"audit-{digest}"


async def write_audit_log(
    ctx: Context, customer: str, sku: str, amount: int, stock_ref: str, charge_ref: str
) -> str:
    # A two-step child workflow. ``place_order`` never sees these steps run --
    # they execute *after* ``place_order`` has returned, on whichever worker
    # the server hands the task to. That decoupling is the whole point of
    # ``detached``.
    digest = await ctx.run(hash_payload, customer, sku, amount)
    location = await ctx.run(ship_to_warehouse, digest)
    print(f"  [write_audit_log] stock={stock_ref} charge={charge_ref} -> {location}")
    return location


# -- Orchestrator ----------------------------------------------------------


async def place_order(
    ctx: Context, customer: str, sku: str, qty: int, amount: int
) -> tuple[str, str, str]:
    # Foreground work -- the order needs both of these to commit.
    stock_ref = await ctx.run(reserve_stock, sku, qty)
    charge_ref = await ctx.run(charge_card, customer, amount)

    # Fire-and-forget: dispatch the audit workflow by NAME, hand off its
    # arguments, and get back the durable id of its root promise. The audit's
    # body never runs in this task. ``place_order`` only awaits the id of the
    # promise (which resolves the instant create_promise succeeds), NOT the
    # result.
    audit_future = ctx.detached(
        "write_audit_log",
        customer=customer,
        sku=sku,
        amount=amount,
        stock_ref=stock_ref,
        charge_ref=charge_ref,
    )
    audit_id = await audit_future.id()
    print(f"  [place_order] dispatched audit id={audit_id} (not waiting on it)")

    # Return immediately. The audit may still be running -- that is fine.
    return stock_ref, charge_ref, audit_id


# -- main ------------------------------------------------------------------


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)

    # Register both the orchestrator and the audit workflow. ``ctx.detached``
    # dispatches by name through the server, so the audit needs to be
    # registered on whichever group will execute it -- here, the same
    # instance, but in a real deployment this is typically a different group
    # (pass ``with_opts(target="audit-workers")`` before ``detached``).
    r.register(place_order)
    r.register(write_audit_log)

    try:
        order_id = f"order-{time.time_ns()}"
        print(f"[place_order] starting workflow id={order_id}")
        handle = r.run(order_id, place_order, "alice", "WIDGET-7", 2, 199)
        stock_ref, charge_ref, audit_id = await handle.result()
        assert stock_ref == "STK-WIDGET-7-2"
        assert charge_ref == "CH-alice-199"
        print(f"[place_order] OK: stock={stock_ref} charge={charge_ref}")

        # The order has committed. The audit may already be done, may be
        # mid-flight, or -- on a slow worker -- not yet started. Attach to its
        # durable promise by id and await it, *separately* from the order. In
        # a real system this could be a wholly different process, hours later.
        print(f"[audit] attaching to detached workflow id={audit_id}")
        audit_handle = await r.get(audit_id)
        location = await audit_handle.result()
        print(f"[audit] OK: {location}")
        assert isinstance(location, str)
        assert location.startswith("audit-")
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
