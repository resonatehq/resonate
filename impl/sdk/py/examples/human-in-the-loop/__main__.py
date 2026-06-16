"""human shows a human-in-the-loop durable workflow.

An order-fulfillment orchestrator does some prep work, then *suspends* on a
durable promise that some external party -- a reviewer, a webhook, a UI -- is
expected to resolve. While the workflow is suspended, the worker holds no
state: the orchestrator can be restarted, the server can be restarted, even
days can pass. Whenever the external resolve eventually arrives, replay picks
up exactly where it left off and proceeds with the human's decision.

The mechanism is :meth:`Context.promise`: a "dependency-injected" durable
promise with a global, externally addressable id. The orchestrator awaits it;
anyone with the id can settle it through the regular promise API
(``r.promises.resolve(id, ...)``), the CLI, or HTTP.

Start a Resonate server on localhost:8001 (``resonate dev``) in one terminal,
then::

    uv run python examples/human                  # simulated approval (happy)
    uv run python examples/human --decision reject

Note on replay: the orchestrator re-executes from the top on every suspend, so
any side effect (printing the promise id, calling a notification service)
belongs in a leaf -- ``notify_reviewer`` here -- which settles once and never
re-runs.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from typing import TYPE_CHECKING

import msgspec

from resonate.resonate import Resonate
from resonate.types import Value

if TYPE_CHECKING:
    from resonate.context import Context


# -- Reviewer inbox --------------------------------------------------------


class ReviewerInbox:
    """Signal channel from the worker to a waiting reviewer.

    ``notify_reviewer`` is the leaf that *publishes* the durable promise id
    (in a real system: to Slack, email, a dashboard). Here it also completes
    ``approval_id`` so anything awaiting an approval -- like the simulated
    reviewer in :func:`main` -- learns the id the instant the promise exists,
    rather than polling ``r.promises.resolve`` until it stops 404-ing.

    Injected through :meth:`Resonate.with_dependency` and fetched in the leaf
    via ``ctx.get_dependency(ReviewerInbox)``.
    """

    def __init__(self) -> None:
        self.approval_id: asyncio.Future[str] = (
            asyncio.get_running_loop().create_future()
        )

    def publish(self, approval_id: str) -> None:
        # ``notify_reviewer`` is a leaf and settles once, but be defensive:
        # a future can only be resolved a single time.
        if not self.approval_id.done():
            self.approval_id.set_result(approval_id)


# -- Domain types ----------------------------------------------------------


class Decision(msgspec.Struct, frozen=True):
    approve: bool
    note: str


# -- Leaf functions (each prints once, settles once) -----------------------


async def notify_reviewer(
    ctx: Context, order_id: str, amount: int, approval_id: str
) -> str:
    # In a real system this would call Slack / email / a dashboard. The point
    # is that the side effect (and the promise id the reviewer must resolve)
    # lives in a leaf, not in the orchestrator -- so it happens exactly once.
    print(
        f"  [notify_reviewer] order {order_id} (${amount}) "
        f"needs approval; resolve promise id: {approval_id!r}"
    )
    # Signal any in-process listener (the simulated reviewer in main()) that
    # the durable promise now exists and tell them its id. A real reviewer
    # would learn the same id from the side effect above.
    ctx.get_dependency(ReviewerInbox).publish(approval_id)
    return approval_id


async def ship_order(ctx: Context, order_id: str, note: str) -> str:
    print(f"  [ship_order] shipping {order_id} (note: {note!r})")
    return f"shipped-{order_id}"


async def cancel_order(ctx: Context, order_id: str, note: str) -> str:
    print(f"  [cancel_order] canceling {order_id} (reason: {note!r})")
    return f"canceled-{order_id}"


# -- Orchestrator ----------------------------------------------------------


async def fulfill_order(ctx: Context, order_id: str, amount: int) -> str:
    # Open the human-decision promise first so its id is deterministic
    # (``{workflow_id}.1``). ctx.promise returns a future whose ``id()`` is
    # awaitable; we publish that id through a leaf so a real reviewer would
    # know where to resolve.
    approval = ctx.promise()  # inherit workflow timeout
    approval_id = await approval.id()

    await ctx.run(
        notify_reviewer,
        order_id=order_id,
        amount=amount,
        approval_id=approval_id,
    )

    # Suspend until the external party resolves the promise. The worker holds
    # no state while suspended; this can be seconds, hours, or days.
    decision_raw = await approval
    decision = msgspec.convert(decision_raw, Decision)

    if decision.approve:
        return await ctx.run(ship_order, order_id=order_id, note=decision.note)
    return await ctx.run(cancel_order, order_id=order_id, note=decision.note)


# -- main ------------------------------------------------------------------


async def simulate_reviewer(
    r: Resonate, inbox: ReviewerInbox, decision: Decision
) -> None:
    """Stand in for an external system that eventually resolves the promise.

    Waits for ``notify_reviewer`` to publish the durable promise id on the
    inbox -- exactly mirroring how a real reviewer learns where to resolve --
    then settles it. No polling, no hardcoded id.
    """
    approval_id = await inbox.approval_id
    await r.promises.resolve(approval_id, Value(data=decision))
    print(
        f"[reviewer] resolved {approval_id} -> "
        f"approve={decision.approve} note={decision.note!r}"
    )


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--decision", choices=("approve", "reject"), default="approve")
    parser.add_argument("--order", default="order-42")
    parser.add_argument("--amount", type=int, default=199)
    args = parser.parse_args()

    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)
    # Hand the inbox to every context -- ``notify_reviewer`` will publish on
    # it the moment the durable approval promise is created.
    inbox = ReviewerInbox()
    r.with_dependency(inbox)
    r.register(fulfill_order)

    try:
        wid = f"fulfill-{time.time_ns()}"
        print(f"[fulfill_order] starting workflow id={wid} decision={args.decision!r}")
        handle = r.run(wid, fulfill_order, args.order, args.amount)

        decision = Decision(
            approve=args.decision == "approve",
            note="looks good" if args.decision == "approve" else "policy violation",
        )
        reviewer = asyncio.create_task(simulate_reviewer(r, inbox, decision))

        out = await handle.result()
        await reviewer
        if args.decision == "approve":
            assert out == "shipped-order-42"
        else:
            assert out == "canceled-order-42"
        print(f"[fulfill_order] OK: {out}")
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
