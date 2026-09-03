r"""End-to-end against a real resonate-pg database.

Skipped unless ``RESONATE_PG_DSN`` is set. Everything else in this suite runs
against in-memory fakes; these tests are the only proof that the addresses,
tags, and SQL this connector emits are the ones the database actually
delivers on. Each one replays an SDK headline workflow the way a user would,
on the substrate where a drifted address or a missing tag is a silent
no-delivery bug:

* a durable workflow to completion -- local run, cross-worker rpc, durable sleep
* rpc dispatch across worker groups
* human-in-the-loop: an external party resolving a ``ctx.promise()``
* human-in-the-loop timeout: nobody resolves, the workflow wakes rejected
* a failing task raising through the durability boundary
* a detached workflow outliving its parent

For a database to run them against::

    docker run -d --name resonate-pg -e POSTGRES_PASSWORD=resonate -p 5433:5432 postgres:16
    curl -sSL https://raw.githubusercontent.com/resonatehq/resonate-pg/main/resonate.sql \\
      | docker exec -i resonate-pg psql -U postgres -d postgres -v ON_ERROR_STOP=1 -f -
    RESONATE_PG_DSN=postgresql://postgres:resonate@localhost:5433/postgres \\
      uv run pytest packages/resonate-pg/tests/test_integration.py -v

No pg_cron: the connector's own pump drives timers, which is exactly what
these tests want proven. CI runs this file against a postgres:16 service
container -- the ``integration-pg`` job in ``.github/workflows/ci.yml``.

This is the only test file in the package that imports ``resonate``: the
shipped connector depends on ``resonate-base`` alone, and tests are not
shipped.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from resonate_pg import PostgresConnection

from resonate.error import ApplicationError
from resonate.resonate import Resonate
from resonate.retry import Never
from resonate.types import Value

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from resonate.context import Context
    from resonate.retry import RetryPolicy

pytestmark = pytest.mark.skipif(
    not os.environ.get("RESONATE_PG_DSN"),
    reason="set RESONATE_PG_DSN to run the Postgres integration tests",
)


# ═══════════════════════════════════════════════════════════════
#  Harness
# ═══════════════════════════════════════════════════════════════


def _dsn() -> str:
    return os.environ["RESONATE_PG_DSN"]


def _group() -> str:
    # A unique group per instance: two live instances sharing a group would
    # fight over each other's outbox rows, and stale rows from a previous run
    # of the suite would find their way into a new one.
    return f"itest-{uuid.uuid4().hex[:8]}"


@contextlib.asynccontextmanager
async def _resonate(
    group: str | None = None, *, retry_policy: RetryPolicy | None = None
) -> AsyncIterator[Resonate]:
    """Start a connector-backed Resonate, stop it however the body exits."""
    g = group if group is not None else _group()
    conn = PostgresConnection(_dsn(), group=g, tick=0.05)
    resonate = Resonate(network=conn, group=g, retry_policy=retry_policy)
    try:
        yield resonate
    finally:
        await resonate.stop()


async def _resolve_when_created(
    resonate: Resonate, promise_id: str, value: Value
) -> None:
    """Resolve ``promise_id`` the instant it exists, as an external party would.

    The workflow creates the promise mid-flight, so the first attempts fail
    with not-found; retrying is what a real reviewer process does implicitly.
    """
    for _ in range(200):
        try:
            await resonate.promises.resolve(promise_id, value)
        except Exception:
            await asyncio.sleep(0.05)
        else:
            return
    msg = f"{promise_id} never became resolvable"
    raise AssertionError(msg)


# ═══════════════════════════════════════════════════════════════
#  Durable functions -- module level, so pickle can round-trip them
# ═══════════════════════════════════════════════════════════════


async def doubled(ctx: Context, n: int) -> int:
    return n * 2


async def flow(ctx: Context, n: int) -> int:
    # A local step, a remote step, and a durable sleep: the three paths that
    # exercise task.fulfill, the execute outbox, and process_timeouts.
    local: int = await ctx.run(doubled, n)
    remote: int = await ctx.rpc(doubled, local)
    await ctx.sleep(timedelta(milliseconds=200))
    return remote


async def greet(ctx: Context, name: str) -> str:
    return f"hello from backend, {name}!"


async def await_approval(ctx: Context) -> str:
    approval = ctx.promise()
    await approval.id()
    decision = await approval
    return f"approved:{decision['note']}"


async def never_approved(ctx: Context) -> str:
    approval = ctx.promise(timeout=timedelta(milliseconds=300))
    await approval.id()
    return await approval


class PaymentDeclinedError(Exception):
    """A plain domain error -- the original type must cross the boundary."""


async def charge(ctx: Context, amount: int) -> str:
    msg = f"card declined for ${amount}"
    raise PaymentDeclinedError(msg)


async def order(ctx: Context, amount: int) -> str:
    return await ctx.run(charge, amount)


async def hash_receipt(ctx: Context, customer: str) -> str:
    return f"receipt-{customer}"


async def file_audit(ctx: Context, customer: str) -> str:
    receipt = await ctx.run(hash_receipt, customer)
    return f"audited:{receipt}"


async def place_order(ctx: Context, customer: str) -> str:
    # Fire-and-forget: dispatch the audit by NAME, return its durable id --
    # the order never waits on the audit, which may run after this task ends.
    audit_future = ctx.detached("file_audit", customer)
    return await audit_future.id()


# ═══════════════════════════════════════════════════════════════
#  The workflows
# ═══════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_a_durable_workflow_runs_to_completion_on_postgres() -> None:
    async with _resonate() as resonate:
        resonate.register(doubled)
        resonate.register(flow)
        handle = resonate.run(f"itest-flow-{uuid.uuid4().hex[:8]}", flow, 21)
        assert await handle.result() == 84


@pytest.mark.asyncio
async def test_rpc_dispatches_across_groups() -> None:
    """One worker's group owns the function; another's dispatches by name."""
    backend_group = _group()
    async with _resonate(backend_group) as backend, _resonate() as frontend:
        backend.register(greet)
        handle = frontend.options(target=backend_group).rpc(
            f"itest-rpc-{uuid.uuid4().hex[:8]}", "greet", "world"
        )
        assert await handle.result() == "hello from backend, world!"


@pytest.mark.asyncio
async def test_an_externally_resolved_promise_wakes_the_workflow() -> None:
    """Human-in-the-loop: the reviewer settles the promise; the workflow wakes."""
    async with _resonate() as resonate:
        resonate.register(await_approval)
        wid = f"itest-approve-{uuid.uuid4().hex[:8]}"
        handle = resonate.run(wid, await_approval)

        # The approval promise is the workflow's first child: {wid}:1.
        resolver = asyncio.create_task(
            _resolve_when_created(
                resonate, f"{wid}:1", Value(data={"note": "looks good"})
            )
        )
        assert await handle.result() == "approved:looks good"
        await resolver


@pytest.mark.asyncio
async def test_an_unresolved_promise_times_out_and_rejects_the_workflow() -> None:
    """Nobody resolves, the short timeout elapses, the workflow wakes rejected.

    ``process_timeouts`` expires external promises -- which ``ctx.promise()``
    now is, via its ``resonate:external`` tag -- and the connector's own pump
    drives that call, since there is no pg_cron here. The awaited future must
    surface the rejection (``ApplicationError``), not hang.
    """
    async with _resonate() as resonate:
        resonate.register(never_approved)
        handle = resonate.run(f"itest-timeout-{uuid.uuid4().hex[:8]}", never_approved)
        with pytest.raises(ApplicationError):
            await asyncio.wait_for(handle.result(), timeout=15)


@pytest.mark.asyncio
async def test_a_failing_task_raises_through_the_boundary() -> None:
    """The original domain type is reconstructed from the durable promise.

    ``Never`` because the failure is deterministic -- under the default
    exponential policy the SDK would retry the root task ~forever, which is
    the documented SDK behavior, not what this test is about.
    """
    async with _resonate(retry_policy=Never()) as resonate:
        resonate.register(order)
        handle = resonate.run(f"itest-error-{uuid.uuid4().hex[:8]}", order, 42)
        with pytest.raises(PaymentDeclinedError, match="card declined for \\$42"):
            await handle.result()


@pytest.mark.asyncio
async def test_a_detached_workflow_outlives_its_parent() -> None:
    """The parent returns the audit's durable id; the audit settles later."""
    async with _resonate() as resonate:
        resonate.register(place_order)
        resonate.register(file_audit)
        handle = resonate.run(
            f"itest-detach-{uuid.uuid4().hex[:8]}", place_order, "alice"
        )
        audit_id = await handle.result()
        assert audit_id.startswith("itest-detach-")

        # Attach to the detached workflow by id -- a wholly separate durable
        # promise that may still be running.
        audit_handle = await resonate.get(audit_id)
        assert await audit_handle.result() == "audited:receipt-alice"
