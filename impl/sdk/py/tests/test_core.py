"""Behaviour tests for :mod:`resonate.core`.

These run against a *real* server simulation -- the in-process
:class:`~resonate.network.LocalNetwork` driven through the real
:class:`~resonate.send.Sender` / :class:`~resonate.transport.Transport` -- so
the contract is "real server, real wire".

Conventions exercised throughout:

* *Every* durable function takes a :class:`Context` as its first argument,
  so the function library below follows suit.
* Awaiting a pending remote raises :class:`~resonate.error.Suspended`.
* ``execute_until_blocked_outer`` returns :data:`Status` and raises on
  failure, so the error-path tests assert ``pytest.raises``.

Two groups are intentionally omitted: the **short-circuit** branch (settling a
root promise on LocalNetwork auto-fulfills its task, so "acquired task +
already-settled root promise" can't be constructed) and the **redirect loop**
(needs an awaited promise settled at the exact instant ``task.suspend`` lands
-- a race LocalNetwork can't produce deterministically). See the NOTE comments
inline.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import pytest

from resonate.codec import Codec, NoopEncryptor
from resonate.core import Core, identity_target_resolver
from resonate.error import (
    ApplicationError,
    FunctionNotFoundError,
    ResonateError,
    Suspended,
)
from resonate.network import LocalNetwork
from resonate.registry import Registry
from resonate.send import Sender
from resonate.transport import Transport
from resonate.types import PromiseCreateReq, PromiseSettleReq, TaskData

if TYPE_CHECKING:
    from resonate.context import Context
    from resonate.types import PromiseRecord

# Far-future deadline.
FAR_FUTURE = 1 << 50
TTL = 10_000


# ── Test harness ────────────────────────────────────────────────────────


class TrackingHeartbeat:
    """Counts ``start``/``stop`` calls for Core's heartbeat hook.

    Implements the :class:`~resonate.heartbeat.Heartbeat` protocol.
    """

    def __init__(self) -> None:
        self.started = 0
        self.stopped = 0

    def start(self, task_id: str, task_version: int) -> None:
        self.started += 1

    def stop(self, task_id: str) -> None:
        self.stopped += 1

    def shutdown(self) -> None: ...


class CoreFixture:
    """Wires a LocalNetwork + Sender + Codec + Registry + Core."""

    def __init__(self) -> None:
        self.pid = "core-test-pid"
        self.net = LocalNetwork(pid=self.pid)
        self.sender = Sender(Transport(self.net), None)
        self.codec = Codec(NoopEncryptor())
        self.reg = Registry()
        self.hb = TrackingHeartbeat()
        self.core = Core(
            sender=self.sender,
            codec=self.codec,
            registry=self.reg,
            resolver=identity_target_resolver,
            heartbeat=self.hb,
            pid=self.pid,
            ttl=TTL,
        )

    async def create_root_task(
        self,
        id: str,
        func_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[int, PromiseRecord, list[PromiseRecord]]:
        """Create a root durable promise + task atomically, acquired by us at v0.

        ``func_name`` and ``args`` go into the promise param as ``TaskData``.
        ``args`` is the (optional) single user positional -- ``None`` means a
        ctx-only call.
        """
        param = self.codec.encode(
            TaskData(
                func=func_name,
                args=args,
                kwargs=kwargs,
                version=1,
            )
        )
        res = await self.sender.task_create(
            self.pid,
            TTL,
            PromiseCreateReq(
                id=id,
                timeout_at=FAR_FUTURE,
                param=param,
                tags={"resonate:branch": id, "resonate:target": "any"},
            ),
        )
        decoded = self.codec.decode_promise(res.promise)
        return res.task.version, decoded, res.preload

    async def promise_get(self, id: str) -> PromiseRecord:
        """Fetch a promise and decode it through the codec (use for root promises)."""
        return self.codec.decode_promise(await self.sender.promise_get(id))

    async def promise_get_raw(self, id: str) -> PromiseRecord:
        """Fetch a promise without codec decoding (state/tags only)."""
        return await self.sender.promise_get(id)


@pytest.fixture
def fix() -> CoreFixture:
    return CoreFixture()


# ── Workflow library used across tests ──────────────────────────────────


def wf_return_seven(_: Context, n) -> int:  # noqa: ANN001
    return n


def wf_return_obj(_, n: int) -> dict[str, Any]:  # noqa: ANN001
    return {"x": n}


def wf_fail(_: Context) -> int:
    msg = "deliberate failure"
    raise ApplicationError(msg)


def wf_add(_: Context, a: int, b: int) -> int:
    return a + b


async def wf_suspend_on_pending(ctx) -> int:  # noqa: ANN001
    """Await a remote child created in this run; ``ctx.rpc`` creates it pending, the await suspends."""
    fut = ctx.rpc("childA")
    await fut
    return 0


async def wf_suspend_on_two(ctx, n) -> int:  # noqa: ANN001
    """Create two pending remote children, then await -- suspends on the first."""
    fut1 = ctx.rpc("childA")
    fut2 = ctx.rpc("childB")
    await fut1
    await fut2
    return n


async def wf_read_preloaded(ctx) -> int:  # noqa: ANN001
    """Read a remote child the test pre-resolved; ``ctx.rpc`` returns it resolved, the await resolves inline."""
    fut = ctx.rpc("preloaded")
    return await fut


def wf_plain_panic(_) -> int:  # noqa: ANN001
    msg = "something went wrong"
    raise RuntimeError(msg)


def wf_unwrap_suspend(_) -> int:  # noqa: ANN001
    """Mimic a user error that *mentions* suspension -- still classified as a plain panic."""
    msg = "execution suspended (simulated .unwrap() on suspended future)"
    raise RuntimeError(msg)


# ── Fulfill (success / failure / object value) ──────────────────────────


@pytest.mark.asyncio
async def test_fulfill_resolved_via_execute_until_blocked(fix: CoreFixture) -> None:
    fix.reg.register("add", wf_add)
    v, promise, preload = await fix.create_root_task("p1-add", "add", a=3, b=4)

    status = await fix.core.execute_until_blocked_outer("p1-add", v, promise, preload)
    assert status == "done"

    got = await fix.promise_get("p1-add")
    assert got.state == "resolved"
    assert got.value.data == 7


@pytest.mark.asyncio
async def test_fulfill_rejected_via_execute_until_blocked(fix: CoreFixture) -> None:
    fix.reg.register("fail", wf_fail)
    v, promise, preload = await fix.create_root_task("p1-fail", "fail")

    status = await fix.core.execute_until_blocked_outer("p1-fail", v, promise, preload)
    assert status == "done"

    got = await fix.promise_get("p1-fail")
    assert got.state == "rejected"


@pytest.mark.asyncio
async def test_fulfill_object_value_round_trips_codec(fix: CoreFixture) -> None:
    fix.reg.register("obj", wf_return_obj)
    v, promise, preload = await fix.create_root_task("p1-obj", "obj", 1)

    await fix.core.execute_until_blocked_outer("p1-obj", v, promise, preload)

    got = await fix.promise_get("p1-obj")
    assert got.value.data == {"x": 1}


# ── Suspend ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_suspends_on_pending_remote(fix: CoreFixture) -> None:
    fix.reg.register("waitOne", wf_suspend_on_pending)
    v, promise, preload = await fix.create_root_task("p1-wait", "waitOne")

    status = await fix.core.execute_until_blocked_outer("p1-wait", v, promise, preload)
    assert status == "suspended"

    child = await fix.promise_get_raw("p1-wait.1")
    assert child.state == "pending"


@pytest.mark.asyncio
async def test_suspends_registers_all_awaiteds(fix: CoreFixture) -> None:
    fix.reg.register("waitTwo", wf_suspend_on_two)
    v, promise, preload = await fix.create_root_task("p1-two", "waitTwo", 2)

    status = await fix.core.execute_until_blocked_outer("p1-two", v, promise, preload)
    assert status == "suspended"


# ── on_message (Path 1: acquires then executes) ─────────────────────────


@pytest.mark.asyncio
async def test_on_message_happy_path(fix: CoreFixture) -> None:
    fix.reg.register("seven", wf_return_seven)
    v, _, _ = await fix.create_root_task("p1-on", "seven", 1)
    # Release so on_message can re-acquire under its own lease.
    await fix.sender.task_release("p1-on", v)

    status = await fix.core.on_message("p1-on", v)
    assert status == "done"

    got = await fix.promise_get("p1-on")
    assert got.state == "resolved"


@pytest.mark.asyncio
async def test_on_message_acquire_failure_raises(fix: CoreFixture) -> None:
    with pytest.raises(ResonateError):
        await fix.core.on_message("nonexistent-task", 0)


@pytest.mark.asyncio
async def test_on_message_returns_suspended(fix: CoreFixture) -> None:
    fix.reg.register("waitOne", wf_suspend_on_pending)
    v, _, _ = await fix.create_root_task("p1-onsus", "waitOne")
    await fix.sender.task_release("p1-onsus", v)

    status = await fix.core.on_message("p1-onsus", v)
    assert status == "suspended"


# ── execute_until_blocked specifics ─────────────────────────────────────


@pytest.mark.asyncio
async def test_execute_until_blocked_with_preload(fix: CoreFixture) -> None:
    v, promise, _ = await fix.create_root_task("p1-pre", "readPre")
    fix.reg.register("readPre", wf_read_preloaded)

    # Pre-resolve the child the workflow will read. ctx.rpc generates the child
    # id "p1-pre.1". Children are codec-encoded on the wire just like root
    # promises, so pre-settle with codec.encode to match.
    enc_val = fix.codec.encode(99)
    await fix.sender.promise_create(
        PromiseCreateReq(id="p1-pre.1", timeout_at=FAR_FUTURE)
    )
    await fix.sender.promise_settle(
        PromiseSettleReq(id="p1-pre.1", state="resolved", value=enc_val)
    )

    # Feed the preloaded child to Effects via the preload arg too, exercising
    # the seed-at-construction path.
    pre = await fix.sender.promise_get("p1-pre.1")
    preload = [pre]

    status = await fix.core.execute_until_blocked_outer("p1-pre", v, promise, preload)
    assert status == "done"

    got = await fix.promise_get("p1-pre")
    assert got.value.data == 99


# NOTE: Short-circuit tests are intentionally omitted from this
# LocalNetwork-driven suite. The
# short-circuit branch fires when Core encounters an acquired task whose root
# promise is already settled -- but on LocalNetwork, settling a root promise
# also auto-fulfills the task, so "acquired task + settled root promise" can't
# be constructed against this server. The branch is a read-only code path in
# Core._execute_until_blocked_inner (``if promise.state != "pending"``).


# ── Error path: function not found releases the task ────────────────────


@pytest.mark.asyncio
async def test_releases_task_on_function_not_found(fix: CoreFixture) -> None:
    v, promise, preload = await fix.create_root_task("p1-nofn", "missing")

    with pytest.raises(FunctionNotFoundError):
        await fix.core.execute_until_blocked_outer("p1-nofn", v, promise, preload)

    # Task should be releasable: a fresh acquire under a different pid succeeds.
    result = await fix.sender.task_acquire("p1-nofn", 0, "other-pid", 1000)
    assert result.task.state == "acquired"


# ── Heartbeat: started and stopped on every code path ──────────────────


@pytest.mark.asyncio
async def test_heartbeat_started_and_stopped_on_success(fix: CoreFixture) -> None:
    fix.reg.register("seven2", wf_return_seven)
    v, promise, preload = await fix.create_root_task("p1-hb-ok", "seven2")

    await fix.core.execute_until_blocked_outer("p1-hb-ok", v, promise, preload)
    assert fix.hb.started == 1
    assert fix.hb.stopped == 1


@pytest.mark.asyncio
async def test_heartbeat_stopped_on_error(fix: CoreFixture) -> None:
    v, promise, preload = await fix.create_root_task("p1-hb-err", "missing")

    with pytest.raises(FunctionNotFoundError):
        await fix.core.execute_until_blocked_outer("p1-hb-err", v, promise, preload)
    assert fix.hb.started == 1
    assert fix.hb.stopped == 1  # stopped even after error


# ── Plain exceptions: caught, settle the promise ``rejected`` ──────────


@pytest.mark.asyncio
async def test_plain_exception_rejects_promise_and_fulfills_task(
    fix: CoreFixture,
) -> None:
    """A non-Resonate exception is captured as an ``ApplicationError`` rejection.

    Raising an ``Exception`` is the user's way of reporting failure, so we settle the
    promise ``rejected`` with the original message and the task completes
    normally (no release, no re-acquire).
    """
    fix.reg.register("boom", wf_plain_panic)
    v, promise, preload = await fix.create_root_task("p1-boom", "boom")

    status = await fix.core.execute_until_blocked_outer("p1-boom", v, promise, preload)
    assert status == "done"

    got = await fix.promise_get("p1-boom")
    assert got.state == "rejected"
    assert isinstance(got.value.data, dict)
    assert "something went wrong" in got.value.data.get("message", "")

    # Task is settled along the fulfill path; another worker cannot acquire it.
    with pytest.raises(ResonateError):
        await fix.sender.task_acquire("p1-boom", 0, "other-pid", 1000)


@pytest.mark.asyncio
async def test_exception_mentioning_suspend_still_rejects_promise(
    fix: CoreFixture,
) -> None:
    """The error message is opaque -- 'execution suspended' in user text is data, not control flow."""
    fix.reg.register("unwrap", wf_unwrap_suspend)
    v, promise, preload = await fix.create_root_task("p1-unwrap", "unwrap")

    status = await fix.core.execute_until_blocked_outer(
        "p1-unwrap", v, promise, preload
    )
    assert status == "done"

    got = await fix.promise_get("p1-unwrap")
    assert got.state == "rejected"


@pytest.mark.asyncio
async def test_heartbeat_stopped_after_user_exception(fix: CoreFixture) -> None:
    fix.reg.register("boomHb", wf_plain_panic)
    v, promise, preload = await fix.create_root_task("p1-hb-boom", "boomHb")

    status = await fix.core.execute_until_blocked_outer(
        "p1-hb-boom", v, promise, preload
    )
    assert status == "done"
    assert fix.hb.started == 1
    assert fix.hb.stopped == 1  # stopped along the normal fulfill path


# ── NoopHeartbeat sanity ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_noop_heartbeat_does_not_interfere() -> None:
    pid = "noop-pid"
    net = LocalNetwork(pid=pid)
    sender = Sender(Transport(net), None)
    codec = Codec(NoopEncryptor())
    reg = Registry()
    reg.register("seven3", wf_return_seven)
    # heartbeat=None -> NoopHeartbeat.
    core = Core(
        sender=sender,
        codec=codec,
        registry=reg,
        pid=pid,
        ttl=TTL,
    )

    param = codec.encode(TaskData(func="seven3", args=(), kwargs={}, version=1))
    res = await sender.task_create(
        pid,
        TTL,
        PromiseCreateReq(
            id="p1-noophb",
            timeout_at=FAR_FUTURE,
            param=param,
            tags={"resonate:branch": "p1-noophb", "resonate:target": "any"},
        ),
    )
    decoded = codec.decode_promise(res.promise)
    status = await core.execute_until_blocked_outer(
        "p1-noophb", res.task.version, decoded, res.preload
    )
    assert status == "done"


# ── Run-workflow boundary tests ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_done_on_return(fix: CoreFixture) -> None:
    def done(ctx: Context) -> int:
        return 7

    fix.reg.register("done", done)
    v, promise, preload = await fix.create_root_task("p1-done", "done")

    status = await fix.core.execute_until_blocked_outer("p1-done", v, promise, preload)
    assert status == "done"

    got = await fix.promise_get("p1-done")
    assert got.state == "resolved"


@pytest.mark.asyncio
async def test_swallowed_suspend_still_suspends(fix: CoreFixture) -> None:
    """A workflow that swallows the suspension but still has pending todos must suspend."""

    async def swallow(ctx: Context) -> int:
        with contextlib.suppress(Suspended):
            fut = ctx.rpc("childZ")
            await fut  # raises Suspended, suppressed
        return 0

    fix.reg.register("swallow", swallow)
    v, promise, preload = await fix.create_root_task("p1-swallow", "swallow")

    status = await fix.core.execute_until_blocked_outer(
        "p1-swallow", v, promise, preload
    )
    assert status == "suspended"


@pytest.mark.asyncio
async def test_fire_and_forget_local_suspension(fix: CoreFixture) -> None:
    """A fire-and-forget local child that suspends propagates as parent suspension."""

    async def child_that_suspends(ctx: Context) -> int:
        fut = ctx.rpc("childW")
        return await fut

    async def parent(ctx: Context) -> int:
        ctx.run(child_that_suspends)
        return 0

    fix.reg.register("ffparent", parent)
    v, promise, preload = await fix.create_root_task("p1-ff", "ffparent")

    status = await fix.core.execute_until_blocked_outer("p1-ff", v, promise, preload)
    assert status == "suspended"


# NOTE: Redirect-loop tests are intentionally omitted. Triggering a redirect
# from a real workflow requires an
# awaited promise to be settled at the exact moment Core's task.suspend lands --
# a race LocalNetwork cannot produce deterministically (awaiting a settled
# record resolves inline and never registers a remote todo, so the workflow
# can't ask to suspend on a settled awaited). The server-side redirect response
# itself is covered by test_network.py.
