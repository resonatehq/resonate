"""What :class:`~resonate.core.Core` does to a task, in what order.

``Core``'s release-on-failure guarantee -- "a fulfill or suspend that fails is
always followed by a release" -- is a claim about *sequence*, and a return
value cannot express it. Before the :class:`~resonate.send.TaskLifecycle` port
existed there was nothing to observe it with: ``Core`` held a concrete
``Sender | None``, so the only ways to reach these paths were a live server or
a subclass of the real sender.

Now the port is four methods, and a recorder makes the sequence an assertion.
"""

from __future__ import annotations

import msgspec
import pytest

from resonate.codec import Codec, NoopEncryptor
from resonate.core import Core, identity_target_resolver
from resonate.error import ServerError
from resonate.registry import Registry
from resonate.send import TaskAcquireResult
from resonate.testing import (
    FAR_FUTURE,
    RecordingTaskLifecycle,
    UnusedFencing,
    pending_promise,
    resolved_promise,
)
from resonate.types import PromiseRecord, TaskData, TaskRecord, Value

TTL = 10_000


#: A root promise as ``execute_until_blocked_outer`` expects it: already run
#: through ``Codec.decode_promise``, so ``param.data`` holds plain builtins.
def _root(func: str = "leaf") -> PromiseRecord:
    return pending_promise(
        "p1",
        param=Value(data={"func": func, "args": [], "kwargs": {}, "version": 1}),
    )


def _encoded_root(func: str = "leaf") -> PromiseRecord:
    """Return the same root in wire form -- what ``task.acquire`` hands back."""
    return msgspec.structs.replace(
        _root(func),
        param=Codec(NoopEncryptor()).encode(
            TaskData(func=func, args=(), kwargs={}, version=1)
        ),
    )


def _settled_root(value: int) -> PromiseRecord:
    """Return a resolved root that still carries a decodable param."""
    return msgspec.structs.replace(
        resolved_promise("p1", value),
        param=Value(data={"func": "leaf", "args": [], "kwargs": {}, "version": 1}),
    )


def _core(lifecycle: RecordingTaskLifecycle, registry: Registry) -> Core:
    return Core(
        sender=lifecycle,
        # A pure leaf performs no durable ops, so the fencing port must stay
        # untouched -- and this stand-in proves it.
        fencing=UnusedFencing(),
        codec=Codec(NoopEncryptor()),
        registry=registry,
        resolver=identity_target_resolver,
        pid="lifecycle-test",
        ttl=TTL,
    )


def _registry_with_leaf() -> Registry:
    registry = Registry()

    async def leaf(ctx: object) -> int:
        return 7

    registry.register("leaf", leaf)
    return registry


def _registry_with_failing_leaf() -> Registry:
    registry = Registry()

    async def boom(ctx: object) -> int:
        msg = "user failure"
        raise RuntimeError(msg)

    registry.register("leaf", boom)
    return registry


# ── The happy path ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_success_fulfills_and_never_releases() -> None:
    lifecycle = RecordingTaskLifecycle()
    core = _core(lifecycle, _registry_with_leaf())

    status = await core.execute_until_blocked_outer("t1", 1, _root(), [])

    assert status == "done"
    assert lifecycle.calls == ["task_fulfill"]
    assert lifecycle.fulfilled[0].state == "resolved"


@pytest.mark.asyncio
async def test_user_failure_still_fulfills_with_a_rejection() -> None:
    """A user error is a *result*, not a platform failure: fulfill, do not release."""
    lifecycle = RecordingTaskLifecycle()
    core = _core(lifecycle, _registry_with_failing_leaf())

    status = await core.execute_until_blocked_outer("t1", 1, _root(), [])

    assert status == "done"
    assert lifecycle.calls == ["task_fulfill"]
    assert lifecycle.fulfilled[0].state == "rejected"


# ── Release on failure -- the ordering guarantee ───────────────────


@pytest.mark.asyncio
async def test_failed_fulfill_is_followed_by_a_release() -> None:
    lifecycle = RecordingTaskLifecycle(
        fail_on={"task_fulfill"}, error=ServerError(503, "down")
    )
    core = _core(lifecycle, _registry_with_leaf())

    with pytest.raises(ServerError) as exc:
        await core.execute_until_blocked_outer("t1", 1, _root(), [])

    # The order *is* the contract: the release must come after the failure.
    assert lifecycle.calls == ["task_fulfill", "task_release"]
    # And the caller sees the original ResonateError, not the PlatformError
    # wrapper the SDK used internally to carry it out.
    assert exc.value.code == 503


@pytest.mark.asyncio
async def test_a_failing_release_does_not_mask_the_original_error() -> None:
    """Both calls fail; the caller must still learn why the *first* one did."""
    lifecycle = RecordingTaskLifecycle(
        fail_on={"task_fulfill", "task_release"}, error=ServerError(503, "down")
    )
    core = _core(lifecycle, _registry_with_leaf())

    with pytest.raises(ServerError) as exc:
        await core.execute_until_blocked_outer("t1", 1, _root(), [])

    assert lifecycle.calls == ["task_fulfill", "task_release"]
    assert exc.value.code == 503


@pytest.mark.asyncio
async def test_function_not_found_releases_without_fulfilling() -> None:
    """Nothing ran, so nothing may be settled -- release and let redelivery retry."""
    lifecycle = RecordingTaskLifecycle()
    core = _core(lifecycle, Registry())

    with pytest.raises(Exception, match="function not found"):
        await core.execute_until_blocked_outer("t1", 1, _root("missing"), [])

    assert lifecycle.calls == ["task_release"]
    assert lifecycle.released == [("t1", 1)]


# ── Acquire ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_on_message_acquires_before_executing() -> None:
    lifecycle = RecordingTaskLifecycle(
        acquire=TaskAcquireResult(
            task=TaskRecord(id="t1", state="acquired", version=1),
            promise=_encoded_root(),
            preload=[],
        )
    )
    core = _core(lifecycle, _registry_with_leaf())

    status = await core.on_message("t1", 1)

    assert status == "done"
    assert lifecycle.calls == ["task_acquire", "task_fulfill"]


@pytest.mark.asyncio
async def test_undecodable_root_promise_releases_the_lease_immediately() -> None:
    """A corrupt root must not hold the lease until TTL expiry."""
    corrupt = PromiseRecord(
        id="p1",
        state="pending",
        timeout_at=FAR_FUTURE,
        param=Value(data="!!! not base64 !!!"),
        value=Value(),
        tags={},
    )
    lifecycle = RecordingTaskLifecycle(
        acquire=TaskAcquireResult(
            task=TaskRecord(id="t1", state="acquired", version=4),
            promise=corrupt,
            preload=[],
        )
    )
    core = _core(lifecycle, _registry_with_leaf())

    with pytest.raises(Exception, match="base64"):
        await core.on_message("t1", 1)

    assert lifecycle.calls == ["task_acquire", "task_release"]
    # Released at the *acquired* version, which is what the server fenced on.
    assert lifecycle.released == [("t1", 4)]


# ── Short-circuits: no lifecycle call at all ───────────────────────


@pytest.mark.asyncio
async def test_a_not_yet_due_timer_task_touches_nothing() -> None:
    """Dropped, deliberately: no fulfill (early wake), no release (redelivery spin)."""
    lifecycle = RecordingTaskLifecycle()
    core = _core(lifecycle, _registry_with_leaf())
    timer = pending_promise("timer-1", tags={"resonate:timer": "true"})

    status = await core.execute_until_blocked_outer("t1", 1, timer, [])

    assert status == "suspended"
    assert lifecycle.calls == []


@pytest.mark.asyncio
async def test_an_already_settled_root_fulfills_without_running_the_function() -> None:
    ran = {"n": 0}
    registry = Registry()

    async def leaf(ctx: object) -> int:
        ran["n"] += 1
        return 7

    registry.register("leaf", leaf)

    lifecycle = RecordingTaskLifecycle()
    core = _core(lifecycle, registry)

    status = await core.execute_until_blocked_outer("t1", 1, _settled_root(3), [])

    assert status == "done"
    assert ran["n"] == 0
    assert lifecycle.calls == ["task_fulfill"]
