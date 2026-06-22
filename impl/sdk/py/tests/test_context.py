"""Behaviour tests for :mod:`resonate.context` -- focused on ``Context.run``.

``run`` executes the child inline in async-await form: it returns the value
directly, raises on rejection, and raises
:class:`~resonate.error.Suspended` when a dependency is still pending.

The harness builds a root :class:`~resonate.context.Context` over a real
:class:`~resonate.network.LocalNetwork` (as :mod:`tests.test_durable` does), so
``create_promise``/``settle_promise`` exercise the actual durability boundary.
"""

from __future__ import annotations

import asyncio
import threading
from contextlib import contextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, patch

import msgspec
import pytest

from resonate import now_ms
from resonate.codec import Codec, NoopEncryptor, _encode_error
from resonate.context import Context, Opts, _hash_id
from resonate.dependencies import DependencyMap
from resonate.durable import DurableFunction
from resonate.effects import ResonateEffects
from resonate.error import (
    ApplicationError,
    FunctionNotFoundError,
    PlatformError,
    SerializationError,
    Suspended,
)
from resonate.network import LocalNetwork
from resonate.network.local import Task
from resonate.registry import Registry
from resonate.retry import Constant, Never
from resonate.send import Sender
from resonate.transport import Transport
from resonate.types import PromiseRecord, TaskData, Value

if TYPE_CHECKING:
    from collections.abc import Iterator

    from resonate.retry import RetryPolicy

I64_MAX = 2**63 - 1


# =============================================================================
# Harness
# =============================================================================


def _codec() -> Codec:
    return Codec(NoopEncryptor())


def _root(
    preload: list[PromiseRecord] | None = None,
    *,
    timeout_at: int = I64_MAX,
    deps: DependencyMap | None = None,
    retry_policy: RetryPolicy | None = None,
    registry: Registry | None = None,
) -> Context:
    """Build a root ``Context`` over a fresh ``LocalNetwork``.

    ``retry_policy`` defaults to ``None`` -> ``Never`` (the engine-layer default),
    so a failing pure leaf settles on the first attempt; pass a policy to exercise
    the inherited-default path that ``ctx.run`` children resolve against.

    ``registry`` defaults to ``None`` -> an empty registry, so the by-name
    durable ops resolve to not-found; pass one (with functions registered) to
    exercise by-name ``run`` / by-object ``rpc`` resolution.
    """
    net = LocalNetwork()
    net.state.tasks["root"] = Task(
        id="root",
        state="acquired",
        version=1,
        pid="test-pid",
        ttl=60_000,
        resumes=set(),
    )
    sender = Sender(Transport(net), None)
    effects = ResonateEffects(sender, _codec(), "root", 1, preload or [])
    return Context.root(
        id="root",
        origin_id="root",
        prefix_id="root",
        timeout_at=timeout_at,
        func_name="root",
        effects=effects,
        target_resolver=lambda target: target or "",
        deps=deps or DependencyMap(),
        retry_policy=retry_policy,
        registry=registry,
    )


def _resolved(id: str, value: Any) -> PromiseRecord:
    """Build a pre-settled *resolved* record, wire-encoded for the preload cache."""
    return PromiseRecord(
        id=id,
        state="resolved",
        timeout_at=I64_MAX,
        param=Value(),
        value=_codec().encode(value),
        tags={},
        created_at=0,
        settled_at=1,
    )


def _rejected(id: str, message: str) -> PromiseRecord:
    """Build a pre-settled *rejected* record carrying an encoded error payload."""
    return PromiseRecord(
        id=id,
        state="rejected",
        timeout_at=I64_MAX,
        param=Value(),
        value=_codec().encode(_encode_error(ApplicationError(message))),
        tags={},
        created_at=0,
        settled_at=1,
    )


# =============================================================================
# Durable functions under test (leaves and workflows)
# =============================================================================


async def double(ctx: Context, x: int) -> int:
    """Double ``x`` -- a leaf that ignores the injected Context."""
    return x * 2


async def add(ctx: Context, a: int, b: int) -> int:
    """Add ``a`` and ``b``, ignoring the injected Context."""
    return a + b


async def beat(ctx: Context) -> str:
    """Return a constant -- a ctx-only leaf (no user arguments)."""
    return "ok"


async def failing(ctx: Context) -> int:
    msg = "denied"
    raise ApplicationError(msg)


class BookingError(Exception):
    """A plain domain exception -- deliberately NOT a Resonate error."""


async def failing_plain(ctx: Context) -> int:
    msg = "card declined"
    raise BookingError(msg)


class Point(msgspec.Struct, frozen=True):
    x: int
    y: int


async def sum_point(ctx: Context, p: Point) -> int:
    return p.x + p.y


class _Resource:
    """A live object msgspec cannot encode (it holds a ``threading.Lock``).

    Stands in for the kind of argument -- a client, a connection, a handle --
    that a local ``ctx.run`` must now accept, since its args are neither
    serialized into the promise param nor coerced against their annotation.
    """

    def __init__(self, label: str) -> None:
        self.label = label
        self.lock = threading.Lock()


async def parent_workflow(ctx: Context, x: int) -> int:
    """Run nested leaves on this workflow's own child context."""
    a = await ctx.run(double, x)
    b = await ctx.run(double, a)
    return a + b


async def blocks_on_remote(ctx: Context) -> int:
    """Mimic ``ctx.rpc``/``sleep``/``promise`` on a *pending* promise.

    Those register the awaited promise id and unwind via ``Suspended``;
    until they exist, this stands in so ``run``'s suspension path is exercised.
    """
    ctx._state.spawned_remote.append("remote-dep")
    raise Suspended


async def fire_and_forget(ctx: Context) -> int:
    """Complete normally but leave a pending remote child registered."""
    ctx._state.spawned_remote.append("ff-dep")
    return 7


# =============================================================================
# Context plumbing (next_id, child linkage, child_timeout)
# =============================================================================


def test_next_id_sequential() -> None:
    ctx = _root()
    assert ctx._next_id() == "root.1"
    assert ctx._next_id() == "root.2"
    assert ctx._next_id() == "root.3"


def test_child_parent_is_current_id() -> None:
    # Regression: child.parent_id must be the *current* id, not the parent's
    # own parent_id.
    ctx = _root()
    child = ctx._child("root.1", "fn", I64_MAX)
    assert child.info.parent_id == "root"
    assert child.info.origin_id == "root"
    assert child.info.branch_id == "root.1"


def test_child_timeout_caps_to_parent() -> None:
    cap = now_ms() + 1_000
    ctx = _root(timeout_at=cap)
    # A requested deadline beyond the parent's is capped to the parent.
    assert ctx._child_timeout(timedelta(days=1)) == cap
    # A nearer deadline is honoured.
    assert ctx._child_timeout(timedelta(milliseconds=500)) <= cap


# =============================================================================
# get_dependency: type-keyed lookup into the shared DependencyMap
#
# ``Context.get_dependency`` is a thin pass-through to ``deps.get(type)`` -- the
# same map ``Resonate.with_dependency`` populates. It returns the stored value
# by concrete type and surfaces the map's ``KeyError`` when absent.
# =============================================================================


class _Config:
    def __init__(self, value: str) -> None:
        self.value = value


def test_get_dependency_returns_stored_value() -> None:
    deps = DependencyMap()
    cfg = _Config("hello")
    deps.insert(cfg)
    ctx = _root(deps=deps)
    # Keyed by concrete type -- the very object that was inserted comes back.
    assert ctx.get_dependency(_Config) is cfg
    assert ctx.get_dependency(_Config).value == "hello"


def test_get_dependency_missing_raises_keyerror() -> None:
    ctx = _root(deps=DependencyMap())
    with pytest.raises(KeyError):
        ctx.get_dependency(_Config)


def test_get_dependency_shared_with_child_context() -> None:
    # Children inherit the same ``deps`` map, so a dependency is visible on every
    # context in the tree, not just the root.
    deps = DependencyMap()
    cfg = _Config("shared")
    deps.insert(cfg)
    ctx = _root(deps=deps)
    child = ctx._child("root.1", "fn", I64_MAX)
    assert child.get_dependency(_Config) is cfg


# =============================================================================
# run: leaves -- create, execute, settle, return
# =============================================================================


@pytest.mark.asyncio
async def test_run_leaf_returns_and_settles_resolved() -> None:
    ctx = _root()
    assert await ctx.run(double, 21) == 42
    record = ctx._state.effects.cache["root.1"]
    assert record.state == "resolved"
    assert record.value.data == 42


@pytest.mark.asyncio
async def test_run_ctx_only_function() -> None:
    ctx = _root()
    assert await ctx.run(beat) == "ok"
    assert ctx._state.effects.cache["root.1"].state == "resolved"


@pytest.mark.asyncio
async def test_run_passes_live_struct_arg() -> None:
    # A local child receives the live object as-is (no serialization boundary),
    # so a struct argument reaches the function unchanged.
    ctx = _root()
    assert await ctx.run(sum_point, Point(x=3, y=4)) == 7


@pytest.mark.asyncio
async def test_run_sequential_child_ids() -> None:
    ctx = _root()
    assert await ctx.run(double, 2) == 4  # root.1
    assert await ctx.run(double, 3) == 6  # root.2
    assert ctx._state.effects.cache["root.1"].value.data == 4
    assert ctx._state.effects.cache["root.2"].value.data == 6


@pytest.mark.asyncio
async def test_run_rejects_non_callable() -> None:
    not_callable: Any = 42
    ctx = _root()
    with pytest.raises(ApplicationError):
        await ctx.run(not_callable)


# =============================================================================
# run: non-serializable arguments
#
# A local child's args are never written to its promise param (the param is
# write-only -- nothing reads it back) and never coerced against their
# annotation, so ``ctx.run`` accepts any Python object. The return value still
# round-trips, so it must stay serializable.
# =============================================================================


@pytest.mark.asyncio
async def test_run_accepts_non_serializable_unannotated_arg() -> None:
    async def use_resource(ctx: Context, r: Any) -> str:
        return r.label

    resource = _Resource("db")
    ctx = _root()
    # Would raise SerializationError at create_promise before this change.
    assert await ctx.run(use_resource, resource) == "db"


@pytest.mark.asyncio
async def test_run_accepts_non_serializable_annotated_arg_verbatim() -> None:
    # Annotated with its own non-msgspec class: coercion is skipped for local
    # children, so the *same* live object reaches the function rather than being
    # rejected (or copied) by ``msgspec.convert``.
    received: list[_Resource] = []

    async def use_resource(ctx: Context, r: _Resource) -> str:
        received.append(r)
        return r.label

    resource = _Resource("cache")
    ctx = _root()
    assert await ctx.run(use_resource, resource) == "cache"
    assert received[0] is resource  # verbatim identity, not coerced


@pytest.mark.asyncio
async def test_run_local_child_param_is_empty() -> None:
    # The arg is never stored in the child's param, even when serializable: the
    # child executes from the in-memory payload and recovery reads the settled
    # value, never the param.
    ctx = _root()
    await ctx.run(double, 21)
    assert ctx._state.effects.cache["root.1"].param == Value()


@pytest.mark.asyncio
async def test_rpc_still_rejects_non_serializable_arg() -> None:
    # Global scope is unchanged: a different worker reconstructs an ``rpc`` call
    # from the persisted param, so a non-serializable arg still fails to encode.
    # Inside a durable execution that encode failure is a platform error (the
    # task must be released, not rejected), carrying the SerializationError as
    # its cause.
    ctx = _root()
    with pytest.raises(PlatformError) as excinfo:
        await ctx.rpc("remote_fn", _Resource("db"))
    assert isinstance(excinfo.value.__cause__, SerializationError)


# =============================================================================
# run: error handling
# =============================================================================


@pytest.mark.asyncio
async def test_run_function_error_propagates_and_settles_rejected() -> None:
    ctx = _root()
    with pytest.raises(ApplicationError, match="denied"):
        await ctx.run(failing)
    assert ctx._state.effects.cache["root.1"].state == "rejected"


@pytest.mark.asyncio
async def test_run_plain_exception_preserves_type_and_settles_rejected() -> None:
    """A ``ctx.run`` child that raises a plain ``Exception`` settles rejected.

    The original exception type crosses the boundary: the codec pickles it
    best-effort, so a picklable, importable class (``BookingError`` here) is
    reconstructed verbatim on the awaiting side. The local promise still
    settles ``rejected`` and the parent observes the failure via
    ``await ctx.run(...)`` -- now as the original ``BookingError`` rather than a
    flattened ``ApplicationError``.
    """
    ctx = _root()
    with pytest.raises(BookingError, match="card declined"):
        await ctx.run(failing_plain)
    assert ctx._state.effects.cache["root.1"].state == "rejected"


# =============================================================================
# run: idempotent recovery (a pre-settled promise skips execution)
# =============================================================================


@pytest.mark.asyncio
async def test_run_presettled_resolved_skips_execution() -> None:
    calls = 0

    async def counted(ctx: Context, x: int) -> int:
        nonlocal calls
        calls += 1
        return x

    ctx = _root([_resolved("root.1", 99)])
    assert await ctx.run(counted, 1) == 99  # value from the pre-settled record
    assert calls == 0  # the function body never ran


@pytest.mark.asyncio
async def test_run_presettled_rejected_raises_without_execution() -> None:
    calls = 0

    async def counted(ctx: Context, x: int) -> int:
        nonlocal calls
        calls += 1
        return x

    ctx = _root([_rejected("root.1", "stored failure")])
    with pytest.raises(ApplicationError, match="stored failure"):
        await ctx.run(counted, 1)
    assert calls == 0


@pytest.mark.asyncio
async def test_run_replay_does_not_reinvoke() -> None:
    calls = 0

    async def counted(ctx: Context, x: int) -> int:
        nonlocal calls
        calls += 1
        return x

    ctx = _root()
    assert await ctx.run(counted, 7) == 7
    assert calls == 1


class _RecoveredPoint(msgspec.Struct, frozen=True):
    x: int
    y: int


async def _make_point(ctx: Context, x: int, y: int) -> _RecoveredPoint:
    return _RecoveredPoint(x=x, y=y)


async def _make_pair(ctx: Context, a: int, b: int) -> tuple[int, str]:
    return a, str(b)


@pytest.mark.asyncio
async def test_run_recovery_coerces_return_to_declared_struct() -> None:
    # The settled child value was stored as plain builtins (a dict). On recovery
    # ctx.run coerces it back to the declared return type, so the parent observes
    # the same in-memory object the live path would have produced -- not a dict.
    ctx = _root([_resolved("root.1", _RecoveredPoint(x=3, y=4))])
    result = await ctx.run(_make_point, 3, 4)
    assert result == _RecoveredPoint(x=3, y=4)
    assert isinstance(result, _RecoveredPoint)


@pytest.mark.asyncio
async def test_run_recovery_coerces_return_to_tuple() -> None:
    # JSON has no tuple, so the settled value round-trips as a list; the return
    # annotation (tuple[int, str]) drives coercion back to a real tuple, matching
    # the live path (which would have returned a tuple).
    ctx = _root([_resolved("root.1", (5, "6"))])
    result = await ctx.run(_make_pair, 5, 6)
    assert result == (5, "6")
    assert isinstance(result, tuple)


# =============================================================================
# run: the LIVE path coerces the return too (symmetric with recovery)
#
# Return coercion is the mirror of argument coercion, which runs on every
# invocation (live and replay). The live (first-run) path of ``ctx.run`` must
# therefore coerce its return to the declared type just as the recovery
# short-circuit does -- otherwise a function whose return msgspec reshapes
# (``-> float`` returning an ``int``) hands back the raw object on the first run
# and the coerced object on replay, diverging silently.
# =============================================================================


async def _make_float(ctx: Context, x: int) -> float:
    # Returns an int, but the annotation declares float -- msgspec widens it.
    return x


@pytest.mark.asyncio
async def test_run_live_path_coerces_return_to_declared_type() -> None:
    ctx = _root()
    result = await ctx.run(_make_float, 3)
    # The parent observes a float, not the raw int the function returned.
    assert result == 3.0
    assert isinstance(result, float)
    # The promise stores the raw return (an int); coercion is applied only at the
    # return boundary, matching the top-level run (which stores raw and coerces
    # at the handle).
    assert ctx._state.effects.cache["root.1"].value.data == 3


@pytest.mark.asyncio
async def test_run_live_and_recovery_returns_are_identical() -> None:
    # Full parity: the same function run live vs recovered from a settled record
    # holding the raw value the live path stored (int 3) hands back an equal,
    # same-typed object on both paths.
    live = await _root().run(_make_float, 3)
    recovered = await _root([_resolved("root.1", 3)]).run(_make_float, 3)
    assert live == recovered == 3.0
    assert isinstance(live, float)
    assert isinstance(recovered, float)


@pytest.mark.asyncio
async def test_run_live_path_coerces_return_to_struct() -> None:
    # A struct return: the live path already returns the struct, but it is still
    # routed through coerce_result, so it stays the declared type -- the exact
    # object the recovery path rebuilds from the stored dict.
    ctx = _root()
    result = await ctx.run(_make_point, 3, 4)
    assert result == _RecoveredPoint(x=3, y=4)
    assert isinstance(result, _RecoveredPoint)


# =============================================================================
# run: nested workflow (structured concurrency, happy path)
# =============================================================================


@pytest.mark.asyncio
async def test_workflow_runs_nested_leaves() -> None:
    ctx = _root()
    assert await ctx.run(parent_workflow, 5) == 30  # a=10, b=20
    assert ctx._state.spawned_remote == []  # nothing pending -> no suspension
    assert ctx._state.effects.cache["root.1"].state == "resolved"
    # Nested children live under the workflow's own id.
    assert ctx._state.effects.cache["root.1.1"].value.data == 10
    assert ctx._state.effects.cache["root.1.2"].value.data == 20


# =============================================================================
# run: suspension (child blocks on a remote dependency)
# =============================================================================


@pytest.mark.asyncio
async def test_run_suspends_when_child_blocks_on_remote() -> None:
    ctx = _root()
    with pytest.raises(Suspended):
        await ctx.run(blocks_on_remote)
    # The child's todo is merged up so the task can suspend on it...
    assert ctx._state.spawned_remote == ["remote-dep"]
    # ...and the child promise is left pending (not settled).
    assert ctx._state.effects.cache["root.1"].state == "pending"


@pytest.mark.asyncio
async def test_run_suspends_when_child_completes_with_pending_remote() -> None:
    # When the function finished but left remote todos, the value is dropped
    # in favour of suspension.
    ctx = _root()
    with pytest.raises(Suspended):
        await ctx.run(fire_and_forget)
    assert ctx._state.spawned_remote == ["ff-dep"]
    assert ctx._state.effects.cache["root.1"].state == "pending"


# =============================================================================
# run: structured-concurrency propagation under suspension
#
# When a deeply nested child blocks on a remote dependency, its todo must
# travel through every intermediate ``ctx.run`` up to the root, and every
# promise on the suspension path must be left pending (not settled):
# at each level, ``take_remote_todos`` drains the child and
# extends the parent before re-raising ``Suspended``.
# =============================================================================


async def deep_inner(ctx: Context) -> int:
    """Leaf stand-in for rpc/sleep -- registers a remote dep and suspends."""
    ctx._state.spawned_remote.append("deep-dep")
    raise Suspended


async def deep_middle(ctx: Context) -> int:
    return await ctx.run(deep_inner)


async def deep_top(ctx: Context) -> int:
    return await ctx.run(deep_middle)


async def completes_then_suspends(ctx: Context) -> int:
    """Settle one child, then suspend on a sibling."""
    a = await ctx.run(double, 21)
    b = await ctx.run(blocks_on_remote)
    return a + b


async def multi_remote(ctx: Context) -> int:
    """Register multiple remote deps before suspending -- a multi-todo leaf."""
    ctx._state.spawned_remote.extend(["dep-a", "dep-b", "dep-c"])
    raise Suspended


async def parent_with_fire_and_forget(ctx: Context) -> int:
    """Spawn a child that suspends, then return without awaiting it.

    The unawaited bg task only runs once ``flush_local_work`` joins it; if
    ``ctx.run`` didn't register the task in ``spawned_locals``, its
    ``spawned_remote.append`` would happen after the parent had already
    decided to settle, and the todo would be lost.
    """
    _ = ctx.run(blocks_on_remote)
    return 99


@pytest.mark.asyncio
async def test_run_suspension_propagates_through_intermediate_workflow() -> None:
    # A grandchild that suspends must bubble its todo up through the parent
    # workflow into the root's ``spawned_remote``.
    ctx = _root()
    with pytest.raises(Suspended):
        await ctx.run(deep_middle)
    assert ctx._state.spawned_remote == ["deep-dep"]
    # Both promises along the suspension path are left pending.
    assert ctx._state.effects.cache["root.1"].state == "pending"  # middle
    assert ctx._state.effects.cache["root.1.1"].state == "pending"  # inner


@pytest.mark.asyncio
async def test_run_suspension_propagates_through_three_levels() -> None:
    # Same property at one more level of nesting: todos travel arbitrarily deep.
    ctx = _root()
    with pytest.raises(Suspended):
        await ctx.run(deep_top)
    assert ctx._state.spawned_remote == ["deep-dep"]
    assert ctx._state.effects.cache["root.1"].state == "pending"  # top
    assert ctx._state.effects.cache["root.1.1"].state == "pending"  # middle
    assert ctx._state.effects.cache["root.1.1.1"].state == "pending"  # inner


@pytest.mark.asyncio
async def test_run_completed_sibling_settles_but_parent_still_suspends() -> None:
    # The first child runs to completion and is settled, but a later child
    # suspends -- the parent must surface the suspension and stay pending,
    # even though one of its sub-promises is already resolved.
    ctx = _root()
    with pytest.raises(Suspended):
        await ctx.run(completes_then_suspends)
    assert ctx._state.spawned_remote == ["remote-dep"]
    # First child got fully settled with its computed value.
    assert ctx._state.effects.cache["root.1.1"].state == "resolved"
    assert ctx._state.effects.cache["root.1.1"].value.data == 42
    # Second child remains pending -- its body raised Suspended.
    assert ctx._state.effects.cache["root.1.2"].state == "pending"
    # Parent workflow itself stays pending: ``outcome == "suspended"`` skips
    # the settle_promise call at the bottom of ``run``.
    assert ctx._state.effects.cache["root.1"].state == "pending"


@pytest.mark.asyncio
async def test_run_merges_multiple_todos_from_single_child() -> None:
    # ``take_remote_todos`` drains the full list, not just the first entry,
    # so a child registering N pending deps surfaces all N at the parent.
    ctx = _root()
    with pytest.raises(Suspended):
        await ctx.run(multi_remote)
    assert ctx._state.spawned_remote == ["dep-a", "dep-b", "dep-c"]


@pytest.mark.asyncio
async def test_run_fire_and_forget_child_suspension_propagates() -> None:
    # Parent spawns a child via ctx.run but doesn't await it. The child's bg
    # task only runs once flush_local_work joins it; that join must pull the
    # child's todo into the parent's spawned_remote and force the parent to
    # suspend even though the body returned 99.
    #
    # This is the "function returned but left remote todos" scenario -- it
    # depends on spawned_locals being populated so flush has something to
    # wait for.
    ctx = _root()
    with pytest.raises(Suspended):
        await ctx.run(parent_with_fire_and_forget)
    assert ctx._state.spawned_remote == ["remote-dep"]
    # Parent's value (99) was dropped in favour of suspension; both promises
    # along the suspension path are left pending.
    assert ctx._state.effects.cache["root.1"].state == "pending"
    assert ctx._state.effects.cache["root.1.1"].state == "pending"


# =============================================================================
# run: structured concurrency -- unawaited child still blocks parent settlement
#
# ``ctx.run`` appends the child to ``spawned_locals``, and the parent's
# ``flush_local_work`` joins it before deciding to settle. The invariant is
# that a fire-and-forget child must not be orphaned: the parent's
# ``settle_promise`` cannot run until every child registered on the parent's
# context has reached a terminal state (settled or merged remote todos up).
# =============================================================================


async def quiet_child(ctx: Context) -> int:
    # Yield a few times so a broken impl (one that doesn't join spawned_locals)
    # would let the parent settle ahead of the child.
    for _ in range(5):
        await asyncio.sleep(0)
    return 42


async def parent_does_not_await_child(ctx: Context) -> int:
    _ = ctx.run(quiet_child)
    return 1


@pytest.mark.asyncio
async def test_run_unawaited_child_settles_before_parent() -> None:
    # Parent spawns the child via ``ctx.run`` but never awaits the returned
    # future. ``flush_local_work`` must still join the child's bg task before
    # the parent calls ``settle_promise``, so the child's settlement is
    # observed strictly before the parent's.
    ctx = _root()
    settle_order: list[str] = []
    original = ctx._state.effects.settle_promise

    async def recorder(id: str, value: Any) -> Any:
        settle_order.append(id)
        return await original(id, value)

    with patch.object(
        ctx._state.effects, "settle_promise", new=AsyncMock(side_effect=recorder)
    ):
        assert await ctx.run(parent_does_not_await_child) == 1

    # Child settled first; parent second. If ``flush_local_work`` were a no-op
    # the parent's bg would settle ahead of the still-pending child task and
    # this order would flip.
    assert settle_order == ["root.1.1", "root.1"]
    assert ctx._state.effects.cache["root.1.1"].state == "resolved"
    assert ctx._state.effects.cache["root.1.1"].value.data == 42
    assert ctx._state.effects.cache["root.1"].state == "resolved"
    assert ctx._state.effects.cache["root.1"].value.data == 1


# =============================================================================
# run: blocks until the durable promise has been created
#
# ``Context.run`` returns a Task immediately, but the underlying
# ``wait_for_signal(durable_promise_created)`` guard means that Task must not
# be observable as done -- and the function body must not run -- until
# ``effects.create_promise`` has either returned a record or raised. These
# tests pin execution at that exact boundary by gating ``create_promise`` on
# an asyncio.Event.
# =============================================================================


@contextmanager
def _gated_create_promise(ctx: Context, gate: asyncio.Event) -> Iterator[asyncio.Event]:
    entered = asyncio.Event()
    original = ctx._state.effects.create_promise

    async def gated(req: Any) -> Any:
        entered.set()
        await gate.wait()
        return await original(req)

    mock = AsyncMock(side_effect=gated)
    with patch.object(ctx._state.effects, "create_promise", new=mock):
        yield entered


@pytest.mark.asyncio
async def test_run_task_pending_while_create_promise_blocked() -> None:
    # The Task returned by ``ctx.run`` must not be ``done()`` while
    # ``create_promise`` has not yet resolved -- the ``wait_for_signal`` guard
    # parks the outer task at ``event.wait()`` until then.
    ctx = _root()
    gate = asyncio.Event()

    with _gated_create_promise(ctx, gate) as entered:
        task = ctx.run(double, 21)

        # Wait until we *know* the inner coroutine is parked inside create_promise.
        await entered.wait()

        # And the durable record is not in the cache yet.
        assert "root.1" not in ctx._state.effects.cache

        # Releasing the gate lets create_promise return, the event fire, and
        # the body run through to settlement.
        gate.set()
        assert await task == 42
        assert ctx._state.effects.cache["root.1"].state == "resolved"


@pytest.mark.asyncio
async def test_run_body_does_not_execute_before_promise_created() -> None:
    # The user-supplied function body must not be invoked until
    # ``create_promise`` has returned. If it ran earlier, a still-pending
    # promise creation could be raced by side effects in the body.
    body_ran = asyncio.Event()

    async def observed(ctx: Context, x: int) -> int:
        body_ran.set()
        return x

    ctx = _root()
    gate = asyncio.Event()

    with _gated_create_promise(ctx, gate) as entered:
        task = ctx.run(observed, 7)

        await entered.wait()
        # We're now parked inside create_promise -- the body must not have run.
        assert not body_ran.is_set()

        gate.set()
        assert await task == 7
        assert body_ran.is_set()


@pytest.mark.asyncio
async def test_run_durable_promise_visible_in_cache_before_task_resolves() -> None:
    # Once ``await ctx.run(...)`` yields a value to its caller, the durable
    # promise record is necessarily in ``effects.cache``. This is the
    # "happens-before" guarantee the rest of the SDK leans on (e.g. replay
    # picking up the cached record).
    ctx = _root()
    result = await ctx.run(double, 5)
    assert result == 10
    assert "root.1" in ctx._state.effects.cache
    # And it was created *before* it was settled -- the cached record reflects
    # the post-settle state by the time we observe the result.
    assert ctx._state.effects.cache["root.1"].state == "resolved"


@pytest.mark.asyncio
async def test_run_releases_event_when_create_promise_raises() -> None:
    # If ``create_promise`` raises, the body re-raises through the
    # ``except Exception:`` branch after rejecting the chain link
    # (``created.set_exception``) -- so the Task surfaces the original error
    # promptly and a chained successor inherits the failure rather than hanging.
    ctx = _root()

    failing = AsyncMock(side_effect=RuntimeError("network down"))
    with patch.object(ctx._state.effects, "create_promise", new=failing):
        task = ctx.run(double, 1)

        with pytest.raises(RuntimeError, match="network down"):
            await task

    failing.assert_awaited_once()
    # Nothing was cached, because creation never succeeded.
    assert "root.1" not in ctx._state.effects.cache


@pytest.mark.asyncio
async def test_run_does_not_settle_before_create_returns() -> None:
    # ``settle_promise`` must come strictly *after* ``create_promise``. The
    # only way to reach the settle path is through the post-event branch in
    # ``_``, so while creation is gated, no settle call can sneak through.
    ctx = _root()
    gate = asyncio.Event()

    # ``wraps=`` lets the mock both record calls *and* delegate to the real
    # ``settle_promise`` so settlement still hits the cache as usual.
    settle_mock = AsyncMock(wraps=ctx._state.effects.settle_promise)
    with (
        patch.object(ctx._state.effects, "settle_promise", new=settle_mock),
        _gated_create_promise(ctx, gate) as entered,
    ):
        task = ctx.run(double, 3)

        await entered.wait()
        settle_mock.assert_not_awaited()

        gate.set()
        assert await task == 6
        settle_mock.assert_awaited_once_with("root.1", 6)


# =============================================================================
# run: options(timeout=...) and per-call opts scoping
#
# ``options`` mints an independent ``Context`` over the same shared state,
# carrying the override only for its next durable op. The originating context
# is never mutated, so an override applies to exactly the one
# ``ctx.options(...).run(...)`` it is chained onto and a later bare ``ctx.run``
# still sees the defaults -- there is no consume/reset step.
# =============================================================================


@pytest.mark.asyncio
async def test_run_with_options_timeout_sets_child_deadline() -> None:
    ctx = _root()
    before = now_ms()
    assert await ctx.options(timeout=timedelta(seconds=30)).run(double, 5) == 10
    after = now_ms()
    record = ctx._state.effects.cache["root.1"]
    assert before + 30_000 <= record.timeout_at <= after + 30_000


@pytest.mark.asyncio
async def test_run_with_options_timeout_capped_to_parent() -> None:
    cap = now_ms() + 5_000
    ctx = _root(timeout_at=cap)
    # A year-long timeout still cannot outlive the parent's deadline.
    await ctx.options(timeout=timedelta(days=365)).run(double, 1)
    assert ctx._state.effects.cache["root.1"].timeout_at == cap


@pytest.mark.asyncio
async def test_run_options_do_not_leak_to_base_context() -> None:
    ctx = _root()
    await ctx.options(timeout=timedelta(seconds=30)).run(double, 1)  # root.1
    short = ctx._state.effects.cache["root.1"].timeout_at
    # The next run is issued on the base context, which still carries no
    # options -> the 24h default, well past the 30s one. The override rode the
    # throwaway handle and never touched ``ctx``.
    await ctx.run(double, 1)  # root.2
    assert ctx._state.effects.cache["root.2"].timeout_at > short
    assert ctx.opts == Opts()


@pytest.mark.asyncio
async def test_options_returns_independent_handle_sharing_state() -> None:
    # ``options`` returns a *new* context (not ``self``) carrying the override,
    # over the *same* shared state -- so the base context is never mutated.
    ctx = _root()
    scoped = ctx.options(timeout=timedelta(seconds=5))
    assert scoped is not ctx
    assert scoped.opts == Opts(timeout=timedelta(seconds=5))
    assert ctx.opts == Opts()
    assert scoped._state is ctx._state


# =============================================================================
# options: the Context is an isolated opts container over shared state
#
# ``options`` is the only way to set per-call ``Opts``, and it does so by minting
# a brand-new ``Context`` rather than mutating ``self``. Two complementary
# properties follow, and these tests pin both halves of "isolated container":
#
# * Opts are isolated -- every ``options(...)`` handle builds its own fresh
#   ``Opts`` from the keyword defaults. It never merges with the base handle's
#   opts (so a *chained* ``options`` resets each unspecified field to its
#   default rather than carrying it forward), and sibling handles minted off the
#   same base never observe each other's overrides.
# * State is shared -- all of those handles point at the *same* ``_state``, so
#   execution-level mutations (the id sequence, the ``workflow`` flag, the
#   spawned-children lists, the durable cache) are visible across every handle.
#   The container isolates ``_opts``, deliberately NOT ``_state``.
# =============================================================================


def test_options_builds_fresh_opts_not_merged_from_base() -> None:
    # A chained ``options`` does not inherit the previous handle's overrides:
    # each call constructs ``Opts`` from the keyword defaults, so a field the
    # second call omits falls back to its default rather than carrying forward.
    ctx = _root()
    scoped = ctx.options(target="worker-1").options(version=2)
    # ``version`` is the only field the second call set; ``target`` reset to its
    # default (None) -- it was NOT carried over from the first ``options``.
    assert scoped.opts == Opts(version=2)
    assert scoped.opts.target is None


def test_options_no_args_yields_default_opts_on_a_new_handle() -> None:
    # A bare ``options()`` still mints a distinct handle, carrying a default
    # ``Opts`` equal to -- but not identical with -- the base context's.
    ctx = _root()
    scoped = ctx.options()
    assert scoped is not ctx
    assert scoped.opts == Opts()
    assert scoped._state is ctx._state


def test_options_carries_every_field_independently() -> None:
    # All four overridable fields ride the throwaway handle; none touches base.
    ctx = _root()
    policy = Constant(max_retries=4, delay=0)
    scoped = ctx.options(
        timeout=timedelta(seconds=7),
        target="w",
        version=3,
        retry_policy=policy,
    )
    assert scoped.opts == Opts(
        timeout=timedelta(seconds=7), target="w", version=3, retry_policy=policy
    )
    # The base handle's opts are untouched -- still the bare defaults.
    assert ctx.opts == Opts()


def test_options_sibling_handles_are_mutually_isolated() -> None:
    # Two handles minted off the same base never see each other's overrides, and
    # neither leaks back onto the base -- yet all three share one ``_state``.
    ctx = _root()
    a = ctx.options(timeout=timedelta(seconds=5))
    b = ctx.options(target="worker-2")
    assert a.opts == Opts(timeout=timedelta(seconds=5))
    assert b.opts == Opts(target="worker-2")
    assert a.opts != b.opts
    assert ctx.opts == Opts()
    assert a._state is ctx._state is b._state


def test_options_handles_share_id_sequence() -> None:
    # ``_next_id`` mutates the shared ``_state.seq``, so ids advance across every
    # handle minted off the same base: the opts are per-handle, the sequence is
    # not. A broken impl that copied state per handle would re-mint ``root.1``.
    ctx = _root()
    assert ctx.options(target="x")._next_id() == "root.1"
    assert ctx.options(version=2)._next_id() == "root.2"
    assert ctx._next_id() == "root.3"


@pytest.mark.asyncio
async def test_options_op_flips_shared_workflow_flag() -> None:
    # A durable op issued through a throwaway ``options`` handle flips the
    # ``workflow`` flag on the *shared* state, so the base handle observes it --
    # the flag lives on ``_state``, not the per-call ``Context``.
    ctx = _root()
    assert ctx._state.workflow is False
    with pytest.raises(Suspended):
        await ctx.options(target="x").rpc("fn")
    assert ctx._state.workflow is True


@pytest.mark.asyncio
async def test_options_handles_share_spawned_state() -> None:
    # Ops issued through different throwaway ``options`` handles all record onto
    # the one shared ``_state``: their child promises land in the same cache and
    # their pending remote todos accumulate in the same ``spawned_remote`` in
    # call order -- regardless of which handle issued them.
    ctx = _root()
    f1 = ctx.options(target="a").rpc("fn")  # root.1
    f2 = ctx.options(target="b").rpc("fn")  # root.2
    with pytest.raises(Suspended):
        await f1
    with pytest.raises(Suspended):
        await f2
    assert ctx._state.spawned_remote == ["root.1", "root.2"]
    assert ctx._state.effects.cache["root.1"].state == "pending"
    assert ctx._state.effects.cache["root.2"].state == "pending"


# =============================================================================
# run: promise creation order under concurrency
#
# ``Context.run`` returns its ``ResonateFuture`` immediately and the inner bg
# task is what awaits ``create_promise``. The chain on ``Context._tail`` is the
# guarantee that those bg tasks issue ``create_promise`` in call order, no
# matter how asyncio schedules them.
# =============================================================================


@pytest.mark.asyncio
async def test_run_promise_creation_order_under_concurrency() -> None:
    # Mirror of ``test_rpc_promise_creation_order_under_concurrency`` for
    # ``ctx.run``: even with bg tasks spawned concurrently, the chain
    # serializes ``create_promise`` calls into call order.
    ctx = _root()
    seen: list[str] = []
    original = ctx._state.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        # Yield so a non-chained implementation would let root.2 race ahead.
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx._state.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.run(double, 1)
        f2 = ctx.run(double, 2)
        # Await in reverse so completion order can't accidentally line up.
        assert await f2 == 4
        assert await f1 == 2

    assert seen == ["root.1", "root.2"]


@pytest.mark.asyncio
async def test_run_chain_blocks_second_create_until_first_returns() -> None:
    ctx = _root()

    gate = asyncio.Event()
    first_entered = asyncio.Event()
    entered_second = asyncio.Event()

    seen: list[str] = []
    original = ctx._state.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)

        if req.id == "root.1":
            first_entered.set()
            await gate.wait()
        else:
            entered_second.set()

        return await original(req)

    with patch.object(
        ctx._state.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.run(double, 1)
        f2 = ctx.run(double, 2)

        # Wait until bg #1 has actually entered create_promise.
        await first_entered.wait()

        # A non-chained implementation would allow bg #2 to enter
        # create_promise before bg #1 completes.
        assert seen == ["root.1"]
        assert not entered_second.is_set()

        gate.set()

        assert await f1 == 2
        assert await f2 == 4

    assert seen == ["root.1", "root.2"]


# =============================================================================
# run/rpc: a failing create propagates down the chain (the chain fails as a unit)
#
# ``_created``/``_tail`` are ``asyncio.Future`` (not ``asyncio.Event``): a link
# resolves its future with ``set_result`` on a successful create and
# ``set_exception`` on a failed one. A successor parks on ``await prev_created``,
# so a predecessor that failed propagates its exception forward -- the successor
# re-raises the SAME upstream error and never issues its own ``create_promise``,
# rather than (as the old Event-in-``finally`` design did) being released to
# create on top of an inconsistent prefix.
# =============================================================================


@pytest.mark.asyncio
async def test_run_chain_failure_propagates_to_successor() -> None:
    ctx = _root()
    boom = RuntimeError("network down")
    seen: list[str] = []
    original = ctx._state.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        if req.id == "root.1":
            raise boom
        return await original(req)

    with patch.object(
        ctx._state.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.run(double, 1)
        f2 = ctx.run(double, 2)
        with pytest.raises(RuntimeError, match="network down") as ei1:
            await f1
        with pytest.raises(RuntimeError, match="network down") as ei2:
            await f2

    # The successor re-raises the *same* upstream exception, not a fresh one.
    assert ei1.value is boom
    assert ei2.value is boom
    # Only the first link ever attempted a create: the successor parked on the
    # failed predecessor and unwound without issuing its own create_promise.
    assert seen == ["root.1"]
    assert "root.1" not in ctx._state.effects.cache
    assert "root.2" not in ctx._state.effects.cache


@pytest.mark.asyncio
async def test_rpc_chain_failure_propagates_to_successor() -> None:
    # Parity with the run path: rpc/sleep/promise share ``_await_remote``, which
    # routes through the same chain, so a failed predecessor likewise propagates
    # to its successor and the successor never creates its own promise.
    ctx = _root()
    boom = RuntimeError("network down")
    seen: list[str] = []
    original = ctx._state.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        if req.id == "root.1":
            raise boom
        return await original(req)

    with patch.object(
        ctx._state.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.rpc("fn")
        f2 = ctx.rpc("fn")
        with pytest.raises(RuntimeError, match="network down") as ei1:
            await f1
        with pytest.raises(RuntimeError, match="network down") as ei2:
            await f2

    assert ei1.value is boom
    assert ei2.value is boom
    assert seen == ["root.1"]
    assert "root.1" not in ctx._state.effects.cache
    assert "root.2" not in ctx._state.effects.cache
    # A failed create never registered a remote todo.
    assert ctx._state.spawned_remote == []


@pytest.mark.asyncio
async def test_run_chain_failure_poisons_every_later_link() -> None:
    # The failure propagates the length of the chain, not just to the immediate
    # successor: ``_tail`` keeps pointing at the rejected future, so EVERY later
    # link awaits it and re-raises the same upstream error -- and none of them
    # issues its own create. A failed ``create_promise`` tears the workflow task
    # down, so a permanently-poisoned chain is the intended "fails as a unit".
    ctx = _root()
    boom = RuntimeError("network down")
    seen: list[str] = []
    original = ctx._state.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        if req.id == "root.1":
            raise boom
        return await original(req)

    with patch.object(
        ctx._state.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.run(double, 1)  # fails
        f2 = ctx.run(double, 2)  # inherits f1's failure
        f3 = ctx.run(double, 3)  # inherits f2's failure (same chain)
        for f in (f1, f2, f3):
            with pytest.raises(RuntimeError, match="network down") as ei:
                await f
            assert ei.value is boom

    # Only the first link reached create_promise; the rest unwound on the chain.
    assert seen == ["root.1"]
    assert not any(
        id in ctx._state.effects.cache for id in ("root.1", "root.2", "root.3")
    )


# =============================================================================
# ResonateFuture.id(): a Future-backed ``_created`` surfaces create failures
#
# ``id()`` awaits ``_created``. Now that ``_created`` is an ``asyncio.Future``,
# a successful create resolves it (``id()`` returns the id) and a failed create
# rejects it (``id()`` raises the create error) -- the old ``asyncio.Event``
# only ever fired, so ``id()`` could hand back an id for a promise that was
# never created.
# =============================================================================


@pytest.mark.asyncio
async def test_future_id_returns_id_after_create() -> None:
    ctx = _root()
    fut = ctx.run(double, 21)
    assert await fut.id() == "root.1"
    # And the body still runs through to its value.
    assert await fut == 42


@pytest.mark.asyncio
async def test_future_id_raises_when_create_fails() -> None:
    ctx = _root()
    failing_mock = AsyncMock(side_effect=RuntimeError("network down"))
    with patch.object(ctx._state.effects, "create_promise", new=failing_mock):
        fut = ctx.run(double, 1)
        with pytest.raises(RuntimeError, match="network down"):
            await fut.id()
        # The task itself surfaces the same failure (retrieved so no warning).
        with pytest.raises(RuntimeError, match="network down"):
            await fut


# =============================================================================
# rpc: pending -> register a remote todo and suspend
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_pending_registers_todo_and_suspends() -> None:
    # A fresh remote promise is created pending; awaiting the future appends
    # its id to ``spawned_remote`` and raises ``Suspended``.
    ctx = _root()
    fut = ctx.rpc("remote_fn", 1, 2)
    with pytest.raises(Suspended):
        await fut
    assert ctx._state.spawned_remote == ["root.1"]
    assert ctx._state.effects.cache["root.1"].state == "pending"


# =============================================================================
# rpc: idempotent recovery (pre-settled records short-circuit)
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_presettled_resolved_returns_value() -> None:
    ctx = _root([_resolved("root.1", "remote-result")])
    assert await ctx.rpc("remote_fn") == "remote-result"
    assert ctx._state.spawned_remote == []


@pytest.mark.asyncio
async def test_rpc_presettled_rejected_raises() -> None:
    ctx = _root([_rejected("root.1", "remote failure")])
    with pytest.raises(ApplicationError, match="remote failure"):
        await ctx.rpc("remote_fn")
    assert ctx._state.spawned_remote == []


# =============================================================================
# rpc: request shape (tags + TaskData envelope)
# =============================================================================


@contextmanager
def _spy_create_promise(ctx: Context) -> Iterator[list[Any]]:
    """Capture every PromiseCreateReq passed to ``effects.create_promise``."""
    captured: list[Any] = []
    original = ctx._state.effects.create_promise

    async def spy(req: Any) -> Any:
        captured.append(req)
        return await original(req)

    with patch.object(
        ctx._state.effects, "create_promise", new=AsyncMock(side_effect=spy)
    ):
        yield captured


@pytest.mark.asyncio
async def test_rpc_request_tags_and_param() -> None:
    ctx = _root()
    with _spy_create_promise(ctx) as captured, pytest.raises(Suspended):
        await ctx.rpc("remote_fn", 1, 2, k="v")

    [req] = captured
    assert req.id == "root.1"
    assert req.tags == {
        "resonate:scope": "global",
        "resonate:target": "",
        "resonate:branch": "root.1",
        "resonate:parent": "root",
        "resonate:origin": "root",
        # Non-detached: prefix mirrors origin.
        "resonate:prefix": "root",
    }
    # ``TaskData`` flattens func + args + kwargs + version; ``args`` is the live
    # tuple here (it serializes to a JSON array on the wire). The codec
    # serializes this struct on the way out, so ``data`` holds it verbatim.
    assert req.param.data == TaskData(
        func="remote_fn", args=(1, 2), kwargs={"k": "v"}, version=1
    )


@pytest.mark.asyncio
async def test_rpc_no_args_param_is_empty() -> None:
    # An empty call still carries a complete ``TaskData``: empty args/kwargs and
    # the (context-level) version 0, never a partial payload.
    ctx = _root()
    with _spy_create_promise(ctx) as captured, pytest.raises(Suspended):
        await ctx.rpc("remote_fn")

    assert captured[0].param.data == TaskData(
        func="remote_fn", args=(), kwargs={}, version=1
    )


# =============================================================================
# rpc: child id allocation matches call order
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_sequential_child_ids() -> None:
    ctx = _root([_resolved("root.1", "a"), _resolved("root.2", "b")])
    assert await ctx.rpc("fn") == "a"  # root.1
    assert await ctx.rpc("fn") == "b"  # root.2


@pytest.mark.asyncio
async def test_rpc_promise_creation_order_under_concurrency() -> None:
    # The chain in ``_advance_promise_chain`` must serialize ``create_promise``
    # calls into call order even when both rpcs are spawned concurrently.
    ctx = _root([_resolved("root.1", "a"), _resolved("root.2", "b")])
    seen: list[str] = []
    original = ctx._state.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        # Yield so a non-chained implementation would let root.2 race ahead.
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx._state.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.rpc("fn")
        f2 = ctx.rpc("fn")
        # Await in reverse so completion order can't accidentally line up.
        assert await f2 == "b"
        assert await f1 == "a"

    assert seen == ["root.1", "root.2"]


# =============================================================================
# rpc: options(timeout=, target=) and per-call opts scoping
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_with_options_target_sets_tag() -> None:
    ctx = _root()
    with _spy_create_promise(ctx) as captured, pytest.raises(Suspended):
        await ctx.options(target="worker-1").rpc("fn")
    assert captured[0].tags["resonate:target"] == "worker-1"


@pytest.mark.asyncio
async def test_rpc_with_options_timeout_sets_child_deadline() -> None:
    ctx = _root()
    before = now_ms()
    with pytest.raises(Suspended):
        await ctx.options(timeout=timedelta(seconds=30)).rpc("fn")
    after = now_ms()
    record = ctx._state.effects.cache["root.1"]
    assert before + 30_000 <= record.timeout_at <= after + 30_000


@pytest.mark.asyncio
async def test_rpc_with_options_timeout_capped_to_parent() -> None:
    cap = now_ms() + 5_000
    ctx = _root(timeout_at=cap)
    with pytest.raises(Suspended):
        await ctx.options(timeout=timedelta(days=365)).rpc("fn")
    assert ctx._state.effects.cache["root.1"].timeout_at == cap


@pytest.mark.asyncio
async def test_rpc_options_do_not_leak_to_base_context() -> None:
    ctx = _root([_resolved("root.1", "a")])
    await ctx.options(timeout=timedelta(seconds=30), target="x").rpc("fn")
    # The override rode the throwaway handle; the base context is untouched.
    assert ctx.opts == Opts()


@pytest.mark.asyncio
async def test_rpc_on_base_context_ignores_a_discarded_options_call() -> None:
    # ``options`` returns a new handle; discarding it leaves the base context's
    # defaults intact, so a bare ``ctx.rpc`` carries no target (empty tag).
    ctx = _root()
    ctx.options(timeout=timedelta(seconds=5), target="x")
    with _spy_create_promise(ctx) as captured, pytest.raises(Suspended):
        await ctx.rpc("fn")
    assert captured[0].tags["resonate:target"] == ""


# =============================================================================
# rpc: blocks until the durable promise has been created
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_task_pending_while_create_promise_blocked() -> None:
    # Mirrors ``test_run_task_pending_while_create_promise_blocked``: the Task
    # must not be ``done()`` until ``create_promise`` resolves.
    ctx = _root()
    gate = asyncio.Event()

    with _gated_create_promise(ctx, gate) as entered:
        task = ctx.rpc("fn")

        await entered.wait()

        assert "root.1" not in ctx._state.effects.cache

        gate.set()
        with pytest.raises(Suspended):
            await task
        assert ctx._state.effects.cache["root.1"].state == "pending"


@pytest.mark.asyncio
async def test_rpc_releases_event_when_create_promise_raises() -> None:
    # If ``create_promise`` raises, the chain link is rejected
    # (``created.set_exception`` in ``_create_promise_in_chain``) and the Task
    # surfaces the original error -- a chained successor inherits the failure
    # rather than hanging.
    ctx = _root()

    failing_mock = AsyncMock(side_effect=RuntimeError("network down"))
    with patch.object(ctx._state.effects, "create_promise", new=failing_mock):
        task = ctx.rpc("fn")
        with pytest.raises(RuntimeError, match="network down"):
            await task

    failing_mock.assert_awaited_once()
    assert "root.1" not in ctx._state.effects.cache
    assert ctx._state.spawned_remote == []


# =============================================================================
# sleep: pending -> register a remote todo and suspend
#
# ``sleep`` is structurally identical to ``rpc`` (both go through the shared
# ``_await_remote`` body): a fresh timer promise is created pending, awaiting
# the future appends its id to ``spawned_remote`` and raises ``Suspended``.
# The differences are in the request shape (a ``resonate:timer`` tag, no param,
# no func envelope) and that ``sleep`` takes its deadline from the duration
# argument rather than from ``opts``.
# =============================================================================


@pytest.mark.asyncio
async def test_sleep_pending_registers_todo_and_suspends() -> None:
    ctx = _root()
    fut = ctx.sleep(timedelta(seconds=30))
    with pytest.raises(Suspended):
        await fut
    assert ctx._state.spawned_remote == ["root.1"]
    assert ctx._state.effects.cache["root.1"].state == "pending"


# =============================================================================
# sleep: idempotent recovery (a pre-settled timer short-circuits)
# =============================================================================


@pytest.mark.asyncio
async def test_sleep_presettled_resolved_returns_none() -> None:
    # An elapsed timer is recovered as a resolved record carrying no payload;
    # ``_decode_settled`` yields its (empty) value and no todo is registered.
    ctx = _root([_resolved("root.1", None)])
    assert await ctx.sleep(timedelta(seconds=1)) is None
    assert ctx._state.spawned_remote == []


# =============================================================================
# sleep: request shape (timer tag, empty param, no func envelope)
# =============================================================================


@pytest.mark.asyncio
async def test_sleep_request_tags_and_timer_flag() -> None:
    ctx = _root()
    with _spy_create_promise(ctx) as captured, pytest.raises(Suspended):
        await ctx.sleep(timedelta(seconds=30))

    [req] = captured
    assert req.id == "root.1"
    assert req.tags == {
        "resonate:scope": "global",
        "resonate:branch": "root.1",
        "resonate:parent": "root",
        "resonate:origin": "root",
        "resonate:prefix": "root",
        "resonate:timer": "true",
    }
    # A timer promise carries no param payload (unlike rpc's func envelope).
    assert req.param.data is None


@pytest.mark.asyncio
async def test_sleep_timeout_at_is_now_plus_duration() -> None:
    # The wake time is ``now + duration``, taken straight from the argument.
    ctx = _root()
    before = now_ms()
    with pytest.raises(Suspended):
        await ctx.sleep(timedelta(seconds=30))
    after = now_ms()
    record = ctx._state.effects.cache["root.1"]
    assert before + 30_000 <= record.timeout_at <= after + 30_000


@pytest.mark.asyncio
async def test_sleep_duration_capped_to_parent_timeout() -> None:
    # A wake time beyond the parent's deadline is capped, like every other
    # child promise (``child_timeout``).
    cap = now_ms() + 5_000
    ctx = _root(timeout_at=cap)
    with pytest.raises(Suspended):
        await ctx.sleep(timedelta(days=365))
    assert ctx._state.effects.cache["root.1"].timeout_at == cap


# =============================================================================
# sleep: duration comes from the argument, NOT from opts
#
# Unlike run/rpc, ``sleep`` does not read ``opts.timeout`` for its deadline.
# It still consumes any opts set via with_opts() so they cannot leak into the
# next entrypoint call.
# =============================================================================


@pytest.mark.asyncio
async def test_sleep_ignores_opts_timeout_for_duration() -> None:
    # The 30s argument wins over the 5s opt -- the opt does not touch the
    # timer's deadline at all.
    ctx = _root()
    before = now_ms()
    with pytest.raises(Suspended):
        await ctx.options(timeout=timedelta(seconds=5)).sleep(timedelta(seconds=30))
    after = now_ms()
    record = ctx._state.effects.cache["root.1"]
    assert before + 30_000 <= record.timeout_at <= after + 30_000


@pytest.mark.asyncio
async def test_sleep_options_do_not_leak_to_base_context() -> None:
    # sleep ignores opts for its own duration, and a discarded ``options()``
    # call never mutates the base context, so a stray override before a sleep
    # cannot bleed into the next entrypoint.
    ctx = _root()
    ctx.options(timeout=timedelta(seconds=5), target="x")
    with pytest.raises(Suspended):
        await ctx.sleep(timedelta(seconds=1))
    assert ctx.opts == Opts()


# =============================================================================
# sleep: child id allocation matches call order
# =============================================================================


@pytest.mark.asyncio
async def test_sleep_sequential_child_ids() -> None:
    ctx = _root([_resolved("root.1", None), _resolved("root.2", None)])
    assert await ctx.sleep(timedelta(seconds=1)) is None  # root.1
    assert await ctx.sleep(timedelta(seconds=1)) is None  # root.2


@pytest.mark.asyncio
async def test_sleep_promise_creation_order_under_concurrency() -> None:
    # ``sleep`` joins the same ``_advance_promise_chain`` as run/rpc, so two
    # timers spawned concurrently still issue ``create_promise`` in call order.
    ctx = _root([_resolved("root.1", None), _resolved("root.2", None)])
    seen: list[str] = []
    original = ctx._state.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        # Yield so a non-chained implementation would let root.2 race ahead.
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx._state.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.sleep(timedelta(seconds=1))
        f2 = ctx.sleep(timedelta(seconds=1))
        # Await in reverse so completion order can't accidentally line up.
        assert await f2 is None
        assert await f1 is None

    assert seen == ["root.1", "root.2"]


# =============================================================================
# sleep: blocks until the durable promise has been created
# =============================================================================


@pytest.mark.asyncio
async def test_sleep_releases_event_when_create_promise_raises() -> None:
    # If ``create_promise`` raises, the shared ``_await_remote`` body rejects
    # the chain link (``created.set_exception``) and the Task surfaces the
    # original error -- a chained successor inherits the failure rather than
    # hanging.
    ctx = _root()

    failing_mock = AsyncMock(side_effect=RuntimeError("network down"))
    with patch.object(ctx._state.effects, "create_promise", new=failing_mock):
        task = ctx.sleep(timedelta(seconds=1))
        with pytest.raises(RuntimeError, match="network down"):
            await task

    failing_mock.assert_awaited_once()
    assert "root.1" not in ctx._state.effects.cache
    assert ctx._state.spawned_remote == []


# =============================================================================
# promise: pending -> register a remote todo and suspend
#
# ``promise`` creates a *deferred* (DI) durable promise -- one with no func
# envelope and no timer flag, meant to be resolved/rejected by some external
# party. It shares the same ``_await_remote`` body as ``rpc``/``sleep``: a fresh
# pending record appends its id to ``spawned_remote`` and raises
# ``Suspended``; a pre-settled record short-circuits with its value. The
# distinguishing trait is the request shape (empty param, no ``resonate:timer``,
# no ``resonate:target``) and that the deadline comes from the explicit
# ``timeout`` argument rather than from ``opts``.
# =============================================================================


@pytest.mark.asyncio
async def test_promise_pending_registers_todo_and_suspends() -> None:
    ctx = _root()
    fut = ctx.promise(timedelta(seconds=30))
    with pytest.raises(Suspended):
        await fut
    assert ctx._state.spawned_remote == ["root.1"]
    assert ctx._state.effects.cache["root.1"].state == "pending"


# =============================================================================
# promise: idempotent recovery (a pre-settled DI promise short-circuits)
# =============================================================================


@pytest.mark.asyncio
async def test_promise_presettled_resolved_returns_value() -> None:
    # A DI promise resolved by an external party is recovered as a resolved
    # record; ``_decode_settled`` yields its payload and no todo is registered.
    ctx = _root([_resolved("root.1", "external-result")])
    assert await ctx.promise(timedelta(seconds=1)) == "external-result"
    assert ctx._state.spawned_remote == []


@pytest.mark.asyncio
async def test_promise_presettled_rejected_raises() -> None:
    ctx = _root([_rejected("root.1", "external failure")])
    with pytest.raises(ApplicationError, match="external failure"):
        await ctx.promise(timedelta(seconds=1))
    assert ctx._state.spawned_remote == []


# =============================================================================
# promise: request shape (empty param, no timer flag, no func envelope)
# =============================================================================


@pytest.mark.asyncio
async def test_promise_request_tags_and_empty_param() -> None:
    ctx = _root()
    with _spy_create_promise(ctx) as captured, pytest.raises(Suspended):
        await ctx.promise(timedelta(seconds=30))

    [req] = captured
    assert req.id == "root.1"
    # A DI promise carries the standard scope/lineage tags only -- no timer
    # flag (unlike sleep) and no target (unlike rpc).
    assert req.tags == {
        "resonate:scope": "global",
        "resonate:branch": "root.1",
        "resonate:parent": "root",
        "resonate:origin": "root",
        "resonate:prefix": "root",
    }
    assert "resonate:timer" not in req.tags
    assert "resonate:target" not in req.tags
    # No param payload and no func envelope: the promise is filled in later.
    assert req.param.data is None


@pytest.mark.asyncio
async def test_promise_timeout_at_is_now_plus_timeout() -> None:
    ctx = _root()
    before = now_ms()
    with pytest.raises(Suspended):
        await ctx.promise(timedelta(seconds=30))
    after = now_ms()
    record = ctx._state.effects.cache["root.1"]
    assert before + 30_000 <= record.timeout_at <= after + 30_000


@pytest.mark.asyncio
async def test_promise_none_timeout_uses_default() -> None:
    # ``timeout=None`` falls back to ``DEFAULT_TIMEOUT`` (24h) via
    # ``child_timeout``, well past any explicit short timeout.
    ctx = _root()
    before = now_ms()
    with pytest.raises(Suspended):
        await ctx.promise(None)
    after = now_ms()
    record = ctx._state.effects.cache["root.1"]
    day_ms = 24 * 60 * 60 * 1000
    assert before + day_ms <= record.timeout_at <= after + day_ms


@pytest.mark.asyncio
async def test_promise_timeout_capped_to_parent() -> None:
    cap = now_ms() + 5_000
    ctx = _root(timeout_at=cap)
    with pytest.raises(Suspended):
        await ctx.promise(timedelta(days=365))
    assert ctx._state.effects.cache["root.1"].timeout_at == cap


# =============================================================================
# promise: deadline comes from the argument, NOT from opts
# =============================================================================


@pytest.mark.asyncio
async def test_promise_ignores_opts_timeout_for_deadline() -> None:
    # Like ``sleep``, ``promise`` takes its deadline from its argument; a stray
    # ``with_opts(timeout=...)`` must not touch the DI promise's deadline.
    ctx = _root()
    before = now_ms()
    with pytest.raises(Suspended):
        await ctx.options(timeout=timedelta(seconds=5)).promise(timedelta(seconds=30))
    after = now_ms()
    record = ctx._state.effects.cache["root.1"]
    assert before + 30_000 <= record.timeout_at <= after + 30_000


@pytest.mark.asyncio
async def test_promise_options_do_not_leak_to_base_context() -> None:
    # ``promise`` ignores opts for its own deadline, and a discarded
    # ``options()`` call never mutates the base context, so a stray override
    # cannot bleed into the next entrypoint call.
    ctx = _root()
    ctx.options(timeout=timedelta(seconds=5), target="x")
    with pytest.raises(Suspended):
        await ctx.promise(timedelta(seconds=1))
    assert ctx.opts == Opts()


# =============================================================================
# promise: child id allocation matches call order
# =============================================================================


@pytest.mark.asyncio
async def test_promise_sequential_child_ids() -> None:
    ctx = _root([_resolved("root.1", "a"), _resolved("root.2", "b")])
    assert await ctx.promise(timedelta(seconds=1)) == "a"  # root.1
    assert await ctx.promise(timedelta(seconds=1)) == "b"  # root.2


@pytest.mark.asyncio
async def test_promise_creation_order_under_concurrency() -> None:
    # ``promise`` joins the same ``_advance_promise_chain`` as run/rpc/sleep, so
    # two DI promises spawned concurrently still create in call order.
    ctx = _root([_resolved("root.1", "a"), _resolved("root.2", "b")])
    seen: list[str] = []
    original = ctx._state.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        # Yield so a non-chained implementation would let root.2 race ahead.
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx._state.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.promise(timedelta(seconds=1))
        f2 = ctx.promise(timedelta(seconds=1))
        # Await in reverse so completion order can't accidentally line up.
        assert await f2 == "b"
        assert await f1 == "a"

    assert seen == ["root.1", "root.2"]


# =============================================================================
# promise: blocks until the durable promise has been created
# =============================================================================


@pytest.mark.asyncio
async def test_promise_releases_event_when_create_promise_raises() -> None:
    # If ``create_promise`` raises, the shared ``_await_remote`` body rejects
    # the chain link (``created.set_exception``) and the Task surfaces the
    # original error -- a chained successor inherits the failure rather than
    # hanging.
    ctx = _root()

    failing_mock = AsyncMock(side_effect=RuntimeError("network down"))
    with patch.object(ctx._state.effects, "create_promise", new=failing_mock):
        task = ctx.promise(timedelta(seconds=1))
        with pytest.raises(RuntimeError, match="network down"):
            await task

    failing_mock.assert_awaited_once()
    assert "root.1" not in ctx._state.effects.cache
    assert ctx._state.spawned_remote == []


# =============================================================================
# detached: fire-and-forget remote dispatch
#
# ``detached`` creates a *remote* promise like ``rpc`` (func envelope + target),
# but is fire-and-forget: it is NOT registered in ``spawned_remote``, the
# workflow never suspends on it, and the returned future resolves to the child
# *id* rather than a remote result. The id lives outside the structured
# ``{id}.{seq}`` namespace -- it is ``{prefix}.d{blake2b 16 hex of the raw seq}``
# (the ``d`` marks it a detached segment) -- so it is deterministic across replay
# yet distinct from awaited children.
# =============================================================================


def _detached_id(prefix: str, raw: str) -> str:
    """Return the id ``detached`` derives for raw seq id ``raw`` under ``prefix``."""
    return f"{prefix}.d{_hash_id(raw)}"


@pytest.mark.asyncio
async def test_detached_returns_id_without_suspending() -> None:
    # The defining property: a fresh (pending) detached promise does NOT raise
    # Suspended and does NOT register a remote todo -- the future just
    # yields the child id.
    ctx = _root()
    child_id = _detached_id("root", "root.1")
    assert await ctx.detached("remote_fn", 1, 2) == child_id
    assert ctx._state.spawned_remote == []
    # The durable promise was created (and left pending -- nobody settles it
    # on our behalf within this workflow).
    assert ctx._state.effects.cache[child_id].state == "pending"


@pytest.mark.asyncio
async def test_detached_id_is_prefix_rooted_hash() -> None:
    # The id is ``{prefix}.d{16-hex}`` -- prefix-rooted, not parent-rooted, and
    # outside the ``root.1`` structured namespace.
    ctx = _root()
    child_id = await ctx.detached("remote_fn")
    assert child_id == _detached_id("root", "root.1")
    assert child_id != "root.1"
    # A ``d`` marker plus 16 lowercase hex chars after the prefix segment.
    _, _, suffix = child_id.partition(".")
    assert suffix[0] == "d"
    assert len(suffix) == 17
    assert all(c in "0123456789abcdef" for c in suffix[1:])


@pytest.mark.asyncio
async def test_detached_consumes_seq_and_yields_distinct_ids() -> None:
    # Each call consumes a seq slot (root.1, root.2, ...) so ids stay stable
    # across replay; the resulting hashed ids are distinct.
    ctx = _root()
    id1 = await ctx.detached("fn")
    id2 = await ctx.detached("fn")
    assert id1 == _detached_id("root", "root.1")
    assert id2 == _detached_id("root", "root.2")
    assert id1 != id2


@pytest.mark.asyncio
async def test_detached_request_tags_and_param() -> None:
    ctx = _root()
    child_id = _detached_id("root", "root.1")
    with _spy_create_promise(ctx) as captured:
        await ctx.detached("remote_fn", 1, 2, k="v")

    [req] = captured
    assert req.id == child_id
    # A detached promise carries the remote (rpc-style) tag set: global scope, a
    # resolved target, and -- like every child -- branch == its own id. Unlike
    # every other creator, detached starts a fresh lineage: ``resonate:origin``
    # is its OWN id (== branch), while ``resonate:prefix`` keeps the parent's
    # propagated prefix ("root") so recursive detached ids stay bounded.
    assert req.tags == {
        "resonate:scope": "global",
        "resonate:target": "",
        "resonate:branch": child_id,
        "resonate:parent": "root",
        "resonate:origin": child_id,
        "resonate:prefix": "root",
    }
    # Like rpc, detached dispatches a flattened ``TaskData`` (args is the live
    # tuple, serialized to a JSON array on the wire).
    assert req.param.data == TaskData(
        func="remote_fn", args=(1, 2), kwargs={"k": "v"}, version=1
    )


@pytest.mark.asyncio
async def test_detached_no_args_param_is_empty() -> None:
    ctx = _root()
    with _spy_create_promise(ctx) as captured:
        await ctx.detached("remote_fn")
    assert captured[0].param.data == TaskData(
        func="remote_fn", args=(), kwargs={}, version=1
    )


@pytest.mark.asyncio
async def test_detached_idempotent_on_preloaded_record() -> None:
    # On replay the durable promise already exists; create_promise is
    # idempotent and detached still returns the id and never suspends,
    # regardless of the record's state.
    ctx = _root()
    child_id = _detached_id("root", "root.1")
    ctx._state.effects.cache[child_id] = _resolved(child_id, "external-result")
    assert await ctx.detached("fn") == child_id
    assert ctx._state.spawned_remote == []


@pytest.mark.asyncio
async def test_detached_with_options_target_and_timeout() -> None:
    ctx = _root()
    child_id = _detached_id("root", "root.1")
    before = now_ms()
    with _spy_create_promise(ctx) as captured:
        await ctx.options(target="worker-1", timeout=timedelta(seconds=30)).detached(
            "fn"
        )
    after = now_ms()
    [req] = captured
    assert req.tags["resonate:target"] == "worker-1"
    assert before + 30_000 <= req.timeout_at <= after + 30_000
    assert ctx._state.effects.cache[child_id].timeout_at == req.timeout_at


@pytest.mark.asyncio
async def test_detached_options_do_not_leak_to_base_context() -> None:
    ctx = _root()
    await ctx.options(target="x", timeout=timedelta(seconds=30)).detached("fn")
    # The override rode the throwaway handle; the base context is untouched.
    assert ctx.opts == Opts()


@pytest.mark.asyncio
async def test_detached_timeout_capped_to_parent() -> None:
    cap = now_ms() + 5_000
    ctx = _root(timeout_at=cap)
    child_id = _detached_id("root", "root.1")
    await ctx.options(timeout=timedelta(days=365)).detached("fn")
    assert ctx._state.effects.cache[child_id].timeout_at == cap


@pytest.mark.asyncio
async def test_detached_creation_order_under_concurrency() -> None:
    # ``detached`` joins the same ``_advance_promise_chain`` as the other
    # entrypoints, so two detached dispatches spawned concurrently still issue
    # ``create_promise`` in call order (under their hashed ids).
    ctx = _root()
    id1 = _detached_id("root", "root.1")
    id2 = _detached_id("root", "root.2")
    seen: list[str] = []
    original = ctx._state.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx._state.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.detached("fn")
        f2 = ctx.detached("fn")
        # Await in reverse so completion order can't accidentally line up.
        assert await f2 == id2
        assert await f1 == id1

    assert seen == [id1, id2]


@pytest.mark.asyncio
async def test_detached_releases_event_when_create_promise_raises() -> None:
    # If ``create_promise`` raises, the chain link is rejected
    # (``created.set_exception`` in ``_create_promise_in_chain``) and the Task
    # surfaces the original error -- a chained successor inherits the failure
    # rather than hanging.
    ctx = _root()
    failing_mock = AsyncMock(side_effect=RuntimeError("network down"))
    with patch.object(ctx._state.effects, "create_promise", new=failing_mock):
        task = ctx.detached("fn")
        with pytest.raises(RuntimeError, match="network down"):
            await task
    failing_mock.assert_awaited_once()
    assert ctx._state.spawned_remote == []


# =============================================================================
# detached: structured-concurrency exemption
#
# A detached child must NOT force its parent workflow to suspend: it is not in
# ``spawned_remote`` and not joined via ``spawned_locals``. A workflow that only
# dispatches a detached child therefore completes and settles normally, with
# the detached promise left pending (settled by someone outside our contract).
# =============================================================================


async def dispatches_detached(ctx: Context) -> str:
    await ctx.detached("remote_fn", 1)
    return "done"


@pytest.mark.asyncio
async def test_detached_does_not_force_parent_to_suspend() -> None:
    ctx = _root()
    # The workflow returns its value -- it is NOT dropped in favour of
    # suspension the way a pending rpc/promise dependency would force.
    assert await ctx.run(dispatches_detached) == "done"
    assert ctx._state.spawned_remote == []
    assert ctx._state.effects.cache["root.1"].state == "resolved"
    assert ctx._state.effects.cache["root.1"].value.data == "done"
    # The detached promise was created under the child's own origin-rooted id
    # and left pending.
    detached_id = _detached_id("root", "root.1.1")
    assert ctx._state.effects.cache[detached_id].state == "pending"


# =============================================================================
# detached: create_promise is guaranteed before the workflow exits
#
# The detached body's sole side effect is ``create_promise``; it deferred that
# call through the creation chain on a bg task. Because the body neither
# appends to ``spawned_remote`` nor raises ``Suspended``, nothing else
# pulls it along -- so it must be registered in ``spawned_remote_tasks`` and
# joined by ``flush_local_work`` before the parent settles. Otherwise a
# fire-and-forget detached child whose future is never awaited could leave the
# event loop with its durable promise still uncreated when the workflow exits.
# =============================================================================


@pytest.mark.asyncio
async def test_detached_bg_task_registered_for_flush() -> None:
    # The bg task is parked in ``spawned_remote_tasks`` (the bucket
    # ``flush_local_work`` joins), and the join drains the list.
    ctx = _root()
    fut = ctx.detached("fn")
    assert len(ctx._state.spawned_remote_tasks) == 1
    assert ctx._state.spawned_remote_tasks[0] is fut._task
    await ctx.flush_local_work()
    assert ctx._state.spawned_remote_tasks == []


@pytest.mark.asyncio
async def test_detached_create_promise_completes_by_flush_when_unawaited() -> None:
    # The discriminating case: dispatch a detached child but NEVER await its
    # future. The bg body is deferred, so right after the call the durable
    # promise does not exist yet...
    ctx = _root()
    child_id = _detached_id("root", "root.1")
    _ = ctx.detached("remote_fn", 1, 2)
    assert child_id not in ctx._state.effects.cache
    # ...``flush_local_work`` joins the body before the workflow can settle, so
    # the durable promise IS created by the time we exit. (Without the
    # registration, flush would be a no-op here and the promise would never be
    # created.)
    await ctx.flush_local_work()
    assert ctx._state.effects.cache[child_id].state == "pending"
    # The join added no remote todo, so the parent is still free to fulfill.
    assert ctx.take_remote_todos() == []


async def dispatches_detached_fire_and_forget(ctx: Context) -> str:
    # Dispatch a detached child and return WITHOUT awaiting it -- the future is
    # dropped on the floor, so only ``flush_local_work`` can drive its create.
    _ = ctx.detached("remote_fn", 1)
    return "done"


@pytest.mark.asyncio
async def test_detached_promise_created_before_parent_settles_unawaited() -> None:
    # End-to-end happens-before: a workflow whose only durable op is a
    # fire-and-forget detached child fulfills normally, and the detached
    # ``create_promise`` is observed strictly BEFORE the parent's
    # ``settle_promise``. The body never awaits the future and never yields
    # between dispatching it and returning, so the bg create can only be driven
    # by ``flush_local_work`` joining it -- without that join the parent settles
    # first (or the create never happens at all).
    ctx = _root()
    detached_id = _detached_id("root", "root.1.1")
    order: list[str] = []
    orig_create = ctx._state.effects.create_promise
    orig_settle = ctx._state.effects.settle_promise

    async def rec_create(req: Any) -> Any:
        record = await orig_create(req)
        order.append(f"create:{req.id}")
        return record

    async def rec_settle(id: str, value: Any) -> Any:
        order.append(f"settle:{id}")
        return await orig_settle(id, value)

    with (
        patch.object(
            ctx._state.effects, "create_promise", new=AsyncMock(side_effect=rec_create)
        ),
        patch.object(
            ctx._state.effects, "settle_promise", new=AsyncMock(side_effect=rec_settle)
        ),
    ):
        assert await ctx.run(dispatches_detached_fire_and_forget) == "done"

    assert order == [
        "create:root.1",  # parent workflow promise
        f"create:{detached_id}",  # detached child -- flushed before settle
        "settle:root.1",  # parent settles last
    ]
    assert ctx._state.effects.cache["root.1"].state == "resolved"
    assert ctx._state.effects.cache[detached_id].state == "pending"
    # Fire-and-forget: it never became a remote todo for the parent.
    assert ctx._state.spawned_remote == []


# =============================================================================
# run + rpc: shared promise-creation chain orders both code paths
# =============================================================================


@pytest.mark.asyncio
async def test_mixed_run_and_rpc_create_in_call_order() -> None:
    # ``run`` and ``rpc`` share the same ``_tail`` chain on Context. A mixed
    # sequence must see ``create_promise`` called in call order across both
    # code paths -- otherwise interleaving the two would break id allocation.
    ctx = _root(
        [
            _resolved("root.2", "remote-1"),
            _resolved("root.4", "remote-2"),
        ]
    )
    seen: list[str] = []
    original = ctx._state.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        # Yield so a non-chained implementation could let later calls race ahead.
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx._state.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.run(double, 1)  # root.1 (executes locally)
        f2 = ctx.rpc("remote_fn")  # root.2 (preloaded resolved)
        f3 = ctx.run(double, 3)  # root.3 (executes locally)
        f4 = ctx.rpc("remote_fn")  # root.4 (preloaded resolved)
        # Await in reverse so completion order is decoupled from call order.
        assert await f4 == "remote-2"
        assert await f3 == 6
        assert await f2 == "remote-1"
        assert await f1 == 2

    assert seen == ["root.1", "root.2", "root.3", "root.4"]


@pytest.mark.asyncio
async def test_mixed_run_rpc_sleep_create_in_call_order() -> None:
    # ``sleep`` shares the same ``_tail`` chain as ``run`` and ``rpc``; a mixed
    # sequence across all three must still call ``create_promise`` in call order.
    ctx = _root(
        [
            _resolved("root.2", "remote-1"),
            _resolved("root.3", None),
        ]
    )
    seen: list[str] = []
    original = ctx._state.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx._state.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.run(double, 1)  # root.1 (executes locally)
        f2 = ctx.rpc("remote_fn")  # root.2 (preloaded resolved)
        f3 = ctx.sleep(timedelta(seconds=1))  # root.3 (preloaded resolved timer)
        # Await in reverse so completion order is decoupled from call order.
        assert await f3 is None
        assert await f2 == "remote-1"
        assert await f1 == 2

    assert seen == ["root.1", "root.2", "root.3"]


@pytest.mark.asyncio
async def test_mixed_run_rpc_sleep_promise_create_in_call_order() -> None:
    # All four entrypoints (``run``/``rpc``/``sleep``/``promise``) share the
    # same ``_tail`` chain; a mixed sequence across every code path must still
    # call ``create_promise`` in call order.
    ctx = _root(
        [
            _resolved("root.2", "remote-1"),
            _resolved("root.3", None),
            _resolved("root.4", "di-1"),
        ]
    )
    seen: list[str] = []
    original = ctx._state.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx._state.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.run(double, 1)  # root.1 (executes locally)
        f2 = ctx.rpc("remote_fn")  # root.2 (preloaded resolved)
        f3 = ctx.sleep(timedelta(seconds=1))  # root.3 (preloaded resolved timer)
        f4 = ctx.promise(timedelta(seconds=1))  # root.4 (preloaded resolved DI)
        # Await in reverse so completion order is decoupled from call order.
        assert await f4 == "di-1"
        assert await f3 is None
        assert await f2 == "remote-1"
        assert await f1 == 2

    assert seen == ["root.1", "root.2", "root.3", "root.4"]


# =============================================================================
# Context.invoke_with_retry -- retry only pure-function failures
# =============================================================================
#
# The rule under test: a body that performs any durable op (run/rpc/sleep/
# promise/detached, all of which flip ``workflow`` via ``_begin_durable_op``) is a
# workflow and settles its failure on the first attempt; only a pure leaf -- no
# durable footprint -- is retried, up to what the policy allows. ``delay=0`` keeps
# the in-process ``asyncio.sleep`` between attempts instant.


@pytest.mark.asyncio
async def test_invoke_with_retry_returns_on_first_success() -> None:
    ctx = _root()
    calls = 0

    async def leaf(ctx: Context) -> int:
        nonlocal calls
        calls += 1
        return 7

    df = DurableFunction(leaf)
    out = await ctx.invoke_with_retry(
        df, df.pack_args(), Constant(max_retries=3, delay=0)
    )
    assert out == 7
    assert calls == 1


@pytest.mark.asyncio
async def test_invoke_with_retry_retries_pure_leaf_until_success() -> None:
    ctx = _root()
    calls = 0

    async def flaky(ctx: Context) -> int:
        nonlocal calls
        calls += 1
        if calls < 3:
            msg = "transient"
            raise RuntimeError(msg)
        return 42

    df = DurableFunction(flaky)
    out = await ctx.invoke_with_retry(
        df, df.pack_args(), Constant(max_retries=5, delay=0)
    )
    assert out == 42
    assert calls == 3  # failed twice, succeeded on the third attempt


@pytest.mark.asyncio
async def test_invoke_with_retry_exhausts_then_raises() -> None:
    ctx = _root()
    calls = 0

    async def always_fails(ctx: Context) -> int:
        nonlocal calls
        calls += 1
        msg = "denied"
        raise RuntimeError(msg)

    df = DurableFunction(always_fails)
    with pytest.raises(RuntimeError, match="denied"):
        await ctx.invoke_with_retry(
            df, df.pack_args(), Constant(max_retries=2, delay=0)
        )
    # attempt 0 (initial) + attempts 1, 2 (the two allowed retries) = 3 calls.
    assert calls == 3


@pytest.mark.asyncio
async def test_invoke_with_retry_never_policy_does_not_retry() -> None:
    ctx = _root()
    calls = 0

    async def always_fails(ctx: Context) -> int:
        nonlocal calls
        calls += 1
        msg = "denied"
        raise RuntimeError(msg)

    df = DurableFunction(always_fails)
    with pytest.raises(RuntimeError, match="denied"):
        await ctx.invoke_with_retry(df, df.pack_args(), Never())
    assert calls == 1


@pytest.mark.asyncio
async def test_invoke_with_retry_workflow_never_retries() -> None:
    ctx = _root()
    calls = 0

    async def workflow(ctx: Context) -> int:
        nonlocal calls
        calls += 1
        # Touch a durable op -- ``_begin_durable_op`` is the chokepoint every
        # run/rpc/sleep/promise/detached shares, so this is exactly what flips
        # the ``workflow`` flag -- then fail. A generous policy must NOT retry.
        ctx._state.workflow = True
        msg = "boom after durable op"
        raise RuntimeError(msg)

    df = DurableFunction(workflow)
    with pytest.raises(RuntimeError, match="boom after durable op"):
        await ctx.invoke_with_retry(
            df, df.pack_args(), Constant(max_retries=9, delay=0)
        )
    assert ctx._state.workflow is True
    assert calls == 1


@pytest.mark.asyncio
async def test_invoke_with_retry_propagates_suspended_without_retry() -> None:
    ctx = _root()
    calls = 0

    async def suspends(ctx: Context) -> int:
        nonlocal calls
        calls += 1
        raise Suspended

    df = DurableFunction(suspends)
    with pytest.raises(Suspended):
        await ctx.invoke_with_retry(
            df, df.pack_args(), Constant(max_retries=9, delay=0)
        )
    assert calls == 1


# ── ctx.run inherits / overrides the context's default policy ────────────────


@pytest.mark.asyncio
async def test_ctx_run_child_inherits_context_default_retry_policy() -> None:
    """A failing leaf is retried per the context's inherited default policy."""
    calls = 0

    async def flaky(ctx: Context) -> int:
        nonlocal calls
        calls += 1
        msg = "boom"
        raise ApplicationError(msg)

    ctx = _root(retry_policy=Constant(max_retries=2, delay=0))
    with pytest.raises(ApplicationError, match="boom"):
        await ctx.run(flaky)
    assert calls == 3  # initial + 2 inherited retries, then settled


@pytest.mark.asyncio
async def test_ctx_run_with_opts_retry_policy_overrides_context_default() -> None:
    """``with_opts(retry_policy=...)`` wins over the inherited default."""
    calls = 0

    async def flaky(ctx: Context) -> int:
        nonlocal calls
        calls += 1
        msg = "boom"
        raise ApplicationError(msg)

    # Generous inherited default, but the per-call override is Never.
    ctx = _root(retry_policy=Constant(max_retries=9, delay=0))
    with pytest.raises(ApplicationError, match="boom"):
        await ctx.options(retry_policy=Never()).run(flaky)
    assert calls == 1  # override Never -> no retry


# =============================================================================
# run: by-name dispatch (registry lookup)
#
# ``run`` also accepts a registered *name*. Unlike the by-object form (which
# wraps the object directly), a name is resolved to a local ``DurableFunction``
# via the context's registry and executed here as an ordinary local child --
# versioned by ``opts.version`` since a name carries none.
# =============================================================================


@pytest.mark.asyncio
async def test_run_by_name_resolves_and_executes_from_registry() -> None:
    registry = Registry()
    registry.register("double", double)
    ctx = _root(registry=registry)
    # Same local-child semantics as ``ctx.run(double, 21)``, reached by name.
    assert await ctx.run("double", 21) == 42


@pytest.mark.asyncio
async def test_run_by_name_dispatches_opts_version() -> None:
    async def v1(ctx: Context) -> str:
        return "one"

    async def v2(ctx: Context) -> str:
        return "two"

    registry = Registry()
    registry.register("impl", v1, 1)
    registry.register("impl", v2, 2)
    ctx = _root(registry=registry)
    # A name carries no version, so the call's ``opts.version`` selects the impl.
    assert await ctx.run("impl") == "one"  # default version 1
    assert await ctx.options(version=2).run("impl") == "two"


@pytest.mark.asyncio
async def test_run_by_name_unregistered_raises_function_not_found() -> None:
    # Resolution happens in the synchronous body of ``run``, so an unknown name
    # fails fast at the call site -- before any background task is spawned. A
    # registry miss on this worker is a deployment-skew condition (like the root
    # lookup in Core), so it surfaces as an unswallowable PlatformError that
    # releases the task, not a domain rejection -- with the FunctionNotFoundError
    # as its cause.
    ctx = _root()  # empty registry
    with pytest.raises(PlatformError, match="missing") as excinfo:
        ctx.run("missing")
    assert isinstance(excinfo.value.causes[0], FunctionNotFoundError)


# =============================================================================
# rpc: by-object dispatch (reverse registry lookup)
#
# ``rpc`` also accepts the function *object*: its registered name is recovered
# by identity (reverse lookup) and dispatched over the wire, carrying its own
# registered version. An unregistered object raises -- its registry name is not
# its ``__name__``, so the dispatch target cannot be guessed.
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_by_object_dispatches_registered_name_and_version() -> None:
    registry = Registry()
    registry.register("remote_impl", double, 3)
    ctx = _root(registry=registry)
    with _spy_create_promise(ctx) as captured, pytest.raises(Suspended):
        await ctx.rpc(double, 5)

    [req] = captured
    # Dispatched by the registered name (not ``double.__name__``) and the
    # registered version -- not ``opts.version``.
    assert req.param.data == TaskData(
        func="remote_impl", args=(5,), kwargs={}, version=3
    )


@pytest.mark.asyncio
async def test_rpc_by_object_version_from_identity_not_opts() -> None:
    registry = Registry()
    registry.register("remote_impl", double, 2)
    ctx = _root(registry=registry)
    with _spy_create_promise(ctx) as captured, pytest.raises(Suspended):
        await ctx.options(version=9).rpc(double, 5)

    # The object carries its own version, so ``opts.version`` (9) is ignored.
    assert captured[0].param.data.version == 2


@pytest.mark.asyncio
async def test_rpc_by_object_unregistered_raises() -> None:
    async def stranger(ctx: Context) -> None: ...

    # No reverse entry: refuse to guess a dispatch name from ``__name__``. Raised
    # synchronously at the call site, before any promise is created. Like a
    # by-name ``run`` miss, the unregistered target is a deployment-skew
    # condition, so it releases the task as an unswallowable PlatformError
    # (wrapping the FunctionNotFoundError) rather than settling rejected.
    ctx = _root()  # empty registry
    with (
        _spy_create_promise(ctx) as captured,
        pytest.raises(PlatformError, match="stranger") as excinfo,
    ):
        ctx.rpc(stranger)
    assert isinstance(excinfo.value.causes[0], FunctionNotFoundError)
    assert captured == []  # nothing dispatched


# =============================================================================
# rpc: by-object dispatch is typed -- the settled remote result is coerced to
# the registered function's declared return type (symmetric with ``ctx.run``).
# A by-name dispatch has no local function, so its result stays raw builtins.
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_by_object_recovery_coerces_return_to_declared_struct() -> None:
    # The settled remote value was stored as plain builtins (a dict). Because the
    # object dispatch recovers the target's DurableFunction, the settled record is
    # coerced back to the declared return type -- the parent observes a
    # ``_RecoveredPoint``, not a dict (genuine type safety, not just a static
    # annotation).
    registry = Registry()
    registry.register("make_point", _make_point, 1)
    ctx = _root([_resolved("root.1", _RecoveredPoint(x=3, y=4))], registry=registry)
    result = await ctx.rpc(_make_point, 3, 4)
    assert isinstance(result, _RecoveredPoint)
    assert result == _RecoveredPoint(x=3, y=4)


@pytest.mark.asyncio
async def test_rpc_by_name_recovery_stays_raw_builtins() -> None:
    # The contrast case: a by-name dispatch has no local function to coerce
    # against, so the settled value passes through as raw builtins (a dict),
    # untyped by design -- matching the top-level ``rpc``/``get``.
    ctx = _root([_resolved("root.1", _RecoveredPoint(x=3, y=4))])
    result = await ctx.rpc("make_point", 3, 4)
    assert isinstance(result, dict)
    assert result == {"x": 3, "y": 4}
