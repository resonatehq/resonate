"""Behaviour tests for :mod:`resonate.resonate`.

Key API properties exercised here:

* ``run`` / ``rpc`` are **synchronous** fire-and-forget triggers returning a
  :class:`~resonate.handle.ResonateHandle` -- the task is created (and, when this
  process wins the race, executed) in the background, exactly like ``ctx.run``.
  The result is awaited via ``handle.result()``.
* Per-call options come from :meth:`~resonate.resonate.Resonate.with_opts`
  (chained), not keyword arguments.
* ``get`` stays ``async`` (a lookup whose listener registration surfaces a 404).

Like the rest of the suite these run against the real in-process
:class:`~resonate.network.LocalNetwork` driven through the real
:class:`~resonate.send.Sender` / :class:`~resonate.transport.Transport` -- "real
server, real wire", no mocks.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest import mock

import msgspec
import pytest

from resonate.durable import DurableFunction
from resonate.error import (
    AlreadyRegisteredError,
    ApplicationError,
    FunctionNotFoundError,
    ServerError,
)
from resonate.handle import ResonateHandle
from resonate.heartbeat import AsyncHeartbeat, NoopHeartbeat
from resonate.network import LocalNetwork
from resonate.resonate import (
    DEFAULT_MAX_CONCURRENT_TASKS,
    DEFAULT_TTL,
    HEARTBEAT_INTERVAL_DIVISOR,
    Opts,
    Resonate,
)
from resonate.retry import Never

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from resonate.codec import Encryptor
    from resonate.context import Context
    from resonate.types import PromiseRecord


# ── Harness ──────────────────────────────────────────────────────────────


@contextlib.asynccontextmanager
async def local(
    *,
    group: str | None = None,
    pid: str | None = None,
    ttl: timedelta | None = None,
    encryptor: Encryptor | None = None,
    prefix: str | None = None,
    max_concurrent_tasks: int | None = None,
) -> AsyncIterator[Resonate]:
    """Yield a local-mode Resonate, stopping it (and its refresh task) on exit."""
    # Pin ``Never`` so a failing pure leaf settles immediately: the SDK default
    # is now an effectively-unbounded Exponential, which would retry such a leaf
    # forever and hang these tests. Tests asserting retry behavior live in
    # ``test_context.py`` with explicit policies.
    r = Resonate(
        group=group,
        pid=pid,
        ttl=ttl,
        encryptor=encryptor,
        prefix=prefix,
        max_concurrent_tasks=max_concurrent_tasks,
        retry_policy=Never(),
    )
    try:
        yield r
    finally:
        await r.stop()


async def wait_for_promise(r: Resonate, id: str, tries: int = 200) -> PromiseRecord:
    """Poll until the durable promise ``id`` exists, for fire-and-forget creates.

    ``rpc`` (and ``run`` before its result settles) creates the promise in the
    background; a remote ``rpc`` promise never settles in local mode, so its
    creation can't be observed by awaiting a result.
    """
    for _ in range(tries):
        try:
            return await r.promises.get(id)
        except ServerError:
            await asyncio.sleep(0)
    msg = f"promise {id} was never created"
    raise AssertionError(msg)


# ── Workflow library ───────────────────────────────────────────────────────


async def noop(ctx: Context) -> None:
    return None


async def add(ctx: Context, x: int, y: int) -> int:
    return x + y


async def boom(ctx: Context) -> int:
    msg = "deliberate failure"
    raise ApplicationError(msg)


class Point(msgspec.Struct, frozen=True):
    x: int
    y: int


async def make_point(ctx: Context, x: int, y: int) -> Point:
    return Point(x=x, y=y)


@dataclasses.dataclass(frozen=True)
class Vec:
    dx: int
    dy: int


async def make_vec(ctx: Context, dx: int, dy: int) -> Vec:
    return Vec(dx=dx, dy=dy)


# Deliberately unannotated: annotations are optional in Python, so the SDK must
# run a function that declares none. No param annotations means arg coercion is
# skipped (pass-through); no return annotation means the result decodes as Any.
def bare_add(ctx, x, y):  # noqa: ANN001, ANN201
    return x + y


async def add_via_child(ctx: Context, x: int, y: int) -> int:
    # A multi-step workflow: spawns a child via ``ctx.run`` and awaits it. The
    # child function need not be registered -- ``ctx.run`` takes the object.
    return await ctx.run(add, x, y)


# ═══════════════════════════════════════════════════════════════════════════
#  Constructor / configuration
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_local_constructor_sets_defaults() -> None:
    async with local() as r:
        assert r._pid == "default"
        assert r._id_prefix == ""
        assert r._ttl == DEFAULT_TTL
        assert isinstance(r._network, LocalNetwork)


@pytest.mark.asyncio
async def test_config_with_custom_pid_and_group() -> None:
    async with local(pid="worker-1", group="workers") as r:
        assert r._pid == "worker-1"
        assert "worker-1" in r._network.unicast()
        assert "workers" in r._network.unicast()


@pytest.mark.asyncio
async def test_config_with_prefix() -> None:
    async with local(prefix="myapp", ttl=timedelta(seconds=30)) as r:
        assert r._id_prefix == "myapp:"
        assert r._ttl == timedelta(seconds=30)


@pytest.mark.asyncio
async def test_config_with_empty_prefix() -> None:
    async with local(prefix="") as r:
        assert r._id_prefix == ""


@pytest.mark.asyncio
async def test_default_ttl_is_one_minute() -> None:
    async with local() as r:
        assert r._ttl == timedelta(minutes=1)


@pytest.mark.asyncio
async def test_network_identity_local_mode() -> None:
    async with local() as r:
        assert r._network.unicast().startswith("local://uni@")
        assert r._network.anycast().startswith("local://any@")
        assert r._network.group() == "default"
        assert r._network.pid() == "default"


@pytest.mark.asyncio
async def test_target_resolver_returns_local_anycast() -> None:
    async with local() as r:
        assert r._network.target_resolver("my-target") == "local://any@my-target"


@pytest.mark.asyncio
async def test_local_mode_uses_noop_heartbeat() -> None:
    async with local() as r:
        assert isinstance(r._heartbeat, NoopHeartbeat)


@pytest.mark.asyncio
async def test_remote_network_uses_async_heartbeat() -> None:
    # A non-Local network selects the AsyncHeartbeat branch without any HTTP.
    r = Resonate(network=_FakeNetwork())
    try:
        assert isinstance(r._heartbeat, AsyncHeartbeat)
    finally:
        await r.stop()


@pytest.mark.asyncio
async def test_explicit_heartbeat_override_wins() -> None:
    hb = NoopHeartbeat()
    r = Resonate(network=_FakeNetwork(), heartbeat=hb)
    try:
        assert r._heartbeat is hb
    finally:
        await r.stop()


@pytest.mark.asyncio
async def test_heartbeat_interval_is_a_third_of_the_ttl() -> None:
    """The heartbeat beats ``ttl/HEARTBEAT_INTERVAL_DIVISOR``, not ``ttl/2``.

    Three beats per lease tolerate two slow/missed round-trips before a lapse,
    which (with start-anchored pacing) is what keeps leases alive under load.
    """
    r = Resonate(network=_FakeNetwork(), ttl=timedelta(seconds=60))
    try:
        assert isinstance(r._heartbeat, AsyncHeartbeat)
        assert r._heartbeat.interval_ms == 60_000 // HEARTBEAT_INTERVAL_DIVISOR
    finally:
        await r.stop()


class _FakeNetwork:
    """Minimal non-Local :class:`~resonate.network.Network` for heartbeat tests."""

    def pid(self) -> str:
        return "fake"

    def group(self) -> str:
        return "g"

    def unicast(self) -> str:
        return "fake://uni@g/fake"

    def anycast(self) -> str:
        return "fake://any@g/fake"

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, req: str) -> str:
        return "{}"

    def recv(self, callback: Callable[[str], None]) -> None: ...
    def target_resolver(self, target: str) -> str:
        return f"fake://any@{target}"


# ═══════════════════════════════════════════════════════════════════════════
#  register
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_register_then_run_succeeds() -> None:
    async with local() as r:
        r.register(add)
        assert await r.run("t", add, 1, 2).result() == 3


@pytest.mark.asyncio
async def test_register_with_custom_name() -> None:
    async with local() as r:
        r.register(add, name="sum")
        # run takes the function object; its registered name ("sum", not the
        # __name__ "add") is recovered by identity.
        assert await r.run("t", add, 4, 5).result() == 9


@pytest.mark.asyncio
async def test_register_duplicate_raises() -> None:
    async with local() as r:
        r.register(noop)
        with pytest.raises(AlreadyRegisteredError):
            r.register(noop)


# ═══════════════════════════════════════════════════════════════════════════
#  run
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_run_is_synchronous_and_returns_handle() -> None:
    async with local() as r:
        r.register(noop)
        h = r.run("greet-1", noop)
        assert isinstance(h, ResonateHandle)
        assert not asyncio.iscoroutine(h)
        assert await h.id() == "greet-1"


@pytest.mark.asyncio
async def test_run_starts_execution_immediately() -> None:
    async with local() as r:
        started = asyncio.Event()

        @r.register
        async def slow(ctx: Context) -> int:
            started.set()
            return 1

        h = r.run("s", slow)
        # The body runs in the background without awaiting the result.
        await asyncio.wait_for(started.wait(), timeout=1)
        assert await h.result() == 1


@pytest.mark.asyncio
async def test_run_resolves_result() -> None:
    async with local() as r:
        r.register(add)
        assert await r.run("a", add, 2, 3).result() == 5


@pytest.mark.asyncio
async def test_run_with_kwargs() -> None:
    async with local() as r:
        r.register(add)
        assert await r.run("a", add, x=4, y=6).result() == 10


@pytest.mark.asyncio
async def test_run_decodes_struct_result() -> None:
    async with local() as r:
        r.register(make_point)
        assert await r.run("pt", make_point, 1, 2).result() == Point(x=1, y=2)


@pytest.mark.asyncio
async def test_run_decodes_dataclass_result() -> None:
    # A ``-> Vec`` (stdlib dataclass) return annotation is resolved end-to-end and
    # the wire dict is coerced back into a ``Vec`` instance, not left a dict.
    async with local() as r:
        r.register(make_vec)
        result = await r.run("vec", make_vec, 3, 4).result()
        assert result == Vec(dx=3, dy=4)
        assert isinstance(result, Vec)


@pytest.mark.asyncio
async def test_run_rejected_workflow_raises() -> None:
    async with local() as r:
        r.register(boom)
        with pytest.raises(ApplicationError, match="deliberate failure"):
            await r.run("b", boom).result()


@pytest.mark.asyncio
async def test_run_with_prefix_prepends_id() -> None:
    async with local(prefix="app") as r:
        r.register(noop)
        h = r.run("my-id", noop)
        assert await h.id() == "app:my-id"


@pytest.mark.asyncio
async def test_run_unregistered_raises_synchronously() -> None:
    async with local() as r:

        async def unregistered(ctx: Context) -> None:
            return None

        # Raised at the call site (not from an awaited coroutine): an
        # unregistered function object is refused outright -- its registry name
        # is not its __name__, so the dispatch target cannot be guessed (the same
        # rule rpc follows).
        with pytest.raises(FunctionNotFoundError):
            r.run("x", unregistered)


@pytest.mark.asyncio
async def test_run_by_object_unregistered_does_not_dispatch_name_collision() -> None:
    # By-object resolution is by identity, not by ``__name__``: an unregistered
    # object whose ``__name__`` collides with a *different* registered function
    # must be refused, never silently dispatched to that function. (Under a
    # ``__name__`` fallback this would have wrongly run ``add``.)
    async def impostor(ctx: Context) -> int:
        return -1

    impostor.__name__ = "add"  # collide with the registered ``add``

    async with local() as r:
        r.register(add)
        with pytest.raises(FunctionNotFoundError):
            r.run("x", impostor)


@pytest.mark.asyncio
async def test_run_idempotent_same_id() -> None:
    async with local() as r:
        r.register(add)
        assert await r.run("dup", add, 1, 1).result() == 2
        # Second run with the same id observes the existing settled promise.
        assert await r.run("dup", add, 1, 1).result() == 2


@pytest.mark.asyncio
async def test_run_unannotated_function_resolves() -> None:
    # Annotations are optional: a function with no param/return annotations runs
    # end-to-end. The result decodes as Any (pass-through) since there is no
    # return annotation to coerce against.
    async with local() as r:
        r.register(bare_add)
        assert await r.run("bare", bare_add, 2, 3).result() == 5


@pytest.mark.asyncio
async def test_run_handle_id_resolves_to_created_id() -> None:
    # ``id()`` is gated on the background promise creation; once that confirms it
    # yields the (prefixed) id. Awaiting the result guarantees creation happened.
    async with local() as r:
        r.register(add)
        h = r.run("rid", add, 1, 1)
        assert await h.id() == "rid"
        await h.result()
        # Still available -- and immediate -- after settling.
        assert await h.id() == "rid"


@pytest.mark.asyncio
async def test_run_done_false_until_settled() -> None:
    async with local() as r:
        gate = asyncio.Event()

        @r.register
        async def waits(ctx: Context) -> int:
            await gate.wait()
            return 7

        h = r.run("rd", waits)
        assert h.done() is False
        gate.set()
        assert await h.result() == 7
        assert h.done() is True


@pytest.mark.asyncio
async def test_run_returns_none_result() -> None:
    async with local() as r:
        r.register(noop)
        assert await r.run("rn", noop).result() is None


@pytest.mark.asyncio
async def test_run_multistep_workflow_resolves() -> None:
    # A top-level run of a workflow that itself spawns a child via ctx.run.
    async with local() as r:
        r.register(add_via_child)
        assert await r.run("wf", add_via_child, 4, 5).result() == 9


@pytest.mark.asyncio
async def test_run_default_target_uses_network_resolver() -> None:
    async with local() as r:
        r.register(noop)
        await r.run("rt", noop).result()
        record = await r.promises.get("rt")
        assert record.tags["resonate:target"] == "local://any@default"
        assert record.tags["resonate:scope"] == "global"


# ── run by name (registry lookup) ────────────────────────────────────────────
#    ``run`` also accepts the registered *name*. The function must still be
#    registered locally (run executes here); a name carries no version, so it is
#    dispatched at ``options``'s version.


@pytest.mark.asyncio
async def test_run_by_name_resolves_from_registry() -> None:
    async with local() as r:
        r.register(add)
        assert await r.run("a", "add", 2, 3).result() == 5


@pytest.mark.asyncio
async def test_run_by_name_decodes_struct_result() -> None:
    async with local() as r:
        r.register(make_point)
        # A by-name run still resolves a local DurableFunction, so its return is
        # coerced to the declared type, exactly like the by-object form.
        assert await r.run("pt", "make_point", 1, 2).result() == Point(x=1, y=2)


@pytest.mark.asyncio
async def test_run_by_name_uses_opts_version() -> None:
    async def v1(ctx: Context) -> str:
        return "one"

    async def v2(ctx: Context) -> str:
        return "two"

    async with local() as r:
        r.register(v1, name="impl", version=1)
        r.register(v2, name="impl", version=2)
        assert await r.run("x1", "impl").result() == "one"  # default version 1
        assert await r.options(version=2).run("x2", "impl").result() == "two"


@pytest.mark.asyncio
async def test_run_by_name_unregistered_raises_synchronously() -> None:
    async with local() as r:
        # By-name resolution happens in the synchronous body of ``run``.
        with pytest.raises(FunctionNotFoundError, match="ghost"):
            r.run("x", "ghost")


# ═══════════════════════════════════════════════════════════════════════════
#  rpc
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_rpc_is_synchronous_and_returns_handle() -> None:
    async with local() as r:
        h = r.rpc("rpc-1", "remote_fn", 1)
        assert isinstance(h, ResonateHandle)
        assert not asyncio.iscoroutine(h)
        assert await h.id() == "rpc-1"


@pytest.mark.asyncio
async def test_rpc_does_not_require_registration() -> None:
    async with local() as r:
        r.rpc("rpc-1", "remote_fn", 1)
        # The promise is created even though no function is registered locally.
        record = await wait_for_promise(r, "rpc-1")
        assert record.state == "pending"


@pytest.mark.asyncio
async def test_rpc_with_prefix() -> None:
    async with local(prefix="svc") as r:
        h = r.rpc("rpc-2", "remote", ())
        assert await h.id() == "svc:rpc-2"
        await wait_for_promise(r, "svc:rpc-2")


@pytest.mark.asyncio
async def test_rpc_args_and_kwargs_round_trip_into_param() -> None:
    async with local() as r:
        r.rpc("rpc-args", "remote", 1, 2, flag=True)
        record = await wait_for_promise(r, "rpc-args")
        assert record.param.data == {
            "func": "remote",
            "args": [1, 2],
            "kwargs": {"flag": True},
            "version": 1,
        }


@pytest.mark.asyncio
async def test_rpc_no_args_has_empty_args() -> None:
    async with local() as r:
        r.rpc("rpc-empty", "remote")
        record = await wait_for_promise(r, "rpc-empty")
        assert record.param.data == {
            "func": "remote",
            "args": [],
            "kwargs": {},
            "version": 1,
        }


@pytest.mark.asyncio
async def test_rpc_default_target() -> None:
    async with local() as r:
        r.rpc("rpc-dt", "remote")
        record = await wait_for_promise(r, "rpc-dt")
        assert record.tags["resonate:target"] == "local://any@default"


@pytest.mark.asyncio
async def test_rpc_done_false_while_pending() -> None:
    # A remote rpc promise never settles in local mode, so the handle stays
    # un-done after the promise is created.
    async with local() as r:
        h = r.rpc("rpc-pending", "remote")
        await wait_for_promise(r, "rpc-pending")
        assert h.done() is False


@pytest.mark.asyncio
async def test_rpc_idempotent_same_id() -> None:
    # Two rpc calls with the same id both yield handles to the one promise.
    async with local() as r:
        h1 = r.rpc("rpc-dup", "remote", 1)
        h2 = r.rpc("rpc-dup", "remote", 1)
        assert await h1.id() == "rpc-dup"
        assert await h2.id() == "rpc-dup"
        record = await wait_for_promise(r, "rpc-dup")
        # The first create wins; the param reflects that single promise.
        assert record.param.data
        assert record.param.data["func"] == "remote"


@pytest.mark.asyncio
async def test_rpc_handle_id_resolves() -> None:
    async with local() as r:
        h = r.rpc("rpc-id", "remote")
        assert await h.id() == "rpc-id"
        assert h.done() is False


# ── rpc by object (reverse registry lookup) ──────────────────────────────────
#    ``rpc`` also accepts the function *object*: its registered name is recovered
#    by identity and dispatched over the wire, carrying its own registered
#    version. The object form is locally registered by definition, so -- unlike
#    by-name rpc -- its result is decoded against the declared return type.


@pytest.mark.asyncio
async def test_rpc_by_object_dispatches_registered_name() -> None:
    async with local() as r:
        r.register(add, name="sum", version=1)
        r.rpc("rpc-obj", add, 1, 2)
        record = await wait_for_promise(r, "rpc-obj")
        # Dispatched by the registered name, not ``add.__name__``.
        assert record.param.data
        assert record.param.data["func"] == "sum"
        assert record.param.data["version"] == 1
        assert record.param.data["args"] == [1, 2]


@pytest.mark.asyncio
async def test_rpc_by_object_version_from_identity() -> None:
    async def impl(ctx: Context) -> None: ...

    async with local() as r:
        r.register(impl, name="impl", version=4)
        # The object carries its own version, so ``options(version=9)`` is ignored.
        r.options(version=9).rpc("rpc-ver", impl)
        record = await wait_for_promise(r, "rpc-ver")
        assert record.param.data
        assert record.param.data["func"] == "impl"
        assert record.param.data["version"] == 4


@pytest.mark.asyncio
async def test_rpc_by_object_unregistered_raises() -> None:
    async def stranger(ctx: Context) -> None: ...

    async with local() as r:
        # A function object's registry name is not its ``__name__``, so an
        # unregistered object cannot be dispatched: refuse rather than guess.
        # Raised synchronously at the call site, so no promise is created.
        with pytest.raises(FunctionNotFoundError, match="stranger"):
            r.rpc("rpc-stranger", stranger)


@pytest.mark.asyncio
async def test_rpc_by_object_handle_is_typed_by_name_is_any() -> None:
    async with local() as r:
        r.register(make_point)
        # A remote rpc promise never settles in local mode, so this asserts the
        # handle's decode type directly (white-box). A by-object dispatch carries
        # the registered function's return annotation, exactly like ``run``; a
        # by-name dispatch has no local function to read one from, so it is ``Any``.
        typed = r.rpc("rpc-typed", make_point, 1, 2)
        untyped = r.rpc("rpc-untyped", "make_point", 1, 2)
        await wait_for_promise(r, "rpc-typed")
        await wait_for_promise(r, "rpc-untyped")
        assert typed._type is Point
        assert untyped._type is Any


# ═══════════════════════════════════════════════════════════════════════════
#  with_dependency (DI)
#
# ``with_dependency`` stores a value keyed by concrete type into the shared
# DependencyMap, and a running workflow reads it back via ``ctx.get_dependency``.
# ═══════════════════════════════════════════════════════════════════════════


class Config:
    def __init__(self, value: str) -> None:
        self.value = value


class Counter:
    def __init__(self, count: int) -> None:
        self.count = count


async def read_config(ctx: Context) -> str:
    return ctx.get_dependency(Config).value


async def read_two_deps(ctx: Context) -> str:
    cfg = ctx.get_dependency(Config)
    counter = ctx.get_dependency(Counter)
    return f"{cfg.value}:{counter.count}"


@pytest.mark.asyncio
async def test_with_dependency_returns_self_for_chaining() -> None:
    async with local() as r:
        assert r.with_dependency(Config("x")) is r


@pytest.mark.asyncio
async def test_workflow_reads_dependency_via_context() -> None:
    async with local() as r:
        r.with_dependency(Config("hello-from-di"))
        r.register(read_config)
        assert await r.run("di-ctx", read_config).result() == "hello-from-di"


@pytest.mark.asyncio
async def test_multiple_dependencies() -> None:
    # Each ``with_dependency`` keys by concrete type, so distinct types coexist
    # and a workflow can read every one of them.
    async with local() as r:
        r.with_dependency(Config("multi")).with_dependency(Counter(42))
        r.register(read_two_deps)
        assert await r.run("di-multi", read_two_deps).result() == "multi:42"


# ═══════════════════════════════════════════════════════════════════════════
#  options
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_options_mints_new_handle_sharing_state() -> None:
    # Mirrors Context.options: a *new* handle over the same engine, carrying
    # its own opts; the originating handle keeps its defaults untouched.
    async with local() as r:
        scoped = r.options(timeout=timedelta(seconds=1))
        assert scoped is not r
        # The shallow copy shares everything by reference: the rebound-state
        # container and the construction-time wiring alike.
        assert scoped._runtime is r._runtime
        assert scoped.promises is r.promises
        assert scoped.schedules is r.schedules
        assert scoped.opts == Opts(timeout=timedelta(seconds=1))
        assert r.opts == Opts()


@pytest.mark.asyncio
async def test_options_handles_are_holdable_and_reusable() -> None:
    # Two held handles never interfere -- each run/rpc reads its own frozen
    # opts, so there is no consume/reset step for a second handle to clobber.
    async with local() as r:
        a = r.options(target="worker-a")
        b = r.options(target="worker-b")
        a.rpc("held-a", "remote")
        b.rpc("held-b", "remote")
        # ``a`` still carries worker-a after ``b`` was created and used.
        a.rpc("held-a2", "remote")
        for id, target in [
            ("held-a", "worker-a"),
            ("held-b", "worker-b"),
            ("held-a2", "worker-a"),
        ]:
            record = await wait_for_promise(r, id)
            assert record.tags["resonate:target"] == f"local://any@{target}"


@pytest.mark.asyncio
async def test_with_opts_bare_name_target_rewritten() -> None:
    async with local() as r:
        r.options(target="my-worker").rpc("t-bare", "remote")
        record = await wait_for_promise(r, "t-bare")
        assert record.tags["resonate:target"] == "local://any@my-worker"


@pytest.mark.asyncio
async def test_with_opts_url_target_passes_through() -> None:
    async with local() as r:
        url = "https://remote:9000/workers/hello"
        r.options(target=url).rpc("t-url", "remote")
        record = await wait_for_promise(r, "t-url")
        assert record.tags["resonate:target"] == url


@pytest.mark.asyncio
async def test_run_version_comes_from_registration() -> None:
    # run() recovers the version by function identity, not from with_opts:
    # the registered object carries its own version.
    async with local() as r:
        r.register(noop, version=99)
        await r.run("t-tags", noop).result()
        record = await r.promises.get("t-tags")
        assert record.param.data
        assert record.param.data.get("version") == 99
        # SDK tags still present.
        assert record.tags["resonate:scope"] == "global"


@pytest.mark.asyncio
async def test_rpc_version_comes_from_opts() -> None:
    # rpc() dispatches by name, so with_opts(version=) selects the version.
    async with local() as r:
        r.options(version=7).rpc("t-rpc-ver", "remote")
        record = await wait_for_promise(r, "t-rpc-ver")
        assert record.param.data
        assert record.param.data.get("version") == 7


@pytest.mark.asyncio
async def test_with_opts_applies_to_run_target() -> None:
    async with local() as r:
        r.register(noop)
        await r.options(target="my-target").run("rt2", noop).result()
        record = await r.promises.get("rt2")
        assert record.tags["resonate:target"] == "local://any@my-target"


# ═══════════════════════════════════════════════════════════════════════════
#  get
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_get_nonexistent_raises_404() -> None:
    async with local() as r:
        with pytest.raises(ServerError) as excinfo:
            await r.get("nonexistent")
        assert excinfo.value.code == 404


@pytest.mark.asyncio
async def test_get_existing_returns_handle() -> None:
    async with local() as r:
        r.register(add)
        await r.run("g1", add, 1, 2).result()
        handle = await r.get("g1")
        assert await handle.id() == "g1"
        assert await handle.result() == 3


@pytest.mark.asyncio
async def test_get_with_prefix_prepends() -> None:
    async with local(prefix="ns") as r:
        r.rpc("p1", "remote")
        await wait_for_promise(r, "ns:p1")
        handle = await r.get("p1")
        assert await handle.id() == "ns:p1"


@pytest.mark.asyncio
async def test_get_pending_promise_returns_unsettled_handle() -> None:
    # get on a still-pending promise returns a handle that is not yet done.
    async with local() as r:
        r.rpc("g-pending", "remote")
        await wait_for_promise(r, "g-pending")
        handle = await r.get("g-pending")
        assert handle.done() is False


@pytest.mark.asyncio
async def test_get_rejected_promise_raises_on_result() -> None:
    async with local() as r:
        r.register(boom)
        with contextlib.suppress(ApplicationError):
            await r.run("g-boom", boom).result()
        handle = await r.get("g-boom")
        with pytest.raises(ApplicationError, match="deliberate failure"):
            await handle.result()


@pytest.mark.asyncio
async def test_get_decodes_result_as_any() -> None:
    # get is untyped: a struct result that run would decode to ``Point`` comes
    # back through get as the raw mapping, since there is no type to coerce to.
    async with local() as r:
        r.register(make_point)
        await r.run("g-pt", make_point, 1, 2).result()
        handle = await r.get("g-pt")
        assert await handle.result() == {"x": 1, "y": 2}


@pytest.mark.asyncio
async def test_get_twice_shares_subscription() -> None:
    async with local() as r:
        r.register(add)
        await r.run("g-share", add, 1, 2).result()
        h1 = await r.get("g-share")
        h2 = await r.get("g-share")
        assert await h1.result() == 3
        assert await h2.result() == 3


# ═══════════════════════════════════════════════════════════════════════════
#  Multiple handles to the same id
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_multiple_handles_same_id_all_resolve() -> None:
    async with local() as r:
        r.register(add)
        h1 = r.run("multi", add, 2, 3)
        h2 = await r.get("multi")
        assert await h1.result() == 5
        assert await h2.result() == 5


# ═══════════════════════════════════════════════════════════════════════════
#  id prefix consistency
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_prefix_applied_consistently_to_run_rpc_get() -> None:
    async with local(prefix="p") as r:
        r.register(add)
        h1 = r.run("id1", add, 1, 1)
        assert await h1.id() == "p:id1"
        h2 = r.rpc("id2", "remote")
        assert await h2.id() == "p:id2"
        await wait_for_promise(r, "p:id2")
        h3 = await r.get("id2")
        assert await h3.id() == "p:id2"


# ═══════════════════════════════════════════════════════════════════════════
#  schedule
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_schedule_creates_and_deletes() -> None:
    async with local() as r:
        schedule = await r.schedule("my-schedule", "*/5 * * * *", "my-func")
        assert schedule.name == "my-schedule"
        # Deleting a created schedule does not raise.
        await schedule.delete()


@pytest.mark.asyncio
async def test_schedule_injects_resonate_target_tag() -> None:
    async with local() as r:
        await r.schedule("tagged-schedule", "*/5 * * * *", "my-func")
        record = await r.schedules.get("tagged-schedule")
        assert record.promise_tags["resonate:target"] == "local://any@default"


@pytest.mark.asyncio
async def test_schedule_resolves_target_from_options() -> None:
    async with local() as r:
        await r.options(target="workers").schedule(
            "targeted-schedule", "*/5 * * * *", "my-func"
        )
        record = await r.schedules.get("targeted-schedule")
        assert record.promise_tags["resonate:target"] == "local://any@workers"


# ═══════════════════════════════════════════════════════════════════════════
#  stop
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_stop_is_clean_and_idempotent() -> None:
    r = Resonate()
    await r.stop()
    await r.stop()  # second stop is a no-op


@pytest.mark.asyncio
async def test_stop_cancels_refresh_task() -> None:
    r = Resonate()
    handle = r._runtime.refresh_handle
    assert handle is not None
    assert not handle.done()
    await r.stop()
    assert r._runtime.refresh_handle is None
    # Let the cancellation finish processing, then confirm the task is done.
    with contextlib.suppress(asyncio.CancelledError):
        await handle
    assert handle.cancelled()


# ═════════════════════════════════════════════════════════════════════════
#  Promise-gone settlement -- handle must surface a 404 instead of hanging
# ═════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_handle_settles_with_error_when_listener_register_returns_404() -> None:
    """A 404 on the initial listener register must settle the handle.

    Reproduces the silent hang when the server forgets the promise mid-flight
    (ephemeral-storage restart, manual purge): without the 404 catch in
    ``_register_and_settle``, the bg task logs a warning and the subscription
    sits pending for the rest of the process lifetime; ``handle.result()``
    waits on the event forever and the user only learns the workflow is
    unrecoverable by hitting Ctrl+C.
    """
    async with local() as r:

        async def gone(*_args: object, **_kwargs: object) -> object:
            raise ServerError(404, "Awaited promise not found")

        with mock.patch.object(
            r._sender, "promise_register_listener", side_effect=gone
        ):
            # Use rpc so the promise stays pending in local mode -- nothing else
            # can race the 404 to settle the subscription naturally.
            handle = r.rpc("zombie", "remote")
            with pytest.raises(ApplicationError, match="Awaited promise not found"):
                await asyncio.wait_for(handle.result(), timeout=2.0)


@pytest.mark.asyncio
async def test_subscription_refresh_settles_handle_on_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 60s refresh must also settle on 404, not just the initial register.

    Without this, a workflow that started healthy and *later* loses its
    promise (server purge, retention expiry) would hang once the SSE-pushed
    ``unblock`` is no longer possible.
    """
    # Collapse the refresh interval so the test does not actually wait 60s.
    monkeypatch.setattr("resonate.resonate._SUBSCRIPTION_REFRESH_SECS", 0.01)
    async with local() as r:
        # Start with a pending rpc whose listener registers successfully.
        handle = r.rpc("vanish", "remote")
        await wait_for_promise(r, "vanish")

        # Now make the next refresh tick observe a 404 -- the promise has
        # "vanished" from the server.
        async def gone(*_args: object, **_kwargs: object) -> object:
            raise ServerError(404, "Awaited promise not found")

        with (
            mock.patch.object(r._sender, "promise_register_listener", side_effect=gone),
            pytest.raises(ApplicationError, match="Awaited promise not found"),
        ):
            await asyncio.wait_for(handle.result(), timeout=2.0)


@pytest.mark.asyncio
async def test_non_404_server_errors_do_not_settle_the_handle() -> None:
    """Only 404 maps to a synthetic rejection; transient 5xx errors are logged.

    A 500/503 is presumed transient -- the periodic refresh re-registers the
    listener, and the SSE push will eventually settle the handle when the
    promise actually resolves. Settling the handle on those would mask real
    progress; the contract is "only 404 means *definitely* gone".
    """
    async with local() as r:

        async def transient(*_args: object, **_kwargs: object) -> object:
            raise ServerError(503, "transient")

        with mock.patch.object(
            r._sender, "promise_register_listener", side_effect=transient
        ):
            handle = r.rpc("flaky", "remote")
            # Should NOT raise -- the handle stays pending despite the 503.
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(handle.result(), timeout=0.3)


# ── Bounded execution concurrency ────────────────────────────────────────


@pytest.mark.asyncio
async def test_bounded_execute_caps_concurrent_executions() -> None:
    """``_bounded_execute`` never runs more coroutines than the semaphore allows.

    A task holds its lease for the whole acquire→execute→suspend/fulfill span,
    so capping concurrent executions is what keeps the live-lease count low
    enough for the heartbeat to keep every lease alive under a heavy ``execute``
    fan-out (the 409-storm fix). Here we drive many coroutines through the gate
    and assert the observed peak never exceeds the configured ceiling.
    """
    async with local(max_concurrent_tasks=2) as r:
        live = 0
        peak = 0
        gate = asyncio.Event()

        async def work() -> None:
            nonlocal live, peak
            live += 1
            peak = max(peak, live)
            # Hold the permit across an await so peers must contend for it.
            await gate.wait()
            live -= 1

        tasks = [asyncio.create_task(r._bounded_execute(work())) for _ in range(10)]
        # Let everything that can start, start; only the ceiling should be live.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert peak <= 2, f"expected at most 2 concurrent executions, saw {peak}"
        assert live <= 2

        # Release them and confirm they all drain.
        gate.set()
        await asyncio.gather(*tasks)
        assert live == 0
        assert peak <= 2


@pytest.mark.asyncio
async def test_default_concurrency_ceiling_applied() -> None:
    """With no override the semaphore uses :data:`DEFAULT_MAX_CONCURRENT_TASKS`."""
    async with local() as r:
        assert r._runtime.execute_sema._value == DEFAULT_MAX_CONCURRENT_TASKS


# ── DurableFunction.return_type resolution ──────────────────────────────────
#
# The top-level :meth:`Resonate.run` decodes a settled value against
# ``DurableFunction.return_type`` -- the type-shaped view of the resolved return
# annotation, owned by the same ``DurableFunction`` that packs the arguments
# (no separate re-resolution). This module sets ``from __future__ import
# annotations``, so every annotation below is a forward-ref *string* -- the shape
# the resolver handles. ``Context`` is imported under ``TYPE_CHECKING`` only, so
# the module-level workflows double as a guard that an unresolvable *parameter*
# annotation never sabotages return resolution.


def test_return_type_builtin_scalar() -> None:
    assert DurableFunction(add).return_type is int


def test_return_type_builtin_container() -> None:
    def make_list(ctx: Context) -> list[int]:
        return [1, 2, 3]

    assert DurableFunction(make_list).return_type == list[int]


def test_return_type_msgspec_struct() -> None:
    assert DurableFunction(make_point).return_type is Point


def test_return_type_dataclass() -> None:
    assert DurableFunction(make_vec).return_type is Vec


def test_return_type_no_annotation_is_any() -> None:
    # ``bare_add`` declares no return annotation -> passthrough (Any).
    assert DurableFunction(bare_add).return_type is Any


def test_return_type_none_annotation_is_any() -> None:
    # ``-> None`` is a pass-through annotation, so it collapses to ``Any`` --
    # ``convert(None, Any)`` and ``convert(None, None)`` both yield ``None``, and
    # ``Any`` keeps the top-level decode consistent with ``coerce_result``, which
    # treats ``-> None`` as pass-through.
    assert DurableFunction(noop).return_type is Any


def test_return_type_ignores_unresolvable_param() -> None:
    # Regression guard: ``ctx: Context`` is annotated with a TYPE_CHECKING-only
    # name. Resolving the whole signature would raise NameError; only the user
    # params and the return are resolved, so the struct return still resolves.
    assert "Context" not in globals()  # the name is genuinely unbound at runtime
    assert DurableFunction(make_point).return_type is Point


def test_return_type_non_string_annotation_passthrough() -> None:
    # When the return annotation is a real object (no ``from __future__``
    # stringification), it is used as-is without going through resolution.
    def already_typed(ctx: Any) -> Any:
        return None

    already_typed.__annotations__["return"] = dict[str, int]
    assert DurableFunction(already_typed).return_type == dict[str, int]
