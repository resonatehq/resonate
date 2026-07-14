"""The top-level Resonate SDK entrypoint.

:class:`Resonate` is the object applications construct and use directly:
register durable functions with :meth:`Resonate.register`, start them with
:meth:`Resonate.run` or :meth:`Resonate.rpc`, look up existing invocations
with :meth:`Resonate.get`, and shut down cleanly with :meth:`Resonate.stop`.

``run`` and ``rpc`` are synchronous fire-and-forget triggers: they return a
:class:`~resonate.handle.ResonateHandle` immediately, while the network work
happens in a background task. Per-call options come from
:meth:`Resonate.options`, which returns a fresh handle over the same shared
engine carrying the overridden :class:`Opts`.

Internally, each promise id maps to a single
:class:`~resonate.handle.Subscription`; every handle to that id awaits the
same subscription, so one settle wakes them all. Background work runs as
:func:`asyncio.create_task` tasks, retained so the event loop does not
garbage-collect them mid-flight and so :meth:`Resonate.stop` can join them.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import logging
import os
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Concatenate, Self, overload

from resonate import now_ms
from resonate.codec import Codec, NoopEncryptor
from resonate.context import Opts
from resonate.core import Core
from resonate.dependencies import DependencyMap
from resonate.error import (
    ApplicationError,
    FunctionNotFoundError,
    ResonateError,
    ServerError,
)
from resonate.handle import PromiseResult, ResonateHandle, Subscription
from resonate.heartbeat import AsyncHeartbeat, NoopHeartbeat
from resonate.network import HttpNetwork, LocalNetwork
from resonate.promises import Promises
from resonate.registry import Registry
from resonate.retry import Exponential
from resonate.schedules import Schedules
from resonate.send import Sender
from resonate.transport import ExecuteMsg, Transport, UnblockMsg
from resonate.types import Args, PromiseCreateReq, PromiseState, Status, TaskData, Value

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine

    from resonate.codec import Encryptor
    from resonate.context import Context
    from resonate.heartbeat import Heartbeat
    from resonate.network import Network
    from resonate.retry import RetryPolicy
    from resonate.transport import Message

logger = logging.getLogger(__name__)

#: Default deadline applied to a top-level :meth:`Resonate.run` /
#: :meth:`Resonate.rpc` when the caller passes none.
DEFAULT_TOP_LEVEL_TIMEOUT = timedelta(days=1)

#: Default per-task lease duration.
DEFAULT_TTL = timedelta(minutes=1)

#: Ceiling on tasks executing concurrently in this process. A task holds a
#: server lease from acquire until it suspends or fulfills; without a bound, a
#: flood of ``execute`` messages (e.g. a deep recursive workflow whose rpc
#: children are all routed back to this worker) could acquire leases faster
#: than the heartbeat can keep them alive, so leases lapse and the server
#: re-delivers each lapsed task. Bounding concurrent executions caps both the
#: live-lease count and the request burst against the connection pool. The
#: bound cannot deadlock a parent waiting on a child: durable functions never
#: block while holding a permit -- they suspend (releasing the permit) and
#: resume later as a fresh ``execute``.
DEFAULT_MAX_CONCURRENT_TASKS = 64

#: Divisor applied to the lease TTL to derive the heartbeat interval, so
#: multiple beats fit within each lease and a slow round-trip does not
#: immediately lapse it.
HEARTBEAT_INTERVAL_DIVISOR = 2

#: How often the background loop re-issues ``promise.register_listener`` for
#: still-pending subscriptions, defending against a dropped SSE connection.
_SUBSCRIPTION_REFRESH_SECS = 60


class ResonateSchedule:
    """Handle to a created schedule."""

    def __init__(self, name: str, schedules: Schedules) -> None:
        self.name = name
        self._schedules = schedules

    async def delete(self) -> None:
        """Delete this schedule."""
        await self._schedules.delete(self.name)


class _Runtime:
    """The shared mutable runtime behind every :class:`Resonate` handle.

    :meth:`Resonate.options` mints handles with ``copy.copy``, so attributes
    set at construction are shared by reference. A shallow copy cannot share
    an attribute that is later *rebound*, though: if one handle reassigned its
    own slot (e.g. ``stopping = True`` in :meth:`Resonate.stop`), the change
    would be invisible to every other handle. This object therefore holds all
    state mutated after construction -- the lifecycle fields (``stopping``,
    ``refresh_handle``) and the runtime containers -- so every handle observes
    the same state. The never-rebound wiring (network, codec, core, ...) lives
    directly on :class:`Resonate`.
    """

    def __init__(self, max_concurrent_tasks: int) -> None:
        #: id -> settle-once subscription. A single subscription is shared by
        #: every handle to that id, so one settle wakes them all.
        self.subs: dict[str, Subscription] = {}
        #: Live fire-and-forget tasks (network start, promise creation, workflow
        #: execution), retained both so the event loop does not collect them
        #: mid-flight and so :meth:`Resonate.stop` can join them.
        self.bg_tasks: set[asyncio.Task[Any]] = set()
        #: Set by :meth:`Resonate.stop` to refuse new ``execute`` work. Without
        #: it, an in-flight job that fails and releases its task triggers the
        #: server to re-deliver an ``execute`` message, which would spawn a
        #: fresh job -- an unbounded loop the join could never drain.
        self.stopping = False
        #: Caps tasks executing (holding a lease) at once. Acquired for the whole
        #: acquire→execute→suspend/fulfill span of every core execution, so the
        #: live-lease count never exceeds it regardless of how fast ``execute``
        #: messages arrive (see :data:`DEFAULT_MAX_CONCURRENT_TASKS`).
        self.execute_sema = asyncio.Semaphore(max_concurrent_tasks)
        #: The 60s listener-refresh loop; cancelled by :meth:`Resonate.stop`.
        self.refresh_handle: asyncio.Task[None] | None = None


class Resonate:
    """The main entry point for the Resonate SDK.

    A thin handle over a shared engine: the instance holds the wiring built
    once at construction (network, sender, codec, core, registry, heartbeat,
    the public ``promises``/``schedules`` clients), a shared
    :class:`_Runtime`, and the :class:`Opts` its ``run``/``rpc`` calls use.
    :meth:`options` mints a fresh handle by shallow copy -- wiring and runtime
    shared by reference -- carrying overridden opts, so a held handle stays
    valid indefinitely and never interferes with another. Everything mutated
    at runtime (subscriptions, background tasks, stopping) lives on the shared
    :class:`_Runtime` and is visible across every handle.

    Network selection precedence: ``url`` > ``network`` > ``RESONATE_URL`` env
    (with a ``RESONATE_HOST``/``RESONATE_PORT`` fallback) > an in-process
    :class:`~resonate.network.LocalNetwork`. A URL or explicit remote network
    gets an :class:`~resonate.heartbeat.AsyncHeartbeat`; local mode gets a
    :class:`~resonate.heartbeat.NoopHeartbeat`.
    """

    def __init__(
        self,
        *,
        url: str | None = None,
        network: Network | None = None,
        group: str | None = None,
        pid: str | None = None,
        ttl: timedelta | None = None,
        token: str | None = None,
        encryptor: Encryptor | None = None,
        heartbeat: Heartbeat | None = None,
        prefix: str | None = None,
        max_concurrent_tasks: int | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        resolved_ttl = ttl if ttl is not None else DEFAULT_TTL
        safe_ttl = _safe_ttl_ms(resolved_ttl)

        # SDK-wide default retry policy, applied to a pure-leaf failure when a
        # function has no per-call (``ctx.options``) or per-function
        # (:meth:`register`) override. Lives on :class:`~resonate.core.Core`;
        # pass ``retry_policy=Never()`` to disable retries process-wide (tests).
        resolved_retry_policy = (
            retry_policy
            if retry_policy is not None
            else Exponential(delay=1, max_delay=(1 << 63) - 1, factor=2, max_retries=30)
        )

        resolved_prefix = (
            prefix if prefix is not None else os.environ.get("RESONATE_PREFIX")
        )
        auth = token if token is not None else os.environ.get("RESONATE_TOKEN")

        net = _select_network(url, network, group, pid, auth)
        net_pid = net.pid()

        transport = Transport(net)
        codec = Codec(encryptor if encryptor is not None else NoopEncryptor())
        registry = Registry()
        sender = Sender(transport, auth)
        deps = DependencyMap()

        if heartbeat is not None:
            hb = heartbeat
        elif isinstance(net, LocalNetwork):
            hb = NoopHeartbeat()
        else:
            interval_ms = max(safe_ttl // HEARTBEAT_INTERVAL_DIVISOR, 1)
            hb = AsyncHeartbeat(net_pid, interval_ms, sender)

        core = Core(
            sender=sender,
            codec=codec,
            registry=registry,
            # A bound method invoked lazily at dispatch time, so handing it over
            # before the wiring is assigned below is safe.
            resolver=self._resolve_target,
            heartbeat=hb,
            pid=net_pid,
            ttl=safe_ttl,
            deps=deps,
            retry_policy=resolved_retry_policy,
        )

        # Construction-time wiring: set once here, never rebound, so every
        # handle minted by :meth:`options` (a shallow copy) shares it by
        # reference automatically.
        self._ttl = resolved_ttl
        self._id_prefix = f"{resolved_prefix}:" if resolved_prefix else ""
        self._network = net
        self._pid = net_pid
        self._codec = codec
        self._registry = registry
        self._sender = sender
        self._deps = deps
        self._heartbeat = hb
        self._core = core
        #: Public clients for direct promise/schedule manipulation.
        self.promises = Promises(sender, codec)
        self.schedules = Schedules(sender, codec)
        #: The runtime state every handle must observe *through rebinding* --
        #: see :class:`_Runtime`.
        self._runtime = _Runtime(
            max_concurrent_tasks
            if max_concurrent_tasks is not None
            else DEFAULT_MAX_CONCURRENT_TASKS
        )
        #: This handle's options, applied by its run/rpc calls. Frozen; an
        #: overriding handle is minted via :meth:`options`.
        self.opts = Opts()

        # Wire push-message dispatch BEFORE starting the network so the initial
        # frames are not missed.
        transport.recv(self._on_message)

        # Start the network (fire-and-forget; HttpNetwork reconnects via SSE
        # backoff, LocalNetwork never fails here).
        self._spawn(net.start())
        self._runtime.refresh_handle = asyncio.create_task(self._run_refresh())

    # ── Public API ────────────────────────────────────────────────────────────

    def with_dependency(self, value: Any) -> Resonate:
        """Store a typed application dependency, shared with every context.

        Dependencies are keyed by their concrete type. Add them **before** the
        system starts processing tasks.
        """
        self._deps.insert(value)
        return self

    def options(
        self,
        *,
        timeout: timedelta | None = None,
        target: str | None = None,
        version: int = 1,
    ) -> Self:
        """Mint a new handle over the same engine, carrying these options.

        Returns a **new** ``Resonate`` made by shallow copy -- wiring and
        :class:`_Runtime` shared by reference -- whose :meth:`run` /
        :meth:`rpc` calls use the given options. The originating handle is
        untouched, so held handles never interfere::

            a = resonate.options(target="a")
            b = resonate.options(target="b")
            a.run(...)  # target=a
            b.run(...)  # target=b -- and both stay reusable

        Options *replace* rather than merge: ``options(a).options(b)`` carries
        only ``b``, and any field not passed resets to its default.

        ``version`` applies to the **by-name** form of :meth:`run` / :meth:`rpc`
        (a name carries no version, so it must be told which to target). It is
        ignored for the **by-object** form: a function object's registered
        version is recovered by identity, so the version is implied by the object
        itself.
        """
        new = copy.copy(self)
        # Same-class access, not a privacy break: ``new`` is the clone being
        # constructed, and ``copy.copy`` has no post-copy hook to set it through.
        new.opts = Opts(timeout=timeout, target=target, version=version)
        return new

    @overload
    def register[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], T],
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[Concatenate[Context, P], T]: ...
    @overload
    def register[**P, T](
        self,
        fn: None = None,
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[
        [Callable[Concatenate[Context, P], T]], Callable[Concatenate[Context, P], T]
    ]: ...
    def register(
        self,
        fn: Callable[Concatenate[Context, ...], Any] | None = None,
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Any:
        """Register a durable function. Usable as a decorator.

        ``name`` defaults to the function's ``__name__``; ``version`` defaults to
        ``1``. The same name may be registered at several versions so multiple
        implementations coexist. Returns ``fn`` unchanged (preserving its type)
        so it can be used bare (``@resonate.register``) or parameterized
        (``@resonate.register(name="...", version=2)``) -- and so the same object
        stays usable from a workflow via ``ctx.run(fn, ...)``. The
        registry's reverse map lets :meth:`run` recover both for the exact ``fn``
        object it is later given (and :meth:`rpc` recover the dispatch name).

        ``retry_policy`` overrides the SDK-wide default for this function when it
        runs as a root task; it only takes effect for a *pure-leaf* failure (a
        body that performs any durable op is a workflow and never retries).
        ``None`` (the default) keeps the SDK-wide default set on the
        :class:`Resonate` constructor. A per-call override for a ``ctx.run`` child
        is set via :meth:`~resonate.context.Context.with_opts` instead.
        """
        if fn is None:
            return lambda f: self.register(
                f, name=name, version=version, retry_policy=retry_policy
            )
        reg_name = name if name is not None else getattr(fn, "__name__", "")
        if not reg_name:
            msg = "register: a name is required for an anonymous function"
            raise ApplicationError(msg)
        self._registry.register(reg_name, fn, version, retry_policy)
        return fn

    @overload
    def run[**P, T](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ResonateHandle[T]: ...
    @overload
    def run[**P, T](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ResonateHandle[T]: ...
    @overload
    def run(
        self, id: str, func: str, *args: Any, **kwargs: Any
    ) -> ResonateHandle[Any]: ...
    def run[T](
        self,
        id: str,
        func: str | Callable[Concatenate[Context, ...], Any],
        *args: Any,
        **kwargs: Any,
    ) -> ResonateHandle[T]:
        """Start a durable invocation of a locally registered function.

        The ``func``/``*args``/``**kwargs`` shape matches
        :meth:`~resonate.context.Context.run`, with the top-level ``id``
        prepended. ``func`` is either the registered **function object** --
        whose name and version are recovered from the registration itself --
        or the registered **name** as a string, dispatched at
        :meth:`options`'s ``version``. Either way the function must be
        registered locally, since ``run`` executes here (dispatching purely to
        a remote worker by name is :meth:`rpc`'s job): an unregistered name,
        or a function object that was never registered, raises
        :class:`~resonate.error.FunctionNotFoundError`. A function object is
        dispatched by its registered name, not its Python ``__name__`` (which
        need not match), so an unregistered object is refused rather than
        guessed -- the same rule :meth:`rpc` follows.

        Returns immediately with a handle; the task is created -- and, if this
        process wins the create-and-acquire race, executed -- in the
        background. ``await handle.result()`` for the value. The result type
        ``T`` is inferred from the function's return annotation. Calling
        ``run`` twice with the same id yields a handle to the same promise.

        The function's own arguments own the keyword space, so per-call
        routing/timeout options come from :meth:`options`
        (``resonate.options(...).run(...)``) instead of keyword arguments.
        """
        opts = self.opts

        if isinstance(func, str):
            # By-name: a string carries no identity, so it is dispatched at
            # ``opts.version``, the same as rpc. Must be registered locally --
            # run executes here -- else it fails fast as not-found below.
            name, version = func, opts.version
        else:
            # By-object: recover name and version from the registration
            # (opts.version applies to the by-name form, not this one). An
            # unregistered object raises rather than falling back to its
            # ``__name__`` -- the same rule ``rpc`` follows (see
            # :meth:`_registered_key`).
            name, version = self._registered_key(func)

        df = self._registry.get(name, version)
        if df is None:
            raise FunctionNotFoundError(name, version)

        prefixed_id = self._prefix_id(id)
        req = self._build_root_promise_create_req(
            prefixed_id,
            name,
            df.pack_args(*args, **kwargs),
            version,
            opts.timeout,
            opts.target,
        )

        created: asyncio.Future[None] = asyncio.Future()
        sub, is_new = self._subscribe(prefixed_id)

        async def _() -> None:
            """Background body for :meth:`run`: create the task, then run it.

            Resolves ``created`` to ``None`` once ``task.create`` returns, or
            rejects it with the creation error, so a failed create never hands
            an id back through :meth:`ResonateHandle.id` for a promise that
            does not exist. If this process won the create-and-acquire race,
            the workflow is executed in its own background task.
            """
            try:
                outcome = await self._sender.task_create_or_conflict(
                    self._pid,
                    _safe_ttl_ms(self._ttl),
                    self._encode_create_req(req),
                )
            except Exception as exc:
                created.set_exception(exc)
                raise

            created.set_result(None)

            if outcome != "conflict" and outcome.task.state == "acquired":
                decoded = self._codec.decode_promise(outcome.promise)
                self._spawn(
                    self._bounded_execute(
                        self._core.execute_until_blocked_outer(
                            outcome.task.id,
                            outcome.task.version,
                            decoded,
                            outcome.preload,
                        )
                    )
                )

            if is_new:
                await self._register_and_settle(req.id, sub)

        self._spawn(_())
        # The return type comes from the same ``DurableFunction`` that packed
        # the arguments above -- the sole owner of this function's
        # serialization -- so the top-level decode resolves the annotation the
        # same way the recovery decode does, including for callable instances.
        return ResonateHandle(prefixed_id, sub, self._codec, df.return_type, created)

    @overload
    def rpc(
        self, id: str, fn: str, *args: Any, **kwargs: Any
    ) -> ResonateHandle[Any]: ...
    @overload
    def rpc[**P, T](
        self,
        id: str,
        fn: Callable[Concatenate[Context, P], Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ResonateHandle[T]: ...
    @overload
    def rpc[**P, T](
        self,
        id: str,
        fn: Callable[Concatenate[Context, P], T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ResonateHandle[T]: ...
    def rpc(
        self,
        id: str,
        fn: str | Callable[Concatenate[Context, ...], Any],
        *args: Any,
        **kwargs: Any,
    ) -> ResonateHandle[Any]:
        """Dispatch a function remotely.

        Returns immediately with a handle; the durable promise is created in
        the background. The ``fn``/``*args``/``**kwargs`` shape matches
        :meth:`~resonate.context.Context.rpc`; ``id`` is the (required)
        top-level promise id. The by-name form does not require local
        registration of the target -- some worker subscribed to the target
        runs it.

        ``fn`` is either the registered **name** as a string -- dispatched at
        :meth:`options`'s ``version``, with an untyped (``Any``) result, since
        there is no local function to read a return annotation from -- or the
        registered **function object**, whose name and version are recovered
        from the registration and whose result is decoded against the declared
        return type, exactly like :meth:`run`. A function object that was
        never registered raises
        :class:`~resonate.error.FunctionNotFoundError`: its registry name is
        not its Python ``__name__`` (which need not match any registered
        name), so the dispatch target cannot be guessed -- pass the name as a
        string to dispatch an unregistered target.
        """
        opts = self.opts
        prefixed_id = self._prefix_id(id)

        # Unlike :meth:`run`, the dispatched function may not run in this
        # process, so the args are always packed raw into ``Args`` (no local
        # pack/validate). By-name dispatch stays untyped, exactly like
        # :meth:`get`; by-object dispatch recovers the name/version from the
        # registration -- which implies local registration, so it decodes the
        # result against the declared return type (the same assumption
        # :meth:`run` makes) and refuses to guess a name for an unregistered
        # object.
        if isinstance(fn, str):
            name, version, return_type = fn, opts.version, Any
        else:
            # By-object: recover the registered key (raising for an
            # unregistered object, see :meth:`_registered_key`). A reverse-map
            # hit guarantees a forward-map one -- both are populated together
            # at register time -- so decode the result against the declared
            # return type, exactly like :meth:`run`.
            name, version = self._registered_key(fn)
            df = self._registry.get(name, version)
            return_type = df.return_type if df is not None else Any

        req = self._build_root_promise_create_req(
            prefixed_id,
            name,
            Args(args=args, kwargs=kwargs),
            version,
            opts.timeout,
            opts.target,
        )

        created: asyncio.Future[None] = asyncio.Future()
        sub, is_new = self._subscribe(prefixed_id)

        async def _() -> None:
            """Background body for :meth:`rpc`: create the promise.

            Resolves ``created`` to ``None`` once ``promise.create`` returns,
            or rejects it with the creation error, so a failed create never
            hands an id back through :meth:`ResonateHandle.id`.
            """
            try:
                await self._sender.promise_create(self._encode_create_req(req))
            except Exception as exc:
                created.set_exception(exc)
                raise
            created.set_result(None)

            if is_new:
                await self._register_and_settle(req.id, sub)

        self._spawn(_())
        return ResonateHandle(prefixed_id, sub, self._codec, return_type, created)

    async def get(self, id: str) -> ResonateHandle[Any]:
        """Return a handle for an existing promise.

        Unlike :meth:`run` / :meth:`rpc`, this is ``async``: a lookup has nothing
        to fire-and-forget, and the listener registration is what surfaces a
        :class:`~resonate.error.ServerError` (code 404) when the promise does not
        exist. The result is untyped (``Any``): with no local function to read a
        return annotation from -- the source :meth:`run` uses, and which :meth:`rpc`
        likewise lacks -- there is nothing to infer a decode type from, so the
        settled value passes through undecoded rather than asking the caller for
        a type.
        """
        id = self._prefix_id(id)

        sub, is_new = self._subscribe(id)
        if is_new:
            try:
                record = await self._sender.promise_register_listener(
                    id, self._network.unicast()
                )
            except ResonateError:
                if self._runtime.subs.get(id) is sub and not sub.settled():
                    del self._runtime.subs[id]
                raise
            if record.state != "pending":
                self._settle_and_cleanup(id, sub, record.state, record.value)

        created: asyncio.Future[None] = asyncio.Future()
        created.set_result(None)
        return ResonateHandle(id, sub, self._codec, Any, created)

    async def schedule(
        self,
        id: str,
        cron: str,
        func_name: str,
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        promise_timeout: timedelta | None = None,
        version: int = 1,
    ) -> ResonateSchedule:
        """Create a schedule for periodic function execution.

        Every promise the schedule fires carries a ``resonate:target`` tag --
        the routing target the server dispatches the invocation to. The target
        comes from :meth:`options` (``target=``), falling back to this
        handle's network group, and resolves through the same rules as
        :meth:`rpc`. The server rejects a schedule whose promise tags lack
        ``resonate:target``.
        """
        promise_timeout = (
            promise_timeout
            if promise_timeout is not None
            else DEFAULT_TOP_LEVEL_TIMEOUT
        )

        await self.schedules.create(
            id=id,
            cron=cron,
            promise_id=f"{self._id_prefix}{{{{.id}}}}.{{{{.timestamp}}}}",
            promise_timeout=_timeout_ms(promise_timeout),
            promise_param=Value(
                data=TaskData(
                    func=func_name,
                    args=args or (),
                    kwargs=kwargs or {},
                    version=version,
                )
            ),
            promise_tags={
                "resonate:target": self._resolve_target(self.opts.target),
            },
        )
        return ResonateSchedule(id, self.schedules)

    async def stop(self) -> None:
        """Tear down background jobs and the network. Idempotent.

        Order matters. The network is stopped **first** -- this cancels its
        internal SSE/tick loop and clears its subscribers, which is what makes
        the subsequent join bounded: an in-flight job that re-dispatches work
        (e.g. a failed execution releasing its task, which the server would
        otherwise immediately re-deliver as a fresh ``execute`` message) finds
        nobody listening, so the spawn chain terminates instead of looping
        forever. With the source of new jobs closed, we then **join** every
        remaining in-flight background job -- promise creation, the run/rpc
        bodies, and workflow execution -- so no work is abandoned mid-flight.
        Draining iteratively covers a job that spawns a child after the
        snapshot. Finally the refresh loop is cancelled and the heartbeat shut
        down.

        Execution tasks are joined too; durable functions suspend (unwind)
        rather than block, so the join completes.
        """
        self._runtime.stopping = True

        handle = self._runtime.refresh_handle
        self._runtime.refresh_handle = None
        if handle is not None:
            handle.cancel()

        with contextlib.suppress(ResonateError):
            await self._network.stop()

        while self._runtime.bg_tasks:
            await asyncio.gather(*self._runtime.bg_tasks, return_exceptions=True)
            # Yield so each finished task's done-callback can discard itself from
            # the set (and any network dispatch settle) before we re-check --
            # otherwise the tight loop re-gathers the same already-done tasks.
            await asyncio.sleep(0)

        self._heartbeat.shutdown()

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _prefix_id(self, id: str) -> str:
        return f"{self._id_prefix}{id}" if self._id_prefix else id

    def _registered_key(
        self, fn: Callable[Concatenate[Context, ...], Any]
    ) -> tuple[str, int]:
        """Recover the ``(name, version)`` a function *object* was registered under.

        Shared by the by-object form of :meth:`run` and :meth:`rpc` so the two
        resolve identically. A function object is dispatched by its registered
        key, not its Python ``__name__`` (which need not match any registered
        name), so an object that was never registered raises
        :class:`~resonate.error.FunctionNotFoundError` rather than letting a
        guessed name dispatch the wrong target -- pass the name as a string to
        dispatch an unregistered target. The version comes from the registration,
        so :meth:`options`'s ``version`` does not apply to the by-object form.
        """
        recorded = self._registry.reverse(fn)
        if recorded is None:
            raise FunctionNotFoundError(getattr(fn, "__name__", "<anonymous>"))
        return recorded

    def _resolve_target(self, target: str | None) -> str:
        """Resolve a routing target.

        Empty falls back to the network group; a URL passes through unchanged; a
        bare name runs through the network resolver. Doubles as the
        :class:`~resonate.context.TargetResolver` handed to :class:`Core`.
        """
        resolved = target if target is not None else self._network.group()

        if "://" in resolved:
            return resolved

        return self._network.target_resolver(resolved)

    def _build_root_promise_create_req(
        self,
        prefixed_id: str,
        func_name: str,
        packed: Args,
        version: int,
        timeout: timedelta | None,
        target: str | None,
    ) -> PromiseCreateReq:
        """Build the root ``PromiseCreateReq`` for a top-level run or rpc.

        The param is a **plaintext** :class:`~resonate.types.Value` wrapping
        the :class:`~resonate.types.TaskData` -- symmetric with the requests
        :class:`~resonate.context.Context` builds for child promises.
        Encryption is deferred to a single boundary,
        :meth:`_encode_create_req`, applied immediately before the request is
        handed to the sender (the same pattern as
        :meth:`~resonate.effects.Effects.create_promise` for child promises
        and :func:`~resonate.promises.encode_value` for the public clients).
        Carries the SDK's ``resonate:origin/branch/parent/scope/target`` tags.
        """
        timeout = timeout if timeout is not None else DEFAULT_TOP_LEVEL_TIMEOUT

        return PromiseCreateReq(
            id=prefixed_id,
            timeout_at=now_ms() + _timeout_ms(timeout),
            param=Value(
                data=TaskData(
                    func=func_name,
                    args=packed.args,
                    kwargs=packed.kwargs,
                    version=version,
                )
            ),
            tags={
                "resonate:origin": prefixed_id,
                # A genuine top-level root is its own lineage origin and its own
                # id-generation prefix, so prefix == origin == id here.
                "resonate:prefix": prefixed_id,
                "resonate:branch": prefixed_id,
                "resonate:parent": prefixed_id,
                "resonate:scope": "global",
                "resonate:target": self._resolve_target(target),
            },
        )

    def _encode_create_req(self, req: PromiseCreateReq) -> PromiseCreateReq:
        """Encode a plaintext root req's ``param`` through the codec for the wire.

        The single symmetric encode boundary for top-level run/rpc, applied
        immediately before the request is handed to the sender -- the same
        pattern as :meth:`resonate.effects.ResonateEffects.create_promise` for
        child promises and :func:`resonate.promises.encode_value` for the
        public clients, keeping the :class:`~resonate.codec.Codec` the sole
        owner of encode/decode -- and so of encryption/decryption. The
        companion decode happens on the way back via
        :meth:`Codec.decode_promise`.
        """
        return PromiseCreateReq(
            id=req.id,
            timeout_at=req.timeout_at,
            param=self._codec.encode(req.param.data),
            tags=req.tags,
        )

    # ── Subscription / handle path ─────────────────────────────────────────────

    def _subscribe(self, id: str) -> tuple[Subscription, bool]:
        """Return ``(sub, is_new)`` for ``id``, creating the subscription if new.

        Synchronous so :meth:`run`/:meth:`rpc` can register the subscription --
        and hand back a handle -- before their background task touches the
        network, keeping an early ``unblock`` from being dropped.
        """
        sub = self._runtime.subs.get(id)
        if sub is not None:
            return sub, False
        sub = Subscription()
        self._runtime.subs[id] = sub
        return sub, True

    async def _register_and_settle(self, id: str, sub: Subscription) -> None:
        """Register a unicast listener for ``id``; settle now if already done.

        Failures are logged, not raised -- the background caller has nobody to
        propagate to, and the periodic refresh re-registers any still-pending
        subscription. A 404 is the one exception: it means the durable promise
        no longer exists on the server (memory-storage restart, manual purge,
        ...), so retrying is pointless; settle the subscription with a
        synthetic rejection so :meth:`ResonateHandle.result` raises an
        :class:`ApplicationError` instead of hanging on the next ``wait``.
        """
        try:
            record = await self._sender.promise_register_listener(
                id, self._network.unicast()
            )
        except ServerError as exc:
            if exc.code == 404:
                self._settle_subscription_gone(id, sub, exc.message)
                return
            logger.warning("listener registration failed id=%s: %s", id, exc)
            return
        except ResonateError as exc:
            logger.warning("listener registration failed id=%s: %s", id, exc)
            return
        if record.state != "pending":
            self._settle_and_cleanup(id, sub, record.state, record.value)

    def _settle_subscription_gone(
        self, id: str, sub: Subscription, reason: str
    ) -> None:
        """Settle ``sub`` with a rejected state when the promise is gone.

        The server returned 404 for ``promise.register_listener`` -- the
        durable promise no longer exists (ephemeral-storage restart, manual
        purge, expired retention, ...). There is no settled value to deliver
        and no future ``unblock`` push will ever come, so fabricate a
        :class:`~resonate.error.ApplicationError` carrying the server's
        reason and route it through the regular ``rejected`` path. Without
        this, :meth:`ResonateHandle.result` waits on the subscription event
        for the rest of the process lifetime -- the user only learns the
        workflow is unrecoverable by hitting Ctrl+C.
        """
        encoded = self._codec.encode(ApplicationError(reason))
        self._settle_and_cleanup(id, sub, "rejected", encoded)

    def _settle_and_cleanup(
        self,
        id: str,
        sub: Subscription,
        state: PromiseState,
        value: Value,
    ) -> None:
        """Settle ``sub`` and remove ``id`` from the subscription map.

        Holders of ``sub`` still observe the settled result; the deletion only
        keeps the map from growing without bound. ``Subscription.settle`` is
        idempotent, so a duplicate settle (e.g. the 60s refresh racing an
        ``unblock``) is safe.

        ``value`` is still in wire form (its ``data`` base64-encoded); decoding
        it is deferred to :class:`~resonate.codec.Codec` when a handle reads the
        result, keeping the codec the sole owner of the durability-boundary
        decode.
        """
        sub.settle(PromiseResult(state=state, value=value))
        if self._runtime.subs.get(id) is sub:
            del self._runtime.subs[id]

    # ── Message dispatch ────────────────────────────────────────────────────────

    def _on_message(self, msg: Message) -> None:
        """Dispatch a push message: execute → core, unblock → subscription map.

        Once :meth:`stop` has begun, ``execute`` messages are dropped so the
        join can drain (see ``_stopping``); a still-pending ``unblock`` is always
        routed so a handle awaiting a last-moment settle is not stranded.
        """
        if isinstance(msg, ExecuteMsg):
            if not self._runtime.stopping:
                self._spawn(
                    self._bounded_execute(
                        self._core.on_message(msg.task_id, msg.version)
                    )
                )
        elif isinstance(msg, UnblockMsg):
            promise = msg.promise
            sub = self._runtime.subs.get(msg.promise.id)
            if sub is None:
                # No one waiting (already settled+cleaned, or a duplicate push).
                return

            self._settle_and_cleanup(promise.id, sub, promise.state, promise.value)

    # ── Background tasks ──────────────────────────────────────────────────────

    async def _bounded_execute(self, coro: Coroutine[Any, Any, Status | None]) -> None:
        """Run a core execution coroutine under the concurrency semaphore.

        Holds a permit for the coroutine's whole lifetime -- which, for both
        :meth:`Core.on_message` and :meth:`Core.execute_until_blocked`, spans
        from acquiring the task's lease to suspending or fulfilling it -- so the
        number of leases held at once never exceeds the ceiling. When permits
        are exhausted, surplus ``execute`` jobs wait here *before* touching the
        network, so no extra lease is taken until one frees up. ``coro`` is an
        un-awaited coroutine, so the ``task.acquire`` round-trip inside it is
        deferred until the permit is in hand.
        """
        async with self._runtime.execute_sema:
            await coro

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        """Fire-and-forget a coroutine, logging failures and retaining the task.

        Uses :func:`asyncio.create_task` (not ``ensure_future``): every caller
        hands in a freshly built coroutine, so the more general ``ensure_future``
        -- which also accepts an existing ``Future`` -- buys nothing here.
        """
        task = asyncio.create_task(coro)

        def _(task: asyncio.Task[Any]) -> None:
            self._runtime.bg_tasks.discard(task)
            if not task.cancelled():
                exc = task.exception()
                if exc is not None:
                    logger.error("background task failed: %s", exc)

        self._runtime.bg_tasks.add(task)
        task.add_done_callback(_)

    async def _run_refresh(self) -> None:
        """Re-register listeners for pending subscriptions every interval.

        Defends a handle against a dropped SSE connection. Returns when
        cancelled by :meth:`stop`. A 404 from the server means the promise
        no longer exists -- settle the subscription with a synthetic rejection
        (see :meth:`_register_and_settle`) so the awaiting handle is not
        stranded for the rest of the process lifetime.
        """
        with contextlib.suppress(asyncio.CancelledError):
            while True:
                await asyncio.sleep(_SUBSCRIPTION_REFRESH_SECS)
                addr = self._network.unicast()

                for id, sub in list(self._runtime.subs.items()):
                    if not sub.settled():
                        try:
                            record = await self._sender.promise_register_listener(
                                id, addr
                            )
                        except ServerError as exc:
                            if exc.code == 404:
                                self._settle_subscription_gone(id, sub, exc.message)
                                continue
                            logger.warning(
                                "subscription refresh failed id=%s: %s", id, exc
                            )
                            continue
                        except ResonateError as exc:
                            logger.warning(
                                "subscription refresh failed id=%s: %s", id, exc
                            )
                            continue

                        if record.state != "pending":
                            self._settle_and_cleanup(
                                id, sub, record.state, record.value
                            )


# ── Module-level helpers ───────────────────────────────────────────────────────


def _timeout_ms(timeout: timedelta) -> int:
    return int(timeout.total_seconds() * 1000)


def _safe_ttl_ms(ttl: timedelta) -> int:
    ms = _timeout_ms(ttl)
    return ms if ms > 0 else (1 << 50)


def _select_network(
    url: str | None,
    network: Network | None,
    group: str | None,
    pid: str | None,
    auth: str | None,
) -> Network:
    if url is not None:
        return HttpNetwork(url=url, pid=pid, group=group, auth=auth)
    if network is not None:
        return network
    env_url = _resolve_env_url()
    if env_url is not None:
        return HttpNetwork(url=env_url, pid=pid, group=group, auth=auth)
    return LocalNetwork(pid=pid, group=group)


def _resolve_env_url() -> str | None:
    """Resolve a server URL from the environment.

    ``RESONATE_URL`` takes precedence; otherwise a URL is assembled from
    ``RESONATE_HOST`` (with optional ``RESONATE_SCHEME``/``RESONATE_PORT``).
    """
    env_url = os.environ.get("RESONATE_URL")
    if env_url:
        return env_url
    host = os.environ.get("RESONATE_HOST")
    if not host:
        return None
    scheme = os.environ.get("RESONATE_SCHEME", "http")
    port = os.environ.get("RESONATE_PORT", "8001")
    return f"{scheme}://{host}:{port}"
