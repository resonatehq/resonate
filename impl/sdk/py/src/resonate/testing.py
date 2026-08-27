"""Test doubles and builders for code that uses the Resonate SDK.

Importing this module is opt-in and costs nothing at runtime: nothing inside
:mod:`resonate` imports it.

Two audiences:

* **Applications.** :func:`local_resonate` gives you a fully working
  :class:`~resonate.resonate.Resonate` backed by the in-process
  :class:`~resonate.connections.LocalConnection`, with time frozen, sleeps
  instant, and retries off -- so a durable workflow can be exercised end to end
  in a unit test with no server, no network, and no waiting.
* **The SDK's own suite.** The recorders and builders below are the shared
  vocabulary its tests are written in.

Every helper here follows one rule: **a helper never returns an error.** It
either produces the value asked for or raises immediately, so call sites read
as a short sequence of steps rather than a chain of error checks.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from resonate.codec import Codec, NoopEncryptor, _encode_error
from resonate.connections import LocalConnection
from resonate.connections.local import Task
from resonate.context import Context
from resonate.dependencies import DependencyMap
from resonate.effects import ResonateEffects
from resonate.error import ApplicationError, ResonateError
from resonate.observability import Dropped, Event, noop_observer
from resonate.registry import Registry
from resonate.resonate import Resonate
from resonate.retry import Never
from resonate.send import Sender, SuspendResult, TaskAcquireResult
from resonate.timing import Clock, now_ms
from resonate.transport import Transport
from resonate.types import PromiseRecord, PromiseState, Value

if TYPE_CHECKING:
    from collections.abc import Mapping

    from resonate.retry import RetryPolicy
    from resonate.types import PromiseRegisterCallbackData, PromiseSettleReq

#: A deadline far enough out that no test hits it, but still a valid i64.
FAR_FUTURE = (1 << 62) - 1


# ═══════════════════════════════════════════════════════════════
#  Time
# ═══════════════════════════════════════════════════════════════


class FakeClock:
    """A :data:`~resonate.timing.Clock` a test moves by hand.

    Pass it anywhere a clock is accepted and every deadline the SDK computes
    becomes an exact, assertable number::

        clock = FakeClock(start=1_000)
        ctx = root_context(clock=clock)
        clock.advance(seconds=30)
    """

    def __init__(self, start: int = 0) -> None:
        self.now = start

    def __call__(self) -> int:
        return self.now

    def advance(self, ms: int = 0, *, seconds: float = 0.0) -> int:
        """Move the clock forward and return the new time in milliseconds."""
        self.now += ms + int(seconds * 1000)
        return self.now


class RecordingSleeper:
    """A :data:`~resonate.timing.Sleeper` that records instead of waiting.

    The point of the post's "test retries deterministically": inject this, run
    the operation, and assert the exact delay sequence a policy produced --
    ``assert sleeper.delays == [1, 2, 4]`` -- in microseconds rather than seven
    real seconds.

    Each call still yields to the event loop, so ordering between concurrent
    tasks stays realistic.
    """

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, secs: float) -> None:
        self.delays.append(secs)
        await asyncio.sleep(0)

    @property
    def total(self) -> float:
        """Total time that *would* have been spent sleeping."""
        return sum(self.delays)


class ManualSleeper:
    """A :data:`~resonate.timing.Sleeper` the test releases by hand.

    Where :class:`RecordingSleeper` returns immediately, this one *parks* the
    caller until :meth:`tick` releases it. That is what makes a periodic loop
    -- the heartbeat, the subscription refresh, an SSE reconnect -- testable:
    the loop advances exactly one iteration per ``tick``, so a test says "beat
    twice" instead of "sleep 120ms and hope".

    Not for loops that must run free; use :class:`RecordingSleeper` there.
    """

    #: Event-loop turns :meth:`tick` will spin waiting for a sleeper to appear
    #: before giving up. Bounded so a loop that never sleeps fails the test
    #: instead of hanging it.
    max_spin = 1000

    def __init__(self) -> None:
        self.delays: list[float] = []
        self._parked: list[asyncio.Future[None]] = []

    async def __call__(self, secs: float) -> None:
        self.delays.append(secs)
        parked = asyncio.get_running_loop().create_future()
        self._parked.append(parked)
        await parked

    async def tick(self, times: int = 1) -> None:
        """Release ``times`` parked sleepers, oldest first, and let them run."""
        for _ in range(times):
            await self._await_parked()
            self._parked.pop(0).set_result(None)
            # Two turns: one to resume the released coroutine, one for whatever
            # it does before parking again.
            await asyncio.sleep(0)
            await asyncio.sleep(0)

    async def _await_parked(self) -> None:
        for _ in range(self.max_spin):
            if self._parked:
                return
            await asyncio.sleep(0)
        msg = "ManualSleeper.tick: nothing is sleeping"
        raise AssertionError(msg)


async def instant_sleeper(secs: float) -> None:
    """Return immediately -- a :data:`~resonate.timing.Sleeper` that never waits.

    Use when the delays are not the thing under test but the waiting would
    still dominate the runtime.
    """
    await asyncio.sleep(0)


# ═══════════════════════════════════════════════════════════════
#  Observability
# ═══════════════════════════════════════════════════════════════


class RecordingObserver:
    """An :data:`~resonate.observability.Observer` that keeps every event.

    Turns the SDK's deliberately-silent behaviours into assertions::

        observer = RecordingObserver()
        ...
        assert observer.dropped("preload-record") == ["p1"]
    """

    def __init__(self) -> None:
        self.events: list[Event] = []

    def __call__(self, event: Event) -> None:
        self.events.append(event)

    def of[T](self, type_: type[T]) -> list[T]:
        """Every recorded event of exactly ``type_``, in order."""
        return [e for e in self.events if isinstance(e, type_)]

    def dropped(self, what: str | None = None) -> list[str]:
        """Ids of everything dropped, optionally filtered to one drop site."""
        return [e.id for e in self.of(Dropped) if what is None or e.what == what]


# ═══════════════════════════════════════════════════════════════
#  Promise records
# ═══════════════════════════════════════════════════════════════


def _codec() -> Codec:
    return Codec(NoopEncryptor())


def pending_promise(
    id: str,
    *,
    timeout_at: int = FAR_FUTURE,
    tags: dict[str, str] | None = None,
    param: Value | None = None,
) -> PromiseRecord:
    """Build a pending promise record."""
    return PromiseRecord(
        id=id,
        state="pending",
        timeout_at=timeout_at,
        param=param if param is not None else Value(),
        value=Value(),
        tags=tags or {},
        created_at=0,
        settled_at=None,
    )


def resolved_promise(
    id: str, value: Any = None, *, timeout_at: int = FAR_FUTURE
) -> PromiseRecord:
    """Build a settled *resolved* record, wire-encoded like the server's."""
    return PromiseRecord(
        id=id,
        state="resolved",
        timeout_at=timeout_at,
        param=Value(),
        value=_codec().encode(value),
        tags={},
        created_at=0,
        settled_at=1,
    )


def rejected_promise(
    id: str,
    message: str = "boom",
    *,
    state: PromiseState = "rejected",
    timeout_at: int = FAR_FUTURE,
) -> PromiseRecord:
    """Build a settled *rejected* record carrying an encoded error payload."""
    return PromiseRecord(
        id=id,
        state=state,
        timeout_at=timeout_at,
        param=Value(),
        value=_codec().encode(_encode_error(ApplicationError(message))),
        tags={},
        created_at=0,
        settled_at=1,
    )


# ═══════════════════════════════════════════════════════════════
#  Ports
# ═══════════════════════════════════════════════════════════════


class RecordingTaskLifecycle:
    """A :class:`~resonate.send.TaskLifecycle` that records the call order.

    Four methods, no network. Because it records *order*, it can assert the
    contracts :class:`~resonate.core.Core` makes that a return value cannot
    show -- most importantly that a failed fulfill or suspend is followed by a
    release::

        lifecycle = RecordingTaskLifecycle(fail_on={"task_fulfill"})
        ...
        assert lifecycle.calls == ["task_acquire", "task_fulfill", "task_release"]

    ``fail_on`` names methods that should raise ``error`` instead of returning.
    """

    def __init__(
        self,
        *,
        acquire: TaskAcquireResult | None = None,
        suspend: SuspendResult = "suspended",
        fail_on: set[str] | None = None,
        error: ResonateError | None = None,
    ) -> None:
        self.calls: list[str] = []
        self.fulfilled: list[PromiseSettleReq] = []
        self.suspended: list[list[PromiseRegisterCallbackData]] = []
        self.released: list[tuple[str, int]] = []
        self._acquire = acquire
        self._suspend = suspend
        self._fail_on = fail_on or set()
        self._error = error if error is not None else ApplicationError("injected")

    def _record(self, name: str) -> None:
        self.calls.append(name)
        if name in self._fail_on:
            raise self._error

    async def task_acquire(
        self, id: str, version: int, pid: str, ttl: int
    ) -> TaskAcquireResult:
        self._record("task_acquire")
        if self._acquire is None:
            msg = "RecordingTaskLifecycle: no acquire result configured"
            raise AssertionError(msg)
        return self._acquire

    async def task_fulfill(
        self, id: str, version: int, action: PromiseSettleReq
    ) -> PromiseRecord:
        self._record("task_fulfill")
        self.fulfilled.append(action)
        return resolved_promise(action.id)

    async def task_suspend(
        self, id: str, version: int, actions: list[PromiseRegisterCallbackData]
    ) -> SuspendResult:
        self._record("task_suspend")
        self.suspended.append(actions)
        return self._suspend

    async def task_release(self, id: str, version: int) -> None:
        self._record("task_release")
        self.released.append((id, version))


class UnusedFencing:
    """A :class:`~resonate.send.PromiseFencing` that must never be called.

    Hand this to a :class:`~resonate.core.Core` whose *inner* loop is under
    test: the inner performs no durable ops of its own, and this turns that
    claim into a checked one -- any call fails the test loudly instead of
    silently going somewhere.
    """

    async def task_fence_create(self, id: str, version: int, req: Any) -> Any:
        msg = "PromiseFencing.task_fence_create must not be called here"
        raise AssertionError(msg)

    async def task_fence_settle(self, id: str, version: int, req: Any) -> Any:
        msg = "PromiseFencing.task_fence_settle must not be called here"
        raise AssertionError(msg)


# ═══════════════════════════════════════════════════════════════
#  Contexts and clients
# ═══════════════════════════════════════════════════════════════


def local_effects(
    *,
    task_id: str = "root",
    task_version: int = 1,
    preload: list[PromiseRecord] | None = None,
    observer: Any = None,
    connection: LocalConnection | None = None,
) -> ResonateEffects:
    """Build real :class:`~resonate.effects.ResonateEffects` over a local server.

    Exercises the genuine durability boundary (encode, fence, decode) with no
    network, so a context built on it behaves exactly as it would in
    production.
    """
    net = connection if connection is not None else LocalConnection()
    net.state.tasks[task_id] = Task(
        id=task_id,
        state="acquired",
        version=task_version,
        pid="test-pid",
        ttl=60_000,
        resumes=set(),
    )
    sender = Sender(Transport(net), None)
    if observer is None:
        return ResonateEffects(sender, _codec(), task_id, task_version, preload or [])
    return ResonateEffects(
        sender, _codec(), task_id, task_version, preload or [], observer
    )


def cache_of(ctx: Context) -> dict[str, PromiseRecord]:
    """Read the durable-promise cache behind a context built by :func:`root_context`.

    The cache is an implementation detail of
    :class:`~resonate.effects.ResonateEffects`, deliberately *not* part of the
    :class:`~resonate.effects.Effects` protocol -- a stand-in should have two
    methods to write, not a dict to maintain. This helper is how a test still
    inspects it, and it names the assumption it makes rather than leaving a
    protocol field to imply it.

    Fails directly if the context was not built over real effects.
    """
    effects = ctx._state.effects  # noqa: SLF001
    if not isinstance(effects, ResonateEffects):
        msg = f"cache_of expects ResonateEffects, got {type(effects).__name__}"
        raise TypeError(msg)
    return effects.cache


def root_context(
    *,
    id: str = "root",
    preload: list[PromiseRecord] | None = None,
    timeout_at: int = FAR_FUTURE,
    deps: DependencyMap | None = None,
    retry_policy: RetryPolicy | None = None,
    registry: Registry | None = None,
    clock: Any = None,
    sleeper: Any = None,
    observer: Any = None,
) -> Context:
    """Build a root :class:`~resonate.context.Context` over a local server.

    Defaults: no retries (``Never``), an empty registry, a far-future deadline,
    and the real clock/sleeper unless overridden. Pass a :class:`FakeClock` and
    a :class:`RecordingSleeper` to make deadlines exact and delays assertable.
    """
    effects = local_effects(task_id=id, preload=preload, observer=observer)
    kwargs: dict[str, Any] = {}
    if clock is not None:
        kwargs["clock"] = clock
    if sleeper is not None:
        kwargs["sleeper"] = sleeper
    return Context.root(
        id=id,
        origin_id=id,
        timeout_at=timeout_at,
        func_name=id,
        effects=effects,
        target_resolver=lambda target: target or "",
        deps=deps if deps is not None else DependencyMap(),
        retry_policy=retry_policy if retry_policy is not None else Never(),
        registry=registry if registry is not None else Registry(),
        **kwargs,
    )


def local_resonate(
    *,
    group: str | None = None,
    pid: str | None = None,
    retry_policy: RetryPolicy | None = None,
    env: Mapping[str, str] | None = None,
    clock: Clock = now_ms,
    sleeper: Any = None,
    observer: Any = None,
    subscription_refresh_secs: float = 0.0,
    autostart: bool = True,
    **kwargs: Any,
) -> Resonate:
    """Build a :class:`~resonate.resonate.Resonate` wired for tests.

    A real client in every respect -- real codec, real registry, real
    :class:`~resonate.core.Core` -- but backed by the in-process
    :class:`~resonate.connections.LocalConnection` and configured so nothing
    waits and nothing leaks between tests:

    * ``env={}`` by default, so ``RESONATE_URL`` in the developer's shell
      cannot silently redirect a test at a real server.
    * ``retry_policy=Never()`` by default, so a failing function surfaces its
      error immediately instead of backing off for half a minute.
    * ``sleeper`` defaults to :func:`instant_sleeper`, so the refresh loop and
      any backoff never hold the test up.

    ``clock`` is threaded into **both** halves -- the client that computes
    deadlines and the in-process server that enforces them. That matters: a
    :class:`FakeClock` on the client alone would date every promise in 1970 and
    the server, still on the wall clock, would time it out on arrival. One
    clock, one notion of "now".

    The server's own tick loop keeps the *real* sleeper deliberately: it is a
    poll, and an instant sleeper would turn it into a busy loop. For the same
    reason the subscription-refresh loop is *off* by default here -- it guards
    against a dropped SSE connection, and an in-process server has none.

    Requires a running event loop unless ``autostart=False``. Remember to
    ``await resonate.stop()`` -- or use the ``resonate`` fixture, which does it
    for you.
    """
    server = LocalConnection(pid=pid, group=group, clock=clock)
    return Resonate(
        network=server,
        retry_policy=retry_policy if retry_policy is not None else Never(),
        env=env if env is not None else {},
        clock=clock,
        sleeper=sleeper if sleeper is not None else instant_sleeper,
        observer=observer if observer is not None else noop_observer,
        subscription_refresh_secs=subscription_refresh_secs,
        autostart=autostart,
        **kwargs,
    )
