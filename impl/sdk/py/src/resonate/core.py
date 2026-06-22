"""Orchestrates the full lifecycle of one task.

A :class:`Core` acquires a task (via :meth:`Core.on_message`) or takes an
already-acquired one (via :meth:`Core.execute_until_blocked_outer`), executes
the registered function, then fulfills on done, suspends on remote work, or
releases on error.

Methods raise a :class:`~resonate.error.ResonateError` on failure and return
the success :data:`~resonate.types.Status` (``"done"`` / ``"suspended"``)
otherwise. On error the task is released before the exception is re-raised.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal

import msgspec

from resonate.codec import Codec
from resonate.context import Context, TargetResolver
from resonate.dependencies import DependencyMap
from resonate.effects import Effects, ResonateEffects
from resonate.error import (
    DecodingError,
    FunctionNotFoundError,
    PlatformError,
    ResonateError,
    SerializationError,
    Suspended,
)
from resonate.heartbeat import Heartbeat, NoopHeartbeat
from resonate.registry import Registry
from resonate.retry import Never, RetryPolicy
from resonate.send import Redirect, Sender
from resonate.tree import Tree
from resonate.types import (
    PromiseRegisterCallbackData,
    PromiseSettleReq,
    TaskData,
    Value,
)

if TYPE_CHECKING:
    from resonate.types import PromiseRecord, Status

logger = logging.getLogger(__name__)

#: The target state for a ``promise.settle`` request; identical to
#: :class:`~resonate.types.PromiseSettleReq`'s ``state``.
SettleState = Literal["resolved", "rejected", "rejected_canceled"]


def identity_target_resolver(override: str | None) -> str:
    """Return the override unchanged, or the empty string when there is none.

    This is the fallback resolver used when a :class:`Core` is built without
    one.
    """
    return override if override is not None else ""


# ═══════════════════════════════════════════════════════════════
#  Execution outcome -- what the inner reports back to the lifecycle loop
# ═══════════════════════════════════════════════════════════════


class _ExecFulfilled(msgspec.Struct, frozen=True, kw_only=True):
    """The workflow finished; the task should be fulfilled with these args.

    ``value`` is already codec-encoded, so the caller has a single uniform
    fulfill path.
    """

    state: SettleState
    value: Value
    tree: Tree


class _ExecSuspended(msgspec.Struct, frozen=True, kw_only=True):
    """The workflow has remote dependencies; suspend on these awaiteds."""

    todos: list[str]
    tree: Tree


#: Outcome reported by :meth:`Core.execute_until_blocked_inner`.
_ExecOutcome = _ExecFulfilled | _ExecSuspended


class Core(msgspec.Struct, kw_only=True):
    """Orchestrates acquire/execute/fulfill-suspend-release for one task.

    ``resolver`` defaults to :func:`identity_target_resolver` and ``heartbeat``
    to :class:`~resonate.heartbeat.NoopHeartbeat`.

    ``deps`` is the :class:`~resonate.DependencyMap` threaded into the root
    context, making registered dependencies available to user functions.
    """

    sender: Sender | None
    codec: Codec
    registry: Registry
    resolver: TargetResolver = msgspec.field(default=identity_target_resolver)
    heartbeat: Heartbeat = msgspec.field(default_factory=NoopHeartbeat)
    pid: str
    ttl: int
    deps: DependencyMap = msgspec.field(default_factory=DependencyMap)
    #: SDK-wide default retry policy for a root function with no per-function
    #: override registered. Threaded in from :class:`~resonate.Resonate`; defaults
    #: to ``Never`` so a directly-constructed :class:`Core` (tests) does not retry.
    retry_policy: RetryPolicy = msgspec.field(default_factory=Never)

    # ═══════════════════════════════════════════════════════════════
    #  Path 1: on_message -- acquire then execute
    # ═══════════════════════════════════════════════════════════════

    async def on_message(self, task_id: str, version: int) -> Status:
        """Handle an execute push message.

        Acquires the task, decodes the root promise, and runs
        :meth:`execute_until_blocked_outer`.
        """
        assert self.sender, "sender must be set"
        res = await self.sender.task_acquire(task_id, version, self.pid, self.ttl)
        logger.debug("core: task acquired task_id=%s", task_id)

        # The lease is now held, but the root-promise decode happens before
        # execute_until_blocked_outer's release boundary -- a corrupt promise
        # (Base64DecodeError / SerializationError) would otherwise leak the
        # lease until TTL expiry. Release immediately so re-delivery retries at
        # once, mirroring the symmetric TaskData decode inside the inner loop.
        try:
            promise = self.codec.decode_promise(res.promise)
        except ResonateError:
            logger.exception(
                "core: failed to decode root promise, releasing task task_id=%s",
                task_id,
            )
            try:
                await self.sender.task_release(task_id, res.task.version)
            except ResonateError:
                logger.exception(
                    "core: failed to release task after decode error task_id=%s",
                    task_id,
                )
            raise

        return await self.execute_until_blocked_outer(
            task_id, res.task.version, promise, res.preload
        )

    # ═══════════════════════════════════════════════════════════════
    #  Path 2: execute_until_blocked -- task already acquired
    # ═══════════════════════════════════════════════════════════════

    async def execute_until_blocked_outer(
        self,
        task_id: str,
        task_version: int,
        promise: PromiseRecord,
        preload: list[PromiseRecord],
    ) -> Status:
        """Run an already-acquired task to completion or suspension.

        The caller owns the acquire step and must pass a ``promise`` whose
        ``param``/``value`` have been run through
        :meth:`~resonate.codec.Codec.decode_promise`.

        Owns the task lifecycle: builds :class:`~resonate.effects.Effects`,
        drives the redirect loop, fulfills or suspends on the inner's outcome,
        and releases on error.
        """
        self.heartbeat.start(task_id, task_version)
        assert self.sender, "sender must be set"
        try:
            try:
                logger.debug(
                    "core: starting execution task_id=%s promise_id=%s",
                    task_id,
                    promise.id,
                )
                current_preload = preload
                while True:
                    effects = ResonateEffects(
                        self.sender,
                        self.codec,
                        task_id,
                        task_version,
                        current_preload,
                    )
                    outcome = await self.execute_until_blocked_inner(promise, effects)

                    match outcome:
                        case _ExecFulfilled(state=state, value=value):
                            # The fulfill is a durable server interaction like
                            # any other durable op; a failure here surfaces
                            # uniformly as a PlatformError, released (and
                            # unwrapped back to its ResonateError cause) by the
                            # outer handler below.
                            try:
                                await self.sender.task_fulfill(
                                    task_id,
                                    task_version,
                                    PromiseSettleReq(
                                        id=promise.id, state=state, value=value
                                    ),
                                )
                            except ResonateError as exc:
                                raise PlatformError([exc]) from exc

                            logger.debug(
                                "core: task fulfilled task_id=%s promise_id=%s",
                                task_id,
                                promise.id,
                            )
                            return "done"

                        case _ExecSuspended(todos=todos):
                            logger.debug(
                                "core: attempting to suspend task task_id=%s "
                                "remote_deps=%d",
                                task_id,
                                len(todos),
                            )
                            # Suspend is a durable server interaction too --
                            # wrap a failure uniformly as PlatformError (see the
                            # fulfill case above).
                            try:
                                sr = await self.sender.task_suspend(
                                    task_id,
                                    task_version,
                                    [
                                        PromiseRegisterCallbackData(
                                            awaited=awaited, awaiter=task_id
                                        )
                                        for awaited in todos
                                    ],
                                )
                            except ResonateError as exc:
                                raise PlatformError([exc]) from exc

                            if not isinstance(sr, Redirect):
                                logger.debug("core: task suspended task_id=%s", task_id)
                                return sr
                            logger.debug(
                                "core: suspend returned redirect, re-executing task "
                                "task_id=%s preload=%d",
                                task_id,
                                len(sr.preload),
                            )
                            current_preload = sr.preload
            # PlatformError is BaseException-derived, so it reaches here past
            # user code untouched; this catch is the single place the
            # release-on-platform-error guarantee lives. Its BaseException-ness
            # has done its job once it arrives (there is no user code above
            # outer), so after the release it is unwrapped: callers always see
            # the original ResonateError, with the PlatformError -- whose
            # traceback records which durable op failed -- chained as cause.
            except (ResonateError, PlatformError) as exc:
                logger.exception(
                    "core: execution failed, releasing task task_id=%s promise_id=%s",
                    task_id,
                    promise.id,
                )
                try:
                    await self.sender.task_release(task_id, task_version)
                except ResonateError:
                    logger.exception(
                        "core: failed to release task after error task_id=%s",
                        task_id,
                    )
                if isinstance(exc, PlatformError):
                    raise exc.cause from exc
                raise
        finally:
            self.heartbeat.stop(task_id)

    async def execute_until_blocked_inner(
        self, promise: PromiseRecord, effects: Effects
    ) -> _ExecOutcome:
        """Run the workflow body once and report the outcome.

        Does not touch task lifecycle APIs (fulfill/suspend/release) -- the
        caller owns those. Encodes return values through the codec so the caller
        has a single, uniform fulfill path.
        """
        # 1. Decode TaskData from the (already-decoded) promise param.
        try:
            task_data = self.codec.convert(promise.param.data, TaskData)
        except SerializationError as exc:
            msg = f"invalid task data: {exc}"
            raise DecodingError(msg) from exc

        # 2. Look up the function in the registry by (name, version). The version
        #    was persisted in TaskData at create time, so this resolves the same
        #    implementation on every replay regardless of later registrations.
        df = self.registry.get(task_data.func, task_data.version)
        if df is None:
            raise FunctionNotFoundError(task_data.func, task_data.version)

        # Retry policy for this root function. A remote dispatch carries no
        # policy on the wire, so resolve it here: the per-function override
        # registered with the function wins, else the SDK-wide default. Only a
        # pure-leaf root honors it -- a workflow root touches ``ctx``, marking
        # it a workflow that never retries (see ``Context.invoke_with_retry``).
        registered = self.registry.get_policy(task_data.func, task_data.version)
        policy = registered if registered is not None else self.retry_policy

        # 3. SHORT-CIRCUIT: if the root promise is already settled, report a
        #    fulfill outcome without invoking the function.
        if promise.state != "pending":
            logger.info(
                "core: promise already settled, fulfilling task without execution "
                "promise_id=%s state=%s",
                promise.id,
                promise.state,
            )
            tree = Tree(root_id=promise.id)
            tree.settle(promise.id)
            return _ExecFulfilled(
                state="rejected"
                if promise.state == "rejected_timedout"
                else promise.state,
                value=self.codec.encode(promise.value.data),
                tree=tree,
            )

        # 4. EXECUTE the workflow.
        root_ctx = Context.root(
            id=promise.id,
            # Take the lineage origin from the promise's ``resonate:origin`` tag,
            # which the dispatcher set: a top-level run / rpc propagates its own
            # origin, while ``detached`` resets it to the child's own id (a new
            # lineage). Falling back to ``promise.id`` when absent keeps a genuine
            # top-level root (whose tag equals its id anyway) and any tag-less
            # promise correct.
            origin_id=promise.tags.get("resonate:origin", promise.id),
            # The id-generation prefix from ``resonate:prefix``. Unlike origin it
            # is propagated *unchanged* across ``detached`` re-roots, so every
            # recursion level mints ``{prefix}.{16hex}`` off the same fixed prefix
            # instead of off its own grown id -- this is what bounds recursive
            # detached ids. Falls back to ``promise.id`` when absent (genuine
            # top-level root / tag-less promise), matching the origin fallback.
            prefix_id=promise.tags.get("resonate:prefix", promise.id),
            timeout_at=promise.timeout_at,
            func_name=task_data.func,
            effects=effects,
            target_resolver=self.resolver,
            deps=self.deps,
            # The root's OWN retry uses ``policy`` (resolved above) via
            # ``invoke_with_retry``; this default is what ``ctx.run`` *children*
            # inherit when they don't override -- the SDK-wide default, not the
            # root function's per-function override.
            retry_policy=self.retry_policy,
            # Share Core's registry so by-name ``ctx.run`` / by-object ``ctx.rpc``
            # resolve against the same functions this worker registered.
            registry=self.registry,
        )

        suspended: bool = False
        run_err: Exception | None = None
        try:
            res = await root_ctx.invoke_with_retry(df, task_data, policy)
        except Suspended:
            suspended = True
        except Exception as exc:
            # User code reported failure by raising. Any ``Exception`` (a
            # deliberately raised ``ApplicationError``, a child rejection
            # surfaced by ``ctx.rpc``, or a plain ``ValueError`` /
            # domain-specific subclass) settles the promise ``rejected``. The
            # original object crosses the boundary verbatim; the codec flattens
            # it to the wire error shape and pickles it best-effort, so an
            # awaiter recovers the original type when it can and an
            # ``ApplicationError`` otherwise (see ``codec._deserialize_error``).
            #
            # ``BaseException`` subclasses (``SystemExit``,
            # ``KeyboardInterrupt``, ``asyncio.CancelledError`` on 3.8+...) are
            # *not* caught here -- they propagate out so the task is released
            # and the runtime can shut down.
            logger.debug(
                "core: user function raised %s in task=%s: %s",
                type(exc).__name__,
                root_ctx.info.id,
                exc,
                exc_info=True,
            )
            run_err = exc

        # Flush local work and collect remote todos. ``flush_local_work`` joins
        # every spawned sibling and re-raises any platform failures aggregated
        # into one ``PlatformError``, so reaching past it guarantees none
        # occurred.
        await root_ctx.flush_local_work()
        todos = root_ctx.take_remote_todos()

        # 5. FINALIZE: fulfill when no remote todos remain and the function did
        #    not request suspension.
        state: SettleState
        if not suspended and not todos:
            # ASSERT the tree matches a Done outcome (U1/U2/U3/D1) before
            # fulfilling. The tree is a parallel assertion-only view --
            # ``well_formed`` never feeds the decision below, it only verifies it
            # (see ``tree.md`` / ``tree.py``).
            root_ctx.tree.well_formed("done", todos)
            if run_err is not None:
                state = "rejected"
                encoded = self.codec.encode(run_err)
            else:
                state = "resolved"
                encoded = self.codec.encode(res)
            return _ExecFulfilled(state=state, value=encoded, tree=root_ctx.tree)

        # If the function returned done but there are pending todos, treat as
        # suspended (structured-concurrency rule; covers the fire-and-forget
        # child case).
        if not suspended and todos:
            logger.warning(
                "core: workflow returned done with pending remote todos -- either a "
                "fire-and-forget child suspended or the function swallowed an error "
                "func=%s id=%s todos=%s",
                task_data.func,
                promise.id,
                todos,
            )

        # ASSERT the tree matches a Suspended outcome (U1/U2/U3/S1/S4): the
        # frontier is non-empty and the awaited ``todos`` are a subset of it.
        root_ctx.tree.well_formed("suspended", todos)
        return _ExecSuspended(todos=todos, tree=root_ctx.tree)
