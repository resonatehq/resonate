from __future__ import annotations

import asyncio
import copy
import inspect
from collections.abc import Awaitable, Callable, Generator
from contextlib import suppress
from datetime import timedelta
from hashlib import blake2b
from typing import TYPE_CHECKING, Any, Concatenate, Self, overload

import msgspec

from resonate import now_ms
from resonate.codec import decode_settled
from resonate.durable import DurableFunction
from resonate.error import (
    FunctionNotFoundError,
    PlatformError,
    ResonateError,
    SerializationError,
    Suspended,
)
from resonate.registry import Registry
from resonate.retry import Never, RetryPolicy
from resonate.tree import Tree
from resonate.types import Info, PromiseCreateReq, Status, TaskData, Value

if TYPE_CHECKING:
    from resonate.dependencies import DependencyMap
    from resonate.effects import Effects
    from resonate.types import PromiseRecord


TargetResolver = Callable[[str | None], str]


class SpawnedLocal(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    handle: asyncio.Task[Any]


class Opts(msgspec.Struct, frozen=True, kw_only=True):
    timeout: timedelta | None = None
    target: str | None = None
    version: int = 1
    # Per-call retry policy override for ``ctx.run``'s child, set via
    # ``ctx.options(retry_policy=...)``. ``None`` means "inherit the context's
    # default" (which traces back to the SDK-wide default). Only honored for a
    # pure leaf function -- a body that performs any durable op is a workflow
    # and never retries (see :meth:`Context.invoke_with_retry`). Opts is a
    # runtime config object, never serialized, so holding a ``RetryPolicy``
    # is fine.
    retry_policy: RetryPolicy | None = None


class ResonateFuture[T](msgspec.Struct, frozen=True, kw_only=True):
    _id: str
    _task: asyncio.Task[T]
    _created: asyncio.Future[None]

    def __await__(self) -> Generator[Any, None, T]:
        return self._task.__await__()

    async def id(self) -> str:
        await self._created
        return self._id


def _hash_id(s: str) -> str:
    return blake2b(s.encode(), digest_size=8).hexdigest()


class _State:
    def __init__(
        self,
        id: str,
        origin_id: str,
        prefix_id: str,
        branch_id: str,
        parent_id: str,
        func_name: str,
        timeout_at: int,
        seq: int,
        effects: Effects,
        target_resolver: TargetResolver,
        spawned_remote: list[str],
        spawned_locals: list[SpawnedLocal],
        deps: DependencyMap,
        tree: Tree,
        retry_policy: RetryPolicy,
        registry: Registry,
    ) -> None:
        self.id = id
        self.origin_id = origin_id

        # The id-generation prefix, carried through the ``resonate:prefix`` tag.
        # Distinct from ``origin_id``: ``origin_id`` is the lineage origin
        # (which ``detached`` resets to the child's own id, starting a new
        # lineage), while ``prefix_id`` propagates *unchanged* across
        # ``detached`` re-roots. That keeps recursive ``detached`` ids bounded:
        # every level mints ``{prefix}.{16hex}`` off the same fixed prefix
        # rather than off its own grown id (see :meth:`Context.detached`). For
        # any non-detached context the two are equal.
        self.prefix_id = prefix_id

        self.branch_id = branch_id
        self.parent_id = parent_id

        self.func_name = func_name

        # The execution tree for this workflow attempt. Shared by reference
        # across the root context and every ``_child`` it spawns, so each spawn
        # records itself under the right parent. Assertion-only: the runtime
        # never reads it.
        self.tree = tree

        self.timeout_at = timeout_at
        self.seq = seq

        self.effects = effects
        self.target_resolver = target_resolver

        self.spawned_remote = spawned_remote
        self.spawned_locals = spawned_locals

        self.deps = deps

        # The function registry, shared by reference from root to child. Backs
        # the by-name forms of the durable ops: ``run("name")`` resolves a
        # local ``DurableFunction`` to execute, and ``rpc(fn_object)`` recovers
        # the registered name to dispatch by (reverse lookup). A by-object
        # ``run`` never consults it. Defaults to an empty registry for a
        # directly-constructed context (tests), where the by-name forms then
        # resolve to not-found.
        self.registry = registry

        # Default retry policy for this context's executions, inherited from
        # root to child by reference. A ``ctx.run`` child uses it unless the
        # call overrides via ``ctx.options(retry_policy=...)``. Traces back to
        # the SDK-wide default the root was built with; a direct
        # ``Context.root`` construction (tests) defaults to ``Never`` -- no
        # retries.
        self.retry_policy = retry_policy

        # Tail of the create-promise chain. Each ctx.run() captures this as
        # its prev-link and installs a fresh event as the new tail, so bg
        # tasks issue create_promise in ctx.run call order under concurrency.
        self.tail: asyncio.Future[None] | None = None

        # Whether this execution has performed any durable operation
        # (run/rpc/sleep/promise/detached) -- i.e. is a workflow rather than a
        # pure leaf function. Set to True at the top of each of the five
        # durable ops and never reset. Lives on the shared state (not the
        # per-call ``Context``) so it accumulates across every
        # ``ctx.options(...).<op>`` minted off the same state. Once True the
        # execution has a durable footprint and must never be re-run, so
        # ``invoke_with_retry`` refuses to retry it (see that method).
        self.workflow: bool = False

        # Background tasks spawned by rpc/sleep/promise (see
        # ``_spawn_remote_await``). Tracked so ``flush_local_work`` can join
        # them, which both retrieves their (almost always ``Suspended``)
        # result -- avoiding asyncio's "exception never retrieved" warning for
        # a future the workflow never awaited -- and makes ``spawned_remote``
        # population deterministic rather than dependent on incidental
        # event-loop scheduling.
        self.spawned_remote_tasks: list[asyncio.Task[Any]] = []


class Context:
    """A thin, per-call handle over shared execution state.

    A ``Context`` pairs the shared execution state with the :class:`Opts` for
    the *next* durable op. :meth:`options` mints a fresh ``Context`` over the
    *same* state carrying overridden opts, so a ``ctx.options(...).run(...)``
    chain configures only that one call and leaves the originating context
    (and its default opts) untouched. Everything mutated during execution (id
    sequence, spawned children, the creation chain, the workflow flag, the
    tree) lives on the shared state and is therefore visible across every
    ``Context`` that shares it.
    """

    def __init__(self, state: _State, opts: Opts) -> None:
        self._state = state
        self.opts = opts

    @property
    def tree(self) -> Tree:
        return self._state.tree

    @classmethod
    def root(
        cls,
        id: str,
        origin_id: str,
        prefix_id: str,
        timeout_at: int,
        func_name: str,
        effects: Effects,
        target_resolver: TargetResolver,
        deps: DependencyMap,
        retry_policy: RetryPolicy | None = None,
        registry: Registry | None = None,
    ) -> Self:
        # ``origin_id`` is the top of the execution lineage, carried through
        # the ``resonate:origin`` tag from whoever dispatched this workflow
        # (top-level run, ``rpc``, or ``detached``). For a genuine top-level
        # root it equals ``id``; a ``detached`` child resets it to its *own*
        # id, starting a fresh lineage.
        #
        # ``prefix_id`` is the id-generation prefix, carried through the
        # ``resonate:prefix`` tag. Unlike the lineage origin it is propagated
        # *unchanged* across ``detached`` re-roots -- which is what keeps
        # recursive ``detached`` ids bounded (``{prefix}.{16hex}``, one segment
        # past the fixed prefix) rather than growing a segment per level. For a
        # genuine top-level root it equals ``id`` (and ``origin_id``).
        #
        # The caller resolves both from the dispatching promise's tags; a
        # re-root must never silently fall back to ``id``, so both are
        # required rather than defaulted.
        return cls(
            state=_State(
                id=id,
                origin_id=origin_id,
                prefix_id=prefix_id,
                branch_id=id,
                parent_id=id,
                func_name=func_name,
                timeout_at=timeout_at,
                seq=0,
                effects=effects,
                target_resolver=target_resolver,
                spawned_locals=[],
                spawned_remote=[],
                deps=deps,
                # The root owns the tree; ``_child`` copies the reference. The root
                # node starts pending and is settled by the task fulfillment that
                # completes this workflow, never from inside the body.
                tree=Tree(id),
                # The SDK-wide default retry policy flows in here. A direct
                # construction (tests) that passes none gets ``Never`` -- no
                # retries unless asked.
                retry_policy=retry_policy if retry_policy is not None else Never(),
                # A direct construction (tests) that passes no registry gets an
                # empty one, so the by-name durable ops resolve to not-found
                # rather than crashing on a missing attribute.
                registry=registry if registry is not None else Registry(),
            ),
            opts=Opts(),
        )

    def _child(self, id: str, func_name: str, timeout_at: int) -> Context:
        assert self._state.timeout_at >= timeout_at, (
            "child timeout_at must be bounded by parents timeout_at"
        )
        return Context(
            state=_State(
                id=id,
                origin_id=self._state.origin_id,
                prefix_id=self._state.prefix_id,
                branch_id=id,
                parent_id=self._state.id,
                func_name=func_name,
                timeout_at=timeout_at,
                seq=0,
                effects=self._state.effects,
                target_resolver=self._state.target_resolver,
                spawned_locals=[],
                spawned_remote=[],
                deps=self._state.deps,
                # Share the parent's tree by reference: this child's own spawns
                # record themselves under ``id`` in the same per-attempt tree.
                tree=self._state.tree,
                # Inherit the parent's default policy; a child's own ``ctx.run``
                # calls fall back to it when not overridden via ``options``.
                retry_policy=self._state.retry_policy,
                # Share the parent's registry by reference: by-name run / by-object
                # rpc resolve against the same functions at every depth.
                registry=self._state.registry,
            ),
            opts=Opts(),
        )

    async def invoke_with_retry(
        self,
        df: DurableFunction,
        payload: Any,
        policy: RetryPolicy,
        *,
        coerce_args: bool = True,
    ) -> Any:
        """Execute ``df`` on this context, retrying a *pure-function* failure.

        Retry is gated on the ``workflow`` flag. The moment this context
        performs a durable operation (run/rpc/sleep/promise/detached, all of
        which set ``workflow``) the execution is a workflow with a durable
        footprint and must never be re-run: a second attempt would mint fresh
        child-promise ids and corrupt durability. So a body that touched any
        durable op settles its failure on the first attempt; only a pure leaf
        function -- zero durable footprint -- is retried, as far as ``policy``
        allows, before the failure is settled. That is what makes retrying
        safe: the only executions ever re-run have no durable side effects.

        ``Suspended`` is a durable suspension, not a failure, so it is
        re-raised untouched ahead of the generic ``Exception`` arm (it *is* an
        ``Exception``).

        ``coerce_args`` is forwarded to :meth:`DurableFunction.invoke`. The root
        path leaves it ``True`` (args arrive as decoded JSON and must be
        reshaped to their declared types); a local ``ctx.run`` child passes
        ``False`` so its in-memory args reach the function verbatim -- they are
        never serialized, so there is nothing to reshape and nothing to reject.
        """
        attempt = 0
        while True:
            try:
                return await df.invoke(self, payload, coerce_args=coerce_args)
            except Exception:
                # ``next(attempt + 1)`` consults the policy for the *upcoming*
                # retry; ``None`` means stop (workflow, or retries exhausted),
                # so the failure propagates and the caller settles it.
                delay = None if self._state.workflow else policy.next(attempt + 1)
                if delay is None:
                    raise
                # Only a pure leaf reaches here -- a durable op would have set
                # ``workflow`` and stopped the retry above. A pure leaf never
                # calls ``_next_id`` (the five durable ops do), so it cannot have
                # advanced ``seq``: it must still be 0, leaving the next attempt
                # to re-run from a clean id-generation state.
                assert self._state.seq == 0, (
                    "retried pure-leaf must not have advanced seq"
                )
                await asyncio.sleep(delay)
                attempt += 1

    def options(
        self,
        *,
        timeout: timedelta | None = None,
        target: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Context:
        new = copy.copy(self)
        # Same-class access, not a privacy break: ``new`` is the clone being
        # constructed, and ``copy.copy`` has no post-copy hook to set it through.
        new.opts = Opts(
            timeout=timeout,
            target=target,
            version=version,
            retry_policy=retry_policy,
        )
        return new

    def get_dependency[T](self, type: type[T]) -> T:
        return self._state.deps.get(type)

    def _next_id(self) -> str:
        self._state.seq += 1
        return f"{self._state.id}.{self._state.seq}"

    async def flush_local_work(self) -> None:
        """Wait for every eagerly spawned task on this context to finish.

        Joins two task groups before the caller drains ``spawned_remote`` via
        :meth:`take_remote_todos`:

        * ``spawned_locals`` -- the ``ctx.run`` children. Each merges its own
          remote todos into ``spawned_remote`` before it exits.
        * ``spawned_remote_tasks`` -- the ``rpc``/``sleep``/``promise``
          background bodies (see :meth:`_spawn_remote_await`) plus the
          ``detached`` body (see :meth:`detached`). The rpc/sleep/promise bodies
          each append their child id to ``spawned_remote`` and unwind via
          ``Suspended`` when the record is pending; joining them here makes
          that append deterministic for futures the workflow created but never
          awaited (e.g. it suspended on a sibling first), instead of relying on
          the event loop happening to run the task before ``take_remote_todos``.
          The detached body neither appends nor suspends -- the join simply
          guarantees its ``create_promise`` (a fire-and-forget child's only side
          effect) has completed before the parent settles.

        Structured concurrency requires every child's remote todos be merged
        before the parent decides to suspend; otherwise the suspend would
        register a partial awaited list.

        Ordinary per-task exceptions are swallowed: by the time a task ends, it
        has either merged its todos into our ``spawned_remote`` (suspended via
        ``Suspended``) or settled its own promise (errored). Either way, the
        error belongs to whoever holds the future -- the asyncio task delivers
        the same exception to the real awaiter, so dropping it here is not
        losing it. ``Suspended`` is suppressed alongside ``Exception`` because
        it extends ``BaseException``, which ``suppress(Exception)`` alone misses.

        ``PlatformError`` is not swallowed: a fire-and-forget child may have no
        other awaiter, so the task must be released. We join every sibling (no
        orphaned task), gather the causes of each platform failure, and re-raise
        them as one aggregated ``PlatformError`` so ``Core`` releases the task.
        """
        locals_ = self._state.spawned_locals
        remotes = self._state.spawned_remote_tasks
        self._state.spawned_locals = []
        self._state.spawned_remote_tasks = []

        # Join every sibling, extending one flat list with the reasons of each
        # platform failure, then re-raise them aggregated. An ordinary rejection
        # and a suspended child's signal belong to the real awaiter -- the
        # asyncio task delivers the same exception there, so dropping them here
        # is not losing them.
        causes: list[ResonateError] = []
        for awaitable in [task.handle for task in locals_] + remotes:
            try:
                with suppress(Exception, Suspended):
                    await awaitable
            except PlatformError as exc:
                causes.extend(exc.causes)

        if causes:
            err = PlatformError(causes)
            raise err from err.cause

    def take_remote_todos(self) -> list[str]:
        """Drain and return all remote todos accumulated on this context."""
        todos = self._state.spawned_remote
        self._state.spawned_remote = []
        return todos

    def _coerce_settled(self, df: DurableFunction, value: Any) -> Any:
        """Reshape a settled value to ``df``'s return type, platform-erroring on failure.

        Symmetric with the *encode* side in
        :meth:`ResonateEffects.settle_promise <resonate.effects.ResonateEffects.settle_promise>`:
        once a value has been durably persisted, failing to reconstruct it for
        the awaiter is a platform failure (release the task), not a user
        rejection -- the SDK never stores a value it then cannot rebuild. The
        caller passes the already-deserialized value, so a *rejected* record's
        reconstructed user error has been raised by ``decode_settled`` before
        this point and is never reached here.

        ponytail: a deterministic type mismatch (function's value vs its declared
        return annotation) now poison-loops on redelivery rather than storing a
        rejection -- the cost of matching the encode side's release-on-failure.
        Flip both sides to user-rejection if a visible error beats a poison task.
        """
        try:
            return df.coerce_result(value)
        except SerializationError as exc:
            raise PlatformError([exc]) from exc

    def _child_timeout(self, requested: timedelta | None) -> int:
        now = now_ms()
        timeout = requested if requested is not None else timedelta(days=1)
        return min(now + int(timeout.total_seconds() * 1000), self._state.timeout_at)

    @property
    def info(self) -> Info:
        return Info(
            id=self._state.id,
            parent_id=self._state.parent_id,
            origin_id=self._state.origin_id,
            branch_id=self._state.branch_id,
            timeout_at=self._state.timeout_at,
            func_name=self._state.func_name,
            tags={},
        )

    def _global_req(
        self,
        id: str,
        timeout: timedelta | None,
        *,
        data: TaskData | None = None,
        target: str | None = None,
        timer: bool = False,
        origin: str | None = None,
    ) -> PromiseCreateReq:
        """Build a global-scope promise request.

        Backs every global-scope creator -- remote dispatch (:meth:`rpc`,
        :meth:`detached`) as well as bare promises (:meth:`sleep`,
        :meth:`promise`). ``data`` carries a ``TaskData`` payload for function
        dispatch (empty otherwise); ``target`` adds the routing tag for remote
        dispatch; ``timer`` adds the ``resonate:timer`` tag that distinguishes a
        sleep from a bare promise. Tags are inserted in a fixed order so the
        serialized form is deterministic.

        ``resonate:prefix`` is *always* this context's :attr:`prefix_id` -- the
        prefix is set once at the top (``resonate.run``/``rpc``) and propagates
        down unchanged forever, including across ``detached`` re-roots, so it
        never tracks the (possibly diverging) lineage origin. ``origin`` defaults
        to ``origin_id``; only :meth:`detached` overrides it, setting origin to
        the child's own id to start a fresh lineage.
        """
        resolved_origin = origin if origin is not None else self._state.origin_id
        tags = {"resonate:scope": "global"}
        if target is not None:
            tags["resonate:target"] = target
        tags["resonate:branch"] = id
        tags["resonate:parent"] = self._state.id
        tags["resonate:origin"] = resolved_origin
        tags["resonate:prefix"] = self._state.prefix_id
        if timer:
            tags["resonate:timer"] = "true"
        return PromiseCreateReq(
            id=id,
            timeout_at=self._child_timeout(timeout),
            param=Value(data=data),
            tags=tags,
        )

    @overload
    def run[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ResonateFuture[T]: ...
    @overload
    def run[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ResonateFuture[T]: ...
    @overload
    def run(self, fn: str, *args: Any, **kwargs: Any) -> ResonateFuture[Any]: ...
    def run(
        self,
        fn: str | Callable[Concatenate[Context, ...], Any],
        *args: Any,
        **kwargs: Any,
    ) -> ResonateFuture[Any]:
        self._state.workflow = True
        # Chain promise creation: capture the previous tail
        # and install ours as the new tail.
        prev_created, created = self._advance_promise_chain()

        # Build id/req synchronously so child-id ordering matches call order
        # without relying on asyncio's task-start scheduling being FIFO.
        # ``pack_args`` validates arity and produces the in-memory ``payload``
        # the child executes from below; it is *not* serialized into the
        # promise param. A local child is run from this ``payload`` (never from
        # the param) and, on recovery, re-derived by the parent's replay -- so
        # its param is write-only and left empty here. That is what lets
        # ``ctx.run`` accept non-serializable arguments: unlike ``rpc``/
        # ``detached`` (whose param a different worker must decode), nothing ever
        # reads a local child's param back. The return value still round-trips
        # (see ``settle_promise`` / ``coerce_result`` below).
        #
        # Resolve the function: a name is a registry lookup (the function must
        # be registered locally -- a local child executes here, in process),
        # versioned by ``opts.version`` since a name carries none, just like
        # :meth:`rpc`; a callable is wrapped directly, carrying its own
        # identity, so it needs no registry round-trip.
        if isinstance(fn, str):
            resolved = self._state.registry.get(fn, self.opts.version)
            if resolved is None:
                exc = FunctionNotFoundError(fn, self.opts.version)
                raise PlatformError([exc]) from exc
            df = resolved
        else:
            df = DurableFunction(fn)
        payload = df.pack_args(*args, **kwargs)

        req = PromiseCreateReq(
            id=self._next_id(),
            timeout_at=self._child_timeout(self.opts.timeout),
            param=Value(),
            tags={
                "resonate:scope": "local",
                "resonate:branch": self._state.branch_id,
                "resonate:parent": self._state.id,
                "resonate:origin": self._state.origin_id,
                # Prefix is set at the top and propagates down unchanged forever.
                "resonate:prefix": self._state.prefix_id,
            },
        )

        # Record the local child in the execution tree before its promise is
        # created. ``ctx.run`` children are Int -- this worker settles them under
        # our task lease when ``bg`` returns. Added synchronously at the call
        # site (not inside ``bg``) so siblings appear in call order, giving the
        # deterministic children-as-prefix shape replay relies on. Idempotent, so
        # a replay re-walking the same body does not duplicate the node.
        self._state.tree.add_child(self._state.id, req.id, "int")

        async def bg() -> Any:
            record = await self._create_promise_in_chain(req, prev_created, created)

            # Idempotent recovery: an already-settled promise short-circuits
            # execution. The settled value comes back as JSON builtins, so coerce
            # it to the function's declared return type -- yielding the same
            # in-memory object the live path below produces, which also runs the
            # return through ``df.coerce_result``.
            #
            # Tree pruning: the local body never executes, so any children it
            # would have spawned are never added -- mark the node settled and
            # stop here.
            if record.state != "pending":
                self._state.tree.settle(req.id)
                return self._coerce_settled(df, decode_settled(record))

            # Pending: execute the child locally on its own Context, which is
            # what every durable function receives as its first argument.
            child = self._child(req.id, df.name, record.timeout_at)

            outcome: Status
            value: Any | Exception
            try:
                # Retry a pure-leaf failure per the call's policy; a child that
                # touches a durable op is a workflow and settles on the first
                # failure (see ``invoke_with_retry``). The override from
                # ``options`` wins; otherwise inherit this context's default.
                # ``opts`` is captured from the enclosing ``run`` call, so the
                # policy is fixed at dispatch.
                policy = (
                    self.opts.retry_policy
                    if self.opts.retry_policy is not None
                    else self._state.retry_policy
                )
                # ``coerce_args=False``: ``payload`` holds the verbatim in-memory
                # arguments (never serialized for a local child), so they pass to
                # the function untouched -- this is what lets ``ctx.run`` accept
                # non-serializable args. Replay re-derives the same objects, so
                # skipping coercion keeps the live and replay paths symmetric.
                value = await child.invoke_with_retry(
                    df, payload, policy, coerce_args=False
                )
                assert not inspect.isawaitable(value)
                outcome = "done"
            except Suspended:
                outcome = "suspended"
            except Exception as exc:
                # Any exception from the local child -- a deliberately raised
                # ``ApplicationError``, a grandchild rejection surfaced by
                # ``ctx.rpc``, or a plain domain exception -- is a rejection.
                # The original object crosses the boundary verbatim; the codec
                # flattens it to the wire error shape (pickling it where it
                # can) when ``settle_promise`` encodes it below. ``BaseException``
                # (``KeyboardInterrupt``, ``SystemExit``, ``CancelledError``) is
                # deliberately not caught -- it propagates so the task unwinds.
                value = exc
                outcome = "error"

            # Always drain the child's sub-work before deciding: a suspended
            # child has already pushed its own todos, and a "done" child may
            # still have spawned background work that registered todos.
            await child.flush_local_work()
            child_remote = child.take_remote_todos()

            # Structured-concurrency: check suspension states first
            if outcome == "suspended" or child_remote:
                self._state.spawned_remote.extend(child_remote)
                raise Suspended

            # Settle, then read the outcome back through the *settled
            # record* -- uniformly for both a resolved and a rejected
            # value, exactly as the recovery short-circuit above does.
            # ``settle_promise`` encodes ``value`` (rejecting any ``Exception``,
            # resolving otherwise) and returns the decoded record;
            # ``decode_settled`` then either returns the JSON builtins a replay
            # would see -- which ``coerce_result`` reshapes to the declared
            # return type -- or, for a rejected record, reconstructs the error
            # from its stored payload and raises it before ``coerce_result`` is
            # reached.
            #
            # Routing both outcomes through encode -> decode ->
            # (coerce | raise) -- rather than coercing the raw in-memory object
            # or re-raising the original exception -- makes the live and
            # recovery paths identical: a return that survives only via its
            # annotation (e.g. a ``datetime`` / ``set``) yields the same object
            # on every run, and a rejection surfaces the same reconstructed
            # error a replay would. The codec's best-effort ``__py_pickle``
            # recovers the original exception's type and attributes when it can
            # round-trip; what it cannot persist (a live traceback, ``__cause__``
            # chain, or an unpicklable class) is therefore identical on the live
            # and recovery paths -- both fall back to the same ``ApplicationError``.
            record = await self._state.effects.settle_promise(req.id, value)
            # The local executor completed (resolved or rejected): mark the
            # node settled. Reached only on the done/error path -- a suspended
            # child raised above with its node left pending.
            self._state.tree.settle(req.id)
            return self._coerce_settled(df, decode_settled(record))

        task = asyncio.create_task(bg())
        # Register for the structured-concurrency flush: a fire-and-forget
        # child that suspends or errors in the background must be joined in
        # ``flush_local_work`` so its todos / settled state are observed
        # before the parent decides what to do.
        self._state.spawned_locals.append(SpawnedLocal(id=req.id, handle=task))
        return ResonateFuture(
            _id=req.id,
            _task=task,
            _created=created,
        )

    @overload
    def rpc(self, fn: str, *args: Any, **kwargs: Any) -> ResonateFuture[Any]: ...
    @overload
    def rpc[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ResonateFuture[T]: ...
    @overload
    def rpc[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ResonateFuture[T]: ...
    def rpc(
        self,
        fn: str | Callable[Concatenate[Context, ...], Any],
        *args: Any,
        **kwargs: Any,
    ) -> ResonateFuture:
        self._state.workflow = True

        prev_created, created = self._advance_promise_chain()

        # A function object is dispatched by the name it was registered under
        # (reverse lookup), carrying its own registered version. Because that
        # implies local registration we also recover its ``DurableFunction`` and
        # thread it through, so the settled remote result is coerced to the
        # declared return type -- making the by-object future genuinely typed,
        # symmetric with ``ctx.run`` and the top-level ``rpc``. A bare string is
        # dispatched as-is at ``opts.version`` with no local function, so its
        # result stays raw builtins (untyped, like ``get``). An unregistered
        # object raises: its registry name is not its ``__name__``, so the target
        # cannot be guessed (pass the name as a string for an unregistered one).
        if isinstance(fn, str):
            name, version, df = fn, self.opts.version, None
        else:
            recorded = self._state.registry.reverse(fn)
            if recorded is None:
                exc = FunctionNotFoundError(getattr(fn, "__name__", "<anonymous>"))
                raise PlatformError([exc]) from exc
            name, version = recorded
            # Present by construction: the reverse and forward maps are populated
            # together at register time, so a reverse hit guarantees a forward one.
            df = self._state.registry.get(name, version)

        req = self._global_req(
            self._next_id(),
            self.opts.timeout,
            data=TaskData(func=name, args=args, kwargs=kwargs, version=version),
            target=self._state.target_resolver(self.opts.target),
        )

        return self._remote_future(req, prev_created, created, df)

    def sleep(self, duration: timedelta) -> ResonateFuture[None]:
        self._state.workflow = True

        prev_created, created = self._advance_promise_chain()

        req = self._global_req(self._next_id(), duration, timer=True)

        return self._remote_future(req, prev_created, created)

    def promise(self, timeout: timedelta | None = None) -> ResonateFuture[Any]:
        self._state.workflow = True

        prev_created, created = self._advance_promise_chain()

        req = self._global_req(self._next_id(), timeout)

        return self._remote_future(req, prev_created, created)

    def detached(self, fn: str, *args: Any, **kwargs: Any) -> ResonateFuture[str]:
        self._state.workflow = True
        prev_created, created = self._advance_promise_chain()

        # Mint the id off ``prefix_id`` -- set at the top and propagated unchanged
        # forever -- as ``{prefix}.d{16hex}``, so recursion stays bounded at one
        # segment past the prefix instead of growing a segment per level. The
        # ``d`` marks the segment as a detached child (vs an rpc child's numeric
        # ``.{seq}``). ``resonate:origin`` is the child's own id (a fresh lineage
        # root: ``origin == id == branch``); ``resonate:prefix`` (set in
        # ``_global_req``) carries the same prefix forward to the next level.
        child_id = f"{self._state.prefix_id}.d{_hash_id(self._next_id())}"
        req = self._global_req(
            child_id,
            self.opts.timeout,
            data=TaskData(func=fn, args=args, kwargs=kwargs, version=self.opts.version),
            target=self._state.target_resolver(self.opts.target),
            origin=child_id,
        )

        # Record the detached child as a Det node. Det subtrees are outside this
        # workflow's contract -- exempt from every well-formedness rule and
        # skipped by the frontier walk -- so a pending detached child never holds
        # the parent in the frontier.
        self._state.tree.add_child(self._state.id, req.id, "det")

        async def bg() -> str:
            """Background body for :meth:`detached`.

            Defers ``create_promise`` through the creation chain like the other
            entrypoints, but -- unlike :meth:`_await_remote` -- never registers a
            remote todo and never suspends: the detached child is fire-and-forget,
            so once the durable promise has been created (idempotent on replay)
            its id is simply returned. A failed create propagates down the
            creation chain rather than deadlocking its successors: each link
            re-raises the upstream exception.
            """
            record = await self._create_promise_in_chain(req, prev_created, created)
            # Det nodes are exempt from the contract, but track settlement anyway
            # so :meth:`Tree.print` and :meth:`Tree.get` reflect reality.
            if record.state != "pending":
                self._state.tree.settle(req.id)
            return req.id

        task = asyncio.create_task(bg())
        self._state.spawned_remote_tasks.append(task)
        return ResonateFuture(
            _id=req.id,
            _task=task,
            _created=created,
        )

    def _advance_promise_chain(
        self,
    ) -> tuple[asyncio.Future[None] | None, asyncio.Future[None]]:
        """Advances the creation chain tail, returning (prev_tail, new_tail)."""
        # No context-side abort gate: the circuit breaker lives in effects
        # (``create_promise`` / ``settle_promise`` short-circuit once
        # ``stopped`` is set), so a body that swallowed a platform error and
        # issued more durable work re-raises off the effects gate when the new
        # op hits the server, and the creation chain propagates an upstream
        # failure to later links.
        prev_tail = self._state.tail
        new_tail = asyncio.Future[None]()
        self._state.tail = new_tail
        return prev_tail, new_tail

    async def _create_promise_in_chain(
        self,
        req: PromiseCreateReq,
        prev_created: asyncio.Future[None] | None,
        created: asyncio.Future[None],
    ) -> PromiseRecord:
        """Create ``req``'s promise in creation-chain order.

        Waits on the previous chain link (so create_promise calls issue in
        ctx.run call order under concurrency), creates the promise, then
        resolves ``created`` to release the next link. ``created`` carries
        this link's outcome so awaiters (the next link, :meth:`ResonateFuture.id`)
        observe it: ``set_result`` on success, ``set_exception`` on failure.
        A predecessor that failed propagates its exception through
        ``await prev_created``; we re-raise it on this link too, so the whole
        chain fails as a unit rather than issuing a create on top of an
        inconsistent prefix. The *same* upstream exception object travels down
        every successor link (each catches it from ``await prev_created`` and
        re-installs it on its own ``created``), so the failure surfaces
        identically no matter how far down the chain it is observed.
        """
        try:
            if prev_created is not None:
                await prev_created

            record = await self._state.effects.create_promise(req)
            created.set_result(None)
        # PlatformError is included so a platform failure still settles this
        # link's ``created`` (successor links and ``ResonateFuture.id()``
        # awaiters would otherwise deadlock). Deliberately not a bare
        # ``BaseException``: that would interfere with task cancellation.
        except (Exception, PlatformError) as e:
            created.set_exception(e)
            # Mark retrieved
            created.exception()
            raise
        return record

    def _remote_future(
        self,
        req: PromiseCreateReq,
        prev_created: asyncio.Future[None] | None,
        created: asyncio.Future[None],
        df: DurableFunction | None = None,
    ) -> ResonateFuture:
        """Wrap a remote ``req`` in a future backed by a deferred-create task.

        Shared by :meth:`rpc`, :meth:`sleep`, and :meth:`promise`: the future's
        body is the :meth:`_await_remote` task spawned via
        :meth:`_spawn_remote_await`.

        ``df`` is the by-object ``rpc`` target's :class:`DurableFunction`, used to
        coerce a settled remote result to its declared return type; ``None`` for
        by-name ``rpc`` and for :meth:`sleep` / :meth:`promise` (no function),
        leaving the result as raw builtins.
        """
        # Record the remote child in the execution tree. All three callers are
        # external -- settled by something we await (another worker, the server
        # timer, an external ``promise.settle``), so a pending external node
        # sits in the suspension frontier. Added here at the (synchronous) call
        # site so the frontier covers even a future the body created but never
        # awaited.
        self._state.tree.add_child(self._state.id, req.id, "ext")
        return ResonateFuture(
            _id=req.id,
            _task=self._spawn_remote_await(req, prev_created, created, df),
            _created=created,
        )

    def _spawn_remote_await(
        self,
        req: PromiseCreateReq,
        prev_created: asyncio.Future[None] | None,
        created: asyncio.Future[None],
        df: DurableFunction | None = None,
    ) -> asyncio.Task[Any]:
        """Spawn the :meth:`_await_remote` task for rpc/sleep/promise.

        Registers the task in ``spawned_remote_tasks`` so
        :meth:`flush_local_work` joins it. The join retrieves the task's result
        -- a never-awaited suspended task would otherwise trip asyncio's
        "exception never retrieved" warning -- and guarantees its
        ``spawned_remote`` append has happened before the caller drains todos.
        Awaiting the returned future still raises ``Suspended`` as before;
        an asyncio task delivers its result to every awaiter, so the flush join
        and the future await do not conflict. ``df`` is forwarded to
        :meth:`_await_remote` for return-type coercion (by-object ``rpc`` only).
        """
        task = asyncio.create_task(self._await_remote(req, prev_created, created, df))
        self._state.spawned_remote_tasks.append(task)
        return task

    async def _await_remote(
        self,
        req: PromiseCreateReq,
        prev_created: asyncio.Future[None] | None,
        created: asyncio.Future[None],
        df: DurableFunction | None = None,
    ) -> Any:
        """Background body shared by :meth:`rpc`, :meth:`sleep`, and :meth:`promise`.

        Defers ``create_promise`` through the creation chain, short-circuits an
        already-settled record (idempotent recovery), and otherwise registers
        the child id as a remote todo and unwinds via ``Suspended``.

        Concurrency: ``spawned_remote.append`` needs no lock. These are asyncio
        tasks on a single event loop, and the append has no ``await`` between
        read and write, so it cannot interleave with a peer task. A failed
        create propagates down the creation chain rather than deadlocking the
        successors waiting on this link: each link re-raises the upstream
        exception.
        """
        record = await self._create_promise_in_chain(req, prev_created, created)

        if record.state != "pending":
            # Already settled: mark the node settled so it leaves the
            # frontier. A by-object ``rpc`` carries the target's
            # ``DurableFunction`` (``df``), so the settled builtins are coerced to
            # its declared return type -- symmetric with ``ctx.run``'s recovery
            # short-circuit, which is what makes the by-object future genuinely
            # typed. By-name ``rpc`` / ``sleep`` / ``promise`` pass ``df=None`` and
            # so leave the result as raw builtins (untyped, like top-level
            # ``rpc``/``get``). A rejected record raises inside ``decode_settled``,
            # before any coercion, exactly as in ``ctx.run``.
            self._state.tree.settle(req.id)
            settled = decode_settled(record)
            return self._coerce_settled(df, settled) if df is not None else settled

        # Still pending: the node stays pending, keeping it in the frontier.
        # The awaited subset (``spawned_remote``) is what callbacks are
        # registered for; the tree's full frontier is its superset.
        self._state.spawned_remote.append(req.id)
        raise Suspended
