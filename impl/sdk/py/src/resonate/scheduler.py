from __future__ import annotations

import os
import sys
import uuid
from inspect import isgenerator, isgeneratorfunction
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

from typing_extensions import ParamSpec, assert_never

from resonate import utils
from resonate.actions import (
    LFC,
    LFI,
    RFC,
    RFI,
    All,
    AllSettled,
    DeferredInvocation,
    Race,
)
from resonate.collections import DoubleDict
from resonate.commands import Command, CreateDurablePromiseReq
from resonate.context import Context
from resonate.dataclasses import (
    CoroAndPromise,
    FnOrCoroutine,
    RouteInfo,
    Runnable,
)
from resonate.dependency_injection import Dependencies
from resonate.encoders import JsonEncoder
from resonate.events import (
    ExecutionAwaited,
    ExecutionInvoked,
    ExecutionResumed,
    ExecutionTerminated,
    PromiseCompleted,
    PromiseCreated,
)
from resonate.functools import AsyncFnWrapper, FnWrapper, wrap_fn
from resonate.itertools import FinalValue, iterate_coro
from resonate.options import Options
from resonate.promise import (
    Promise,
    all_promises_are_done,
    any_promise_is_done,
    get_first_error_if_any,
)
from resonate.queue import DelayQueue, Queue
from resonate.result import Err, Ok
from resonate.time import now
from resonate.tracing.stdout import StdOutAdapter
from resonate.typing import Combinator, PromiseActions

if TYPE_CHECKING:
    from collections.abc import Hashable

    from resonate.record import DurablePromiseRecord
    from resonate.result import Result
    from resonate.retry_policy import RetryPolicy
    from resonate.storage import IPromiseStore
    from resonate.tracing import IAdapter
    from resonate.typing import (
        Awaitables,
        DurableCoro,
        DurableFn,
        EphemeralPromiseMemo,
        RunnableCoroutines,
    )

T = TypeVar("T")
P = ParamSpec("P")


class _SQE(Generic[T]):
    def __init__(
        self,
        route_info: RouteInfo,
        fn: FnWrapper[T] | AsyncFnWrapper[T],
    ) -> None:
        self.route_info = route_info
        self.fn = fn


class _CQE(Generic[T]):
    def __init__(
        self,
        sqe: _SQE[T],
        fn_result: Result[T, Exception],
    ) -> None:
        self.sqe = sqe
        self.fn_result = fn_result


class _Processor:
    def __init__(
        self,
        max_workers: int | None,
        sq: Queue[_SQE[Any]],
        scheduler: Scheduler,
    ) -> None:
        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        assert max_workers > 0, "`max_workers` must be positive."
        self._max_workers = max_workers
        self._sq = sq
        self._scheduler = scheduler
        self._threads = set[Thread]()

    def enqueue(self, sqe: _SQE[Any]) -> None:
        self._sq.put_nowait(sqe)
        self._adjust_thread_count()

    def _run(self) -> None:
        while True:
            sqe = self._sq.dequeue()
            fn_result = sqe.fn.run()
            assert isinstance(fn_result, (Ok, Err)), f"{fn_result} must be a result."
            self._scheduler.enqueue_cqe(
                sqe=sqe,
                value=fn_result,
            )

    def _adjust_thread_count(self) -> None:
        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            t = Thread(target=self._run, daemon=True)
            t.start()
            self._threads.add(t)


@final
class Scheduler:
    def __init__(
        self,
        durable_promise_storage: IPromiseStore,
        *,
        tracing_adapter: IAdapter | None = None,
        processor_threads: int | None = None,
    ) -> None:
        self._sdk_id = uuid.uuid4().hex
        self._registered_function = DoubleDict[str, Any]()
        self._attached_options_to_top_lvl: dict[str, Options] = {}

        self._stg_queue = Queue[tuple[LFI, Promise[Any], Context]]()
        self._completion_queue = Queue[_CQE[Any]]()
        self._submission_queue = Queue[_SQE[Any]]()
        self._combinators_queue = Queue[tuple[Combinator, Promise[Any]]]()

        self._worker_continue = Event()

        self._delay_queue = DelayQueue[RouteInfo](caller_event=self._worker_continue)

        self._deps = Dependencies()
        self._json_encoder = JsonEncoder()
        self._tracing_adapter: IAdapter = (
            tracing_adapter if tracing_adapter is not None else StdOutAdapter()
        )

        self._runnable_coros: RunnableCoroutines = []
        self._awaitables: Awaitables = {}

        self._promises_to_be_resolved: list[
            tuple[Promise[Any], Result[Any, Exception], list[Promise[Any]]]
        ] = []

        self._processor = _Processor(
            processor_threads,
            self._submission_queue,
            self,
        )
        self._durable_promise_storage = durable_promise_storage
        self._emphemeral_promise_memo: EphemeralPromiseMemo = {}

        self._worker_thread = Thread(target=self._run, daemon=True)
        self._worker_thread.start()

    def enqueue_cqe(self, sqe: _SQE[T], value: Result[T, Exception]) -> None:
        self._completion_queue.put(_CQE[Any](sqe=sqe, fn_result=value))
        self._signal()

    def _enqueue_combinator(
        self, combinator: Combinator, promise: Promise[Any]
    ) -> None:
        assert (
            not promise.done()
        ), "Do not enqueue done promises associated to a combinator."
        self._combinators_queue.put_nowait((combinator, promise))
        self._signal()

    def _signal(self) -> None:
        self._worker_continue.set()

    def _complete_durable_promise_record(
        self, promise_id: str, value: Result[Any, Exception]
    ) -> DurablePromiseRecord:
        if isinstance(value, Ok):
            return self._durable_promise_storage.resolve(
                promise_id=promise_id,
                ikey=utils.string_to_ikey(promise_id),
                strict=False,
                headers=None,
                data=self._json_encoder.encode(value.unwrap()),
            )
        if isinstance(value, Err):
            return self._durable_promise_storage.reject(
                promise_id=promise_id,
                ikey=utils.string_to_ikey(promise_id),
                strict=False,
                headers=None,
                data=self._json_encoder.encode(value.err()),
            )
        assert_never(value)

    def _register_callback_or_resolve_ephemeral_promise(
        self, promise: Promise[Any], recv: str = "default"
    ) -> None:
        assert isinstance(promise.action, RFI), "We only register callbacks for rfi"
        durable_promise, created_callback = (
            self._durable_promise_storage.create_callback(
                promise_id=promise.promise_id,
                root_promise_id=promise.root_promise_id,
                timeout=sys.maxsize,
                recv=recv,
            )
        )
        if created_callback is not None:
            return

        assert (
            durable_promise.is_completed()
        ), "Callback won't be created only if durable promise has been completed."
        v = self._get_value_from_durable_promise(durable_promise_record=durable_promise)
        promise.set_result(v)
        assert (
            promise.promise_id in self._emphemeral_promise_memo
        ), "Ephemeral process must have been registered in the memo."

        self._tracing_adapter.process_event(
            PromiseCompleted(
                promise_id=promise.promise_id,
                tick=now(),
                value=v,
                parent_promise_id=promise.parent_promise_id(),
            )
        )
        self._emphemeral_promise_memo.pop(promise.promise_id)

    def _create_durable_promise_record(
        self,
        req: CreateDurablePromiseReq,
    ) -> DurablePromiseRecord:
        assert (
            req.promise_id is not None
        ), "Promise id must be user provided or generated by this point."
        return self._durable_promise_storage.create(
            promise_id=req.promise_id,
            ikey=utils.string_to_ikey(req.promise_id),
            strict=False,
            headers=None,
            data=self._json_encoder.encode(req.data) if req.data is not None else None,
            timeout=sys.maxsize,
            tags=req.tags,
        )

    def _get_value_from_durable_promise(
        self, durable_promise_record: DurablePromiseRecord
    ) -> Result[Any, Exception]:
        assert (
            durable_promise_record.is_completed()
        ), "If you want to get the value then the promise must have been completed"

        v: Result[Any, Exception]
        if durable_promise_record.is_rejected():
            if durable_promise_record.value.data is None:
                raise NotImplementedError

            v = Err(self._json_encoder.decode(durable_promise_record.value.data))
        else:
            assert durable_promise_record.is_resolved()
            if durable_promise_record.value.data is None:
                v = Ok(None)
            else:
                v = Ok(self._json_encoder.decode(durable_promise_record.value.data))
        return v

    def _create_promise(  # noqa: C901, PLR0912
        self,
        parent_promise: Promise[Any] | None,
        promise_id: str | None,
        action: PromiseActions,
    ) -> Promise[Any]:
        if parent_promise is not None:
            p = parent_promise.child_promise(promise_id=promise_id, action=action)
        else:
            assert (
                promise_id is not None
            ), "If creating a root promise must provide a promise id."
            p = Promise[Any](
                promise_id=promise_id, action=action, parent_promise=parent_promise
            )
        assert (
            p.promise_id not in self._emphemeral_promise_memo
        ), "There should not be a new promise with same promise id."

        self._emphemeral_promise_memo[p.promise_id] = p

        req: CreateDurablePromiseReq
        if isinstance(action, LFI):
            if isinstance(action.exec_unit, Command):
                assert not isinstance(
                    action.exec_unit, CreateDurablePromiseReq
                ), "This command is not allowed for lfi"
                req = CreateDurablePromiseReq(promise_id=p.promise_id)
            elif isinstance(action.exec_unit, FnOrCoroutine):
                func_name = self._registered_function.get_from_value(
                    action.exec_unit.exec_unit
                )
                if func_name is not None:
                    tags = self._attached_options_to_top_lvl[func_name]
                    req = action.exec_unit.to_req(
                        promise_id=p.promise_id,
                        func_name=func_name,
                        tags=tags.tags,
                    )
                else:
                    req = CreateDurablePromiseReq(promise_id=p.promise_id)
            else:
                assert_never(action.exec_unit)
        elif isinstance(action, RFI):
            if isinstance(action.exec_unit, Command):
                assert isinstance(
                    action.exec_unit, CreateDurablePromiseReq
                ), "This is the only command allowed for rfi"
                req = action.exec_unit
            elif isinstance(action.exec_unit, FnOrCoroutine):
                func_name = self._registered_function.get_from_value(
                    action.exec_unit.exec_unit
                )
                assert (
                    func_name is not None
                ), "To do a rfi the function must be registered."
                attached_options = self._attached_options_to_top_lvl[func_name]

                final_tags = attached_options.tags
                if "resonate:invoke" not in attached_options.tags:
                    final_tags["resonate:invoke"] = f"poll://default/{self._sdk_id}"

                req = action.exec_unit.to_req(
                    p.promise_id,
                    func_name,
                    tags=final_tags,
                )

            else:
                assert_never(action.exec_unit)
        elif isinstance(action, (All, AllSettled, Race)):
            req = CreateDurablePromiseReq(promise_id=p.promise_id)
        else:
            assert_never(action)

        if not p.durable:
            self._tracing_adapter.process_event(
                PromiseCreated(
                    promise_id=p.promise_id,
                    tick=now(),
                    parent_promise_id=p.parent_promise_id(),
                )
            )
            return p

        durable_promise_record = self._create_durable_promise_record(req=req)
        self._tracing_adapter.process_event(
            PromiseCreated(
                promise_id=p.promise_id,
                tick=now(),
                parent_promise_id=p.parent_promise_id(),
            )
        )
        if durable_promise_record.is_pending():
            return p

        assert (
            durable_promise_record.value.data is not None
        ), "If the promise is not pending, there must be data."

        v = self._get_value_from_durable_promise(
            durable_promise_record=durable_promise_record
        )

        self._resolve_promise(p, v)
        return p

    def register(
        self,
        func: DurableCoro[P, Hashable] | DurableFn[P, Hashable],
        name: str | None = None,
        retry_policy: RetryPolicy | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        if name is None:
            name = func.__name__
        assert (
            self._registered_function.get(name) is None
        ), "There's already a coroutine registered with this name."
        assert (
            name not in self._attached_options_to_top_lvl
        ), "There's already a coroutine registered with this name."
        self._registered_function.add(name, func)
        self._attached_options_to_top_lvl[name] = Options(
            durable=True,
            promise_id=None,
            retry_policy=retry_policy,
            tags=tags,
        )

    def run(
        self,
        promise_id: str,
        coro: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Promise[T]:
        if promise_id in self._emphemeral_promise_memo:
            return self._emphemeral_promise_memo[promise_id]

        function_name = self._registered_function.get_from_value(coro)
        assert (
            function_name is not None
        ), f"There's no function registed for function {coro.__name__}."
        attached_options = self._attached_options_to_top_lvl[function_name]

        assert attached_options.durable, "All top level invocation must be durable."
        top_lvl = LFI(
            FnOrCoroutine(coro, *args, **kwargs),
            opts=attached_options,
        )

        root_ctx = Context(
            seed=None,
            deps=self._deps,
        )

        p: Promise[Any] = self._create_promise(
            promise_id=promise_id,
            parent_promise=None,
            action=top_lvl,
        )

        self._stg_queue.put_nowait((top_lvl, p, root_ctx))
        self._signal()
        return p

    def _route_fn_or_coroutine(
        self,
        route_info: RouteInfo,
    ) -> None:
        assert (
            not route_info.promise.done()
        ), "Only unresolved executions of unresolved promises can be passed here."
        if isgeneratorfunction(route_info.fn_or_coroutine.exec_unit):
            coro = route_info.fn_or_coroutine.exec_unit(
                route_info.ctx,
                *route_info.fn_or_coroutine.args,
                **route_info.fn_or_coroutine.kwargs,
            )
            self._add_coro_to_runnables(
                CoroAndPromise(route_info, coro),
                None,
                was_awaited=False,
            )

        else:
            self._processor.enqueue(
                _SQE(
                    route_info=route_info,
                    fn=wrap_fn(
                        route_info.ctx,
                        route_info.fn_or_coroutine.exec_unit,
                        *route_info.fn_or_coroutine.args,
                        **route_info.fn_or_coroutine.kwargs,
                    ),
                )
            )

        self._tracing_adapter.process_event(
            ExecutionInvoked(
                route_info.promise.promise_id,
                route_info.promise.parent_promise_id(),
                now(),
                route_info.fn_or_coroutine.exec_unit.__name__,
                route_info.fn_or_coroutine.args,
                route_info.fn_or_coroutine.kwargs,
            )
        )

    def _run(self) -> None:  # noqa: C901, PLR0912
        while self._worker_continue.wait():
            self._worker_continue.clear()

            delay_es = self._delay_queue.dequeue_all()
            for delay_e in delay_es:
                if isinstance(delay_e, RouteInfo):
                    self._route_fn_or_coroutine(delay_e)
                else:
                    assert_never(delay_e)

            top_lvls = self._stg_queue.dequeue_all()
            for top_lvl, p, root_ctx in top_lvls:
                assert isinstance(top_lvl.exec_unit, FnOrCoroutine)
                if p.done():
                    self._unblock_coros_waiting_on_promise(p)
                else:
                    self._route_fn_or_coroutine(
                        RouteInfo(root_ctx, p, top_lvl.exec_unit, retry_attempt=0)
                    )

            cqes = self._completion_queue.dequeue_all()
            for cqe in cqes:
                if not cqe.sqe.route_info.to_be_retried(cqe.fn_result):
                    promise_to_resolve = cqe.sqe.route_info.promise
                    assert (
                        promise_to_resolve.promise_id in self._emphemeral_promise_memo
                    ), "Promise must be recorded in ephemeral storage."

                    promise = self._emphemeral_promise_memo[
                        promise_to_resolve.promise_id
                    ]
                    self._tracing_adapter.process_event(
                        ExecutionTerminated(
                            promise_id=promise.promise_id,
                            parent_promise_id=promise.parent_promise_id(),
                            tick=now(),
                        )
                    )
                    self._resolve_promise(promise, value=cqe.fn_result)
                    self._unblock_coros_waiting_on_promise(promise)
                else:
                    self._delay_queue.put_nowait(
                        cqe.sqe.route_info.next_retry_attempt(),
                        delay=cqe.sqe.route_info.next_retry_delay(),
                    )

            for p_to_b_resolved in self._promises_to_be_resolved:
                prom, final_value, children_promises = p_to_b_resolved
                if all_promises_are_done(children_promises):
                    firt_error = get_first_error_if_any(children_promises)
                    if firt_error is None:
                        self._resolve_promise(prom, final_value)
                    else:
                        self._resolve_promise(prom, firt_error)

                    self._unblock_coros_waiting_on_promise(prom)
                    self._promises_to_be_resolved.remove(p_to_b_resolved)

            combinators = self._combinators_queue.dequeue_all()
            for combinator, p in combinators:
                assert not p.done(), (
                    "If the promise related to a combinator is resolved"
                    "already the combinator should not be in the combinators queue"
                )
                if self._combinator_done(combinator):
                    self._resolve_promise(p, self._combinator_result(combinator))
                    self._unblock_coros_waiting_on_promise(p)
                else:
                    self._enqueue_combinator(combinator, p)

            while self._runnable_coros:
                runnable, was_awaited = self._runnable_coros.pop()
                self._advance_runnable_span(runnable=runnable, was_awaited=was_awaited)

            assert not self._runnable_coros, "Runnables should have been all exhausted"

    def _add_coro_to_awaitables(
        self, p: Promise[Any], coro_and_promise: CoroAndPromise[Any]
    ) -> None:
        assert (
            not p.done()
        ), "If the promise is resolved already it makes no sense to block coroutine"
        self._awaitables.setdefault(p, []).append(coro_and_promise)
        self._tracing_adapter.process_event(
            ExecutionAwaited(
                promise_id=coro_and_promise.route_info.promise.promise_id,
                tick=now(),
                parent_promise_id=coro_and_promise.route_info.promise.parent_promise_id(),
            )
        )

    def _add_coro_to_runnables(
        self,
        coro_and_promise: CoroAndPromise[Any],
        value_to_yield_back: Result[Any, Exception] | None,
        *,
        was_awaited: bool,
    ) -> None:
        self._runnable_coros.append(
            (Runnable(coro_and_promise, value_to_yield_back), was_awaited)
        )

    def _unblock_coros_waiting_on_promise(self, p: Promise[Any]) -> None:
        assert p.done(), "Promise must be done to unblock waiting coroutines."
        if self._awaitables.get(p) is None:
            return

        for coro_and_promise in self._awaitables.pop(p):
            self._add_coro_to_runnables(
                coro_and_promise=coro_and_promise,
                value_to_yield_back=p.safe_result(),
                was_awaited=True,
            )

    def _resolve_promise(
        self, promise: Promise[T], value: Result[T, Exception]
    ) -> None:
        if promise.durable:
            completed_record = self._complete_durable_promise_record(
                promise_id=promise.promise_id,
                value=value,
            )
            assert (
                completed_record.is_completed()
            ), "Durable promise record must be completed by this point."
            v = self._get_value_from_durable_promise(completed_record)
            promise.set_result(v)
        else:
            promise.set_result(value)
        assert (
            promise.promise_id in self._emphemeral_promise_memo
        ), "Ephemeral process must have been registered in the memo."

        self._tracing_adapter.process_event(
            PromiseCompleted(
                promise_id=promise.promise_id,
                tick=now(),
                value=value,
                parent_promise_id=promise.parent_promise_id(),
            )
        )
        self._emphemeral_promise_memo.pop(promise.promise_id)

    def _advance_runnable_span(  # noqa: C901, PLR0912, PLR0915. Note: We want to keep all the control flow in the function
        self, runnable: Runnable[Any], *, was_awaited: bool
    ) -> None:
        assert isgenerator(
            runnable.coro_and_promise.coro
        ), "Only coroutines can be advanced"

        yieldable_or_final_value = iterate_coro(runnable=runnable)

        if was_awaited:
            self._tracing_adapter.process_event(
                ExecutionResumed(
                    promise_id=runnable.coro_and_promise.route_info.promise.promise_id,
                    tick=now(),
                    parent_promise_id=runnable.coro_and_promise.route_info.promise.parent_promise_id(),
                )
            )

        if isinstance(yieldable_or_final_value, FinalValue):
            final_value = yieldable_or_final_value.v
            if not runnable.coro_and_promise.route_info.to_be_retried(final_value):
                self._tracing_adapter.process_event(
                    ExecutionTerminated(
                        promise_id=runnable.coro_and_promise.route_info.promise.promise_id,
                        tick=now(),
                        parent_promise_id=runnable.coro_and_promise.route_info.promise.parent_promise_id(),
                    )
                )
                if all_promises_are_done(
                    runnable.coro_and_promise.route_info.promise.children_promises
                ):
                    self._resolve_promise(
                        runnable.coro_and_promise.route_info.promise, final_value
                    )
                    self._unblock_coros_waiting_on_promise(
                        runnable.coro_and_promise.route_info.promise
                    )
                else:
                    for (
                        child_promise
                    ) in runnable.coro_and_promise.route_info.promise.children_promises:
                        if isinstance(child_promise.action, RFI):
                            self._register_callback_or_resolve_ephemeral_promise(
                                child_promise
                            )

                    self._promises_to_be_resolved.append(
                        (
                            runnable.coro_and_promise.route_info.promise,
                            final_value,
                            runnable.coro_and_promise.route_info.promise.children_promises,
                        )
                    )
                del runnable
            else:
                self._delay_queue.put_nowait(
                    runnable.coro_and_promise.route_info.next_retry_attempt(),
                    delay=runnable.coro_and_promise.route_info.next_retry_delay(),
                )

        elif isinstance(yieldable_or_final_value, LFC):
            p = self._process_local_invocation(
                yieldable_or_final_value.to_invocation(), runnable
            )
            assert (
                p not in self._awaitables
            ), "Since it's a call it should be a promise without dependants"

            if p.done():
                self._add_coro_to_runnables(
                    runnable.coro_and_promise, p.safe_result(), was_awaited=False
                )
            else:
                self._add_coro_to_awaitables(p, runnable.coro_and_promise)

        elif isinstance(yieldable_or_final_value, LFI):
            p = self._process_local_invocation(yieldable_or_final_value, runnable)
            self._add_coro_to_runnables(
                runnable.coro_and_promise, Ok(p), was_awaited=False
            )

        elif isinstance(yieldable_or_final_value, Promise):
            p = yieldable_or_final_value
            if p.done():
                self._unblock_coros_waiting_on_promise(p)
                self._add_coro_to_runnables(
                    runnable.coro_and_promise, p.safe_result(), was_awaited=False
                )
            else:
                if isinstance(p.action, RFI):
                    self._register_callback_or_resolve_ephemeral_promise(p)

                if p.done():
                    self._unblock_coros_waiting_on_promise(p)
                    self._add_coro_to_runnables(
                        runnable.coro_and_promise, p.safe_result(), was_awaited=False
                    )
                else:
                    self._add_coro_to_awaitables(p, runnable.coro_and_promise)

        elif isinstance(yieldable_or_final_value, (All, AllSettled, Race)):
            p = self._process_combinator(yieldable_or_final_value, runnable)
            self._add_coro_to_runnables(
                runnable.coro_and_promise, Ok(p), was_awaited=False
            )

        elif isinstance(yieldable_or_final_value, DeferredInvocation):
            deferred_p: Promise[Any] = self.run(
                yieldable_or_final_value.promise_id,
                yieldable_or_final_value.coro.exec_unit,
                *yieldable_or_final_value.coro.args,
                **yieldable_or_final_value.coro.kwargs,
            )
            self._add_coro_to_runnables(
                runnable.coro_and_promise, Ok(deferred_p), was_awaited=False
            )

        elif isinstance(yieldable_or_final_value, RFI):
            p = self._process_remote_invocation(yieldable_or_final_value, runnable)
            self._add_coro_to_runnables(
                runnable.coro_and_promise, Ok(p), was_awaited=False
            )
        elif isinstance(yieldable_or_final_value, RFC):
            p = self._process_remote_invocation(
                yieldable_or_final_value.to_invocation(), runnable
            )
            assert (
                p not in self._awaitables
            ), "Since it's a call it should be a promise without dependants"

            if p.done():
                self._add_coro_to_runnables(
                    runnable.coro_and_promise, p.safe_result(), was_awaited=False
                )
            else:
                self._register_callback_or_resolve_ephemeral_promise(p)
                if p.done():
                    self._add_coro_to_runnables(
                        runnable.coro_and_promise, p.safe_result(), was_awaited=False
                    )
                else:
                    self._add_coro_to_awaitables(p, runnable.coro_and_promise)
        else:
            assert_never(yieldable_or_final_value)

    def _process_remote_invocation(
        self, invocation: RFI, runnable: Runnable[Any]
    ) -> Promise[Any]:
        return self._create_promise(
            parent_promise=runnable.coro_and_promise.route_info.promise,
            promise_id=invocation.promise_id,
            action=invocation,
        )

    def _process_local_invocation(
        self, invocation: LFI, runnable: Runnable[Any]
    ) -> Promise[Any]:
        p = self._create_promise(
            parent_promise=runnable.coro_and_promise.route_info.promise,
            promise_id=invocation.opts.promise_id,
            action=invocation,
        )

        if isinstance(invocation.exec_unit, Command):
            raise NotImplementedError
        if isinstance(invocation.exec_unit, FnOrCoroutine):
            if p.done():
                self._unblock_coros_waiting_on_promise(p)
            else:
                self._route_fn_or_coroutine(
                    RouteInfo(
                        ctx=runnable.coro_and_promise.route_info.ctx,
                        promise=p,
                        fn_or_coroutine=invocation.exec_unit,
                        retry_attempt=0,
                    )
                )
        else:
            assert_never(invocation.exec_unit)
        return p

    def _process_combinator(
        self, combinator: Combinator, runnable: Runnable[Any]
    ) -> Promise[Any]:
        p = self._create_promise(
            parent_promise=runnable.coro_and_promise.route_info.promise,
            promise_id=combinator.opts.promise_id,
            action=combinator,
        )
        if p.done():
            self._unblock_coros_waiting_on_promise(p)
        else:
            self._enqueue_combinator(combinator, p)

        return p

    def _combinator_done(self, combinator: Combinator) -> bool:
        if isinstance(combinator, (All, AllSettled)):
            return all_promises_are_done(combinator.promises)

        if isinstance(combinator, Race):
            return any_promise_is_done(combinator.promises)

        assert_never(combinator)

    def _combinator_result(
        self, combinator: Combinator
    ) -> Result[
        T | list[T | Exception], Exception
    ]:  # Note: We can't possible have single type for all the promises of a Combinator
        if isinstance(combinator, All):
            try:
                return Ok([p.result() for p in combinator.promises])
            except Exception as err:  # noqa: BLE001, Note: We can not predict which Exception we will receive from the user
                return Err(err)

        if isinstance(combinator, AllSettled):
            if not combinator.promises:
                return Ok([])

            res = []
            for p in combinator.promises:
                try:
                    ok = p.result()
                    res.append(ok)
                except Exception as err:  # noqa: BLE001, PERF203
                    res.append(err)

            return Ok(res)

        if isinstance(combinator, Race):
            try:
                return Ok(next(p.result() for p in combinator.promises if p.done()))
            except Exception as err:  # noqa: BLE001, Note: We can not predict which Exception we will receive from the user
                return Err(err)

        assert_never(combinator)
