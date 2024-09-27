from __future__ import annotations

import os
import sys
from inspect import isgenerator, isgeneratorfunction
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

from typing_extensions import ParamSpec, assert_never

from resonate import utils
from resonate.actions import (
    All,
    AllSettled,
    Call,
    DeferredInvocation,
    Invocation,
    Race,
    Sleep,
)
from resonate.collections import DoubleDict
from resonate.context import Context
from resonate.dataclasses import (
    Command,
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
from resonate.typing import Combinator

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
        self._registered_function = DoubleDict[str, Any]()
        self._attached_options_to_top_lvl: dict[str, Options] = {}

        self._stg_queue = Queue[tuple[Invocation, Promise[Any], Context]]()
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

    def _create_durable_promise_record(
        self, promise_id: str, data: dict[str, Any]
    ) -> DurablePromiseRecord:
        return self._durable_promise_storage.create(
            promise_id=promise_id,
            ikey=utils.string_to_ikey(promise_id),
            strict=False,
            headers=None,
            data=self._json_encoder.encode(data),
            timeout=sys.maxsize,
            tags=None,
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

    def _get_ctx_from_ephemeral_memo(
        self, promise_id: str, *, and_delete: bool
    ) -> Context:
        if and_delete:
            return self._emphemeral_promise_memo.pop(promise_id)[-1]
        return self._emphemeral_promise_memo[promise_id][-1]

    def _create_promise(
        self, ctx: Context, action: Invocation | Sleep | Combinator
    ) -> Promise[Any]:
        p = Promise[Any](promise_id=ctx.ctx_id, action=action)
        assert (
            p.promise_id not in self._emphemeral_promise_memo
        ), "There should not be a new promise with same promise id."
        self._emphemeral_promise_memo[p.promise_id] = (p, ctx)
        if isinstance(action, Invocation):
            if isinstance(action.exec_unit, Command):
                raise NotImplementedError
            if isinstance(action.exec_unit, FnOrCoroutine):
                data = action.exec_unit.json_data()
            else:
                assert_never(action.exec_unit)

        elif isinstance(action, (All, AllSettled, Race)):
            data = {}

        elif isinstance(action, Sleep):
            raise NotImplementedError
        else:
            assert_never(action)

        if not p.durable:
            self._tracing_adapter.process_event(
                PromiseCreated(
                    promise_id=p.promise_id,
                    tick=now(),
                    parent_promise_id=ctx.parent_promise_id(),
                )
            )
            return p

        durable_promise_record = self._create_durable_promise_record(
            promise_id=p.promise_id, data=data
        )
        self._tracing_adapter.process_event(
            PromiseCreated(
                promise_id=p.promise_id,
                tick=now(),
                parent_promise_id=ctx.parent_promise_id(),
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
        name: str,
        func: DurableCoro[P, Hashable] | DurableFn[P, Hashable],
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        assert (
            self._registered_function.get(name) is None
        ), "There's already a coroutine registered with this name."
        assert (
            name not in self._attached_options_to_top_lvl
        ), "There's already a coroutine registered with this name."
        self._registered_function.add(name, func)
        self._attached_options_to_top_lvl[name] = Options(
            durable=True, promise_id=None, retry_policy=retry_policy
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
            return self._emphemeral_promise_memo[promise_id][0]

        function_name = self._registered_function.get_from_value(coro)
        assert (
            function_name is not None
        ), f"There's no function registed for function {coro.__name__}."
        attached_options = self._attached_options_to_top_lvl[function_name]

        assert attached_options.durable, "All top level invocation must be durable."
        top_lvl = Invocation(
            FnOrCoroutine(coro, *args, **kwargs),
            opts=attached_options,
        )

        root_ctx = Context(
            ctx_id=promise_id,
            seed=None,
            parent_ctx=None,
            deps=self._deps,
        )

        p: Promise[Any] = self._create_promise(ctx=root_ctx, action=top_lvl)

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
                route_info.ctx.parent_promise_id(),
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

                    promise, ctx = self._emphemeral_promise_memo[
                        promise_to_resolve.promise_id
                    ]
                    self._tracing_adapter.process_event(
                        ExecutionTerminated(
                            promise_id=promise.promise_id,
                            parent_promise_id=ctx.parent_promise_id(),
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
                promise_id=coro_and_promise.prom.promise_id,
                tick=now(),
                parent_promise_id=coro_and_promise.ctx.parent_promise_id(),
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
                parent_promise_id=self._get_ctx_from_ephemeral_memo(
                    promise.promise_id,
                    and_delete=True,
                ).parent_promise_id(),
            )
        )

    def _advance_runnable_span(  # noqa: C901, PLR0912. Note: We want to keep all the control flow in the function
        self, runnable: Runnable[Any], *, was_awaited: bool
    ) -> None:
        assert isgenerator(
            runnable.coro_and_promise.coro
        ), "Only coroutines can be advanced"

        yieldable_or_final_value = iterate_coro(runnable=runnable)

        if was_awaited:
            self._tracing_adapter.process_event(
                ExecutionResumed(
                    promise_id=runnable.coro_and_promise.prom.promise_id,
                    tick=now(),
                    parent_promise_id=runnable.coro_and_promise.ctx.parent_promise_id(),
                )
            )

        if isinstance(yieldable_or_final_value, FinalValue):
            final_value = yieldable_or_final_value.v
            if not runnable.coro_and_promise.route_info.to_be_retried(final_value):
                self._tracing_adapter.process_event(
                    ExecutionTerminated(
                        promise_id=runnable.coro_and_promise.prom.promise_id,
                        tick=now(),
                        parent_promise_id=runnable.coro_and_promise.ctx.parent_promise_id(),
                    )
                )
                if all_promises_are_done(runnable.coro_and_promise.children_promises):
                    self._resolve_promise(runnable.coro_and_promise.prom, final_value)
                    self._unblock_coros_waiting_on_promise(
                        runnable.coro_and_promise.prom
                    )
                else:
                    self._promises_to_be_resolved.append(
                        (
                            runnable.coro_and_promise.prom,
                            final_value,
                            runnable.coro_and_promise.children_promises,
                        )
                    )
                del runnable
            else:
                self._delay_queue.put_nowait(
                    runnable.coro_and_promise.route_info.next_retry_attempt(),
                    delay=runnable.coro_and_promise.route_info.next_retry_delay(),
                )

        elif isinstance(yieldable_or_final_value, Call):
            p = self._process_invocation(
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

        elif isinstance(yieldable_or_final_value, Invocation):
            p = self._process_invocation(yieldable_or_final_value, runnable)
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

        elif isinstance(yieldable_or_final_value, Sleep):
            raise NotImplementedError

        else:
            assert_never(yieldable_or_final_value)

    def _process_invocation(
        self, invokation: Invocation, runnable: Runnable[Any]
    ) -> Promise[Any]:
        child_ctx = runnable.coro_and_promise.ctx.new_child(
            ctx_id=invokation.opts.promise_id,
        )
        p = self._create_promise(child_ctx, invokation)
        runnable.coro_and_promise.children_promises.append(p)

        if isinstance(invokation.exec_unit, Command):
            raise NotImplementedError
        if isinstance(invokation.exec_unit, FnOrCoroutine):
            if p.done():
                self._unblock_coros_waiting_on_promise(p)
            else:
                self._route_fn_or_coroutine(
                    RouteInfo(child_ctx, p, invokation.exec_unit, retry_attempt=0)
                )
        else:
            assert_never(invokation.exec_unit)
        return p

    def _process_combinator(
        self, combinator: Combinator, runnable: Runnable[Any]
    ) -> Promise[Any]:
        child_ctx = runnable.coro_and_promise.ctx.new_child(
            ctx_id=combinator.opts.promise_id
        )
        p = self._create_promise(child_ctx, combinator)
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
        Any | list[Any | Exception], Exception
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
