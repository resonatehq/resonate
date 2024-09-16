from __future__ import annotations

import asyncio
import json
import os
import queue
import sys
from inspect import isgenerator, isgeneratorfunction
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import ParamSpec, assert_never

from resonate import utils
from resonate.actions import Sleep
from resonate.context import (
    Call,
    Context,
    Invoke,
)
from resonate.dataclasses import Command, CoroAndPromise, FnOrCoroutine, Runnable
from resonate.dependency_injection import Dependencies
from resonate.encoders import ErrorEncoder, JsonEncoder
from resonate.events import (
    ExecutionAwaited,
    ExecutionInvoked,
    ExecutionResumed,
    ExecutionTerminated,
    PromiseCompleted,
    PromiseCreated,
)
from resonate.itertools import FinalValue, iterate_coro
from resonate.promise import Promise
from resonate.result import Err, Ok
from resonate.time import now
from resonate.tracing.stdout import StdOutAdapter

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from resonate.options import Options
    from resonate.record import DurablePromiseRecord
    from resonate.result import Result
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
        promise_and_context: tuple[Promise[T], Context],
        fn: Callable[P, T | Coroutine[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.promise_and_context = promise_and_context
        self.fn = fn
        self.args = args
        self.kwargs = kwargs


class _CQE(Generic[T]):
    def __init__(
        self,
        promise_and_context: tuple[Promise[T], Context],
        fn_result: Result[T, Exception],
    ) -> None:
        self.fn_result = fn_result
        self.promise_and_context = promise_and_context


class _Processor:
    def __init__(
        self,
        max_workers: int | None,
        sq: queue.Queue[_SQE[Any]],
        cq: queue.Queue[_CQE[Any]],
        continue_event: Event | None,
    ) -> None:
        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        assert max_workers > 0, "`max_workers` must be positive."
        self._max_workers = max_workers
        self._sq: queue.Queue[_SQE[Any]] = sq
        self._cq: queue.Queue[_CQE[Any]] = cq
        self._continue_event: Event | None = continue_event
        self._threads = set[Thread]()

    def enqueue(self, sqe: _SQE[Any]) -> None:
        self._sq.put_nowait(sqe)
        self._adjust_thread_count()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        while True:
            fn_result: Result[Any, Exception]
            sqe = utils.dequeue(self._sq)
            if asyncio.iscoroutinefunction(sqe.fn):
                try:
                    fn_result = Ok(
                        loop.run_until_complete(sqe.fn(*sqe.args, **sqe.kwargs))
                    )
                except Exception as e:  # noqa: BLE001
                    fn_result = Err(e)
            else:
                try:
                    fn_result = Ok(sqe.fn(*sqe.args, **sqe.kwargs))
                except Exception as e:  # noqa: BLE001
                    fn_result = Err(e)

            self._cq.put_nowait(_CQE(sqe.promise_and_context, fn_result))
            if self._continue_event is not None:
                self._continue_event.set()

    def _adjust_thread_count(self) -> None:
        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            t = Thread(target=self._run, daemon=True)
            t.start()
            self._threads.add(t)


class Scheduler:
    def __init__(
        self,
        durable_promise_storage: IPromiseStore,
        *,
        tracing_adapter: IAdapter | None = None,
        processor_threads: int | None = None,
    ) -> None:
        self._stg_queue = queue.Queue[tuple[Invoke, Promise[Any], Context]]()
        self._completion_queue = queue.Queue[_CQE[Any]]()
        self._function_submission_queue = queue.Queue[_SQE[Any]]()

        self._worker_continue = Event()

        self._deps = Dependencies()
        self._json_encoder = JsonEncoder()
        self._tracing_adapter: IAdapter = (
            tracing_adapter if tracing_adapter is not None else StdOutAdapter()
        )

        self.runnable_coros: RunnableCoroutines = []
        self.awaitables: Awaitables = {}

        self._processor = _Processor(
            processor_threads,
            self._function_submission_queue,
            self._completion_queue,
            self._worker_continue,
        )
        self._durable_promise_storage = durable_promise_storage
        self._emphemeral_promise_memo: EphemeralPromiseMemo = {}

        self._worker_thread = Thread(target=self._run, daemon=True)
        self._worker_thread.start()

    def enqueue_cqe(self, cqe: _CQE[Any]) -> None:
        self._completion_queue.put(cqe)
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
                data=ErrorEncoder.encode(value.err()),
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

            v = Err(ErrorEncoder.decode(durable_promise_record.value.data))
        else:
            assert durable_promise_record.is_resolved()
            if durable_promise_record.value.data is None:
                v = Ok(None)
            else:
                v = Ok(json.loads(durable_promise_record.value.data))
        return v

    def _get_ctx_from_ephemeral_memo(
        self, promise_id: str, *, and_delete: bool
    ) -> Context:
        if and_delete:
            return self._emphemeral_promise_memo.pop(promise_id)[-1]
        return self._emphemeral_promise_memo[promise_id][-1]

    def _create_promise(self, ctx: Context, action: Invoke | Sleep) -> Promise[Any]:
        p = Promise[Any](promise_id=ctx.ctx_id, action=action)
        assert (
            p.promise_id not in self._emphemeral_promise_memo
        ), "There should not be a new promise with same promise id."
        self._emphemeral_promise_memo[p.promise_id] = (p, ctx)
        if isinstance(action, Invoke):
            if isinstance(action.exec_unit, Command):
                raise NotImplementedError
            if isinstance(action.exec_unit, FnOrCoroutine):
                data = {
                    "args": action.exec_unit.args,
                    "kwargs": action.exec_unit.kwargs,
                }
            else:
                assert_never(action.exec_unit)
        elif isinstance(action, Sleep):
            data = {"seconds": action.seconds}
        else:
            assert_never(action)
        durable_promise_record = self._create_durable_promise_record(
            promise_id=p.promise_id, data=data
        )
        self._tracing_adapter.process_event(
            PromiseCreated(
                promise_id=p.promise_id,
                tick=now(),
                parent_promise_id=utils.get_parent_promise_id_from_ctx(ctx),
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

    def run(
        self,
        promise_id: str,
        opts: Options,
        coro: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Promise[T]:
        if promise_id in self._emphemeral_promise_memo:
            return self._emphemeral_promise_memo[promise_id][0]

        top_lvl = Invoke(
            FnOrCoroutine(coro, *args, **kwargs),
            opts=opts,
        )
        root_ctx = Context(
            ctx_id=promise_id, seed=None, parent_ctx=None, deps=self._deps
        )

        p: Promise[Any] = self._create_promise(ctx=root_ctx, action=top_lvl)
        self._stg_queue.put_nowait((top_lvl, p, root_ctx))
        self._signal()
        return p

    def _route_fn_or_coroutine(
        self, ctx: Context, promise: Promise[Any], fn_or_coroutine: FnOrCoroutine
    ) -> None:
        assert (
            not promise.done()
        ), "Only unresolved executions of unresolved promises can be passed here."
        if isgeneratorfunction(fn_or_coroutine.exec_unit):
            coro = fn_or_coroutine.exec_unit(
                ctx, *fn_or_coroutine.args, **fn_or_coroutine.kwargs
            )
            self._add_coro_to_runnables(
                CoroAndPromise(coro, promise, ctx),
                None,
                was_awaited=False,
            )

        else:
            self._processor.enqueue(
                _SQE(
                    (promise, ctx),
                    fn_or_coroutine.exec_unit,
                    ctx,
                    *fn_or_coroutine.args,
                    **fn_or_coroutine.kwargs,
                )
            )

        self._tracing_adapter.process_event(
            ExecutionInvoked(
                promise.promise_id,
                utils.get_parent_promise_id_from_ctx(ctx),
                now(),
                fn_or_coroutine.exec_unit.__name__,
                fn_or_coroutine.args,
                fn_or_coroutine.kwargs,
            )
        )

    def _run(self) -> None:
        while self._worker_continue.wait():
            self._worker_continue.clear()

            top_lvls = utils.dequeue_batch(
                q=self._stg_queue,
                batch_size=self._stg_queue.qsize(),
            )
            for top_lvl, p, root_ctx in top_lvls:
                assert isinstance(top_lvl.exec_unit, FnOrCoroutine)
                if p.done():
                    self._unblock_coros_waiting_on_promise(p)
                else:
                    self._route_fn_or_coroutine(root_ctx, p, top_lvl.exec_unit)

            cqes: list[_CQE[Any]] = utils.dequeue_batch(
                q=self._completion_queue,
                batch_size=self._completion_queue.qsize(),
            )
            for cqe in cqes:
                promise, ctx = cqe.promise_and_context
                self._tracing_adapter.process_event(
                    ExecutionTerminated(
                        promise_id=promise.promise_id,
                        parent_promise_id=utils.get_parent_promise_id_from_ctx(ctx),
                        tick=now(),
                    )
                )
                self._resolve_promise(promise, value=cqe.fn_result)
                self._unblock_coros_waiting_on_promise(promise)

            while self.runnable_coros:
                runnable, was_awaited = self.runnable_coros.pop()
                self._advance_runnable_span(runnable=runnable, was_awaited=was_awaited)

            assert not self.runnable_coros, "Runnables should have been all exhausted"

    def _add_coro_to_awaitables(
        self, p: Promise[Any], coro_and_promise: CoroAndPromise[Any]
    ) -> None:
        assert (
            not p.done()
        ), "If the promise is resolved already it makes no sense to block coroutine"
        self.awaitables.setdefault(p, []).append(coro_and_promise)
        self._tracing_adapter.process_event(
            ExecutionAwaited(
                promise_id=coro_and_promise.prom.promise_id,
                tick=now(),
                parent_promise_id=utils.get_parent_promise_id_from_ctx(
                    coro_and_promise.ctx
                ),
            )
        )

    def _add_coro_to_runnables(
        self,
        coro_and_promise: CoroAndPromise[Any],
        value_to_yield_back: Result[Any, Exception] | None,
        *,
        was_awaited: bool,
    ) -> None:
        self.runnable_coros.append(
            (Runnable(coro_and_promise, value_to_yield_back), was_awaited)
        )

    def _unblock_coros_waiting_on_promise(self, p: Promise[Any]) -> None:
        assert p.done(), "Promise must be done to unblock waiting coroutines."

        if self.awaitables.get(p) is None:
            return

        for coro_and_promise in self.awaitables.pop(p):
            self._add_coro_to_runnables(
                coro_and_promise=coro_and_promise,
                value_to_yield_back=p.safe_result(),
                was_awaited=True,
            )

    def _resolve_promise(
        self, promise: Promise[T], value: Result[T, Exception]
    ) -> None:
        completed_record = self._complete_durable_promise_record(
            promise_id=promise.promise_id,
            value=value,
        )
        assert (
            completed_record.is_completed()
        ), "Durable promise record must be completed by this point."
        v = self._get_value_from_durable_promise(completed_record)
        promise.set_result(v)
        assert (
            promise.promise_id in self._emphemeral_promise_memo
        ), "Ephemeral process must have been registered in the memo."

        self._tracing_adapter.process_event(
            PromiseCompleted(
                promise_id=promise.promise_id,
                tick=now(),
                value=value,
                parent_promise_id=utils.get_parent_promise_id_from_ctx(
                    self._get_ctx_from_ephemeral_memo(
                        promise.promise_id,
                        and_delete=True,
                    )
                ),
            )
        )

    def _advance_runnable_span(
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
                    parent_promise_id=utils.get_parent_promise_id_from_ctx(
                        runnable.coro_and_promise.ctx
                    ),
                )
            )

        if isinstance(yieldable_or_final_value, FinalValue):
            self._tracing_adapter.process_event(
                ExecutionTerminated(
                    promise_id=runnable.coro_and_promise.prom.promise_id,
                    tick=now(),
                    parent_promise_id=utils.get_parent_promise_id_from_ctx(
                        runnable.coro_and_promise.ctx
                    ),
                )
            )
            self._resolve_promise(
                runnable.coro_and_promise.prom, yieldable_or_final_value.v
            )
            self._unblock_coros_waiting_on_promise(runnable.coro_and_promise.prom)

        elif isinstance(yieldable_or_final_value, Call):
            p = self._process_invokation(yieldable_or_final_value.to_invoke(), runnable)
            assert (
                p not in self.awaitables
            ), "Since it's a call it should be a promise without dependants"

            if p.done():
                self._add_coro_to_runnables(
                    runnable.coro_and_promise, p.safe_result(), was_awaited=False
                )
            else:
                self._add_coro_to_awaitables(p, runnable.coro_and_promise)

        elif isinstance(yieldable_or_final_value, Invoke):
            p = self._process_invokation(yieldable_or_final_value, runnable)
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

        elif isinstance(yieldable_or_final_value, Sleep):
            raise NotImplementedError
        else:
            assert_never(yieldable_or_final_value)

    def _process_invokation(
        self, invokation: Invoke, runnable: Runnable[Any]
    ) -> Promise[Any]:
        child_ctx = runnable.coro_and_promise.ctx.new_child()
        p = self._create_promise(child_ctx, invokation)
        if isinstance(invokation.exec_unit, Command):
            raise NotImplementedError
        if isinstance(invokation.exec_unit, FnOrCoroutine):
            if p.done():
                self._unblock_coros_waiting_on_promise(p)
            else:
                self._route_fn_or_coroutine(child_ctx, p, invokation.exec_unit)
        else:
            assert_never(invokation.exec_unit)
        return p
