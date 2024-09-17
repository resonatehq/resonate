from __future__ import annotations

import json
import os
import sys
from inspect import isgenerator, isgeneratorfunction
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

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
from resonate.functools import AsyncFnWrapper, FnWrapper, wrap_fn
from resonate.itertools import FinalValue, iterate_coro
from resonate.promise import Promise
from resonate.queue import Queue
from resonate.result import Err, Ok
from resonate.time import now
from resonate.tracing.stdout import StdOutAdapter

if TYPE_CHECKING:
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
        promise_id: str,
        fn: FnWrapper[T] | AsyncFnWrapper[T],
    ) -> None:
        self.promise_id = promise_id
        self.fn = fn


class _CQE(Generic[T]):
    def __init__(
        self,
        promise_id: str,
        fn_result: Result[T, Exception],
    ) -> None:
        self.fn_result = fn_result
        self.promise_id = promise_id


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
            assert isinstance(fn_result, (Ok, Err))
            self._scheduler.enqueue_cqe(
                promise_id=sqe.promise_id,
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
        self._stg_queue = Queue[tuple[Invoke, Promise[Any], Context]]()
        self._completion_queue = Queue[_CQE[Any]]()
        self._function_submission_queue = Queue[_SQE[Any]]()

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
            self,
        )
        self._durable_promise_storage = durable_promise_storage
        self._emphemeral_promise_memo: EphemeralPromiseMemo = {}

        self._worker_thread = Thread(target=self._run, daemon=True)
        self._worker_thread.start()

    def enqueue_cqe(self, promise_id: str, value: Result[Any, Exception]) -> None:
        self._completion_queue.put(_CQE[Any](promise_id=promise_id, fn_result=value))
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
            raise NotImplementedError
        else:
            assert_never(action)
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
                    promise.promise_id,
                    wrap_fn(
                        ctx,
                        fn_or_coroutine.exec_unit,
                        *fn_or_coroutine.args,
                        **fn_or_coroutine.kwargs,
                    ),
                )
            )

        self._tracing_adapter.process_event(
            ExecutionInvoked(
                promise.promise_id,
                ctx.parent_promise_id(),
                now(),
                fn_or_coroutine.exec_unit.__name__,
                fn_or_coroutine.args,
                fn_or_coroutine.kwargs,
            )
        )

    def _run(self) -> None:
        while self._worker_continue.wait():
            self._worker_continue.clear()

            top_lvls = self._stg_queue.dequeue_batch(self._stg_queue.qsize())
            for top_lvl, p, root_ctx in top_lvls:
                assert isinstance(top_lvl.exec_unit, FnOrCoroutine)
                if p.done():
                    self._unblock_coros_waiting_on_promise(p)
                else:
                    self._route_fn_or_coroutine(root_ctx, p, top_lvl.exec_unit)

            cqes = self._completion_queue.dequeue_batch(self._completion_queue.qsize())
            for cqe in cqes:
                assert (
                    cqe.promise_id in self._emphemeral_promise_memo
                ), "Promise must be recorded in ephemeral storage."

                promise, ctx = self._emphemeral_promise_memo[cqe.promise_id]
                self._tracing_adapter.process_event(
                    ExecutionTerminated(
                        promise_id=promise.promise_id,
                        parent_promise_id=ctx.parent_promise_id(),
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
                parent_promise_id=self._get_ctx_from_ephemeral_memo(
                    promise.promise_id,
                    and_delete=True,
                ).parent_promise_id(),
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
                    parent_promise_id=runnable.coro_and_promise.ctx.parent_promise_id(),
                )
            )

        if isinstance(yieldable_or_final_value, FinalValue):
            self._tracing_adapter.process_event(
                ExecutionTerminated(
                    promise_id=runnable.coro_and_promise.prom.promise_id,
                    tick=now(),
                    parent_promise_id=runnable.coro_and_promise.ctx.parent_promise_id(),
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
        child_ctx = runnable.coro_and_promise.ctx.new_child(
            ctx_id=invokation.opts.promise_id
        )
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
