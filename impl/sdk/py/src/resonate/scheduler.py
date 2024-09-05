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
from resonate.itertools import FinalValue, iterate_coro
from resonate.promise import Promise
from resonate.result import Err, Ok

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from resonate.options import Options
    from resonate.record import DurablePromiseRecord
    from resonate.result import Result
    from resonate.storage import IPromiseStore
    from resonate.typing import (
        Awaitables,
        DurableCoro,
        DurableFn,
        RunnableCoroutines,
    )

T = TypeVar("T")
P = ParamSpec("P")


class _SQE(Generic[T]):
    def __init__(
        self,
        promise: Promise[T],
        fn: Callable[P, T | Coroutine[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.promise = promise
        self.fn = fn
        self.args = args
        self.kwargs = kwargs


class _CQE(Generic[T]):
    def __init__(self, promise: Promise[T], fn_result: Result[T, Exception]) -> None:
        self.fn_result = fn_result
        self.promise: Promise[T] = promise


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

            self._cq.put_nowait(_CQE(sqe.promise, fn_result))
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
        processor_threads: int | None = None,
    ) -> None:
        self._stg_queue = queue.Queue[tuple[Invoke, Promise[Any]]]()
        self._completion_queue = queue.Queue[_CQE[Any]]()
        self._function_submission_queue = queue.Queue[_SQE[Any]]()

        self._worker_continue = Event()

        self._deps = Dependencies()

        self.runnable_coros: RunnableCoroutines = []
        self.awaitables: Awaitables = {}

        self._processor = _Processor(
            processor_threads,
            self._function_submission_queue,
            self._completion_queue,
            self._worker_continue,
        )
        self._durable_promise_storage = durable_promise_storage

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
                data=json.dumps(value.unwrap()),
            )
        if isinstance(value, Err):
            return self._durable_promise_storage.reject(
                promise_id=promise_id,
                ikey=utils.string_to_ikey(promise_id),
                strict=False,
                headers=None,
                data=json.dumps(str(value.err())),
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
            data=json.dumps(data),
            timeout=sys.maxsize,
            tags=None,
        )

    def _create_promise(self, promise_id: str, action: Invoke | Sleep) -> Promise[Any]:
        p = Promise[Any](promise_id=promise_id, action=action)
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
        if durable_promise_record.is_pending():
            return p
        v: Result[Any, Exception]
        if durable_promise_record.is_rejected():
            v = Err(Exception(durable_promise_record.value.data))
        else:
            assert durable_promise_record.is_resolved()
            if durable_promise_record.value.data is None:
                v = Ok(None)
            else:
                v = Ok(json.loads(durable_promise_record.value.data))

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
        top_lvl = Invoke(
            FnOrCoroutine(coro, *args, **kwargs),
            opts=opts,
        )
        p: Promise[Any] = self._create_promise(promise_id=promise_id, action=top_lvl)
        self._stg_queue.put_nowait((top_lvl, p))
        self._signal()
        return p

    def _route_fn_or_coroutine(
        self, ctx: Context, promise: Promise[Any], fn_or_coroutine: FnOrCoroutine
    ) -> None:
        if promise.done():
            self._unblock_coros_waiting_on_promise(promise)

        elif isgeneratorfunction(fn_or_coroutine.exec_unit):
            coro = fn_or_coroutine.exec_unit(
                ctx, *fn_or_coroutine.args, **fn_or_coroutine.kwargs
            )
            self._add_coro_to_runnables(
                CoroAndPromise(coro, promise, ctx),
                None,
            )

        else:
            self._processor.enqueue(
                _SQE(
                    promise,
                    fn_or_coroutine.exec_unit,
                    ctx,
                    *fn_or_coroutine.args,
                    **fn_or_coroutine.kwargs,
                )
            )

    def _run(self) -> None:
        while self._worker_continue.wait():
            self._worker_continue.clear()

            top_lvls: list[tuple[Invoke, Promise[Any]]] = utils.dequeue_batch(
                q=self._stg_queue,
                batch_size=self._stg_queue.qsize(),
            )
            for top_lvl, p in top_lvls:
                parent_ctx = Context(
                    ctx_id=p.promise_id, seed=None, parent_ctx=None, deps=self._deps
                )
                assert isinstance(top_lvl.exec_unit, FnOrCoroutine)
                self._route_fn_or_coroutine(parent_ctx, p, top_lvl.exec_unit)

            cqes: list[_CQE[Any]] = utils.dequeue_batch(
                q=self._completion_queue,
                batch_size=self._completion_queue.qsize(),
            )
            for cqe in cqes:
                self._resolve_promise(cqe.promise, value=cqe.fn_result)
                self._unblock_coros_waiting_on_promise(cqe.promise)

            while self.runnable_coros:
                self._advance_runnable_span(self.runnable_coros.pop())

            assert not self.runnable_coros, "Runnables should have been all exhausted"

    def _add_coro_to_awaitables(
        self, p: Promise[Any], coro_and_promise: CoroAndPromise[Any]
    ) -> None:
        assert (
            not p.done()
        ), "If the promise is resolved already it makes no sense to block coroutine"
        self.awaitables.setdefault(p, []).append(coro_and_promise)

    def _add_coro_to_runnables(
        self,
        coro_and_promise: CoroAndPromise[Any],
        value_to_yield_back: Result[Any, Exception] | None,
    ) -> None:
        self.runnable_coros.append(Runnable(coro_and_promise, value_to_yield_back))

    def _unblock_coros_waiting_on_promise(self, p: Promise[Any]) -> None:
        assert p.done(), "Promise must be done to unblock waiting coroutines."

        if self.awaitables.get(p) is None:
            return

        for coro_and_promise in self.awaitables.pop(p):
            self._add_coro_to_runnables(
                coro_and_promise=coro_and_promise,
                value_to_yield_back=p.safe_result(),
            )

    def _resolve_promise(
        self, promise: Promise[T], value: Result[T, Exception]
    ) -> None:
        promise.set_result(value)
        completed_record = self._complete_durable_promise_record(
            promise_id=promise.promise_id,
            value=value,
        )
        assert (
            completed_record.is_completed()
        ), "Durable promise record must be completed by this point."

    def _advance_runnable_span(self, runnable: Runnable[Any]) -> None:
        assert isgenerator(
            runnable.coro_and_promise.coro
        ), "Only coroutines can be advanced"

        yieldable_or_final_value = iterate_coro(runnable=runnable)

        if isinstance(yieldable_or_final_value, FinalValue):
            self._resolve_promise(
                runnable.coro_and_promise.prom, yieldable_or_final_value.v
            )
            self._unblock_coros_waiting_on_promise(runnable.coro_and_promise.prom)

        elif isinstance(yieldable_or_final_value, Call):
            p = self._process_invokation(yieldable_or_final_value.to_invoke(), runnable)
            if p.done():
                self._add_coro_to_runnables(runnable.coro_and_promise, p.safe_result())
            else:
                self._add_coro_to_awaitables(p, runnable.coro_and_promise)

        elif isinstance(yieldable_or_final_value, Invoke):
            p = self._process_invokation(yieldable_or_final_value, runnable)
            self._add_coro_to_runnables(runnable.coro_and_promise, Ok(p))

        elif isinstance(yieldable_or_final_value, Promise):
            p = yieldable_or_final_value
            if p.done():
                self._unblock_coros_waiting_on_promise(p)
                self._add_coro_to_runnables(runnable.coro_and_promise, p.safe_result())
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
        p = self._create_promise(child_ctx.ctx_id, invokation)
        if isinstance(invokation.exec_unit, Command):
            raise NotImplementedError
        if isinstance(invokation.exec_unit, FnOrCoroutine):
            self._route_fn_or_coroutine(child_ctx, p, invokation.exec_unit)
        else:
            assert_never(invokation.exec_unit)
        return p
