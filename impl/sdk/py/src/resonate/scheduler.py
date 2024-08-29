from __future__ import annotations

import asyncio
import heapq
import os
import queue
import time
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
from resonate.time import now

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from resonate.result import Result
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
        self.promise = promise


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
        self._sq = sq
        self._cq = cq
        self._continue_event = continue_event
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


class _SleepE(Generic[T]):
    def __init__(self, promise: Promise[T], sleep_time: int) -> None:
        self.promise = promise
        self.sleep_time = sleep_time


class _Metronome:
    def __init__(
        self,
        sq: queue.Queue[_SleepE[None]],
        cq: queue.Queue[_CQE[Any]],
        continue_event: Event | None,
    ) -> None:
        self._sq = sq
        self._cq = cq
        self._continue_event = continue_event

        self._sleeping: list[tuple[int, int, Promise[None]]] = []

        self._worker_continue = Event()

        self._worker_thread = Thread(target=self._run, daemon=True)
        self._worker_thread.start()

    def _signal(self) -> None:
        self._worker_continue.set()

    def enqueue(self, sleep_e: _SleepE[None]) -> None:
        self._sq.put_nowait(sleep_e)
        self._signal()

    def _run(self) -> None:
        while self._worker_continue.wait():
            self._worker_continue.clear()

            current_time = now()
            sleep_e = utils.dequeue_batch(self._sq, batch_size=self._sq.qsize())
            for idx, e in enumerate(sleep_e):
                heapq.heappush(
                    self._sleeping, (current_time + e.sleep_time, idx, e.promise)
                )

            while self._sleeping and self._sleeping[0][0] <= current_time:
                se = heapq.heappop(self._sleeping)
                self._cq.put_nowait(_CQE(se[-1], Ok(None)))
                if self._continue_event:
                    self._continue_event.set()

            time_to_sleep = (
                (self._sleeping[0][0] - current_time) if self._sleeping else 0
            )
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)
                self._signal()


class Scheduler:
    def __init__(self, processor_threads: int | None = None) -> None:
        self._stg_queue = queue.Queue[tuple[Invoke, Promise[Any]]]()
        self._completion_queue = queue.Queue[_CQE[Any]]()
        self._sleep_submission_queue = queue.Queue[_SleepE[None]]()
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
        self._metronome = _Metronome(
            self._sleep_submission_queue,
            self._completion_queue,
            self._worker_continue,
        )

        self._worker_thread = Thread(target=self._run, daemon=True)
        self._worker_thread.start()

    def enqueue_cqe(self, cqe: _CQE[Any]) -> None:
        self._completion_queue.put(cqe)
        self._signal()

    def _signal(self) -> None:
        self._worker_continue.set()

    def run(
        self,
        promise_id: str,
        coro: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Promise[T]:
        top_lvl = Invoke(FnOrCoroutine(coro, *args, **kwargs))
        p = Promise[T](promise_id, top_lvl)
        self._stg_queue.put_nowait((top_lvl, p))
        self._signal()
        return p

    def _route_fn_or_coroutine(
        self, ctx: Context, promise: Promise[Any], fn_or_coroutine: FnOrCoroutine
    ) -> None:
        if isgeneratorfunction(fn_or_coroutine.exec_unit):
            coro = fn_or_coroutine.exec_unit(
                ctx, *fn_or_coroutine.args, **fn_or_coroutine.kwargs
            )
            self._add_coro_to_runnables(CoroAndPromise(coro, promise, ctx), None)
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
                self._stg_queue,
                batch_size=self._stg_queue.qsize(),
            )
            for top_lvl, p in top_lvls:
                parent_ctx = Context(
                    ctx_id=p.promise_id, seed=None, parent_ctx=None, deps=self._deps
                )
                assert isinstance(top_lvl.exec_unit, FnOrCoroutine)
                self._route_fn_or_coroutine(parent_ctx, p, top_lvl.exec_unit)

            cqes = utils.dequeue_batch(
                q=self._completion_queue,
                batch_size=self._completion_queue.qsize(),
            )
            for cqe in cqes:
                cqe.promise.set_result(cqe.fn_result)
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
            self._add_coro_to_runnables(coro_and_promise, p.safe_result())

    def _advance_runnable_span(self, runnable: Runnable[Any]) -> None:
        assert isgenerator(
            runnable.coro_and_promise.coro
        ), "Only coroutines can be advanced"

        yieldable_or_final_value = iterate_coro(runnable=runnable)

        if isinstance(yieldable_or_final_value, FinalValue):
            v = yieldable_or_final_value.v
            runnable.coro_and_promise.prom.set_result(v)
            self._unblock_coros_waiting_on_promise(runnable.coro_and_promise.prom)

        elif isinstance(yieldable_or_final_value, Call):
            called = yieldable_or_final_value
            p = self._process_invokation(called.to_invoke(), runnable)
            if not p.done():
                self._add_coro_to_awaitables(p, runnable.coro_and_promise)
            else:
                self._add_coro_to_runnables(runnable.coro_and_promise, p.safe_result())

        elif isinstance(yieldable_or_final_value, Invoke):
            invokation = yieldable_or_final_value
            p = self._process_invokation(invokation, runnable)
            self._add_coro_to_runnables(runnable.coro_and_promise, Ok(p))

        elif isinstance(yieldable_or_final_value, Promise):
            p = yieldable_or_final_value
            if p.done():
                self._add_coro_to_runnables(runnable.coro_and_promise, p.safe_result())
                self._unblock_coros_waiting_on_promise(p)
            else:
                self._add_coro_to_awaitables(p, runnable.coro_and_promise)

        elif isinstance(yieldable_or_final_value, Sleep):
            p = Promise[None](
                promise_id=runnable.coro_and_promise.ctx.new_child().ctx_id,
                action=yieldable_or_final_value,
            )
            self._metronome.enqueue(
                sleep_e=_SleepE(p, sleep_time=yieldable_or_final_value.seconds)
            )
            self._add_coro_to_awaitables(p, runnable.coro_and_promise)
        else:
            assert_never(yieldable_or_final_value)

    def _process_invokation(
        self, invokation: Invoke, runnable: Runnable[Any]
    ) -> Promise[Any]:
        parent_ctx = runnable.coro_and_promise.ctx
        child_ctx = parent_ctx.new_child()
        p = Promise[Any](child_ctx.ctx_id, invokation)
        if isinstance(invokation.exec_unit, Command):
            raise NotImplementedError
        if isinstance(invokation.exec_unit, FnOrCoroutine):
            self._route_fn_or_coroutine(child_ctx, p, invokation.exec_unit)
        else:
            assert_never(invokation.exec_unit)
        return p
