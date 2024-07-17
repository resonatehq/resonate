from __future__ import annotations

from collections.abc import Generator
from functools import partial
from inspect import isgeneratorfunction
from queue import Queue
from threading import Event, Lock, Thread
from typing import TYPE_CHECKING, Any, Callable

from result import Ok
from typing_extensions import Concatenate, ParamSpec, TypeVar, assert_never

from resonate import utils
from resonate.context import Call, Context, Invoke
from resonate.dependency_injection import Dependencies
from resonate.logging import logger
from resonate.processor import SQE, Processor
from resonate.typing import CoroAndPromise, Runnable

from .itertools import (
    FinalValue,
    PendingToRun,
    WaitingForPromiseResolution,
    callback,
    iterate_coro,
    unblock_depands_coros,
)
from .shared import (
    Promise,
    wrap_fn_into_cmd,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.typing import Yieldable


T = TypeVar("T")
P = ParamSpec("P")


class Scheduler:
    def __init__(self, batch_size: int = 5, max_wokers: int | None = None) -> None:
        self._stg_q = Queue[CoroAndPromise[Any]]()
        self._w_thread: Thread | None = None
        self._w_continue = Event()
        self._lock = Lock()

        self._processor = Processor(
            max_workers=max_wokers,
            scheduler=self,
        )
        self._batch_size = batch_size
        self.deps = Dependencies()

    def signal(self) -> None:
        with self._lock:
            self._w_continue.set()

    def add(
        self,
        coro: Callable[Concatenate[Context, P], Generator[Yieldable, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Promise[T]:
        p = Promise[T]()
        ctx = Context(deps=self.deps)
        self._stg_q.put(item=CoroAndPromise(coro(ctx, *args, **kwargs), p, ctx))
        self.signal()

        return p

    def run(self) -> None:
        if self._w_thread is None:
            self._run_worker()

    def _run_worker(self) -> None:
        assert self._w_thread is None, "Worker thread already exists"
        if self._w_thread is None:
            t = Thread(
                target=self._run,
                daemon=True,
            )
            t.start()

            self._w_thread = t

    def _run(self) -> None:
        pending_to_run: PendingToRun = []
        waiting_for_prom_resolution: WaitingForPromiseResolution = {}

        while True:
            n_pending_to_run = len(pending_to_run)
            if n_pending_to_run == 0:
                self._w_continue.wait()
                with self._lock:
                    self._w_continue.clear()

            self._run_cqe_callbacks()

            new_r = self._runnables_from_stg_q()

            if new_r is not None:
                pending_to_run.extend(new_r)

            for _ in range(n_pending_to_run):
                runnable = pending_to_run.pop()
                self._process_each_runnable(
                    runnable=runnable,
                    waiting_for_promise=waiting_for_prom_resolution,
                    pending_to_run=pending_to_run,
                )

    def _process_each_runnable(
        self,
        runnable: Runnable[Any],
        pending_to_run: PendingToRun,
        waiting_for_promise: WaitingForPromiseResolution,
    ) -> None:
        yieldable_or_final_value = iterate_coro(runnable)

        if isinstance(yieldable_or_final_value, FinalValue):
            value = yieldable_or_final_value.v
            logger.debug("Processing final value `%s`", value)
            runnable.coro_and_promise.prom.set_result(value)
            unblock_depands_coros(
                p=runnable.coro_and_promise.prom,
                waiting=waiting_for_promise,
                runnables=pending_to_run,
            )

        elif isinstance(yieldable_or_final_value, Call):
            self._handle_call(
                call=yieldable_or_final_value,
                runnable=runnable,
                pending_to_run=pending_to_run,
                waiting_for_promise=waiting_for_promise,
            )

        elif isinstance(yieldable_or_final_value, Invoke):
            self._handle_invocation(
                invocation=yieldable_or_final_value,
                runnable=runnable,
                pending_to_run=pending_to_run,
                waiting_for_promise=waiting_for_promise,
            )

        elif isinstance(yieldable_or_final_value, Promise):
            waiting_for_promise.setdefault(yieldable_or_final_value, []).append(
                runnable.coro_and_promise,
            )
            if yieldable_or_final_value.done():
                unblock_depands_coros(
                    p=yieldable_or_final_value,
                    waiting=waiting_for_promise,
                    runnables=pending_to_run,
                )

        else:
            assert_never(yieldable_or_final_value)

    def _handle_call(
        self,
        call: Call,
        runnable: Runnable[T],
        waiting_for_promise: WaitingForPromiseResolution,
        pending_to_run: PendingToRun,
    ) -> None:
        logger.debug("Processing call")
        p = Promise[Any]()
        waiting_for_promise[p] = [runnable.coro_and_promise]
        child_ctx = runnable.coro_and_promise.ctx.new_child()
        if not isgeneratorfunction(call.fn):
            self._processor.enqueue(
                SQE(
                    cmd=wrap_fn_into_cmd(child_ctx, call.fn, *call.args, **call.kwargs),
                    callback=partial(
                        callback,
                        p,
                        waiting_for_promise,
                        pending_to_run,
                    ),
                )
            )
        else:
            coro = call.fn(child_ctx, *call.args, **call.kwargs)
            pending_to_run.append(
                Runnable(CoroAndPromise(coro, p, child_ctx), next_value=None)
            )

    def _handle_invocation(
        self,
        invocation: Invoke,
        runnable: Runnable[T],
        pending_to_run: PendingToRun,
        waiting_for_promise: WaitingForPromiseResolution,
    ) -> None:
        logger.debug("Processing invocation")
        p = Promise[Any]()
        pending_to_run.append(Runnable(runnable.coro_and_promise, Ok(p)))
        child_ctx = runnable.coro_and_promise.ctx.new_child()
        if not isgeneratorfunction(invocation.fn):
            self._processor.enqueue(
                SQE(
                    cmd=wrap_fn_into_cmd(
                        child_ctx,
                        invocation.fn,
                        *invocation.args,
                        **invocation.kwargs,
                    ),
                    callback=partial(
                        callback,
                        p,
                        waiting_for_promise,
                        pending_to_run,
                    ),
                )
            )
        else:
            coro = invocation.fn(child_ctx, *invocation.args, **invocation.kwargs)
            pending_to_run.append(
                Runnable(CoroAndPromise(coro, p, child_ctx), next_value=None)
            )

    def _runnables_from_stg_q(self) -> PendingToRun | None:
        new_runnables: PendingToRun = []
        stg_qes = utils.dequeue_batch(q=self._stg_q, batch_size=self._batch_size)
        if stg_qes is None:
            return None

        logger.debug("%s elements popped from the staging queue", len(stg_qes))
        for coro_and_prom in stg_qes:
            new_runnables.append(
                Runnable(coro_and_promise=coro_and_prom, next_value=None)
            )

        return new_runnables

    def _run_cqe_callbacks(self) -> None:
        cqes = self._processor.dequeue_batch(batch_size=self._batch_size)
        if cqes is None:
            return
        logger.debug("%s elements popped from the completion queue", len(cqes))
        for cqe in cqes:
            cqe.callback(cqe.cmd_result)
