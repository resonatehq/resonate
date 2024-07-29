from __future__ import annotations

from collections.abc import Generator
from functools import partial
from inspect import isgeneratorfunction
from queue import Queue
from threading import Event, Lock, Thread
from typing import TYPE_CHECKING, Any, Callable

from typing_extensions import Concatenate, ParamSpec, TypeVar, assert_never

from resonate import utils
from resonate.context import Call, Context, FnOrCoroutine, Invoke
from resonate.dependency_injection import Dependencies
from resonate.itertools import (
    Awaitables,
    FinalValue,
    Runnables,
    callback,
    iterate_coro,
    unblock_depands_coros,
)
from resonate.logging import logger
from resonate.processor import SQE, Processor
from resonate.promise import Promise
from resonate.result import Ok
from resonate.typing import CoroAndPromise, Runnable

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

        self._promise_created = 0

    def signal(self) -> None:
        with self._lock:
            self._w_continue.set()

    def _increate_promise_created(self) -> int:
        self._promise_created += 1
        return self._promise_created

    def add(
        self,
        coro: Callable[Concatenate[Context, P], Generator[Yieldable, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Promise[T]:
        ctx = Context(deps=self.deps, ctx_id=str(self._increate_promise_created()))
        p = Promise[T](
            promise_id=ctx.ctx_id,
            invocation=Invoke(FnOrCoroutine(coro, *args, **kwargs)),
        )
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
        runnables: Runnables = []
        awaitables: Awaitables = {}

        while True:
            n_pending_to_run = len(runnables)
            if n_pending_to_run == 0:
                self._w_continue.wait()
                with self._lock:
                    self._w_continue.clear()

            self._run_cqe_callbacks()

            new_r = self._runnables_from_stg_q()

            if new_r is not None:
                runnables.extend(new_r)

            for _ in range(n_pending_to_run):
                runnable = runnables.pop()
                self._process_each_runnable(
                    runnable=runnable,
                    awaitables=awaitables,
                    runnables=runnables,
                )

    def _process_each_runnable(
        self,
        runnable: Runnable[Any],
        runnables: Runnables,
        awaitables: Awaitables,
    ) -> None:
        yieldable_or_final_value = iterate_coro(runnable)

        if isinstance(yieldable_or_final_value, FinalValue):
            value = yieldable_or_final_value.v
            logger.debug("Processing final value `%s`", value)
            runnable.coro_and_promise.prom.set_result(value)
            unblock_depands_coros(
                p=runnable.coro_and_promise.prom,
                awaitables=awaitables,
                runnables=runnables,
            )

        elif isinstance(yieldable_or_final_value, Call):
            self._handle_call(
                call=yieldable_or_final_value,
                runnable=runnable,
                runnables=runnables,
                awaitables=awaitables,
            )

        elif isinstance(yieldable_or_final_value, Invoke):
            self._handle_invocation(
                invocation=yieldable_or_final_value,
                runnable=runnable,
                runnables=runnables,
                awaitables=awaitables,
            )

        elif isinstance(yieldable_or_final_value, Promise):
            awaitables.setdefault(yieldable_or_final_value, []).append(
                runnable.coro_and_promise,
            )
            if yieldable_or_final_value.done():
                unblock_depands_coros(
                    p=yieldable_or_final_value,
                    awaitables=awaitables,
                    runnables=runnables,
                )

        else:
            assert_never(yieldable_or_final_value)

    def _handle_call(
        self,
        call: Call,
        runnable: Runnable[T],
        awaitables: Awaitables,
        runnables: Runnables,
    ) -> None:
        logger.debug("Processing call")
        child_ctx = runnable.coro_and_promise.ctx.new_child()
        p = Promise[Any](child_ctx.ctx_id, call.to_invoke())
        awaitables[p] = [runnable.coro_and_promise]
        assert isinstance(
            call.exec_unit, FnOrCoroutine
        ), "execution unit must be fn or coroutine at this point."
        if not isgeneratorfunction(call.exec_unit.fn):
            self._processor.enqueue(
                SQE(
                    cmd=utils.wrap_fn_into_cmd(
                        child_ctx,
                        call.exec_unit.fn,
                        *call.exec_unit.args,
                        **call.exec_unit.kwargs,
                    ),
                    callback=partial(
                        callback,
                        p,
                        awaitables,
                        runnables,
                    ),
                )
            )
        else:
            coro = call.exec_unit.fn(
                child_ctx, *call.exec_unit.args, **call.exec_unit.kwargs
            )
            runnables.append(
                Runnable(CoroAndPromise(coro, p, child_ctx), next_value=None)
            )

    def _handle_invocation(
        self,
        invocation: Invoke,
        runnable: Runnable[T],
        runnables: Runnables,
        awaitables: Awaitables,
    ) -> None:
        logger.debug("Processing invocation")
        child_ctx = runnable.coro_and_promise.ctx.new_child()
        p = Promise[Any](child_ctx.ctx_id, invocation)
        runnables.append(Runnable(runnable.coro_and_promise, Ok(p)))
        assert isinstance(
            invocation.exec_unit, FnOrCoroutine
        ), "execution unit must be fn or coroutine at this point."
        if not isgeneratorfunction(invocation.exec_unit.fn):
            self._processor.enqueue(
                SQE(
                    cmd=utils.wrap_fn_into_cmd(
                        child_ctx,
                        invocation.exec_unit.fn,
                        *invocation.exec_unit.args,
                        **invocation.exec_unit.kwargs,
                    ),
                    callback=partial(
                        callback,
                        p,
                        awaitables,
                        runnables,
                    ),
                )
            )
        else:
            coro = invocation.exec_unit.fn(
                child_ctx, *invocation.exec_unit.args, **invocation.exec_unit.kwargs
            )
            runnables.append(
                Runnable(CoroAndPromise(coro, p, child_ctx), next_value=None)
            )

    def _runnables_from_stg_q(self) -> Runnables | None:
        new_runnables: Runnables = []
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
