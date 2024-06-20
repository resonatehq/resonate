from __future__ import annotations

import random
from collections.abc import Generator
from functools import partial
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, cast

from result import Ok, Result
from typing_extensions import ParamSpec, TypeVar, assert_never

from resonate_sdk_py.logging import logger

from .shared import (
    Call,
    CoroAndPromise,
    FinalValue,
    Invoke,
    PendingToRun,
    Promise,
    Runnable,
    WaitingForPromiseResolution,
    Yieldable,
    callback,
    iterate_coro,
    unblock_depands_coros,
    wrap_fn_into_cmd,
)

if TYPE_CHECKING:
    from collections.abc import Generator


T = TypeVar("T")
P = ParamSpec("P")


class DSTScheduler:
    def __init__(self, seed: int | None = None) -> None:
        self._pending_to_run: PendingToRun = []
        if seed is not None:
            random.seed(seed)

    def add(self, coros: list[Generator[Yieldable, Any, T]]) -> list[Promise[T]]:
        promises: list[Promise[T]] = []
        for coro in coros:
            p = self._add(coro=coro)
            promises.append(p)

        self._run()
        return promises

    def _add(
        self,
        coro: Generator[Yieldable, Any, T],
    ) -> Promise[T]:
        p = Promise[T]()
        self._pending_to_run.append(
            Runnable(coro_and_promise=CoroAndPromise(coro, p), next_value=None)
        )
        return p

    def _run(self) -> None:
        waiting_for_prom_resolution: WaitingForPromiseResolution = {}
        n_pending_to_run = len(self._pending_to_run)

        while n_pending_to_run > 0:
            random.shuffle(self._pending_to_run)
            for _ in range(n_pending_to_run):
                runnable = self._pending_to_run.pop()
                self._process_each_runnable(
                    runnable=runnable,
                    waiting_for_promise=waiting_for_prom_resolution,
                    pending_to_run=self._pending_to_run,
                )
            n_pending_to_run = len(self._pending_to_run)

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
        if not isgeneratorfunction(call.fn):
            v = cast(
                Result[Any, Exception],
                wrap_fn_into_cmd(call.fn, *call.args, **call.kwargs).run(),
            )
            partial(callback, p, waiting_for_promise, pending_to_run)

            callback(
                p=p,
                waiting_for_promise=waiting_for_promise,
                pending_to_run=pending_to_run,
                v=v,
            )
        else:
            coro = call.fn(*call.args, **call.kwargs)
            pending_to_run.append(Runnable(CoroAndPromise(coro, p), next_value=None))

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
        if not isgeneratorfunction(invocation.fn):
            v = cast(
                Result[Any, Exception],
                wrap_fn_into_cmd(
                    invocation.fn, *invocation.args, **invocation.kwargs
                ).run(),
            )
            callback(
                p=p,
                waiting_for_promise=waiting_for_promise,
                pending_to_run=pending_to_run,
                v=v,
            )
        else:
            coro = invocation.fn(*invocation.args, **invocation.kwargs)
            pending_to_run.append(Runnable(CoroAndPromise(coro, p), next_value=None))
