from __future__ import annotations

import random
from collections.abc import Generator
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Callable, cast

from result import Ok, Result
from typing_extensions import ParamSpec, TypeVar, assert_never

from resonate.context import Call, Context, Invoke
from resonate.dependency_injection import Dependencies
from resonate.logging import logger
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
    from functools import partial

    from resonate.typing import Yieldable


T = TypeVar("T")
P = ParamSpec("P")


class DSTScheduler:
    def __init__(self, seed: int) -> None:
        self._pending_to_run: PendingToRun = []
        self._waiting_for_prom_resolution: WaitingForPromiseResolution = {}
        self._execution_events: list[str] = []
        self._callbacks_to_run: list[Callable[..., None]] = []
        self.seed = seed
        self.random = random.Random(self.seed)  # noqa: RUF100, S311
        self.deps = Dependencies()

    def _add(
        self,
        coro: partial[Generator[Yieldable, Any, T]],
    ) -> Promise[T]:
        p = Promise[T]()
        ctx = Context(dst=True, deps=self.deps)
        self._pending_to_run.append(
            Runnable(
                coro_and_promise=CoroAndPromise(coro(ctx), p),
                next_value=None,
            )
        )
        return p

    def run(
        self, coros: list[partial[Generator[Yieldable, Any, Any]]]
    ) -> list[Promise[Any]]:
        promises: list[Promise[Any]] = []
        for coro in coros:
            p = self._add(coro)
            promises.append(p)

        while True:
            next_step = self.random.choice(["callbacks", "runnables"])
            if next_step == "callbacks" and self._callbacks_to_run:
                cb = get_random_element(self._callbacks_to_run, r=self.random)
                cb()

            if next_step == "runnables" and self._pending_to_run:
                runnable = get_random_element(self._pending_to_run, r=self.random)
                self._process_each_runnable(runnable=runnable)

            if not self._callbacks_to_run and not self._pending_to_run:
                break
        assert all(p.done() for p in promises), "All promises should be resolved."
        return promises

    def _process_each_runnable(
        self,
        runnable: Runnable[Any],
    ) -> None:
        yieldable_or_final_value = iterate_coro(runnable)

        if isinstance(yieldable_or_final_value, FinalValue):
            value = yieldable_or_final_value.v
            logger.debug("Processing final value `%s`", value)
            runnable.coro_and_promise.prom.set_result(value)
            unblock_depands_coros(
                p=runnable.coro_and_promise.prom,
                waiting=self._waiting_for_prom_resolution,
                runnables=self._pending_to_run,
            )

            self._execution_events.append(f"Promise resolved with value {value}")

        elif isinstance(yieldable_or_final_value, Call):
            self._handle_call(
                call=yieldable_or_final_value,
                runnable=runnable,
            )
            self._execution_events.append(
                f"Call {yieldable_or_final_value.fn.__name__} with params args={yieldable_or_final_value.args} kwargs={yieldable_or_final_value.kwargs} handled"  # noqa: E501
            )

        elif isinstance(yieldable_or_final_value, Invoke):
            self._handle_invocation(
                invocation=yieldable_or_final_value,
                runnable=runnable,
            )
            self._execution_events.append(
                f"Invocation {yieldable_or_final_value.fn.__name__} with params args={yieldable_or_final_value.args} kwargs={yieldable_or_final_value.kwargs} handled"  # noqa: E501
            )

        elif isinstance(yieldable_or_final_value, Promise):
            self._waiting_for_prom_resolution.setdefault(
                yieldable_or_final_value, []
            ).append(
                runnable.coro_and_promise,
            )
            if yieldable_or_final_value.done():
                unblock_depands_coros(
                    p=yieldable_or_final_value,
                    waiting=self._waiting_for_prom_resolution,
                    runnables=self._pending_to_run,
                )

        else:
            assert_never(yieldable_or_final_value)

    def _handle_call(
        self,
        call: Call,
        runnable: Runnable[T],
    ) -> None:
        logger.debug("Processing call")
        p = Promise[Any]()
        self._waiting_for_prom_resolution[p] = [runnable.coro_and_promise]
        if not isgeneratorfunction(call.fn):
            v = cast(
                Result[Any, Exception],
                wrap_fn_into_cmd(call.ctx, call.fn, *call.args, **call.kwargs).run(),
            )

            self._callbacks_to_run.append(
                lambda: callback(
                    p=p,
                    waiting_for_promise=self._waiting_for_prom_resolution,
                    pending_to_run=self._pending_to_run,
                    v=v,
                )
            )
        else:
            coro = call.fn(call.ctx, *call.args, **call.kwargs)
            self._pending_to_run.append(
                Runnable(CoroAndPromise(coro, p), next_value=None)
            )

    def _handle_invocation(
        self,
        invocation: Invoke,
        runnable: Runnable[T],
    ) -> None:
        logger.debug("Processing invocation")
        p = Promise[Any]()
        self._pending_to_run.append(Runnable(runnable.coro_and_promise, Ok(p)))
        if not isgeneratorfunction(invocation.fn):
            v = cast(
                Result[Any, Exception],
                wrap_fn_into_cmd(
                    invocation.ctx, invocation.fn, *invocation.args, **invocation.kwargs
                ).run(),
            )
            self._callbacks_to_run.append(
                lambda: callback(
                    p=p,
                    waiting_for_promise=self._waiting_for_prom_resolution,
                    pending_to_run=self._pending_to_run,
                    v=v,
                )
            )

        else:
            coro = invocation.fn(invocation.ctx, *invocation.args, **invocation.kwargs)
            self._pending_to_run.append(
                Runnable(CoroAndPromise(coro, p), next_value=None)
            )

    def get_events(self) -> list[str]:
        return self._execution_events


def get_random_element(array: list[T], r: random.Random) -> T:
    return array.pop(r.randrange(len(array)))
