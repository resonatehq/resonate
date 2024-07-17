from __future__ import annotations

import random
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Callable, Literal, cast

from result import Err, Ok, Result
from typing_extensions import Concatenate, ParamSpec, TypeAlias, TypeVar, assert_never

from resonate.context import Call, Context, Invoke
from resonate.dependency_injection import Dependencies
from resonate.logging import logger
from resonate.typing import CoroAndPromise, Runnable, Yieldable

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
    from collections.abc import Coroutine, Generator


T = TypeVar("T")
P = ParamSpec("P")
Steps: TypeAlias = Literal["callbacks", "runnables"]
Mode: TypeAlias = Literal["concurrent", "sequential"]


class DSTFailureError(Exception):
    def __init__(self) -> None:
        super().__init__()


class DSTScheduler:
    def __init__(  # noqa: PLR0913
        self,
        seed: int,
        mocks: dict[
            Callable[Concatenate[Context, ...], Any | Coroutine[Any, Any, Any]],
            Callable[[], Any],
        ]
        | None = None,
        max_failures: int = 2,
        failure_chance: float = 0,
        mode: Mode = "concurrent",
    ) -> None:
        self._failure_chance = failure_chance
        self._max_failures: int = max_failures
        self.current_failures: int = 0
        self.tick: int = -1
        self._top_level_invocations: list[Invoke] = []
        self._mode: Mode = mode

        self._pending_to_run: PendingToRun = []
        self._waiting_for_prom_resolution: WaitingForPromiseResolution = {}
        self._execution_events: list[str] = []
        self._callbacks_to_run: list[Callable[..., None]] = []

        self.seed = seed
        self.random = random.Random(self.seed)  # noqa: RUF100, S311
        self.deps = Dependencies()
        self.mocks = mocks or {}

    def add(
        self,
        coro: Callable[Concatenate[Context, P], Generator[Yieldable, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self._top_level_invocations.append(Invoke(coro, *args, **kwargs))

    def _reset(self) -> None:
        self._pending_to_run.clear()
        self._waiting_for_prom_resolution.clear()
        self._execution_events.clear()
        self._callbacks_to_run.clear()

    def run(self) -> list[Promise[Any]]:
        while True:
            try:
                return self._run()
            except DSTFailureError:  # noqa: PERF203
                self.current_failures += 1
                self._reset()

    def _initialize_runnables(self) -> list[Promise[Any]]:
        promises: list[Promise[Any]] = []
        for invocation in self._top_level_invocations:
            if not isgeneratorfunction(invocation.fn):
                msg = "Invoking a not generator function."
                raise RuntimeError(msg)

            p = Promise[Any]()
            ctx = Context(dst=True, deps=self.deps)
            self._pending_to_run.append(
                Runnable(
                    coro_and_promise=CoroAndPromise(
                        invocation.fn(ctx, *invocation.args, **invocation.kwargs),
                        p,
                        ctx,
                    ),
                    next_value=None,
                )
            )
            promises.append(p)
        return promises

    def _run(self) -> list[Promise[Any]]:
        promises = self._initialize_runnables()
        assert all(
            not p.done() for p in promises
        ), "No promise should be resolved by now."

        while True:
            self.tick += 1
            if (
                self.current_failures < self._max_failures
                and self.random.uniform(0, 100) < self._failure_chance
            ):
                raise DSTFailureError

            next_step: Steps
            if self._mode == "sequential":
                next_step = "callbacks" if self._callbacks_to_run else "runnables"
            else:
                next_step = self.random.choice(["callbacks", "runnables"])
            if next_step == "callbacks" and self._callbacks_to_run:
                cb = get_random_element(
                    self._callbacks_to_run, r=self.random, mode=self._mode
                )
                cb()

            if next_step == "runnables" and self._pending_to_run:
                runnable = get_random_element(
                    self._pending_to_run, r=self.random, mode=self._mode
                )
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
        child_ctx = runnable.coro_and_promise.ctx.new_child()
        if not isgeneratorfunction(call.fn):
            mock_fn = self.mocks.get(call.fn)
            if mock_fn is not None:
                v = _safe_run(fn=mock_fn)
            else:
                v = cast(
                    Result[Any, Exception],
                    wrap_fn_into_cmd(
                        child_ctx, call.fn, *call.args, **call.kwargs
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
            coro = call.fn(child_ctx, *call.args, **call.kwargs)
            self._pending_to_run.append(
                Runnable(CoroAndPromise(coro, p, child_ctx), next_value=None)
            )

    def _handle_invocation(
        self,
        invocation: Invoke,
        runnable: Runnable[T],
    ) -> None:
        logger.debug("Processing invocation")
        p = Promise[Any]()
        self._pending_to_run.append(Runnable(runnable.coro_and_promise, Ok(p)))
        child_ctx = runnable.coro_and_promise.ctx.new_child()
        if not isgeneratorfunction(invocation.fn):
            mock_fn = self.mocks.get(invocation.fn)
            if mock_fn is not None:
                v = _safe_run(mock_fn)
            else:
                v = cast(
                    Result[Any, Exception],
                    wrap_fn_into_cmd(
                        child_ctx,
                        invocation.fn,
                        *invocation.args,
                        **invocation.kwargs,
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
            coro = invocation.fn(child_ctx, *invocation.args, **invocation.kwargs)
            self._pending_to_run.append(
                Runnable(CoroAndPromise(coro, p, child_ctx), next_value=None)
            )

    def get_events(self) -> list[str]:
        return self._execution_events


def get_random_element(array: list[T], r: random.Random, mode: Mode) -> T:
    if mode == "concurrent":
        r.shuffle(array)
    return array.pop()


def _safe_run(fn: Callable[[], T]) -> Result[T, Exception]:
    result: Result[T, Exception]
    try:
        result = Ok(fn())
    except Exception as e:  # noqa: BLE001
        result = Err(e)
    return result
