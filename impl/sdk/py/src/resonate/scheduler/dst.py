from __future__ import annotations

import os
import random
import shutil
from collections import deque
from inspect import isgeneratorfunction
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Literal, cast

from result import Err, Ok, Result
from typing_extensions import Concatenate, ParamSpec, TypeAlias, TypeVar, assert_never

from resonate.context import Call, Context, Invoke
from resonate.dependency_injection import Dependencies
from resonate.logging import logger
from resonate.scheduler.events import (
    AwaitedForPromise,
    ExecutionStarted,
    PromiseCreated,
    PromiseResolved,
    SchedulerEvents,
)
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


class _TopLevelInvoke:
    def __init__(
        self,
        fn: Callable[Concatenate[Context, P], Generator[Yieldable, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def to_invocation(self) -> Invoke:
        return Invoke(self.fn, *self.args, **self.kwargs)


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
        log_file: str | None = None,
        max_failures: int = 2,
        failure_chance: float = 0,
        mode: Mode = "concurrent",
    ) -> None:
        self._failure_chance = failure_chance
        self._max_failures: int = max_failures
        self.current_failures: int = 0
        self.tick: int = 0
        self._top_level_invocations: deque[_TopLevelInvoke] = deque()
        self._mode: Mode = mode

        self._pending_to_run: PendingToRun = []
        self._waiting_for_prom_resolution: WaitingForPromiseResolution = {}
        self._callbacks_to_run: list[Callable[..., None]] = []

        self.seed = seed
        self.random = random.Random(self.seed)  # noqa: RUF100, S311
        self.deps = Dependencies()
        self.mocks = mocks or {}

        self._events: list[SchedulerEvents] = []
        self._promise_created: int = 0
        self._log_file = log_file

    def add(
        self,
        coro: Callable[Concatenate[Context, P], Generator[Yieldable, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self._top_level_invocations.appendleft(_TopLevelInvoke(coro, *args, **kwargs))

    def _reset(self) -> None:
        self._pending_to_run.clear()
        self._waiting_for_prom_resolution.clear()
        self._events.clear()
        self._callbacks_to_run.clear()

    def _write_logs(self) -> None:
        if self._log_file is None:
            return
        logs_folder = Path().cwd() / self._log_file
        if not logs_folder.exists() and not logs_folder.is_file():
            logs_folder.mkdir(exist_ok=False)

        for content in os.listdir(logs_folder):
            content_path = logs_folder / content
            if content_path.is_file():
                content_path.unlink()
            else:
                shutil.rmtree(content_path)

        all_logs: str = "".join(f"{event}\n" for event in self._events)
        with (logs_folder / f"{self.seed}.txt").open(mode="w") as file:
            file.write(all_logs)

    def run(self) -> list[Promise[Any]]:
        while True:
            try:
                return self._run()
            except DSTFailureError:  # noqa: PERF203
                self.current_failures += 1
                self._reset()

    def _initialize_runnables(self) -> list[Promise[Any]]:
        init_promises: list[Promise[Any] | None] = [
            None for _ in range(len(self._top_level_invocations))
        ]
        for idx, top_level_invocation in enumerate(self._top_level_invocations):
            p = Promise[Any](
                promise_id=self._increate_promise_created(),
                invocation=top_level_invocation.to_invocation(),
            )

            self._events.append(
                PromiseCreated(
                    promise_id=p.promise_id,
                    tick=self.tick,
                    fn_name=top_level_invocation.fn.__name__,
                    args=top_level_invocation.args,
                    kwargs=top_level_invocation.kwargs,
                )
            )
            ctx = Context(dst=True, deps=self.deps)
            self._pending_to_run.append(
                Runnable(
                    coro_and_promise=CoroAndPromise(
                        top_level_invocation.fn(
                            ctx,
                            *top_level_invocation.args,
                            **top_level_invocation.kwargs,
                        ),
                        p,
                        ctx,
                    ),
                    next_value=None,
                )
            )
            init_promises[-idx - 1] = p

        promises: list[Promise[Any]] = []
        for p in init_promises:
            assert p is not None, "There should only be promises here"
            promises.append(p)

        return promises

    def _next_step(self) -> Steps:
        next_step: Steps
        if self._mode == "sequential":
            next_step = "callbacks" if self._callbacks_to_run else "runnables"
        elif not self._callbacks_to_run:
            next_step = "runnables"
            assert self._pending_to_run, "There should something in callbacks"

        elif not self._pending_to_run:
            next_step = "callbacks"
            assert self._callbacks_to_run, "There should something in callbacks"

        else:
            next_step = self.random.choice(["callbacks", "runnables"])

        return next_step

    def _run(self) -> list[Promise[Any]]:
        promises = self._initialize_runnables()
        assert all(
            not p.done() for p in promises
        ), "No promise should be resolved by now."

        while True:
            if not self._callbacks_to_run and not self._pending_to_run:
                break
            self.tick += 1

            if (
                self.current_failures < self._max_failures
                and self.random.uniform(0, 100) < self._failure_chance
            ):
                raise DSTFailureError

            next_step = self._next_step()
            if next_step == "callbacks":
                cb = get_random_element(
                    self._callbacks_to_run, r=self.random, mode=self._mode
                )
                cb()

            if next_step == "runnables":
                runnable = get_random_element(
                    self._pending_to_run, r=self.random, mode=self._mode
                )
                self._process_each_runnable(runnable=runnable)

        assert all(p.done() for p in promises), "All promises should be resolved."
        self._write_logs()
        return promises

    def _process_each_runnable(
        self,
        runnable: Runnable[Any],
    ) -> None:
        if runnable.next_value is None:
            self._events.append(
                ExecutionStarted(
                    promise_id=runnable.coro_and_promise.prom.promise_id,
                    tick=self.tick,
                    fn_name=runnable.coro_and_promise.prom.invocation.fn.__name__,
                    args=runnable.coro_and_promise.prom.invocation.args,
                    kwargs=runnable.coro_and_promise.prom.invocation.kwargs,
                )
            )
        yieldable_or_final_value = iterate_coro(runnable)

        if isinstance(yieldable_or_final_value, FinalValue):
            value = yieldable_or_final_value.v
            logger.debug("Processing final value `%s`", value)
            runnable.coro_and_promise.prom.set_result(value)
            self._events.append(
                PromiseResolved(runnable.coro_and_promise.prom.promise_id, self.tick)
            )
            unblock_depands_coros(
                p=runnable.coro_and_promise.prom,
                waiting=self._waiting_for_prom_resolution,
                runnables=self._pending_to_run,
            )

        elif isinstance(yieldable_or_final_value, Call):
            self._handle_call(
                call=yieldable_or_final_value,
                runnable=runnable,
            )

        elif isinstance(yieldable_or_final_value, Invoke):
            self._handle_invocation(
                invocation=yieldable_or_final_value,
                runnable=runnable,
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
        p = Promise[Any](
            promise_id=self._increate_promise_created(), invocation=call.to_invoke()
        )

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
            self._events.append(
                AwaitedForPromise(promise_id=p.promise_id, tick=self.tick)
            )
        else:
            coro = call.fn(child_ctx, *call.args, **call.kwargs)
            self._pending_to_run.append(
                Runnable(CoroAndPromise(coro, p, child_ctx), next_value=None)
            )

    def _increate_promise_created(self) -> int:
        self._promise_created += 1
        return self._promise_created

    def _handle_invocation(
        self,
        invocation: Invoke,
        runnable: Runnable[T],
    ) -> None:
        logger.debug("Processing invocation")
        p = Promise[Any](
            promise_id=self._increate_promise_created(), invocation=invocation
        )
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

    def get_events(self) -> list[SchedulerEvents]:
        return self._events


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
