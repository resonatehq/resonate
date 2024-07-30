from __future__ import annotations

import random
from collections import deque
from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Callable, Generic, Literal, cast

from typing_extensions import Concatenate, ParamSpec, TypeAlias, TypeVar, assert_never

from resonate import utils
from resonate.contants import CWD
from resonate.context import Call, Command, Context, FnOrCoroutine, Invoke
from resonate.dependency_injection import Dependencies
from resonate.events import (
    AwaitedForPromise,
    ExecutionStarted,
    PromiseCreated,
    PromiseResolved,
    SchedulerEvents,
)
from resonate.itertools import (
    Awaitables,
    FinalValue,
    Runnables,
    callback,
    iterate_coro,
    unblock_depands_coros,
)
from resonate.logging import logger
from resonate.promise import Promise
from resonate.result import Err, Ok, Result
from resonate.typing import CoroAndPromise, Runnable, Yieldable

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator


T = TypeVar("T")
Cmd = TypeVar("Cmd", bound=Command)
P = ParamSpec("P")

Step: TypeAlias = Literal["callbacks", "runnables"]
Mode: TypeAlias = Literal["concurrent", "sequential"]


class _TopLevelInvoke:
    def __init__(
        self,
        fn: Callable[Concatenate[Context, P], Generator[Yieldable, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def to_invocation(self) -> Invoke:
        return Invoke(FnOrCoroutine(self.fn, *self.args, **self.kwargs))


class DSTFailureError(Exception):
    def __init__(self) -> None:
        super().__init__()


class MaxCapacityReachedError(Exception):
    def __init__(self) -> None:
        super().__init__("Max capacity has been reached")


@dataclass
class _ListWithLenght(Generic[T]):
    max_length: int
    elements: list[T] = field(init=False, default_factory=list)

    def append(self, e: T, /) -> None:
        self.elements.append(e)

    def length(self) -> int:
        return len(self.elements)

    def pop_all(self) -> list[T]:
        all_elements: list[T] = []
        while self.elements:
            all_elements.append(self.elements.pop())
        return all_elements


class DSTScheduler:
    """
    The DSTScheduler class manages coroutines in a deterministic way, allowing for
    controlled execution with reproducibility. It can handle errors gracefully by
    resetting its state and retrying up to a specified number of times.
    The scheduler maintains a log of events, which can be dumped to a file for analysis.
    """

    def __init__(  # noqa: PLR0913
        self,
        seed: int,
        mocks: dict[
            Callable[Concatenate[Context, ...], Any | Coroutine[Any, Any, Any]],
            Callable[[], Any],
        ]
        | None,
        log_file: str | None,
        max_failures: int,
        failure_chance: float,
        mode: Mode,
        probe: Callable[[Dependencies, int], Any] | None,
    ) -> None:
        self._failure_chance = failure_chance
        self._max_failures: int = max_failures
        self.current_failures: int = 0
        self.tick: int = 0
        self._top_level_invocations: deque[_TopLevelInvoke] = deque()
        self._mode: Mode = mode

        self.runnables: Runnables = []
        self.awaitables: Awaitables = {}
        self._callbacks_to_run: list[Callable[..., None]] = []

        self.seed = seed
        self.random = random.Random(self.seed)  # noqa: RUF100, S311
        self.deps = Dependencies()
        self.mocks = mocks or {}

        self._events: list[SchedulerEvents] = []
        self._promise_created: int = 0
        self._log_file = log_file

        self._handlers: dict[
            type[Command], Callable[[list[Any]], list[Any] | None]
        ] = {}
        self._handler_queues: dict[
            type[Command], _ListWithLenght[tuple[Promise[Any], Command]]
        ] = {}

        self._probe = probe
        self._probe_results: list[Any] = []

    def register_command(
        self,
        cmd: type[Cmd],
        handler: Callable[[list[Cmd]], list[Any] | None],
        max_batch: int,
    ) -> None:
        self._handlers[cmd] = handler
        self._handler_queues[cmd] = _ListWithLenght[tuple[Promise[Any], Command]](
            max_length=max_batch,
        )

    def get_handler(
        self, cmd: type[Command]
    ) -> Callable[[list[Any]], list[Any] | None]:
        return self._handlers[cmd]

    def add(
        self,
        coro: Callable[Concatenate[Context, P], Generator[Yieldable, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self._top_level_invocations.appendleft(_TopLevelInvoke(coro, *args, **kwargs))

    def _reset(self) -> None:
        self.runnables.clear()
        self.awaitables.clear()
        self._events.clear()
        self._callbacks_to_run.clear()

    def dump(self, file: str) -> None:
        log_file = CWD / (file % (self.seed))

        log_file.parent.mkdir(parents=True, exist_ok=True)

        all_logs: str = "".join(f"{event}\n" for event in self._events)
        with log_file.open(mode="w") as f:
            f.write(all_logs)

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
            ctx = Context(
                dst=True, deps=self.deps, ctx_id=str(self._increate_promise_created())
            )
            p = Promise[Any](
                promise_id=ctx.ctx_id,
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

            try:
                coro = top_level_invocation.fn(
                    ctx,
                    *top_level_invocation.args,
                    **top_level_invocation.kwargs,
                )
            except Exception as e:  # noqa: BLE001
                p.set_result(Err(e))

            if not p.done():
                self.runnables.append(
                    Runnable(
                        coro_and_promise=CoroAndPromise(
                            coro,
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

    def _next_step(self) -> Step:
        next_step: Step
        if self._mode == "sequential":
            next_step = "callbacks" if self._callbacks_to_run else "runnables"
        elif not self._callbacks_to_run:
            next_step = "runnables"
            assert self.runnables, "There should something in callbacks"

        elif not self.runnables:
            next_step = "callbacks"
            assert self._callbacks_to_run, "There should something in callbacks"

        else:
            next_step = self.random.choice(["callbacks", "runnables"])

        return next_step

    def _cmds_waiting_to_be_executed(
        self,
    ) -> dict[type[Command], _ListWithLenght[tuple[Promise[Any], Command]]]:
        return {k: v for k, v in self._handler_queues.items() if v.length() > 0}

    def _execute_commands(
        self,
        cmd: type[Command],
    ) -> None:
        cmd_buffer = self._handler_queues[cmd]
        for subbuffer in _split_array_by_max_length(
            cmd_buffer.pop_all(), cmd_buffer.max_length
        ):
            n_cmds: int = 0
            promises: list[Promise[Any]] = []
            cmds_to_run: list[Command] = []

            assert (
                len(subbuffer) <= cmd_buffer.max_length
            ), "Subbuffer length is greater that max batch size"

            for p, c in subbuffer:
                promises.append(p)
                cmds_to_run.append(c)
                n_cmds += 1

            try:
                results = self._handlers[cmd](cmds_to_run)
                if results is None:
                    for p in promises:
                        callback(p, self.awaitables, self.runnables, Ok(None))

                else:
                    assert (
                        len(results) == n_cmds
                    ), "Numbers of commands and number of results must be the same"
                    for i in range(n_cmds):
                        callback(
                            promises[i],
                            self.awaitables,
                            self.runnables,
                            Ok(results[i]),
                        )

            except Exception as e:  # noqa: BLE001
                for p in promises:
                    callback(p, self.awaitables, self.runnables, Err(e))

    def _run(self) -> list[Promise[Any]]:
        promises = self._initialize_runnables()

        while True:
            if not self._callbacks_to_run and not self.runnables:
                cmds_to_be_executed = self._cmds_waiting_to_be_executed()
                if len(cmds_to_be_executed) == 0:
                    break

                for cmd in cmds_to_be_executed:
                    self._execute_commands(cmd)

            if self._probe is not None:
                self._probe_results.append(self._probe(self.deps, self.tick))
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
                    self.runnables, r=self.random, mode=self._mode
                )
                self._process_each_runnable(runnable=runnable)

        assert all(p.done() for p in promises), "All promises should be resolved."
        if self._log_file is not None:
            self.dump(file=self._log_file)
        return promises

    def _process_each_runnable(
        self,
        runnable: Runnable[Any],
    ) -> None:
        yieldable_or_final_value = iterate_coro(runnable)

        if (
            isinstance(
                runnable.coro_and_promise.prom.invocation.exec_unit, FnOrCoroutine
            )
            and runnable.next_value is None
        ):
            self._events.append(
                ExecutionStarted(
                    promise_id=runnable.coro_and_promise.prom.promise_id,
                    tick=self.tick,
                    fn_name=runnable.coro_and_promise.prom.invocation.exec_unit.fn.__name__,
                    args=runnable.coro_and_promise.prom.invocation.exec_unit.args,
                    kwargs=runnable.coro_and_promise.prom.invocation.exec_unit.kwargs,
                )
            )

        if isinstance(yieldable_or_final_value, FinalValue):
            value = yieldable_or_final_value.v
            logger.debug("Processing final value `%s`", value)
            callback(
                runnable.coro_and_promise.prom, self.awaitables, self.runnables, value
            )
            self._events.append(
                PromiseResolved(runnable.coro_and_promise.prom.promise_id, self.tick)
            )
        elif isinstance(yieldable_or_final_value, Call):
            p = self._handle_invocation(
                invocation=yieldable_or_final_value.to_invoke(),
                runnable=runnable,
            )
            self.awaitables[p] = [runnable.coro_and_promise]
            self._events.append(
                AwaitedForPromise(promise_id=p.promise_id, tick=self.tick)
            )

        elif isinstance(yieldable_or_final_value, Invoke):
            p = self._handle_invocation(
                invocation=yieldable_or_final_value,
                runnable=runnable,
            )
            self.runnables.append(Runnable(runnable.coro_and_promise, Ok(p)))

        elif isinstance(yieldable_or_final_value, Promise):
            self.awaitables.setdefault(yieldable_or_final_value, []).append(
                runnable.coro_and_promise,
            )
            if yieldable_or_final_value.done():
                unblock_depands_coros(
                    p=yieldable_or_final_value,
                    awaitables=self.awaitables,
                    runnables=self.runnables,
                )
        else:
            assert_never(yieldable_or_final_value)

    def _increate_promise_created(self) -> int:
        self._promise_created += 1
        return self._promise_created

    def _handle_invocation(
        self,
        invocation: Invoke,
        runnable: Runnable[T],
    ) -> Promise[Any]:
        logger.debug("Processing invocation")
        child_ctx = runnable.coro_and_promise.ctx.new_child()
        p = Promise[Any](promise_id=child_ctx.ctx_id, invocation=invocation)

        if isinstance(invocation.exec_unit, FnOrCoroutine):
            if not isgeneratorfunction(invocation.exec_unit.fn):
                mock_fn = self.mocks.get(invocation.exec_unit.fn)
                if mock_fn is not None:
                    v = _safe_run(mock_fn)
                else:
                    v = cast(
                        Result[Any, Exception],
                        utils.wrap_fn_into_cmd(
                            child_ctx,
                            invocation.exec_unit.fn,
                            *invocation.exec_unit.args,
                            **invocation.exec_unit.kwargs,
                        ).run(),
                    )
                self._callbacks_to_run.append(
                    lambda: callback(
                        p=p,
                        awaitables=self.awaitables,
                        runnables=self.runnables,
                        v=v,
                    )
                )

            else:
                coro = invocation.exec_unit.fn(
                    child_ctx,
                    *invocation.exec_unit.args,
                    **invocation.exec_unit.kwargs,
                )
                self.runnables.append(
                    Runnable(CoroAndPromise(coro, p, child_ctx), next_value=None)
                )
        elif isinstance(invocation.exec_unit, Command):
            self._handler_queues[type(invocation.exec_unit)].append(
                (p, invocation.exec_unit)
            )

        else:
            assert_never(invocation.exec_unit)
        return p

    def get_events(self) -> list[SchedulerEvents]:
        return self._events


def get_random_element(array: list[T], r: random.Random, mode: Mode) -> T:
    pop_idx = -1
    if mode == "concurrent":
        pop_idx = r.randint(0, len(array) - 1)
    return array.pop(pop_idx)


def _safe_run(fn: Callable[[], T]) -> Result[T, Exception]:
    result: Result[T, Exception]
    try:
        result = Ok(fn())
    except Exception as e:  # noqa: BLE001
        result = Err(e)
    return result


def _split_array_by_max_length(arr: list[T], max_length: int) -> Generator[list[T]]:
    return (arr[i : i + max_length] for i in range(0, len(arr), max_length))
