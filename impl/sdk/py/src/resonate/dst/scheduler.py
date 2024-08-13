from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Callable, Generic, Literal

from typing_extensions import Concatenate, ParamSpec, TypeAlias, TypeVar, assert_never

from resonate import utils
from resonate.contants import CWD
from resonate.context import Call, Command, Context, FnOrCoroutine, Invoke
from resonate.dependency_injection import Dependencies
from resonate.events import (
    ExecutionAwaited,
    ExecutionInvoked,
    ExecutionResumed,
    ExecutionTerminated,
    PromiseCompleted,
    PromiseCreated,
)
from resonate.itertools import (
    FinalValue,
    iterate_coro,
)
from resonate.promise import Promise
from resonate.result import Err, Ok, Result
from resonate.typing import (
    AsyncCmd,
    Awaitables,
    CoroAndPromise,
    FnCmd,
    Runnable,
    RunnableCoroutines,
    RunnableFunctions,
    Yieldable,
)

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator

    from resonate import random
    from resonate.events import (
        SchedulerEvents,
    )


T = TypeVar("T")
Cmd = TypeVar("Cmd", bound=Command)
P = ParamSpec("P")

Step: TypeAlias = Literal["functions", "coroutines"]
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
class _CmdBuffer(Generic[T]):
    max_length: int
    elements: list[list[T]] = field(init=False, default_factory=list)

    def append(self, e: T, /) -> None:
        if len(self.elements) == 0:
            self.elements.append([e])
        elif len(self.elements[-1]) < self.max_length:
            self.elements[-1].append(e)
        else:
            self.elements.append([e])

    def clear(self) -> None:
        self.elements.clear()


class DSTScheduler:
    """
    The DSTScheduler class manages coroutines in a deterministic way, allowing for
    controlled execution with reproducibility. It can handle errors gracefully by
    resetting its state and retrying up to a specified number of times.
    The scheduler maintains a log of events, which can be dumped to a file for analysis.
    """

    def __init__(  # noqa: PLR0913
        self,
        random: random.Random,
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
        assert_eventually: Callable[[Dependencies, int], None] | None,
        assert_always: Callable[[Dependencies, int, int], None] | None,
    ) -> None:
        self._assert_eventually = assert_eventually
        self._assert_always = assert_always
        self._runnable_coroutines: RunnableCoroutines = []
        self._awaitables: Awaitables = {}
        self._runnable_functions: RunnableFunctions = []

        self._failure_chance = failure_chance
        self._max_failures: int = max_failures
        self.current_failures: int = 0
        self.tick: int = 0
        self._top_level_invocations: deque[_TopLevelInvoke] = deque()
        self._mode: Mode = mode

        self.random = random
        self.seed = random.seed

        self.deps = Dependencies()
        self.mocks = mocks or {}

        self._events: list[SchedulerEvents] = []
        self._log_file = log_file

        self._handlers: dict[
            type[Command], Callable[[list[Any]], list[Any] | None]
        ] = {}
        self._handler_queues: dict[
            type[Command], _CmdBuffer[tuple[Promise[Any], Command]]
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
        self._handler_queues[cmd] = _CmdBuffer[tuple[Promise[Any], Command]](
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
        self._runnable_coroutines.clear()
        self._runnable_functions.clear()
        self._awaitables.clear()
        self._promise_created = 0

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

    def _create_promise(
        self, ctx: Context, invocation: Invoke | _TopLevelInvoke
    ) -> Promise[Any]:
        if isinstance(invocation, _TopLevelInvoke):
            invocation = invocation.to_invocation()

        p = Promise[Any](ctx.ctx_id, invocation=invocation)
        self._events.append(PromiseCreated(promise_id=p.promise_id, tick=self.tick))
        return p

    def _initialize_runnables(self) -> list[Promise[Any]]:
        init_promises: list[Promise[Any] | None] = [
            None for _ in range(len(self._top_level_invocations))
        ]
        num_top_level_invocations = len(self._top_level_invocations)
        for idx, top_level_invocation in enumerate(self._top_level_invocations):
            ctx = Context(
                ctx_id=str(num_top_level_invocations - idx),
                parent_ctx=None,
                deps=self.deps,
                dst=True,
            )
            p = self._create_promise(ctx, top_level_invocation)

            try:
                self._events.append(
                    ExecutionInvoked(
                        promise_id=p.promise_id,
                        tick=self.tick,
                        fn_name=top_level_invocation.fn.__name__,
                        args=top_level_invocation.args,
                        kwargs=top_level_invocation.kwargs,
                    )
                )
                coro = top_level_invocation.fn(
                    ctx,
                    *top_level_invocation.args,
                    **top_level_invocation.kwargs,
                )
            except Exception as e:  # noqa: BLE001
                self._complete_promise(p, Err(e))

            if not p.done():
                self._add_coro_to_runnables(
                    CoroAndPromise(
                        coro,
                        p,
                        ctx,
                    ),
                    None,
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
            next_step = "functions" if self._runnable_functions else "coroutines"

        elif not self._runnable_functions:
            next_step = "coroutines"
            assert (
                self._runnable_coroutines
            ), "There should something in runnable coroutines"

        elif not self._runnable_coroutines:
            next_step = "functions"
            assert (
                self._runnable_functions
            ), "There should something in runnable functions"

        else:
            next_step = self.random.choice(("functions", "coroutines"))

        return next_step

    def _cmds_waiting_to_be_executed(
        self,
    ) -> dict[type[Command], _CmdBuffer[tuple[Promise[Any], Command]]]:
        return {k: v for k, v in self._handler_queues.items() if len(v.elements) > 0}

    def _complete_promise(self, p: Promise[T], value: Result[T, Exception]) -> None:
        p.set_result(value)

        self._events.append(
            ExecutionTerminated(promise_id=p.promise_id, tick=self.tick)
        )
        self._events.append(
            PromiseCompleted(promise_id=p.promise_id, tick=self.tick, value=value)
        )

    def _execute_commands(
        self,
        cmd: type[Command],
    ) -> None:
        cmd_buffer = self._handler_queues[cmd]
        for subbuffer in cmd_buffer.elements:
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
                results = self.get_handler(cmd)(cmds_to_run)
                if results is None:
                    for p in promises:
                        self._complete_promise(p, Ok(None))
                        self._unblock_dependant_coros(p)
                else:
                    assert (
                        len(results) == n_cmds
                    ), "Numbers of commands and number of results must be the same"
                    for i in range(n_cmds):
                        self._complete_promise(promises[i], Ok(results[i]))
                        self._unblock_dependant_coros(promises[i])

            except Exception as e:  # noqa: BLE001
                for p in promises:
                    self._complete_promise(p, Err(e))
                    self._unblock_dependant_coros(p)
        cmd_buffer.clear()

    def _run_function_and_move_awaitables_to_runnables(
        self, fn_wraper: FnCmd[Any] | AsyncCmd[Any], promise: Promise[Any]
    ) -> None:
        mock_fn = self.mocks.get(fn_wraper.fn)
        v = _safe_run(mock_fn) if mock_fn is not None else fn_wraper.run()
        assert isinstance(v, (Ok, Err)), f"{v} must be a result."
        self._complete_promise(promise, v)
        self._unblock_dependant_coros(promise)

    def _run(self) -> list[Promise[Any]]:  # noqa: C901
        promises = self._initialize_runnables()

        while True:
            if self._probe is not None:
                self._probe_results.append(self._probe(self.deps, self.tick))

            if self._assert_always is not None:
                self._assert_always(
                    self.deps,
                    self.seed,
                    self.tick,
                )

            if not self._runnable_functions and not self._runnable_coroutines:
                cmds_to_be_executed = self._cmds_waiting_to_be_executed()
                if len(cmds_to_be_executed) == 0:
                    break

                for cmd in cmds_to_be_executed:
                    self._execute_commands(cmd)

            self.tick += 1

            if (
                self.current_failures < self._max_failures
                and self.random.uniform(0, 100) < self._failure_chance
            ):
                raise DSTFailureError

            next_step = self._next_step()
            if next_step == "functions":
                function_wrapper, p = self._get_random_element(self._runnable_functions)
                self._run_function_and_move_awaitables_to_runnables(function_wrapper, p)

            elif next_step == "coroutines":
                runnable = self._get_random_element(
                    self._runnable_coroutines,
                )
                self._process_each_runnable(runnable=runnable)
            else:
                assert_never(next_step)

        if self._assert_eventually is not None:
            self._assert_eventually(self.deps, self.seed)

        assert all(p.done() for p in promises), "All promises should be resolved."
        if self._log_file is not None:
            self.dump(file=self._log_file)
        return promises

    def _add_coro_to_awaitables(
        self, p: Promise[Any], coro_and_promise: CoroAndPromise[Any]
    ) -> None:
        assert not p.done(), "Do not await on an already resolved promise"
        self._awaitables.setdefault(p, []).append(coro_and_promise)
        self._events.append(ExecutionAwaited(promise_id=p.promise_id, tick=self.tick))

    def _add_coro_to_runnables(
        self,
        coro_and_promise: CoroAndPromise[Any],
        value_to_yield: Result[Any, Exception] | None,
    ) -> None:
        self._runnable_coroutines.append(Runnable(coro_and_promise, value_to_yield))
        self._events.append(
            ExecutionResumed(
                promise_id=coro_and_promise.prom.promise_id, tick=self.tick
            )
        )

    def _process_each_runnable(
        self,
        runnable: Runnable[Any],
    ) -> None:
        yieldable_or_final_value = iterate_coro(runnable)

        if isinstance(yieldable_or_final_value, FinalValue):
            value = yieldable_or_final_value.v
            self._complete_promise(runnable.coro_and_promise.prom, value)
            self._unblock_dependant_coros(p=runnable.coro_and_promise.prom)

        elif isinstance(yieldable_or_final_value, Call):
            p = self._invoke_execution(
                invocation=yieldable_or_final_value.to_invoke(),
                runnable=runnable,
            )
            if not p.done():
                self._add_coro_to_awaitables(
                    p=p, coro_and_promise=runnable.coro_and_promise
                )
            else:
                self._add_coro_to_runnables(
                    coro_and_promise=runnable.coro_and_promise,
                    value_to_yield=p.safe_result(),
                )
        elif isinstance(yieldable_or_final_value, Invoke):
            p = self._invoke_execution(
                invocation=yieldable_or_final_value,
                runnable=runnable,
            )
            self._add_coro_to_runnables(runnable.coro_and_promise, Ok(p))

        elif isinstance(yieldable_or_final_value, Promise):
            if not yieldable_or_final_value.done():
                self._add_coro_to_awaitables(
                    yieldable_or_final_value, runnable.coro_and_promise
                )
            else:
                self._add_coro_to_runnables(
                    coro_and_promise=runnable.coro_and_promise,
                    value_to_yield=yieldable_or_final_value.safe_result(),
                )
                self._unblock_dependant_coros(yieldable_or_final_value)
        else:
            assert_never(yieldable_or_final_value)

    def _unblock_dependant_coros(self, p: Promise[T]) -> None:
        assert p.done(), "Promise must be done to unblock dependant coros"

        if self._awaitables.get(p) is None:
            return

        res = p.safe_result()

        coros_to_unblock = self._awaitables.pop(p)
        for c_and_p in coros_to_unblock:
            self._add_coro_to_runnables(coro_and_promise=c_and_p, value_to_yield=res)

    def _invoke_execution(
        self,
        invocation: Invoke,
        runnable: Runnable[T],
    ) -> Promise[Any]:
        child_ctx = runnable.coro_and_promise.ctx.new_child()
        p = self._create_promise(ctx=child_ctx, invocation=invocation)

        if isinstance(invocation.exec_unit, FnOrCoroutine):
            self._events.append(
                ExecutionInvoked(
                    promise_id=p.promise_id,
                    tick=self.tick,
                    fn_name=invocation.exec_unit.fn.__name__,
                    args=invocation.exec_unit.args,
                    kwargs=invocation.exec_unit.kwargs,
                )
            )
            if not isgeneratorfunction(invocation.exec_unit.fn):
                self._runnable_functions.append(
                    (
                        utils.wrap_fn_into_cmd(
                            child_ctx,
                            invocation.exec_unit.fn,
                            *invocation.exec_unit.args,
                            **invocation.exec_unit.kwargs,
                        ),
                        p,
                    )
                )

            else:
                coro = invocation.exec_unit.fn(
                    child_ctx,
                    *invocation.exec_unit.args,
                    **invocation.exec_unit.kwargs,
                )
                self._add_coro_to_runnables(
                    CoroAndPromise(coro, p, child_ctx), value_to_yield=None
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

    def _get_random_element(self, array: list[T]) -> T:
        pop_idx: int
        if self._mode == "sequential":
            pop_idx = -1
        elif self._mode == "concurrent":
            pop_idx = self.random.randint(0, len(array) - 1)
        else:
            assert_never(self._mode)
        return array.pop(pop_idx)


def _safe_run(fn: Callable[[], T]) -> Result[T, Exception]:
    result: Result[T, Exception]
    try:
        result = Ok(fn())
    except Exception as e:  # noqa: BLE001
        result = Err(e)
    return result
