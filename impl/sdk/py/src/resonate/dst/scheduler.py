from __future__ import annotations

import asyncio
from collections import deque
from inspect import iscoroutinefunction, isfunction, isgenerator, isgeneratorfunction
from typing import TYPE_CHECKING, Any, Callable, Generic, Literal, Union

from typing_extensions import ParamSpec, TypeAlias, TypeVar, assert_never

from resonate.actions import Call, Invoke, Sleep
from resonate.batching import CmdBuffer
from resonate.contants import CWD
from resonate.context import (
    Context,
)
from resonate.dataclasses import Command, CoroAndPromise, FnOrCoroutine, Runnable
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

if TYPE_CHECKING:
    from resonate import random
    from resonate.events import (
        SchedulerEvents,
    )
    from resonate.typing import (
        Awaitables,
        CommandHandlerQueues,
        CommandHandlers,
        DurableAsyncFn,
        DurableCoro,
        DurableFn,
        DurableSyncFn,
        MockFn,
        RunnableCoroutines,
    )


T = TypeVar("T")
Cmd = TypeVar("Cmd", bound=Command)
P = ParamSpec("P")

Step: TypeAlias = Literal["functions", "coroutines"]
Mode: TypeAlias = Literal["concurrent", "sequential"]


class _FnWrapper(Generic[T]):
    def __init__(
        self,
        ctx: Context,
        fn: DurableSyncFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.ctx = ctx
        self.args = args
        self.kwargs = kwargs

    def run(self) -> Result[T, Exception]:
        result: Result[T, Exception]
        try:
            result = Ok(self.fn(self.ctx, *self.args, **self.kwargs))
        except Exception as e:  # noqa: BLE001
            result = Err(e)

        return result


class _AsyncFnWrapper(Generic[T]):
    def __init__(
        self,
        ctx: Context,
        fn: DurableAsyncFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.ctx = ctx
        self.args = args
        self.kwargs = kwargs

    async def run(self) -> Result[T, Exception]:
        result: Result[T, Exception]
        try:
            result = Ok(asyncio.run(self.fn(self.ctx, *self.args, **self.kwargs)))
        except Exception as e:  # noqa: BLE001
            result = Err(e)
        return result


RunnableFunctions: TypeAlias = list[
    tuple[Union[_FnWrapper[Any], _AsyncFnWrapper[Any]], Promise[Any]]
]


def _wrap_fn(
    ctx: Context,
    fn: DurableFn[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> _FnWrapper[T] | _AsyncFnWrapper[T]:
    cmd: _FnWrapper[T] | _AsyncFnWrapper[T]
    if isfunction(fn):
        cmd = _FnWrapper(ctx, fn, *args, **kwargs)
    else:
        assert iscoroutinefunction(fn)
        cmd = _AsyncFnWrapper(ctx, fn, *args, **kwargs)
    return cmd


class _DSTFailureError(Exception):
    def __init__(self) -> None:
        super().__init__()


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
            DurableCoro[..., Any] | DurableFn[..., Any],
            MockFn[Any],
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
        self._top_level_invokations: deque[Invoke] = deque()
        self._mode: Mode = mode

        self.random = random
        self.seed = random.seed

        self.deps = Dependencies()
        self.mocks = mocks or {}

        self._events: list[SchedulerEvents] = []
        self._log_file = log_file

        self._handlers: CommandHandlers = {}
        self._handler_queues: CommandHandlerQueues = {}

        self._probe = probe
        self._probe_results: list[Any] = []

    def register_command(
        self,
        cmd: type[Cmd],
        handler: Callable[[list[Cmd]], list[Any] | None],
        max_batch: int,
    ) -> None:
        self._handlers[cmd] = handler
        self._handler_queues[cmd] = CmdBuffer[tuple[Promise[Any], Command]](
            max_length=max_batch,
        )

    def add(
        self,
        coro: DurableFn[P, Any] | DurableCoro[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self._top_level_invokations.appendleft(
            Invoke(FnOrCoroutine(coro, *args, **kwargs))
        )

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
            except _DSTFailureError:  # noqa: PERF203
                self.current_failures += 1
                self._reset()

    def _create_promise(self, ctx: Context, invokation: Invoke) -> Promise[Any]:
        p = Promise[Any](ctx.ctx_id, invokation)
        self._events.append(PromiseCreated(promise_id=p.promise_id, tick=self.tick))
        return p

    def _initialize_runnables(self) -> list[Promise[Any]]:
        init_promises: list[Promise[Any] | None] = [
            None for _ in range(len(self._top_level_invokations))
        ]
        num_top_level_invokations = len(self._top_level_invokations)
        for idx, top_level_invokation in enumerate(self._top_level_invokations):
            ctx = Context(
                ctx_id=str(num_top_level_invokations - idx),
                parent_ctx=None,
                deps=self.deps,
                seed=self.seed,
            )
            p = self._create_promise(ctx, top_level_invokation)
            assert isinstance(top_level_invokation.exec_unit, FnOrCoroutine)
            try:
                self._events.append(
                    ExecutionInvoked(
                        promise_id=p.promise_id,
                        tick=self.tick,
                        fn_name=top_level_invokation.exec_unit.exec_unit.__name__,
                        args=top_level_invokation.exec_unit.args,
                        kwargs=top_level_invokation.exec_unit.kwargs,
                    )
                )
                coro = top_level_invokation.exec_unit.exec_unit(
                    ctx,
                    *top_level_invokation.exec_unit.args,
                    **top_level_invokation.exec_unit.kwargs,
                )
            except Exception as e:  # noqa: BLE001
                self._complete_promise(p, Err(e))

            if not p.done():
                assert isgenerator(coro)
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
    ) -> CommandHandlerQueues:
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
                results = self._handlers[cmd](cmds_to_run)
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
        self, fn_wraper: _FnWrapper[Any] | _AsyncFnWrapper[Any], promise: Promise[Any]
    ) -> None:
        v = (
            _safe_run(self.mocks[fn_wraper.fn])
            if self.mocks.get(fn_wraper.fn) is not None
            else fn_wraper.run()
        )
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
                raise _DSTFailureError

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
                invokation=yieldable_or_final_value.to_invoke(),
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
                invokation=yieldable_or_final_value,
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
        elif isinstance(yieldable_or_final_value, Sleep):
            raise NotImplementedError
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
        invokation: Invoke,
        runnable: Runnable[T],
    ) -> Promise[Any]:
        child_ctx = runnable.coro_and_promise.ctx.new_child()
        p = self._create_promise(ctx=child_ctx, invokation=invokation)

        if isinstance(invokation.exec_unit, FnOrCoroutine):
            self._events.append(
                ExecutionInvoked(
                    promise_id=p.promise_id,
                    tick=self.tick,
                    fn_name=invokation.exec_unit.exec_unit.__name__,
                    args=invokation.exec_unit.args,
                    kwargs=invokation.exec_unit.kwargs,
                )
            )
            if not isgeneratorfunction(invokation.exec_unit.exec_unit):
                self._runnable_functions.append(
                    (
                        _wrap_fn(
                            child_ctx,
                            invokation.exec_unit.exec_unit,
                            *invokation.exec_unit.args,
                            **invokation.exec_unit.kwargs,
                        ),
                        p,
                    )
                )

            else:
                coro = invokation.exec_unit.exec_unit(
                    child_ctx,
                    *invokation.exec_unit.args,
                    **invokation.exec_unit.kwargs,
                )
                self._add_coro_to_runnables(
                    CoroAndPromise(coro, p, child_ctx), value_to_yield=None
                )

        elif isinstance(invokation.exec_unit, Command):
            self._handler_queues[type(invokation.exec_unit)].append(
                (p, invokation.exec_unit)
            )

        else:
            assert_never(invokation.exec_unit)
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


def _safe_run(fn: MockFn[T]) -> Result[T, Exception]:
    result: Result[T, Exception]
    try:
        result = Ok(fn())
    except Exception as e:  # noqa: BLE001
        result = Err(e)
    return result
