from __future__ import annotations

import json
import sys
from inspect import isgenerator, isgeneratorfunction
from typing import TYPE_CHECKING, Any, Callable, Literal, Union, final

from typing_extensions import ParamSpec, TypeAlias, TypeVar, assert_never

from resonate import utils
from resonate.actions import (
    LFC,
    LFI,
    RFC,
    RFI,
    All,
    AllSettled,
    DeferredInvocation,
    Race,
)
from resonate.batching import CmdBuffer
from resonate.commands import Command
from resonate.contants import CWD
from resonate.context import (
    Context,
)
from resonate.dataclasses import (
    CoroAndPromise,
    FnOrCoroutine,
    RouteInfo,
    Runnable,
)
from resonate.dependency_injection import Dependencies
from resonate.encoders import JsonEncoder
from resonate.events import (
    ExecutionAwaited,
    ExecutionInvoked,
    ExecutionResumed,
    ExecutionTerminated,
    PromiseCompleted,
    PromiseCreated,
)
from resonate.functools import AsyncFnWrapper, FnWrapper, wrap_fn
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
    from resonate.options import Options
    from resonate.record import DurablePromiseRecord
    from resonate.storage import IPromiseStore
    from resonate.typing import (
        Awaitables,
        CommandHandlerQueues,
        CommandHandlers,
        DurableCoro,
        DurableFn,
        EphemeralPromiseMemo,
        MockFn,
        PromiseActions,
        RunnableCoroutines,
    )


T = TypeVar("T")
Cmd = TypeVar("Cmd", bound=Command)
P = ParamSpec("P")

Step: TypeAlias = Literal["functions", "coroutines"]
Mode: TypeAlias = Literal["concurrent", "sequential"]


RunnableFunctions: TypeAlias = list[
    tuple[Union[FnWrapper[Any], AsyncFnWrapper[Any]], Promise[Any]]
]


class _DSTFailureError(Exception):
    def __init__(self) -> None:
        super().__init__()


@final
class DSTScheduler:
    """
    The DSTScheduler class manages coroutines in a deterministic way, allowing for
    controlled execution with reproducibility. It can handle errors gracefully by
    resetting its state and retrying up to a specified number of times.
    The scheduler maintains a log of events, which can be dumped to a file for analysis.
    """

    def __init__(  # noqa: PLR0913
        self,
        max_failures: int,
        failure_chance: float,
        log_file: str | None,
        random: random.Random,
        mode: Mode,
        probe: Callable[[Dependencies, int], Any] | None,
        assert_eventually: Callable[[Dependencies, int], None] | None,
        assert_always: Callable[[Dependencies, int, int], None] | None,
        mocks: dict[
            DurableCoro[..., Any] | DurableFn[..., Any],
            MockFn[Any],
        ]
        | None,
        durable_promise_storage: IPromiseStore,
    ) -> None:
        self._stg_queue: list[tuple[LFI, str]] = []
        self._runnable_coros: RunnableCoroutines = []
        self._awatiables: Awaitables = {}
        self._runnable_functions: RunnableFunctions = []

        self.random = random
        self.seed = self.random.seed

        self.deps = Dependencies()

        self.current_failures: int = 0
        self._max_failures = max_failures
        self._failure_chance = failure_chance

        self.tick: int = 0

        self._mode: Mode = mode

        self._probe = probe
        self.probe_results: list[Any] = []

        self._assert_always = assert_always
        self._assert_eventually = assert_eventually

        self._mocks = mocks or {}

        self._log_file = log_file
        self._events: list[SchedulerEvents] = []

        self._handlers: CommandHandlers = {}
        self._handler_queues: CommandHandlerQueues = {}

        self._durable_promise_storage = durable_promise_storage
        self._json_encoder = JsonEncoder()
        self._emphemeral_promise_memo: EphemeralPromiseMemo = {}

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
                        self._resolve_promise(p, Ok(None))
                        self._unblock_coros_waiting_on_promise(p)
                else:
                    assert (
                        len(results) == n_cmds
                    ), "Numbers of commands and number of results must be the same"
                    for i in range(n_cmds):
                        self._resolve_promise(promises[i], Ok(results[i]))
                        self._unblock_coros_waiting_on_promise(promises[i])

            except Exception as e:  # noqa: BLE001
                for p in promises:
                    self._resolve_promise(p, Err(e))
                    self._unblock_coros_waiting_on_promise(p)
        cmd_buffer.clear()

    def get_events(self) -> list[SchedulerEvents]:
        return self._events

    def _clear_events(self) -> None:
        self._events.clear()

    def _cmds_waiting_to_be_executed(
        self,
    ) -> CommandHandlerQueues:
        return {k: v for k, v in self._handler_queues.items() if len(v.elements) > 0}

    def dump(self, file: str) -> None:
        log_file = CWD / (file % (self.seed))

        log_file.parent.mkdir(parents=True, exist_ok=True)

        all_logs: str = "".join(f"{event}\n" for event in self._events)
        with log_file.open(mode="w") as f:
            f.write(all_logs)

    def add(
        self,
        promise_id: str,
        opts: Options,
        coro: DurableCoro[P, Any] | DurableFn[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        assert (
            promise_id not in self._emphemeral_promise_memo
        ), "There's already a promise with the same id"

        top_lvl = LFI(
            FnOrCoroutine(coro, *args, **kwargs),
            opts=opts,
        )
        self._stg_queue.append((top_lvl, promise_id))

    def _add_coro_to_runnables(
        self,
        coro_and_promise: CoroAndPromise[Any],
        value_to_yield_back: Result[Any, Exception] | None,
        *,
        was_awaited: bool,
    ) -> None:
        self._runnable_coros.append(
            (Runnable(coro_and_promise, value_to_yield_back), was_awaited)
        )

    def _add_function_to_runnables(
        self,
        fn_wrapper: FnWrapper[Any] | AsyncFnWrapper[Any],
        promise: Promise[Any],
    ) -> None:
        self._runnable_functions.append(
            (
                fn_wrapper,
                promise,
            )
        )

    def _route_fn_or_coroutine(
        self,
        info: RouteInfo,
    ) -> None:
        assert (
            not info.promise.done()
        ), "Only unresolved executions of unresolved promises can be passed here."
        if isgeneratorfunction(info.fn_or_coroutine.exec_unit):
            coro = info.fn_or_coroutine.exec_unit(
                info.ctx, *info.fn_or_coroutine.args, **info.fn_or_coroutine.kwargs
            )
            self._add_coro_to_runnables(
                CoroAndPromise(info, coro),
                None,
                was_awaited=False,
            )
        else:
            self._add_function_to_runnables(
                wrap_fn(
                    info.ctx,
                    info.fn_or_coroutine.exec_unit,
                    *info.fn_or_coroutine.args,
                    **info.fn_or_coroutine.kwargs,
                ),
                info.promise,
            )

        self._events.append(
            ExecutionInvoked(
                info.promise.promise_id,
                info.promise.parent_promise_id(),
                self.tick,
                info.fn_or_coroutine.exec_unit.__name__,
                info.fn_or_coroutine.args,
                info.fn_or_coroutine.kwargs,
            )
        )

    def _create_promise(
        self,
        parent_promise: Promise[Any] | None,
        promise_id: str | None,
        action: PromiseActions,
    ) -> Promise[Any]:
        if parent_promise is not None:
            p = parent_promise.child_promise(promise_id=promise_id, action=action)
        else:
            assert (
                promise_id is not None
            ), "If creating a root promise must provide a promise id."
            p = Promise[Any](
                promise_id=promise_id, action=action, parent_promise=parent_promise
            )
        assert (
            p.promise_id not in self._emphemeral_promise_memo
        ), "There should not be a new promise with same promise id."
        self._emphemeral_promise_memo[p.promise_id] = p
        if isinstance(action, LFI):
            if isinstance(action.exec_unit, Command):
                raise NotImplementedError
            if isinstance(action.exec_unit, FnOrCoroutine):
                data = None
            else:
                assert_never(action.exec_unit)
        elif isinstance(action, (All, AllSettled, Race, RFI)):
            raise NotImplementedError
        else:
            assert_never(action)

        if not p.durable:
            self._events.append(
                PromiseCreated(
                    promise_id=p.promise_id,
                    tick=self.tick,
                    parent_promise_id=p.parent_promise_id(),
                )
            )
            return p

        durable_promise_record = self._create_durable_promise_record(
            promise_id=p.promise_id, data=data
        )
        self._events.append(
            PromiseCreated(
                promise_id=p.promise_id,
                tick=self.tick,
                parent_promise_id=p.parent_promise_id(),
            )
        )
        if durable_promise_record.is_pending():
            return p

        assert (
            durable_promise_record.value.data is not None
        ), "If the promise is not pending, there must be data."
        v = self._get_value_from_durable_promise(durable_promise_record)
        self._resolve_promise(p, v)
        return p

    def _get_value_from_durable_promise(
        self, durable_promise_record: DurablePromiseRecord
    ) -> Result[Any, Exception]:
        assert (
            durable_promise_record.is_completed()
        ), "If you want to get the value then the promise must have been completed"

        v: Result[Any, Exception]
        if durable_promise_record.is_rejected():
            if durable_promise_record.value.data is None:
                raise NotImplementedError

            v = Err(self._json_encoder.decode(durable_promise_record.value.data))
        else:
            assert durable_promise_record.is_resolved()
            if durable_promise_record.value.data is None:
                v = Ok(None)
            else:
                v = Ok(json.loads(durable_promise_record.value.data))
        return v

    def _move_next_top_lvl_invocation_to_runnables(
        self, top_lvl: LFI, promise_id: str
    ) -> Promise[Any]:
        assert isinstance(top_lvl.exec_unit, FnOrCoroutine)
        root_ctx = Context(
            seed=self.seed,
            deps=self.deps,
        )
        p = self._create_promise(
            parent_promise=None,
            promise_id=promise_id,
            action=top_lvl,
        )
        assert p.durable, "Top level invocations must be durable"
        if p.done():
            self._unblock_coros_waiting_on_promise(p)
        else:
            self._route_fn_or_coroutine(RouteInfo(root_ctx, p, top_lvl.exec_unit, 0))
        return p

    def _get_random_element(self, array: list[T]) -> T:
        return array.pop(self.random.randint(0, len(array) - 1))

    def _reset(self) -> None:
        self._runnable_coros.clear()
        self._runnable_functions.clear()
        self._awatiables.clear()
        self._emphemeral_promise_memo.clear()
        self.probe_results.clear()
        for queue in self._handler_queues.values():
            queue.clear()

        self.current_failures += 1

    def run(self) -> list[Promise[Any]]:
        self.tick = 0
        self._clear_events()
        while True:
            num_top_lvl_invocations = len(self._stg_queue)
            try:
                if self._mode == "concurrent":
                    promises = self._run_concurrently()
                elif self._mode == "sequential":
                    promises = self._run_sequentially()
                else:
                    assert_never(self._mode)

            except _DSTFailureError:
                self._reset()
            else:
                assert all(
                    p.done() for p in promises
                ), "All promises should be resolved."
                assert num_top_lvl_invocations == len(
                    promises
                ), "There should be one resolved promise per top level invocation"

                if self._log_file is not None:
                    self.dump(file=self._log_file)

                return promises

    def _resolve_promise(
        self, promise: Promise[T], value: Result[T, Exception]
    ) -> None:
        if promise.durable:
            completed_record = self._complete_durable_promise_record(
                promise_id=promise.promise_id,
                value=value,
            )
            assert (
                completed_record.is_completed()
            ), "Durable promise record must be completed by this point."
            v = self._get_value_from_durable_promise(completed_record)
            promise.set_result(v)
        else:
            promise.set_result(value)
        assert (
            promise.promise_id in self._emphemeral_promise_memo
        ), "Resolved promise should be in the memo"

        self._events.append(
            PromiseCompleted(
                promise_id=promise.promise_id,
                tick=self.tick,
                value=value,
                parent_promise_id=promise.parent_promise_id(),
            )
        )
        self._emphemeral_promise_memo.pop(promise.promise_id)

    def _maybe_fail(self) -> None:
        if (
            self.current_failures < self._max_failures
            and self.random.uniform(0, 100) < self._failure_chance
        ):
            raise _DSTFailureError

    def _run(self) -> None:
        while True:
            if self._probe is not None:
                self.probe_results.append(self._probe(self.deps, self.tick))

            if self._assert_always is not None:
                self._assert_always(self.deps, self.seed, self.tick)

            if not self._runnable_functions and not self._runnable_coros:
                cmds_to_be_executed = self._cmds_waiting_to_be_executed()
                if len(cmds_to_be_executed) == 0:
                    break

                for cmd in cmds_to_be_executed:
                    self._execute_commands(cmd)

            self.tick += 1

            self._maybe_fail()

            next_step = self._next_step()
            if next_step == "functions":
                ## This simulates the processor in the production scheduler.
                fn_wrapper, promise = self._get_random_element(self._runnable_functions)
                assert not promise.done(), "Only unresolve promises can be found here."
                assert isinstance(promise.action, LFI)

                v = (
                    _safe_run(self._mocks[fn_wrapper.fn])
                    if self._mocks.get(fn_wrapper.fn) is not None
                    else fn_wrapper.run()
                )
                assert isinstance(v, (Ok, Err)), f"{v} must be a result."

                self._maybe_fail()

                self._events.append(
                    ExecutionTerminated(
                        promise_id=promise.promise_id,
                        tick=self.tick,
                        parent_promise_id=promise.parent_promise_id(),
                    )
                )

                self._resolve_promise(promise, v)
                self._unblock_coros_waiting_on_promise(promise)

            elif next_step == "coroutines":
                runnable, was_awaited = self._get_random_element(
                    array=self._runnable_coros,
                )
                self._advance_runnable_span(runnable=runnable, was_awaited=was_awaited)
            else:
                assert_never(next_step)

        if self._assert_eventually is not None:
            self._assert_eventually(self.deps, self.seed)

    def _complete_durable_promise_record(
        self, promise_id: str, value: Result[Any, Exception]
    ) -> DurablePromiseRecord:
        if isinstance(value, Ok):
            return self._durable_promise_storage.resolve(
                promise_id=promise_id,
                ikey=utils.string_to_ikey(promise_id),
                strict=False,
                headers=None,
                data=self._json_encoder.encode(value.unwrap()),
            )
        if isinstance(value, Err):
            return self._durable_promise_storage.reject(
                promise_id=promise_id,
                ikey=utils.string_to_ikey(promise_id),
                strict=False,
                headers=None,
                data=self._json_encoder.encode(value.err()),
            )
        assert_never(value)

    def _create_durable_promise_record(
        self, promise_id: str, data: dict[str, Any] | None
    ) -> DurablePromiseRecord:
        return self._durable_promise_storage.create(
            promise_id=promise_id,
            ikey=utils.string_to_ikey(promise_id),
            strict=False,
            headers=None,
            data=self._json_encoder.encode(data) if data is not None else None,
            timeout=sys.maxsize,
            tags=None,
        )

    def _process_local_invocation(
        self, invocation: LFI, runnable: Runnable[Any]
    ) -> Promise[Any]:
        p = self._create_promise(
            parent_promise=runnable.coro_and_promise.route_info.promise,
            promise_id=invocation.opts.promise_id,
            action=invocation,
        )
        if isinstance(invocation.exec_unit, Command):
            self._handler_queues[type(invocation.exec_unit)].append(
                (p, invocation.exec_unit)
            )
        elif isinstance(invocation.exec_unit, FnOrCoroutine):
            if p.done():
                self._unblock_coros_waiting_on_promise(p)
            else:
                self._route_fn_or_coroutine(
                    RouteInfo(
                        runnable.coro_and_promise.route_info.ctx,
                        p,
                        invocation.exec_unit,
                        0,
                    )
                )
        else:
            assert_never(invocation.exec_unit)
        return p

    def _add_coro_to_awaitables(
        self, p: Promise[Any], coro_and_promise: CoroAndPromise[Any]
    ) -> None:
        assert (
            not p.done()
        ), "If the promise is resolved already it makes no sense to block coroutine"
        self._awatiables.setdefault(p, []).append(coro_and_promise)
        self._events.append(
            ExecutionAwaited(
                promise_id=coro_and_promise.route_info.promise.promise_id,
                tick=self.tick,
                parent_promise_id=coro_and_promise.route_info.promise.parent_promise_id(),
            )
        )

    def _advance_runnable_span(
        self, runnable: Runnable[Any], *, was_awaited: bool
    ) -> None:
        assert isgenerator(
            runnable.coro_and_promise.coro
        ), "Only coroutines can be advanced"

        yieldable_or_final_value = iterate_coro(runnable)

        if was_awaited:
            self._events.append(
                ExecutionResumed(
                    promise_id=runnable.coro_and_promise.route_info.promise.promise_id,
                    tick=self.tick,
                    parent_promise_id=runnable.coro_and_promise.route_info.promise.parent_promise_id(),
                )
            )

        if isinstance(yieldable_or_final_value, FinalValue):
            self._events.append(
                ExecutionTerminated(
                    promise_id=runnable.coro_and_promise.route_info.promise.promise_id,
                    tick=self.tick,
                    parent_promise_id=runnable.coro_and_promise.route_info.promise.parent_promise_id(),
                )
            )
            self._resolve_promise(
                runnable.coro_and_promise.route_info.promise, yieldable_or_final_value.v
            )
            self._unblock_coros_waiting_on_promise(
                p=runnable.coro_and_promise.route_info.promise
            )
        elif isinstance(yieldable_or_final_value, LFC):
            p = self._process_local_invocation(
                yieldable_or_final_value.to_invocation(), runnable
            )
            assert (
                p not in self._awatiables
            ), "Since it's a call it should be a promise without dependants"
            if p.done():
                self._add_coro_to_runnables(
                    runnable.coro_and_promise,
                    p.safe_result(),
                    was_awaited=False,
                )
            else:
                self._add_coro_to_awaitables(p, runnable.coro_and_promise)

        elif isinstance(yieldable_or_final_value, LFI):
            p = self._process_local_invocation(
                invocation=yieldable_or_final_value, runnable=runnable
            )
            self._add_coro_to_runnables(
                runnable.coro_and_promise, Ok(p), was_awaited=False
            )
        elif isinstance(yieldable_or_final_value, Promise):
            p = yieldable_or_final_value
            if p.done():
                self._unblock_coros_waiting_on_promise(p)
                self._add_coro_to_runnables(
                    runnable.coro_and_promise, p.safe_result(), was_awaited=False
                )
            else:
                self._add_coro_to_awaitables(p, runnable.coro_and_promise)

        elif isinstance(
            yieldable_or_final_value,
            (DeferredInvocation, All, AllSettled, Race, RFC, RFI),
        ):
            raise NotImplementedError

        else:
            assert_never(yieldable_or_final_value)

    def _unblock_coros_waiting_on_promise(self, p: Promise[Any]) -> None:
        assert p.done(), "Promise must be done to unblock waiting coroutines"
        if self._awatiables.get(p) is None:
            return
        for coro_and_promise in self._awatiables.pop(p):
            self._add_coro_to_runnables(
                coro_and_promise, p.safe_result(), was_awaited=True
            )

    def _next_step(self) -> Step:
        next_step: Step
        if self._mode == "sequential":
            next_step = "functions" if self._runnable_functions else "coroutines"

        elif not self._runnable_functions:
            next_step = "coroutines"
            assert self._runnable_coros, "There should something in runnable coroutines"

        elif not self._runnable_coros:
            next_step = "functions"
            assert (
                self._runnable_functions
            ), "There should something in runnable functions"

        else:
            next_step = self.random.choice(("functions", "coroutines"))

        return next_step

    def _run_sequentially(self) -> list[Promise[Any]]:
        promises: list[Promise[Any]] = []
        for top_lvl, promise_id in self._stg_queue:
            promises.append(
                self._move_next_top_lvl_invocation_to_runnables(top_lvl, promise_id)
            )
            self._run()

        return promises

    def _run_concurrently(self) -> list[Promise[Any]]:
        promises: list[Promise[Any]] = []
        for top_lvl, promise_id in self._stg_queue:
            promises.append(
                self._move_next_top_lvl_invocation_to_runnables(top_lvl, promise_id)
            )
        self._run()
        return promises


def _safe_run(fn: MockFn[T]) -> Result[T, Exception]:
    result: Result[T, Exception]
    try:
        result = Ok(fn())
    except Exception as e:  # noqa: BLE001
        result = Err(e)
    return result
