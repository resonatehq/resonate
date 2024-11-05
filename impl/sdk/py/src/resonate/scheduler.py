from __future__ import annotations

import os
import sys
import time
import uuid
from abc import ABC, abstractmethod
from collections import deque
from inspect import isgeneratorfunction
from threading import Event, Thread
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    TypeVar,
    Union,
    final,
)

from typing_extensions import ParamSpec, TypeAlias, assert_never

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
from resonate.batching import CommandBuffer
from resonate.collections import Awaiting, DoubleDict, EphemeralMemo
from resonate.commands import Command, CreateDurablePromiseReq, remote_function
from resonate.context import Context
from resonate.dataclasses import (
    FnOrCoroutine,
    ResonateCoro,
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
from resonate.itertools import FinalValue, iterate_coro
from resonate.logging import logger
from resonate.options import LOptions
from resonate.promise import (
    Promise,
    all_promises_are_done,
    any_promise_is_done,
)
from resonate.queue import DelayQueue, Queue
from resonate.record import Invoke, Resume, TaskRecord
from resonate.result import Err, Ok
from resonate.retry_policy import Never, RetryPolicy, default_policy
from resonate.shells.long_poller import LongPoller
from resonate.storage.traits import IPromiseStore, ITaskStore
from resonate.time import now
from resonate.tracing.stdout import StdOutAdapter
from resonate.typing import (
    CmdHandler,
    Combinator,
)

if TYPE_CHECKING:
    from collections.abc import Hashable

    from resonate.record import DurablePromiseRecord
    from resonate.result import Result
    from resonate.tracing import IAdapter
    from resonate.typing import (
        AwaitingFor,
        CmdHandlerResult,
        DurableCoro,
        DurableFn,
        PromiseActions,
        Runnables,
    )

T = TypeVar("T")
C = TypeVar("C", bound=Command)
P = ParamSpec("P")


def _next_retry_delay(retry_policy: RetryPolicy, retry_attempt: int) -> float:
    assert not isinstance(retry_policy, Never)
    return retry_policy.calculate_delay(attempt=retry_attempt + 1)


def _to_be_retried(
    result: Result[Any, Exception], retry_policy: RetryPolicy, retry_attempt: int
) -> bool:
    return (
        isinstance(result, Err)
        and not isinstance(retry_policy, Never)
        and retry_policy.should_retry(retry_attempt + 1)
    )


class _Retriable(ABC):
    @abstractmethod
    def to_be_retried(self) -> bool: ...

    @abstractmethod
    def next_retry_delay(self) -> float: ...


class _BatchSQE:
    def __init__(  # noqa: PLR0913
        self,
        ctx: Context,
        promises: list[Promise[Any]],
        commands: list[Command],
        handler: CmdHandler[Command],
        retry_attempt: int,
        retry_policy: RetryPolicy,
    ) -> None:
        assert len(promises) == len(
            commands
        ), "There must be the same number of promises and the same number of commands."
        self.ctx = ctx
        self.promises = promises
        self.commands = commands
        self.handler = handler
        self.retry_attempt = retry_attempt
        self.retry_policy = retry_policy


class _BatchCQE(_Retriable):
    def __init__(
        self, sqe: _BatchSQE, result: Result[CmdHandlerResult[Any], Exception]
    ) -> None:
        self.sqe = sqe
        self.result = result

    def to_be_retried(self) -> bool:
        return _to_be_retried(
            self.result,
            retry_attempt=self.sqe.retry_attempt,
            retry_policy=self.sqe.retry_policy,
        )

    def next_retry_delay(self) -> float:
        return _next_retry_delay(self.sqe.retry_policy, self.sqe.retry_attempt)


class _FnSQE(Generic[T]):
    def __init__(
        self,
        route_info: RouteInfo,
        fn: FnWrapper[T] | AsyncFnWrapper[T],
    ) -> None:
        self.route_info = route_info
        self.fn = fn


class _FnCQE(Generic[T], _Retriable):
    def __init__(
        self,
        sqe: _FnSQE[T],
        fn_result: Result[T, Exception],
    ) -> None:
        self.sqe = sqe
        self.fn_result = fn_result

    def to_be_retried(self) -> bool:
        return _to_be_retried(
            self.fn_result,
            retry_attempt=self.sqe.route_info.retry_attempt,
            retry_policy=self.sqe.route_info.retry_policy,
        )

    def next_retry_delay(self) -> float:
        return _next_retry_delay(
            self.sqe.route_info.retry_policy, self.sqe.route_info.retry_attempt
        )


_SQE: TypeAlias = Union[_FnSQE[Any], _BatchSQE]
_CQE: TypeAlias = Union[_FnCQE[Any], _BatchCQE]


class _Processor:
    def __init__(
        self,
        max_workers: int | None,
        sq: Queue[_SQE],
        scheduler: Scheduler,
    ) -> None:
        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        assert max_workers > 0, "`max_workers` must be positive."
        self._max_workers = max_workers
        self._sq = sq
        self._scheduler = scheduler
        self._threads = set[Thread]()

    def enqueue(self, sqe: _SQE) -> None:
        self._sq.put_nowait(sqe)
        self._adjust_thread_count()

    def _run(self) -> None:
        while True:
            sqe = self._sq.dequeue()
            if isinstance(sqe, _FnSQE):
                fn_result = sqe.fn.run()
                assert isinstance(
                    fn_result, (Ok, Err)
                ), f"{fn_result} must be a result."
                self._scheduler.enqueue_cqe(_FnCQE(sqe, fn_result))
            elif isinstance(sqe, _BatchSQE):
                result: Result[CmdHandlerResult[Any], Exception]
                try:
                    result = Ok(sqe.handler(sqe.ctx, sqe.commands))
                except Exception as e:  # noqa: BLE001
                    result = Err(e)
                self._scheduler.enqueue_cqe(_BatchCQE(sqe, result))
            else:
                assert_never(sqe)

    def _adjust_thread_count(self) -> None:
        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            t = Thread(target=self._run, daemon=True)
            t.start()
            self._threads.add(t)


@final
class Scheduler:
    def __init__(
        self,
        durable_promise_storage: IPromiseStore,
        *,
        logic_group: str = "default",
        tracing_adapter: IAdapter | None = None,
        processor_threads: int | None = None,
        with_long_polling: bool | None = None,
    ) -> None:
        self.logic_group: str = logic_group
        self.pid = uuid.uuid4().hex
        self._registered_function = DoubleDict[str, Any]()
        self._attached_options_to_top_lvl: dict[str, LOptions] = {}

        self._stg_queue = Queue[Promise[Any]]()
        self._completion_queue = Queue[_CQE]()
        self._submission_queue = Queue[_SQE]()
        self._combinators_queue = Queue[tuple[Combinator, Promise[Any]]]()
        self._task_record_queue = Queue[TaskRecord]()

        self._worker_continue = Event()
        self._blocked = Event()

        self._delay_queue = DelayQueue[Union[RouteInfo, _BatchSQE]](
            caller_event=self._worker_continue
        )

        self._deps = Dependencies()
        self._ctx = Context(
            seed=None,
            deps=self._deps,
        )
        self._json_encoder = JsonEncoder()
        self._tracing_adapter: IAdapter = (
            tracing_adapter if tracing_adapter is not None else StdOutAdapter()
        )

        self._runnables: Runnables = deque()
        self._awaiting = Awaiting()

        self._processor = _Processor(
            processor_threads,
            self._submission_queue,
            self,
        )

        self._durable_promise_storage = durable_promise_storage

        self._heartbeating_thread: Thread | None = None
        self._claimed_tasks: dict[str, TaskRecord] | None = None
        self._claim_task_while_creating_top_level: bool = False
        if isinstance(self._durable_promise_storage, ITaskStore):
            self._claim_task_while_creating_top_level = True
            self._claimed_tasks = {}
            self._heartbeating_thread = Thread(target=self._heartbeat, daemon=True)
            self._heartbeating_thread.start()
            if with_long_polling is None or with_long_polling:
                self._long_poller = LongPoller(self)

        self._emphemeral_promise_memo = EphemeralMemo[str, Promise[Any]]()

        self._cmd_handlers = dict[type[Command], tuple[CmdHandler[Any], RetryPolicy]]()
        self._cmd_buffers = dict[type[Command], CommandBuffer[Command]]()

        self._worker_thread = Thread(target=self._run, daemon=True)
        self._worker_thread.start()

    def _heartbeat(self) -> None:
        while True:
            assert isinstance(self._durable_promise_storage, ITaskStore)
            affected = self._durable_promise_storage.heartbeat_tasks(pid=self.pid)
            logger.debug("Heatbeat affected %s tasks", affected)
            time.sleep(2)

    def enqueue_task_record(self, task_record: TaskRecord) -> None:
        self._task_record_queue.put_nowait(task_record)
        self._signal()

    def register_command_handler(
        self,
        cmd: type[C],
        func: CmdHandler[C],
        maxlen: int | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        assert not isgeneratorfunction(
            func
        ), "Batch handlers must be a function. Not a generator."
        assert (
            cmd not in self._cmd_handlers
        ), "There's already a command handler registered for that command."
        assert cmd not in self._cmd_buffers

        if retry_policy is None:
            retry_policy = default_policy()

        self._cmd_handlers[cmd] = (func, retry_policy)
        self._cmd_buffers[cmd] = CommandBuffer(maxlen=maxlen)

    def enqueue_cqe(self, cqe: _CQE) -> None:
        self._completion_queue.put_nowait(cqe)
        self._signal()

    def _enqueue_combinator(
        self, combinator: Combinator, promise: Promise[Any]
    ) -> None:
        assert (
            not promise.done()
        ), "Do not enqueue done promises associated to a combinator."
        self._combinators_queue.put_nowait((combinator, promise))
        self._signal()

    def clear_blocked_flag(self) -> None:
        assert self._blocked.is_set(), "Scheduler must be blocked to use this method."
        self._blocked.clear()

    def _signal(self) -> None:
        self._blocked.clear()
        self._worker_continue.set()

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

    def _register_callback_or_resolve_ephemeral_promise(
        self, promise: Promise[Any]
    ) -> None:
        assert isinstance(promise.action, RFI), "We only register callbacks for rfi"
        assert isinstance(
            self._durable_promise_storage, ITaskStore
        ), "Used storage does not support tasks."

        recv = utils.recv_url(group=self.logic_group, pid=self.pid)
        durable_promise, created_callback = (
            self._durable_promise_storage.create_callback(
                promise_id=promise.promise_id,
                root_promise_id=promise.partition_root().promise_id,
                timeout=sys.maxsize,
                recv=recv,
            )
        )
        if created_callback is not None:
            logger.info(
                "Callback pointing to %s has been registered for when %s is completed",
                recv,
                promise.promise_id,
            )
            return

        assert (
            durable_promise.is_completed()
        ), "Callback won't be created only if durable promise has been completed."
        v = self._get_value_from_durable_promise(durable_promise_record=durable_promise)
        self._resolve_ephemeral_promise(promise, v)
        self._pop_from_memo_or_finish_partition_execution(promise)

    def wait_forever(self) -> None:
        Event().wait()

    def wait_until_blocked(self, timeout: float | None = None) -> None:
        self._blocked.wait(timeout)

    def _create_durable_promise_record(
        self,
        req: CreateDurablePromiseReq,
        *,
        claiming_task: bool,
        registering_callback: bool,
        root_promise_id: str | None,
        recv: str | dict[str, Any] | None,
    ) -> DurablePromiseRecord:
        if claiming_task or registering_callback:
            assert (
                claiming_task ^ registering_callback
            ), "Only one of two parameters can be `True`."

        assert (
            req.promise_id is not None
        ), "Promise id must be user provided or generated by this point."
        ikey = utils.string_to_ikey(req.promise_id)
        strict = False
        headers = None
        data = self._json_encoder.encode(req.data) if req.data is not None else None
        timeout = sys.maxsize

        dp_record: DurablePromiseRecord
        if claiming_task:
            assert isinstance(self._durable_promise_storage, ITaskStore)
            dp_record, task_record = self._durable_promise_storage.create_with_task(
                promise_id=req.promise_id,
                ikey=ikey,
                strict=strict,
                headers=headers,
                data=data,
                timeout=timeout,
                tags=req.tags,
                pid=self.pid,
                ttl=5 * 1_000,
                recv=utils.recv_url(self.logic_group, pid=self.pid),
            )
            if task_record is not None:
                assert self._claimed_tasks is not None
                assert dp_record.promise_id not in self._claimed_tasks
                self._claimed_tasks[dp_record.promise_id] = task_record

        elif registering_callback:
            assert isinstance(self._durable_promise_storage, ITaskStore)
            assert root_promise_id is not None
            assert recv is not None
            dp_record = self._durable_promise_storage.create_with_callback(
                promise_id=req.promise_id,
                ikey=ikey,
                strict=strict,
                timeout=timeout,
                tags=req.tags,
                root_promise_id=root_promise_id,
                recv=recv,
                headers=headers,
                data=data,
            )[0]
        else:
            assert not claiming_task
            assert not registering_callback
            dp_record = self._durable_promise_storage.create(
                promise_id=req.promise_id,
                ikey=ikey,
                strict=strict,
                headers=headers,
                data=data,
                timeout=timeout,
                tags=req.tags,
            )
        return dp_record

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
                v = Ok(self._json_encoder.decode(durable_promise_record.value.data))
        return v

    def _dedup_promise(self, promise_id: str) -> Promise[Any] | None:
        return self._emphemeral_promise_memo.get(promise_id)

    def _create_promise(
        self,
        parent_promise: Promise[Any] | None,
        promise_id: str,
        action: PromiseActions,
        *,
        claiming_task: bool,
        registering_callback: bool,
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

        self._emphemeral_promise_memo.add(p.promise_id, p)

        if not p.durable:
            self._tracing_adapter.process_event(
                PromiseCreated(
                    promise_id=p.promise_id,
                    tick=now(),
                    parent_promise_id=p.parent_promise_id(),
                )
            )

            return p

        create_command = self._promise_to_create_durable_promise_req(p)
        recv: dict[str, Any] | None = None
        root_promise_id: str | None = None
        if registering_callback:
            recv = utils.recv_url(group=self.logic_group, pid=self.pid)
            root_promise_id = p.partition_root().promise_id

        durable_promise_record = self._create_durable_promise_record(
            req=create_command,
            claiming_task=claiming_task,
            registering_callback=registering_callback,
            recv=recv,
            root_promise_id=root_promise_id,
        )
        self._tracing_adapter.process_event(
            PromiseCreated(
                promise_id=p.promise_id,
                tick=now(),
                parent_promise_id=p.parent_promise_id(),
            )
        )

        if durable_promise_record.is_pending():
            return p

        assert (
            durable_promise_record.value.data is not None
        ), "If the promise is not pending, there must be data."

        v = self._get_value_from_durable_promise(
            durable_promise_record=durable_promise_record
        )

        self._resolve_ephemeral_promise(p, v)
        self._pop_from_memo_or_finish_partition_execution(p)
        return p

    def _promise_to_create_durable_promise_req(  # noqa: C901, PLR0912
        self, promise: Promise[Any]
    ) -> CreateDurablePromiseReq:
        req: CreateDurablePromiseReq
        if isinstance(promise.action, LFI):
            if isinstance(promise.action.exec_unit, Command):
                assert not isinstance(
                    promise.action.exec_unit, CreateDurablePromiseReq
                ), "This command is not allowed for lfi"
                req = CreateDurablePromiseReq(promise_id=promise.promise_id)
            elif isinstance(promise.action.exec_unit, FnOrCoroutine):
                func_name = self._registered_function.get_from_value(
                    promise.action.exec_unit.exec_unit
                )
                if func_name is not None:
                    tags = self._attached_options_to_top_lvl[func_name]
                    req = promise.action.exec_unit.to_req(
                        promise_id=promise.promise_id,
                        func_name=func_name,
                        tags=tags.tags,
                    )
                else:
                    req = CreateDurablePromiseReq(promise_id=promise.promise_id)
            else:
                assert_never(promise.action.exec_unit)
        elif isinstance(promise.action, RFI):
            assert isinstance(
                self._durable_promise_storage, ITaskStore
            ), "Used storage does not support rfi."
            if isinstance(promise.action.exec_unit, Command):
                assert isinstance(
                    promise.action.exec_unit, CreateDurablePromiseReq
                ), "This is the only command allowed for rfi"
                req = promise.action.exec_unit
                if req.promise_id is None:
                    req.promise_id = promise.promise_id
            elif isinstance(promise.action.exec_unit, FnOrCoroutine):
                func_name = self._registered_function.get_from_value(
                    promise.action.exec_unit.exec_unit
                )
                assert (
                    func_name is not None
                ), "To do a rfi the function must be registered."
                attached_options = self._attached_options_to_top_lvl[func_name]

                final_tags = attached_options.tags.copy()
                target_url: str
                if promise.action.opts.target is not None:
                    target_url = f"poll://{promise.action.opts.target}"
                else:
                    target_url = f"poll://{self.logic_group}"
                final_tags["resonate:invoke"] = target_url

                req = promise.action.exec_unit.to_req(
                    promise.promise_id,
                    func_name,
                    tags=final_tags,
                )

            else:
                assert_never(promise.action.exec_unit)
        elif isinstance(promise.action, (All, AllSettled, Race)):
            req = CreateDurablePromiseReq(promise_id=promise.promise_id)
        else:
            assert_never(promise.action)
        return req

    def register(
        self,
        func: DurableCoro[P, Hashable] | DurableFn[P, Hashable],
        name: str | None = None,
        retry_policy: RetryPolicy | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        if name is None:
            name = func.__name__
        assert (
            self._registered_function.get(name) is None
        ), "There's already a coroutine registered with this name."
        assert (
            name not in self._attached_options_to_top_lvl
        ), "There's already a coroutine registered with this name."
        self._registered_function.add(name, func)
        self._attached_options_to_top_lvl[name] = LOptions(
            durable=True,
            promise_id=None,
            retry_policy=retry_policy,
            tags=tags,
        )

    def run(
        self,
        promise_id: str,
        coro: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Promise[T]:
        return self.lfi(promise_id, coro, *args, **kwargs)

    def rfc(
        self, promise_id: str, func_name: str, args: list[Any], *, target: str
    ) -> Any:  # noqa: ANN401
        return self.rfi(promise_id, func_name, args, target=target).result()

    def rfi(
        self, promise_id: str, func_name: str, args: list[Any], *, target: str
    ) -> Promise[Any]:
        p = self._dedup_promise(promise_id)
        if p is not None:
            return p

        invokable = remote_function(
            func_name, args, target=target, promise_id=promise_id
        )
        p = self._create_promise(
            parent_promise=None,
            promise_id=promise_id,
            action=RFI(invokable),
            claiming_task=False,
            registering_callback=True,
        )
        self._awaiting.append(p, value=None, awaiting_for="remote")
        return p

    def trigger(
        self, promise_id: str, func_name: str, args: list[Any], *, target: str
    ) -> Promise[Any]:
        return self.rfi(promise_id, func_name, args, target=target)

    def lfc(
        self,
        promise_id: str,
        coro: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        return self.lfi(promise_id, coro, *args, **kwargs).result()

    def lfi(
        self,
        promise_id: str,
        coro: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Promise[T]:
        p = self._dedup_promise(promise_id)
        if p is not None:
            return p

        function_name = self._registered_function.get_from_value(coro)
        assert (
            function_name is not None
        ), f"There's no function registed for function {coro.__name__}."
        attached_options = self._attached_options_to_top_lvl[function_name]

        assert attached_options.durable, "All top level invocation must be durable."

        p = self._create_promise(
            promise_id=promise_id,
            parent_promise=None,
            action=LFI(
                FnOrCoroutine(coro, *args, **kwargs),
                opts=attached_options,
            ),
            claiming_task=self._claim_task_while_creating_top_level,
            registering_callback=False,
        )
        self._stg_queue.put_nowait(p)
        self._signal()
        return p

    def _route_fn_or_coroutine(
        self,
        route_info: RouteInfo,
    ) -> None:
        assert (
            not route_info.promise.done()
        ), "Only unresolved executions of unresolved promises can be passed here."
        if isgeneratorfunction(route_info.fn_or_coroutine.exec_unit):
            coro = route_info.fn_or_coroutine.exec_unit(
                route_info.ctx,
                *route_info.fn_or_coroutine.args,
                **route_info.fn_or_coroutine.kwargs,
            )
            self._add_coro_to_runnables(
                ResonateCoro(route_info, coro),
                None,
                was_awaited=False,
            )

        else:
            self._processor.enqueue(
                _FnSQE(
                    route_info=route_info,
                    fn=wrap_fn(
                        route_info.ctx,
                        route_info.fn_or_coroutine.exec_unit,
                        *route_info.fn_or_coroutine.args,
                        **route_info.fn_or_coroutine.kwargs,
                    ),
                )
            )
        if route_info.retry_attempt == 0:
            self._tracing_adapter.process_event(
                ExecutionInvoked(
                    route_info.promise.promise_id,
                    route_info.promise.parent_promise_id(),
                    now(),
                    route_info.fn_or_coroutine.exec_unit.__name__,
                    route_info.fn_or_coroutine.args,
                    route_info.fn_or_coroutine.kwargs,
                )
            )

    def _resolve_promise_with_cqe(self, cqe: _CQE) -> None:
        awaiting_for: AwaitingFor = "local"
        if isinstance(cqe, _FnCQE):
            promise = cqe.sqe.route_info.promise
            self._tracing_adapter.process_event(
                ExecutionTerminated(
                    promise_id=promise.promise_id,
                    parent_promise_id=promise.parent_promise_id(),
                    tick=now(),
                )
            )
            value = cqe.fn_result
            if promise.durable:
                value = self._resolve_durable_promise(promise, value)
            self._resolve_ephemeral_promise(promise, value=value)
            self._pop_from_memo_or_finish_partition_execution(promise)
            self._unblock_coros_waiting_on_promise(promise, awaiting_for)

        elif isinstance(cqe, _BatchCQE):
            if isinstance(cqe.result, Ok) and isinstance(cqe.result.unwrap(), list):
                result = cqe.result.unwrap()
                assert isinstance(result, list)
                assert len(result) == len(
                    cqe.sqe.promises
                ), "Need equal amount for results and promises."

                for res, p in zip(result, cqe.sqe.promises):
                    self._tracing_adapter.process_event(
                        ExecutionTerminated(
                            promise_id=p.promise_id,
                            parent_promise_id=p.parent_promise_id(),
                            tick=now(),
                        )
                    )
                    value = Err(res) if isinstance(res, Exception) else Ok(res)
                    if p.durable:
                        value = self._resolve_durable_promise(p, value)
                    self._resolve_ephemeral_promise(p, value)
                    self._pop_from_memo_or_finish_partition_execution(p)
                    self._unblock_coros_waiting_on_promise(p, awaiting_for)
            else:
                for p in cqe.sqe.promises:
                    self._tracing_adapter.process_event(
                        ExecutionTerminated(
                            promise_id=p.promise_id,
                            parent_promise_id=p.parent_promise_id(),
                            tick=now(),
                        )
                    )
                    value = cqe.result
                    if p.durable:
                        value = self._resolve_durable_promise(p, value)
                    self._resolve_ephemeral_promise(p, value)
                    self._pop_from_memo_or_finish_partition_execution(p)
                    self._unblock_coros_waiting_on_promise(p, awaiting_for)

        else:
            assert_never(cqe)

    def _run(self) -> None:  # noqa: C901, PLR0912, PLR0915
        while self._worker_continue.wait():
            self._worker_continue.clear()

            delay_es = self._delay_queue.dequeue_all()
            for delay_e in delay_es:
                if isinstance(delay_e, RouteInfo):
                    self._route_fn_or_coroutine(delay_e)
                elif isinstance(delay_e, _BatchSQE):
                    self._processor.enqueue(delay_e)
                else:
                    assert_never(delay_e)

            top_lvls = self._stg_queue.dequeue_all()
            for p in top_lvls:
                assert isinstance(p.action, LFI)
                assert isinstance(
                    p.action.exec_unit, FnOrCoroutine
                ), "Only functions or coroutines can be passed at the top level"
                if p.done():
                    self._unblock_coros_waiting_on_promise(p, "local")
                else:
                    self._route_fn_or_coroutine(
                        RouteInfo(
                            self._ctx,
                            p,
                            0,
                        )
                    )

            task_records = self._task_record_queue.dequeue_all()
            for record in task_records:
                assert isinstance(
                    self._durable_promise_storage, ITaskStore
                ), "We should only be receiving tasks messages if using an storage with tasks support"  # noqa: E501
                msg = self._durable_promise_storage.claim_task(
                    task_id=record.task_id,
                    counter=record.counter,
                    pid=self.pid,
                    ttl=5 * 1_000,
                )

                logger.info(
                    "Message arrived %s on woker %s/%s", msg, self.logic_group, self.pid
                )
                assert self._claimed_tasks is not None
                assert (
                    msg.root_promise_store.promise_id not in self._claimed_tasks
                ), "Only one task at a time for a given promise."

                if isinstance(msg, Invoke):
                    self._handle_invoke(
                        durable_promise_record=msg.root_promise_store, record=record
                    )
                elif isinstance(msg, Resume):
                    root_promise = self._emphemeral_promise_memo.get(
                        msg.root_promise_store.promise_id
                    )
                    if root_promise is None:
                        self._handle_invoke(
                            durable_promise_record=msg.root_promise_store, record=record
                        )
                    else:
                        self._handle_resume(
                            durable_promise_record=msg.leaf_promise_store, record=record
                        )
                else:
                    assert_never(msg)

            cqes = self._completion_queue.dequeue_all()
            for cqe in cqes:
                if cqe.to_be_retried():
                    delay = cqe.next_retry_delay()

                    if isinstance(cqe, _FnCQE):
                        cqe.sqe.route_info.retry_attempt += 1
                        self._delay_queue.put_nowait(
                            cqe.sqe.route_info,
                            delay=delay,
                        )
                    elif isinstance(cqe, _BatchCQE):
                        cqe.sqe.retry_attempt += 1
                        self._delay_queue.put_nowait(
                            cqe.sqe,
                            delay=delay,
                        )
                    else:
                        assert_never(cqe)
                else:
                    self._resolve_promise_with_cqe(cqe)

            combinators = self._combinators_queue.dequeue_all()
            for combinator, p in combinators:
                assert not p.done(), (
                    "If the promise related to a combinator is resolved"
                    "already the combinator should not be in the combinators queue"
                )
                if self._combinator_done(combinator):
                    value = self._combinator_result(combinator)
                    if p.durable:
                        value = self._resolve_durable_promise(p, value)
                    self._resolve_ephemeral_promise(p, value)
                    self._pop_from_memo_or_finish_partition_execution(p)
                    self._unblock_coros_waiting_on_promise(p, "local")
                else:
                    self._enqueue_combinator(combinator, p)

            while self._runnables:
                runnable, was_awaited = self._runnables.pop()
                self._advance_runnable_span(runnable=runnable, was_awaited=was_awaited)

            assert len(self._runnables) == 0, "Runnables should have been all exhausted"

            if isinstance(self._durable_promise_storage, ITaskStore):
                assert self._claimed_tasks is not None
                if len(self._claimed_tasks) == 0:
                    self._blocked.set()
            elif self._emphemeral_promise_memo.is_empty():
                self._blocked.set()

    def _all_non_completed_leafs_are_remote_and_in_awaiting(
        self, root_promise: Promise[Any]
    ) -> bool:
        return all(
            self._awaiting.waiting_for(leaf)
            for leaf in root_promise.leaf_promises
            if (isinstance(leaf.action, RFI) and not leaf.done())
        )

    def _handle_resume(
        self,
        durable_promise_record: DurablePromiseRecord,
        record: TaskRecord,
    ) -> None:
        leaf_promise = self._emphemeral_promise_memo.get(
            durable_promise_record.promise_id
        )
        # In theory it's possible this assertion get's raised. But very unlikely
        assert (
            leaf_promise is not None
        ), "If the root is there, we expect the leaf is also there."

        root_promise_id = leaf_promise.partition_root().promise_id
        assert (
            self._emphemeral_promise_memo.get(root_promise_id) is not None
        ), "Root promise must be tracked on memo"
        assert (
            self._claimed_tasks is not None
        ), "You can ony handle resumes if using a storage that supports tasks."
        assert (
            root_promise_id not in self._claimed_tasks
        ), f"{root_promise_id} is already monitored by a task."
        self._claimed_tasks[root_promise_id] = record
        if not leaf_promise.done():
            v = self._get_value_from_durable_promise(durable_promise_record)
            self._resolve_ephemeral_promise(leaf_promise, v)

        self._pop_from_memo_or_finish_partition_execution(promise=leaf_promise)
        assert self._awaiting.waiting_for(
            leaf_promise, "remote"
        ), f"There must be something waiting for {leaf_promise.promise_id}"
        self._unblock_coros_waiting_on_promise(leaf_promise, "remote")

    def _handle_invoke(
        self, durable_promise_record: DurablePromiseRecord, record: TaskRecord
    ) -> None:
        promise_id = durable_promise_record.promise_id
        assert (
            self._claimed_tasks is not None
        ), "You can only handle invokes if using a storage that supports tasks."
        assert (
            promise_id not in self._claimed_tasks
        ), f"{promise_id} is already monitored by a task"
        self._claimed_tasks[promise_id] = record

        invoke_info = durable_promise_record.invoke_info()
        func_name = invoke_info["func_name"]
        func_pointer = self._registered_function.get(func_name)
        assert (
            func_pointer is not None
        ), f"Function {func_name} must be registered to invoke"

        attached_options = self._attached_options_to_top_lvl[func_name]

        p = self._emphemeral_promise_memo.get(promise_id)
        local_action = LFI(
            FnOrCoroutine(func_pointer, *invoke_info["args"], **invoke_info["kwargs"]),
            opts=attached_options,
        )
        if p is None:
            p = Promise[Any](
                promise_id=promise_id,
                parent_promise=None,
                action=local_action,
            )
            self._emphemeral_promise_memo.add(p.promise_id, p)

        else:
            assert isinstance(
                p.action, RFI
            ), "This promise must have been created with RFI"
            p.action = local_action
            p.mark_as_partition_root()

        self._stg_queue.put_nowait(p)
        self._signal()

    def _complete_task_monitoring_promise(self, promise_id: str) -> None:
        assert (
            self._claimed_tasks is not None
        ), "Can only complete tasks if using a storage that supports tasks."
        record = self._claimed_tasks.pop(promise_id)
        assert isinstance(self._durable_promise_storage, ITaskStore)
        self._durable_promise_storage.complete_task(
            task_id=record.task_id, counter=record.counter
        )
        logger.info(
            "Task related to promise %s has been completed from worker %s/%s",
            promise_id,
            self.logic_group,
            self.pid,
        )

    def _add_coro_to_awaitables(
        self,
        p: Promise[Any],
        coro: ResonateCoro[Any],
        awaiting_for: AwaitingFor,
    ) -> None:
        assert (
            not p.done()
        ), "If the promise is resolved already it makes no sense to block coroutine"
        self._awaiting.append(p, coro, awaiting_for)

        if isinstance(p.action, LFI) and isinstance(p.action.exec_unit, Command):
            assert not isinstance(
                p.action.exec_unit, CreateDurablePromiseReq
            ), "This command is reserved for rfi."
            self._send_pending_commands_to_processor(type(p.action.exec_unit))

        promise = coro.route_info.promise
        self._tracing_adapter.process_event(
            ExecutionAwaited(
                promise_id=promise.promise_id,
                tick=now(),
                parent_promise_id=promise.parent_promise_id(),
            )
        )
        if awaiting_for == "local":
            return

        partition_root = promise.partition_root()
        if self._all_non_completed_leafs_are_remote_and_in_awaiting(partition_root):
            self._complete_task_monitoring_promise(promise_id=partition_root.promise_id)

    def _add_coro_to_runnables(
        self,
        coro: ResonateCoro[Any],
        value_to_yield_back: Result[Any, Exception] | None,
        *,
        was_awaited: bool,
    ) -> None:
        self._runnables.appendleft((Runnable(coro, value_to_yield_back), was_awaited))

    def _unblock_coros_waiting_on_promise(
        self,
        p: Promise[Any],
        awaiting_for: AwaitingFor,
    ) -> None:
        assert p.done(), "Promise must be done to unblock waiting coroutines."
        if self._awaiting.get(p, awaiting_for) is None:
            return

        for coro in self._awaiting.pop(p, awaiting_for):
            self._add_coro_to_runnables(
                coro=coro,
                value_to_yield_back=p.safe_result(),
                was_awaited=True,
            )

    def _resolve_durable_promise(
        self, promise: Promise[T], value: Result[T, Exception]
    ) -> Result[T, Exception]:
        assert promise.durable, "Promise must be durable to resolve durable promise"
        completed_record = self._complete_durable_promise_record(
            promise_id=promise.promise_id,
            value=value,
        )
        assert (
            completed_record.is_completed()
        ), "Durable promise record must be completed by this point."
        return self._get_value_from_durable_promise(completed_record)

    def _resolve_ephemeral_promise(
        self, promise: Promise[T], value: Result[T, Exception]
    ) -> None:
        promise.set_result(value)

        assert self._emphemeral_promise_memo.has(
            promise.promise_id
        ), "Ephemeral process must have been registered in the memo."

        self._tracing_adapter.process_event(
            PromiseCompleted(
                promise_id=promise.promise_id,
                tick=now(),
                value=value,
                parent_promise_id=promise.parent_promise_id(),
            )
        )
        if isinstance(self._durable_promise_storage, ITaskStore):
            assert self._claimed_tasks is not None
            if promise.promise_id in self._claimed_tasks:
                self._complete_task_monitoring_promise(promise.promise_id)

    def _pop_from_memo_or_finish_partition_execution(
        self, promise: Promise[Any]
    ) -> None:
        if promise.is_marked_as_partition_root():
            promise.unmark_as_partition_root()
        else:
            self._emphemeral_promise_memo.pop(promise.promise_id)

    def _send_pending_commands_to_processor(self, cmd_type: type[Command]) -> None:
        cmd_buffer = self._cmd_buffers.get(cmd_type)
        assert (
            cmd_buffer is not None
        ), f"There must be a buffer for cmd {cmd_type.__name__}"
        if cmd_buffer.is_empty():
            return

        cmd_handler_and_retry_policy = self._cmd_handlers.get(cmd_type)
        assert (
            cmd_handler_and_retry_policy is not None
        ), "There must be a command handler for given command."
        cmds_to_send, promises_to_resolve = cmd_buffer.pop_all()

        handler, retry_policy = cmd_handler_and_retry_policy
        self._processor.enqueue(
            _BatchSQE(
                ctx=self._ctx,
                promises=promises_to_resolve,
                commands=cmds_to_send,
                handler=handler,
                retry_attempt=0,
                retry_policy=retry_policy,
            )
        )

    def _advance_runnable_span(  # noqa: C901, PLR0912, PLR0915. Note: We want to keep all the control flow in the function
        self, runnable: Runnable[Any], *, was_awaited: bool
    ) -> None:
        yieldable_or_final_value = iterate_coro(runnable=runnable)

        if was_awaited:
            self._tracing_adapter.process_event(
                ExecutionResumed(
                    promise_id=runnable.coro.route_info.promise.promise_id,
                    tick=now(),
                    parent_promise_id=runnable.coro.route_info.promise.parent_promise_id(),
                )
            )

        if isinstance(yieldable_or_final_value, FinalValue):
            final_value = yieldable_or_final_value.v
            if _to_be_retried(
                final_value,
                runnable.coro.route_info.retry_policy,
                runnable.coro.route_info.retry_attempt,
            ):
                runnable.coro.route_info.retry_attempt += 1

                self._delay_queue.put_nowait(
                    runnable.coro.route_info,
                    delay=_next_retry_delay(
                        runnable.coro.route_info.retry_policy,
                        runnable.coro.route_info.retry_attempt,
                    ),
                )

            else:
                promise = runnable.coro.route_info.promise
                self._tracing_adapter.process_event(
                    ExecutionTerminated(
                        promise_id=promise.promise_id,
                        tick=now(),
                        parent_promise_id=promise.parent_promise_id(),
                    )
                )
                value = final_value
                if promise.durable:
                    value = self._resolve_durable_promise(promise, value)
                self._resolve_ephemeral_promise(promise, value)
                self._pop_from_memo_or_finish_partition_execution(promise)
                self._unblock_coros_waiting_on_promise(promise, "local")

        elif isinstance(yieldable_or_final_value, LFC):
            p = self._process_local_invocation(
                yieldable_or_final_value.to_invocation(), runnable
            )
            assert not self._awaiting.waiting_for(
                p
            ), "Since it's a call it should be a promise without dependants"

            if p.done():
                self._add_coro_to_runnables(
                    runnable.coro,
                    p.safe_result(),
                    was_awaited=False,
                )
            else:
                self._add_coro_to_awaitables(p, runnable.coro, "local")

        elif isinstance(yieldable_or_final_value, LFI):
            p = self._process_local_invocation(yieldable_or_final_value, runnable)
            self._add_coro_to_runnables(
                runnable.coro,
                Ok(p),
                was_awaited=False,
            )

        elif isinstance(yieldable_or_final_value, Promise):
            p = yieldable_or_final_value
            awaiting_for: AwaitingFor = (
                "remote" if isinstance(p.action, RFI) else "local"
            )
            if p.done():
                self._unblock_coros_waiting_on_promise(p, awaiting_for)
                self._add_coro_to_runnables(
                    runnable.coro,
                    p.safe_result(),
                    was_awaited=False,
                )
            else:
                if awaiting_for == "remote":
                    self._register_callback_or_resolve_ephemeral_promise(p)

                if p.done():
                    self._unblock_coros_waiting_on_promise(p, awaiting_for)
                    self._add_coro_to_runnables(
                        runnable.coro,
                        p.safe_result(),
                        was_awaited=False,
                    )
                else:
                    self._add_coro_to_awaitables(p, runnable.coro, awaiting_for)

        elif isinstance(yieldable_or_final_value, (All, AllSettled, Race)):
            p = self._process_combinator(yieldable_or_final_value, runnable)
            self._add_coro_to_runnables(runnable.coro, Ok(p), was_awaited=False)

        elif isinstance(yieldable_or_final_value, DeferredInvocation):
            deferred_p: Promise[Any] = self.lfi(
                yieldable_or_final_value.promise_id,
                yieldable_or_final_value.coro.exec_unit,
                *yieldable_or_final_value.coro.args,
                **yieldable_or_final_value.coro.kwargs,
            )
            self._add_coro_to_runnables(
                runnable.coro,
                Ok(deferred_p),
                was_awaited=False,
            )

        elif isinstance(yieldable_or_final_value, RFI):
            p = self._process_remote_invocation(
                yieldable_or_final_value,
                runnable,
                registering_callback=False,
            )
            self._add_coro_to_runnables(
                runnable.coro,
                Ok(p),
                was_awaited=False,
            )
        elif isinstance(yieldable_or_final_value, RFC):
            p = self._process_remote_invocation(
                yieldable_or_final_value.to_invocation(),
                runnable,
                registering_callback=True,
            )
            assert not self._awaiting.waiting_for(
                p
            ), "Since it's a call it should be a promise without dependants"
            if p.done():
                self._add_coro_to_runnables(
                    runnable.coro,
                    p.safe_result(),
                    was_awaited=False,
                )
            else:
                self._add_coro_to_awaitables(p, runnable.coro, "remote")

        else:
            assert_never(yieldable_or_final_value)

    def _process_remote_invocation(
        self, invocation: RFI, runnable: Runnable[Any], *, registering_callback: bool
    ) -> Promise[Any]:
        assert isinstance(
            self._durable_promise_storage, ITaskStore
        ), "Used storage does not support rfi."
        promise_id: str | None
        if not isinstance(invocation.exec_unit, Command):
            promise_id = invocation.opts.promise_id
        else:
            assert isinstance(invocation.exec_unit, CreateDurablePromiseReq)
            promise_id = invocation.exec_unit.promise_id
        if promise_id is None:
            promise_id = runnable.coro.route_info.promise.child_name()

        p = self._dedup_promise(promise_id)
        if p is not None:
            return p

        return self._create_promise(
            parent_promise=runnable.coro.route_info.promise,
            promise_id=promise_id,
            action=invocation,
            claiming_task=False,
            registering_callback=registering_callback,
        )

    def _process_local_invocation(
        self, invocation: LFI, runnable: Runnable[Any]
    ) -> Promise[Any]:
        promise_id = (
            invocation.opts.promise_id
            if invocation.opts.promise_id is not None
            else runnable.coro.route_info.promise.child_name()
        )
        p = self._dedup_promise(promise_id)
        if p is not None:
            return p

        p = self._create_promise(
            parent_promise=runnable.coro.route_info.promise,
            promise_id=promise_id,
            action=invocation,
            claiming_task=False,
            registering_callback=False,
        )

        if isinstance(invocation.exec_unit, Command):
            assert not isinstance(
                invocation.exec_unit, CreateDurablePromiseReq
            ), "This command is reserved only for rfi."

            cmd_type = type(invocation.exec_unit)
            cmd_queue = self._cmd_buffers[cmd_type]
            cmd_queue.add(invocation.exec_unit, p)
            if cmd_queue.is_full():
                self._send_pending_commands_to_processor(cmd_type)

        elif isinstance(invocation.exec_unit, FnOrCoroutine):
            if p.done():
                self._unblock_coros_waiting_on_promise(p, "local")
            else:
                self._route_fn_or_coroutine(
                    RouteInfo(
                        ctx=runnable.coro.route_info.ctx,
                        promise=p,
                        retry_attempt=0,
                    )
                )
        else:
            assert_never(invocation.exec_unit)
        return p

    def _process_combinator(
        self, combinator: Combinator, runnable: Runnable[Any]
    ) -> Promise[Any]:
        promise_id = (
            combinator.opts.promise_id
            if combinator.opts.promise_id is not None
            else runnable.coro.route_info.promise.child_name()
        )
        p = self._dedup_promise(promise_id)
        if p is not None:
            return p

        p = self._create_promise(
            parent_promise=runnable.coro.route_info.promise,
            promise_id=promise_id,
            action=combinator,
            claiming_task=False,
            registering_callback=False,
        )

        if p.done():
            self._unblock_coros_waiting_on_promise(p, "local")
        else:
            self._enqueue_combinator(combinator, p)

        return p

    def _combinator_done(self, combinator: Combinator) -> bool:
        if isinstance(combinator, (All, AllSettled)):
            return all_promises_are_done(combinator.promises)

        if isinstance(combinator, Race):
            return any_promise_is_done(combinator.promises)

        assert_never(combinator)

    def _combinator_result(
        self, combinator: Combinator
    ) -> Result[
        T | list[T | Exception], Exception
    ]:  # Note: We can't possible have single type for all the promises of a Combinator
        if isinstance(combinator, All):
            try:
                return Ok([p.result() for p in combinator.promises])
            except Exception as err:  # noqa: BLE001, Note: We can not predict which Exception we will receive from the user
                return Err(err)

        if isinstance(combinator, AllSettled):
            if not combinator.promises:
                return Ok([])

            res = []
            for p in combinator.promises:
                try:
                    ok = p.result()
                    res.append(ok)
                except Exception as err:  # noqa: BLE001, PERF203
                    res.append(err)

            return Ok(res)

        if isinstance(combinator, Race):
            try:
                return Ok(next(p.result() for p in combinator.promises if p.done()))
            except Exception as err:  # noqa: BLE001, Note: We can not predict which Exception we will receive from the user
                return Err(err)

        assert_never(combinator)
