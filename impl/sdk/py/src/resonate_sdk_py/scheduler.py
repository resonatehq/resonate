from __future__ import annotations

from asyncio import iscoroutinefunction
from concurrent.futures import Future
from dataclasses import dataclass
from functools import partial
from inspect import isgeneratorfunction
from queue import Queue
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Callable, Generic, Union, cast

from result import Err, Ok, Result
from typing_extensions import ParamSpec, TypeAlias, TypeVar, assert_never

from resonate_sdk_py import utils
from resonate_sdk_py.logging import logger
from resonate_sdk_py.processor import SQE, IAsyncCommand, ICommand, Processor

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator


T = TypeVar("T")
P = ParamSpec("P")


class FnCmd(ICommand[T]):
    def __init__(
        self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs
    ) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self) -> Result[T, Exception]:
        result: Result[T, Exception]
        try:
            result = Ok(self.fn(*self.args, **self.kwargs))
        except Exception as e:  # noqa: BLE001
            result = Err(e)
        return result


class AsyncFnCmd(IAsyncCommand[T]):
    def __init__(
        self,
        fn: Callable[P, Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    async def run(self) -> Result[T, Exception]:
        result: Result[T, Exception]
        try:
            result = Ok(await self.fn(*self.args, **self.kwargs))
        except Exception as e:  # noqa: BLE001
            result = Err(e)
        return result


def _wrap_fn_into_cmd(
    fn: Callable[P, T | Coroutine[Any, Any, T]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> FnCmd[T] | AsyncFnCmd[T]:
    cmd: AsyncFnCmd[T] | FnCmd[T]
    if iscoroutinefunction(func=fn):
        cmd = AsyncFnCmd(fn, *args, **kwargs)
    else:
        cmd = FnCmd(cast(Callable[P, T], fn), *args, **kwargs)
    return cmd


class Call:
    def __init__(
        self,
        fn: Callable[P, Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs


class Invoke:
    def __init__(
        self,
        fn: Callable[P, Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs


class Promise(Generic[T]):
    def __init__(self) -> None:
        self.f = Future[T]()

    def result(self, timeout: float | None = None) -> T:
        return self.f.result(timeout=timeout)

    def set_result(self, result: Result[T, Exception]) -> None:
        if isinstance(result, Ok):
            self.f.set_result(result.unwrap())
        elif isinstance(result, Err):
            self.f.set_exception(result.err())
        else:
            assert_never(result)

    def done(self) -> bool:
        return self.f.done()


Yieldable: TypeAlias = Union[Call, Invoke, Promise[Any]]


@dataclass(frozen=True)
class CoroAndPromise(Generic[T]):
    coro: Generator[Yieldable, Any, T]
    prom: Promise[T]


@dataclass(frozen=True)
class Runnable(Generic[T]):
    coro_and_promise: CoroAndPromise[T]
    next_value: Result[Any, Exception] | None


WaitingForPromiseResolution: TypeAlias = dict[Promise[Any], list[CoroAndPromise[Any]]]
PendingToRun: TypeAlias = list[Runnable[Any]]


@dataclass(frozen=True)
class _FinalValue(Generic[T]):
    v: Result[T, Exception]


def iterate_coro(runnable: Runnable[T]) -> Yieldable | _FinalValue[T]:
    yieldable: Yieldable
    try:
        if runnable.next_value is None:
            # next_value is None means we need to initialize the coro
            logger.debug("Initializing coro")
            yieldable = next(runnable.coro_and_promise.coro)
        elif isinstance(runnable.next_value, Ok):
            # next_value is Ok mean we can pass a value to the coro
            logger.debug(
                "Sending successfull value to coro `%s`", runnable.next_value.unwrap()
            )
            yieldable = runnable.coro_and_promise.coro.send(
                runnable.next_value.unwrap()
            )
        elif isinstance(runnable.next_value, Err):
            # next_value is Err mean we can throw an error into the coro
            logger.debug("Sending error to coro `%s`", runnable.next_value.err())
            yieldable = runnable.coro_and_promise.coro.throw(runnable.next_value.err())
        else:
            assert_never(runnable.next_value)
    except StopIteration as e:
        # if stop iteraton is raised it means we finished the coro execution
        return _FinalValue(Ok(e.value))
    except Exception as e:  # noqa: BLE001
        return _FinalValue(Err(e))
    return yieldable


def _unblock_depands_coros(
    p: Promise[T],
    waiting: WaitingForPromiseResolution,
    runnables: PendingToRun,
) -> None:
    assert p.done(), "Promise must be done to unblock dependant coros"

    if waiting.get(p) is None:
        return

    res: Result[T, Exception]
    try:
        res = Ok(p.result())
    except Exception as e:  # noqa: BLE001
        res = Err(e)

    dependant_coros = waiting.pop(p)
    logger.debug("Unblocking `%s` pending promises", len(dependant_coros))

    new_runnables = (Runnable(c_and_p, next_value=res) for c_and_p in dependant_coros)
    runnables.extend(new_runnables)


def _callback(
    p: Promise[T],
    waiting_for_promise: WaitingForPromiseResolution,
    pending_to_run: PendingToRun,
    v: Result[T, Exception],
) -> None:
    p.set_result(v)
    _unblock_depands_coros(p=p, waiting=waiting_for_promise, runnables=pending_to_run)


def _handle_return_promise(
    running: Runnable[T],
    prom: Promise[T],
    waiting_for_prom: WaitingForPromiseResolution,
    pending_to_run: list[Runnable[Any]],
) -> None:
    if prom.done():
        _unblock_depands_coros(
            p=prom,
            waiting=waiting_for_prom,
            runnables=pending_to_run,
        )
    else:
        waiting_for_expired_prom = waiting_for_prom.pop(running.coro_and_promise.prom)
        waiting_for_prom[prom].extend(waiting_for_expired_prom)


class Scheduler:
    def __init__(self, batch_size: int = 5, max_wokers: int | None = None) -> None:
        self._stg_q = Queue[CoroAndPromise[Any]]()
        self._w_thread: Thread | None = None
        self._w_continue = Event()
        self._processor = Processor(
            max_workers=max_wokers,
            scheduler_continue=self._w_continue,
        )
        self._batch_size = batch_size

    def add_multiple(
        self, coros: list[Generator[Yieldable, Any, T]]
    ) -> list[Promise[T]]:
        promises: list[Promise[T]] = []
        for coro in coros:
            p = self.add(coro=coro)
            promises.append(p)
        return promises

    def add(
        self,
        coro: Generator[Yieldable, Any, T],
    ) -> Promise[T]:
        p = Promise[T]()
        self._stg_q.put(item=CoroAndPromise(coro, p))
        self._w_continue.set()
        if self._w_thread is None:
            self._run_worker()
        return p

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
        pending_to_run: PendingToRun = []
        waiting_for_prom_resolution: WaitingForPromiseResolution = {}

        while True:
            n_pending_to_run = len(pending_to_run)
            if n_pending_to_run == 0:
                self._w_continue.wait()
                self._w_continue.clear()

            self._run_cqe_callbacks()

            new_r = self._runnables_from_stg_q()

            if new_r is not None:
                pending_to_run.extend(new_r)

            for _ in range(n_pending_to_run):
                runnable = pending_to_run.pop()
                self._process_each_runnable(
                    runnable=runnable,
                    waiting_for_promise=waiting_for_prom_resolution,
                    pending_to_run=pending_to_run,
                )

    def _process_each_runnable(
        self,
        runnable: Runnable[Any],
        pending_to_run: PendingToRun,
        waiting_for_promise: WaitingForPromiseResolution,
    ) -> None:
        yieldable_or_final_value = iterate_coro(runnable)

        if isinstance(yieldable_or_final_value, _FinalValue):
            value = yieldable_or_final_value.v
            logger.debug("Processing final value `%s`", value)
            runnable.coro_and_promise.prom.set_result(value)
            _unblock_depands_coros(
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
                _unblock_depands_coros(
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
            self._processor.enqueue(
                SQE(
                    cmd=_wrap_fn_into_cmd(call.fn, *call.args, **call.kwargs),
                    callback=partial(
                        _callback,
                        p,
                        waiting_for_promise,
                        pending_to_run,
                    ),
                )
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
            self._processor.enqueue(
                SQE(
                    cmd=_wrap_fn_into_cmd(
                        invocation.fn, *invocation.args, **invocation.kwargs
                    ),
                    callback=partial(
                        _callback,
                        p,
                        waiting_for_promise,
                        pending_to_run,
                    ),
                )
            )
        else:
            coro = invocation.fn(*invocation.args, **invocation.kwargs)
            pending_to_run.append(Runnable(CoroAndPromise(coro, p), next_value=None))

    def _runnables_from_stg_q(self) -> PendingToRun | None:
        new_runnables: PendingToRun = []
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
