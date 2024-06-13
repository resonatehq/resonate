from __future__ import annotations

from asyncio import iscoroutinefunction
from concurrent.futures import Future
from dataclasses import dataclass
from functools import partial
from logging import getLogger
from queue import Queue
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Callable, Generic, Union, cast

from result import Err, Ok, Result
from typing_extensions import ParamSpec, TypeAlias, TypeVar, assert_never

from resonate_sdk_py.processor import SQE, IAsyncCommand, ICommand, Processor

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator

logger = getLogger(__name__)

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
PedingToRun: TypeAlias = list[Runnable[Any]]


def iterate_coro(runnable: Runnable[T]) -> tuple[Yieldable | T, bool]:
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
        return e.value, True
    return yieldable, False


def _resolve_promise(p: Promise[T], value: Result[T, Exception]) -> Promise[T]:
    logger.debug("Solving promise.")
    p.set_result(value)
    return p


def _unblock_depands_coros(
    p: Promise[T],
    waiting: WaitingForPromiseResolution,
    runnables: PedingToRun,
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
    pending_to_run: PedingToRun,
    v: Result[T, Exception],
) -> None:
    p = _resolve_promise(p=p, value=v)
    _unblock_depands_coros(p=p, waiting=waiting_for_promise, runnables=pending_to_run)


def _run_cqe_callbacks(processor: Processor) -> None:
    if processor.cq_qsize() > 0:
        logger.debug("Popping from the completion queue")
        cqe = processor.dequeue()
        cqe.callback(cqe.cmd_result)


def _runnables_from_stg_q(
    stg_q: Queue[CoroAndPromise[T]],
) -> PedingToRun | None:
    new_runnables: PedingToRun = []
    if stg_q.qsize() > 0:
        logger.debug("Popping from the staging queue")
        coro_and_prom = stg_q.get()
        new_runnables.append(Runnable(coro_and_promise=coro_and_prom, next_value=None))
    if len(new_runnables) == 0:
        return None
    return new_runnables


def _handle_call(
    call: Call,
    r: Runnable[T],
    processor: Processor,
    waiting_for_promise: WaitingForPromiseResolution,
    pending_to_run: PedingToRun,
) -> None:
    logger.debug("Processing call")
    p = Promise[Any]()
    waiting_for_promise[p] = [r.coro_and_promise]

    processor.enqueue(
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


def _handle_invocation(
    invocation: Invoke,
    r: Runnable[T],
    processor: Processor,
    pending_to_run: PedingToRun,
    waiting_for_promise: WaitingForPromiseResolution,
) -> None:
    logger.debug("Processing invocation")
    p = Promise[Any]()
    pending_to_run.append(Runnable(r.coro_and_promise, Ok(p)))
    processor.enqueue(
        SQE(
            cmd=_wrap_fn_into_cmd(invocation.fn, *invocation.args, **invocation.kwargs),
            callback=partial(
                _callback,
                p,
                waiting_for_promise,
                pending_to_run,
            ),
        )
    )


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


def _handle_promise(
    r: Runnable[T],
    prom: Promise[Any],
    waiting_for_prom: WaitingForPromiseResolution,
    pending_to_run: list[Runnable[Any]],
) -> None:
    waiting_for_prom[prom] = [r.coro_and_promise]
    if prom.done():
        _unblock_depands_coros(
            p=prom,
            waiting=waiting_for_prom,
            runnables=pending_to_run,
        )


def _worker(
    kill_event: Event,
    stg_q: Queue[CoroAndPromise[T]],
    processor: Processor,
) -> None:
    pending_to_run: PedingToRun = []
    waiting_for_prom_resolution: WaitingForPromiseResolution = {}
    while not kill_event.is_set():
        _run_cqe_callbacks(processor=processor)

        for p in waiting_for_prom_resolution:
            if p.done():
                _unblock_depands_coros(
                    p=p, waiting=waiting_for_prom_resolution, runnables=pending_to_run
                )

        new_r = _runnables_from_stg_q(stg_q=stg_q)
        if new_r is not None:
            pending_to_run.extend(new_r)

        if len(pending_to_run) == 0:
            continue

        r: Runnable[Any] = pending_to_run.pop()
        try:
            yieldable_or_final_value, is_return_v = iterate_coro(r)
        except Exception as e:  # noqa: BLE001
            logger.debug("Processing final error `%s`", e)
            r.coro_and_promise.prom.set_result(Err(e))
            continue

        if isinstance(yieldable_or_final_value, Call):
            assert not is_return_v, "Call cannot be the return value of the coro"
            _handle_call(
                call=yieldable_or_final_value,
                r=r,
                processor=processor,
                pending_to_run=pending_to_run,
                waiting_for_promise=waiting_for_prom_resolution,
            )

        elif isinstance(yieldable_or_final_value, Invoke):
            assert not is_return_v, "Invoke cannot be the return value of the coro"
            _handle_invocation(
                invocation=yieldable_or_final_value,
                r=r,
                processor=processor,
                pending_to_run=pending_to_run,
                waiting_for_promise=waiting_for_prom_resolution,
            )

        elif isinstance(yieldable_or_final_value, Promise):
            if is_return_v:
                _handle_return_promise(
                    running=r,
                    prom=yieldable_or_final_value,
                    waiting_for_prom=waiting_for_prom_resolution,
                    pending_to_run=pending_to_run,
                )
            else:
                _handle_promise(
                    r=r,
                    prom=yieldable_or_final_value,
                    waiting_for_prom=waiting_for_prom_resolution,
                    pending_to_run=pending_to_run,
                )

        else:
            logger.debug("Processing final value %s", yieldable_or_final_value)
            _callback(
                p=r.coro_and_promise.prom,
                waiting_for_promise=waiting_for_prom_resolution,
                pending_to_run=pending_to_run,
                v=Ok(yieldable_or_final_value),
            )

    logger.debug("Scheduler killed")


class Scheduler:
    def __init__(self, max_wokers: int | None = None) -> None:
        self._stg_q = Queue[CoroAndPromise[Any]]()
        self._w_thread: Thread | None = None
        self._w_kill = Event()
        self._processor = Processor(max_workers=max_wokers)

    def add(
        self,
        fn: Callable[P, Generator[Yieldable, Any, T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Promise[T]:
        p = Promise[T]()
        coro = fn(*args, **kwargs)
        self._stg_q.put(item=CoroAndPromise(coro, p))
        if self._w_thread is None:
            self._run_worker()
        return p

    def _run_worker(self) -> None:
        assert self._w_thread is None, "Worker thread already exists"
        if self._w_thread is None:
            t = Thread(
                target=_worker,
                args=(
                    self._w_kill,
                    self._stg_q,
                    self._processor,
                ),
                daemon=True,
            )
            t.start()

            self._w_thread = t

    def close(self) -> None:
        self._processor.close()
        self._w_kill.set()
        assert self._w_thread is not None, "Worker thread was never initialized"
        self._w_thread.join()
        assert not self._w_thread.is_alive()
