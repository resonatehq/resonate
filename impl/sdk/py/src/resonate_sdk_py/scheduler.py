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


@dataclass(frozen=True)
class Blocker(Generic[T]):
    coro_and_promise: CoroAndPromise[T]
    prom_blocking: Promise[Any]


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
        logger.debug("Coro final value reached `%s`", e.value)
        return e.value, True
    return yieldable, False


def _invocation_callback(pending_prom: Promise[T], v: Result[T, Exception]) -> None:
    pending_prom.set_result(v)


def _callback(
    blockers: list[Blocker[Any]],
    blocker: Blocker[T],
    runnables: list[Runnable[Any]],
    v: Result[T, Exception],
) -> None:
    for b in blockers:
        if b.coro_and_promise.prom == blocker.prom_blocking:
            blockers.remove(b)
            b.coro_and_promise.prom.set_result(v)
            runnables.append(Runnable(b.coro_and_promise, v))
    blockers.remove(blocker)
    blocker.prom_blocking.set_result(v)
    runnables.append(Runnable(blocker.coro_and_promise, v))


def _worker(
    kill_event: Event,
    stg_q: Queue[CoroAndPromise[T]],
    processor: Processor,
) -> None:
    runnables: list[Runnable[Any]] = []
    blockers: list[Blocker[Any]] = []
    while not kill_event.is_set():
        while processor.cq_qsize() > 0:
            logger.debug("Popping from the completion queue.")
            cqe = processor.dequeue()
            cqe.callback(cqe.cmd_result)

        while stg_q.qsize() > 0:
            logger.debug("Popping from the staging queue")
            coro_and_prom = stg_q.get()
            runnables.append(Runnable(coro_and_promise=coro_and_prom, next_value=None))

        if len(runnables) == 0:
            continue

        r = runnables.pop()
        try:
            yieldable_or_final_value, end_reached = iterate_coro(r)
        except Exception as e:  # noqa: BLE001
            logger.debug("Processing final error")
            r.coro_and_promise.prom.set_result(Err(e))
            continue

        if isinstance(yieldable_or_final_value, Call):
            call = yieldable_or_final_value
            logger.debug("Processing call")
            p = Promise[Any]()
            b = Blocker(coro_and_promise=r.coro_and_promise, prom_blocking=p)
            blockers.append(b)
            processor.enqueue(
                SQE(
                    cmd=_wrap_fn_into_cmd(call.fn, *call.args, **call.kwargs),
                    callback=partial(_callback, blockers, b, runnables),
                )
            )

        elif isinstance(yieldable_or_final_value, Invoke):
            invoke = yieldable_or_final_value
            logger.debug("Processing invocation")
            p = Promise[Any]()
            runnables.append(
                Runnable(coro_and_promise=r.coro_and_promise, next_value=Ok(p))
            )
            processor.enqueue(
                SQE(
                    cmd=_wrap_fn_into_cmd(invoke.fn, *invoke.args, **invoke.kwargs),
                    callback=partial(_invocation_callback, p),
                )
            )

        elif isinstance(yieldable_or_final_value, Promise):
            prom = yieldable_or_final_value
            logger.debug("Processing promise")
            if end_reached:
                r.coro_and_promise.prom.set_result(Ok(prom))
            elif prom.done():
                try:
                    res = Ok(prom.result())
                except Exception as e:
                    res = Err(e)
                runnables.append(Runnable(r.coro_and_promise, next_value=res))

            else:
                blockers.append(Blocker(r.coro_and_promise, prom_blocking=prom))
        else:
            logger.debug("Processing final value")
            r.coro_and_promise.prom.set_result(Ok(yieldable_or_final_value))


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
