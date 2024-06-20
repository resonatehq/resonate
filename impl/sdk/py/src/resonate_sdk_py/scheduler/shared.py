from __future__ import annotations

import asyncio
from asyncio import iscoroutinefunction
from concurrent.futures import Future
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Generic, Union, cast

from result import Err, Ok, Result
from typing_extensions import ParamSpec, TypeAlias, TypeVar, assert_never

from resonate_sdk_py.logging import logger
from resonate_sdk_py.processor import IAsyncCommand, ICommand

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
            result = Ok(asyncio.run(self.fn(*self.args, **self.kwargs)))
        except Exception as e:  # noqa: BLE001
            result = Err(e)
        return result


def wrap_fn_into_cmd(
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
class FinalValue(Generic[T]):
    v: Result[T, Exception]


def iterate_coro(runnable: Runnable[T]) -> Yieldable | FinalValue[T]:
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
        return FinalValue(Ok(e.value))
    except Exception as e:  # noqa: BLE001
        return FinalValue(Err(e))
    return yieldable


def unblock_depands_coros(
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


def callback(
    p: Promise[T],
    waiting_for_promise: WaitingForPromiseResolution,
    pending_to_run: PendingToRun,
    v: Result[T, Exception],
) -> None:
    p.set_result(v)
    unblock_depands_coros(p=p, waiting=waiting_for_promise, runnables=pending_to_run)


def handle_return_promise(
    running: Runnable[T],
    prom: Promise[T],
    waiting_for_prom: WaitingForPromiseResolution,
    pending_to_run: list[Runnable[Any]],
) -> None:
    if prom.done():
        unblock_depands_coros(
            p=prom,
            waiting=waiting_for_prom,
            runnables=pending_to_run,
        )
    else:
        waiting_for_expired_prom = waiting_for_prom.pop(running.coro_and_promise.prom)
        waiting_for_prom[prom].extend(waiting_for_expired_prom)
