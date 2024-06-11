from __future__ import annotations

import queue
from asyncio import iscoroutinefunction
from concurrent.futures import Future
from dataclasses import dataclass
from functools import partial
from threading import Thread
from typing import TYPE_CHECKING, Any, Callable, Generic, cast

from result import Err, Ok, Result
from typing_extensions import ParamSpec, TypeAlias, TypeVar, assert_never

from resonate_sdk_py.processor import SQE, IAsyncCommand, ICommand, Processor

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator



T = TypeVar("T")
P = ParamSpec("P")


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


Yieldable: TypeAlias = Call


@dataclass(frozen=True)
class CoroutineFuturePair(Generic[T]):
    coro: Generator[Yieldable, Any, T]
    future: Future[T]


@dataclass(frozen=True)
class Runnable(Generic[T]):
    root_coroutine_future_pair: CoroutineFuturePair[T]
    value_to_yield_back: Result[Any, Exception] | None


@dataclass(frozen=True)
class Awaiting(Generic[T]):
    root_coroutine_future_pair: CoroutineFuturePair[T]
    value_promise: Future[Any]


def advance_generator_state(
    runnable: Runnable[T],
) -> Yieldable | T:
    advance_value: Yieldable
    try:
        if runnable.value_to_yield_back is None:
            advance_value = next(runnable.root_coroutine_future_pair.coro)

        elif isinstance(runnable.value_to_yield_back, Ok):
            advance_value = runnable.root_coroutine_future_pair.coro.send(
                runnable.value_to_yield_back.unwrap()
            )
        elif isinstance(runnable.value_to_yield_back, Err):
            advance_value = runnable.root_coroutine_future_pair.coro.throw(
                runnable.value_to_yield_back.err()
            )
        else:
            assert_never(runnable.value_to_yield_back)

    except StopIteration as e:
        return e.value

    return advance_value


class Scheduler:
    def __init__(self) -> None:
        self._staging_queue = queue.Queue[CoroutineFuturePair[Any]]()
        self._thread: Thread | None = None

    def _adjust_thread_count(self) -> None:
        if self._thread is None:
            t = Thread(
                target=_run,
                args=(self._staging_queue,),
                daemon=True,
            )
            self._thread = t
            t.start()

    def add(
        self,
        fn: Callable[P, Generator[Yieldable, Any, T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[Result[T, Exception]]:
        f = Future[Result[T, Exception]]()
        self._staging_queue.put(CoroutineFuturePair(coro=fn(*args, **kwargs), future=f))
        self._adjust_thread_count()
        return f


def _run(staging_queue: queue.Queue[CoroutineFuturePair[Any]]) -> None:
    processor = Processor(max_workers=None)
    runnables: list[Runnable[Any]] = []

    while True:
        while staging_queue.qsize() > 0:
            coro_with_future = staging_queue.get()
            runnables.append(
                Runnable(
                    root_coroutine_future_pair=coro_with_future,
                    value_to_yield_back=None,
                )
            )
            continue
        while processor.cq_qsize() > 0:
            cqe = processor.dequeue()
            cqe.callback(cqe.cmd_result)
            continue

        if len(runnables) == 0:
            continue

        runnable = runnables.pop()

        new_yieldable_or_result = advance_generator_state(runnable=runnable)
        if isinstance(new_yieldable_or_result, Call):
            processor.enqueue(
                SQE(
                    cmd=_wrap_fn_into_cmd(
                        new_yieldable_or_result.fn,
                        *new_yieldable_or_result.args,
                        **new_yieldable_or_result.kwargs,
                    ),
                    callback=partial(
                        call_callback,
                        runnables,
                        runnable.root_coroutine_future_pair,
                    ),
                )
            )

        else:
            runnable.root_coroutine_future_pair.future.set_result(
                new_yieldable_or_result
            )


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


def call_callback(
    runnables: list[Runnable[Any]],
    root_coroutine_future_pair: CoroutineFuturePair[Any],
    value: Result[Any, Exception],
) -> None:
    runnables.append(
        Runnable(
            root_coroutine_future_pair=root_coroutine_future_pair,
            value_to_yield_back=value,
        )
    )


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
