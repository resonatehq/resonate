from __future__ import annotations

import queue
from asyncio import iscoroutinefunction
from concurrent.futures import Future
from dataclasses import dataclass
from functools import partial
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Callable, Generic, Union, cast

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
        self.f = Future[Result[T, Exception]]()

    def result(self, timeout: float | None = None) -> Result[T, Exception]:
        return self.f.result(timeout)

    def set_result(self, result: Result[Any, Exception]) -> None:
        self.f.set_result(result)

    def done(self) -> bool:
        return self.f.done()


Yieldable: TypeAlias = Union[Call, Invoke, Promise[Any]]


@dataclass(frozen=True)
class CoroutinePromisePair(Generic[T]):
    coro: Generator[Yieldable, Any, T]
    promise: Promise[T]


@dataclass(frozen=True)
class Runnable(Generic[T]):
    coroutine_promise_pair: CoroutinePromisePair[T]
    value_to_yield_back: Result[Any, Exception] | None


@dataclass(frozen=True)
class Awaiting(Generic[T]):
    coroutine_promise_pair: CoroutinePromisePair[T]
    value_promise: Promise[T]


def advance_generator_state(
    runnable: Runnable[T],
) -> Yieldable | T:
    advance_value: Yieldable
    try:
        if runnable.value_to_yield_back is None:
            advance_value = next(runnable.coroutine_promise_pair.coro)

        elif isinstance(runnable.value_to_yield_back, Ok):
            advance_value = runnable.coroutine_promise_pair.coro.send(
                runnable.value_to_yield_back.unwrap()
            )
        elif isinstance(runnable.value_to_yield_back, Err):
            advance_value = runnable.coroutine_promise_pair.coro.throw(
                runnable.value_to_yield_back.err()
            )
        else:
            assert_never(runnable.value_to_yield_back)

    except StopIteration as e:
        return e.value

    return advance_value


class Scheduler:
    def __init__(self) -> None:
        self._staging_queue = queue.Queue[CoroutinePromisePair[Any]]()
        self._thread: Thread | None = None
        self._kill_thread = Event()

    def _adjust_thread_count(self) -> None:
        if self._thread is None:
            t = Thread(
                target=_run,
                args=(
                    self._kill_thread,
                    self._staging_queue,
                ),
            )
            self._thread = t
            t.start()

    def add(
        self,
        fn: Callable[P, Generator[Yieldable, Any, T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Promise[T]:
        f = Promise[T]()
        self._staging_queue.put(
            CoroutinePromisePair(coro=fn(*args, **kwargs), promise=f)
        )
        self._adjust_thread_count()
        # _run(staging_queue=self._staging_queue)
        return f

    def close(self) -> None:
        self._kill_thread.set()
        if self._thread is None:
            msg = "Cannot kill a non existing thread."
            raise RuntimeError(msg)

        self._thread.join()


def _run(
    kill_thread: Event,
    staging_queue: queue.Queue[CoroutinePromisePair[Any]],
) -> None:
    runnables: list[Runnable[Any]] = []
    awaitables: list[Awaiting[Any]] = []
    processor = Processor(max_workers=None)

    while True:
        if kill_thread.is_set():
            processor.close()
            return

        while processor.cq_qsize() > 0:
            cqe = processor.dequeue()
            cqe.callback(cqe.cmd_result)

        while staging_queue.qsize() > 0:
            coro_with_future = staging_queue.get()
            runnables.append(
                Runnable(
                    coroutine_promise_pair=coro_with_future,
                    value_to_yield_back=None,
                )
            )

        if len(runnables) == 0:
            continue

        runnable = runnables.pop()

        try:
            new_yieldable_or_result = advance_generator_state(runnable=runnable)
        except Exception as e:  # noqa: BLE001
            runnable.coroutine_promise_pair.promise.set_result(Err(e))
            continue

        if isinstance(new_yieldable_or_result, Call):
            new_awaitable = Awaiting(
                coroutine_promise_pair=runnable.coroutine_promise_pair,
                value_promise=Promise[Any](),
            )

            processor.enqueue(
                SQE(
                    cmd=_wrap_fn_into_cmd(
                        new_yieldable_or_result.fn,
                        *new_yieldable_or_result.args,
                        **new_yieldable_or_result.kwargs,
                    ),
                    callback=partial(
                        call_callback, new_awaitable, awaitables, runnables
                    ),
                )
            )

            awaitables.append(new_awaitable)
        elif isinstance(new_yieldable_or_result, Invoke):
            new_promise = Promise[Any]()

            processor.enqueue(
                SQE(
                    _wrap_fn_into_cmd(
                        new_yieldable_or_result.fn,
                        *new_yieldable_or_result.args,
                        **new_yieldable_or_result.kwargs,
                    ),
                    callback=partial(
                        invoke_callback, new_promise, awaitables, runnables
                    ),
                )
            )
            runnables.append(
                Runnable(
                    coroutine_promise_pair=runnable.coroutine_promise_pair,
                    value_to_yield_back=Ok(new_promise),
                )
            )
        elif isinstance(new_yieldable_or_result, Promise):
            if new_yieldable_or_result.done():
                runnables.append(
                    Runnable(
                        runnable.coroutine_promise_pair,
                        value_to_yield_back=new_yieldable_or_result.result(),
                    )
                )
            else:
                awaitables.append(
                    Awaiting(
                        coroutine_promise_pair=runnable.coroutine_promise_pair,
                        value_promise=new_yieldable_or_result,
                    )
                )
        else:
            runnable.coroutine_promise_pair.promise.set_result(
                Ok(new_yieldable_or_result)
            )
            _unblock_promise(
                promise=runnable.coroutine_promise_pair.promise,
                awaitables=awaitables,
                runnables=runnables,
            )


def _unblock_promise(
    promise: Promise[Any],
    awaitables: list[Awaiting[Any]],
    runnables: list[Runnable[Any]],
) -> None:
    for awaitable in awaitables:
        if awaitable.coroutine_promise_pair.promise == promise:
            awaitables.remove(awaitable)
            runnables.append(
                Runnable(
                    coroutine_promise_pair=awaitable.coroutine_promise_pair,
                    value_to_yield_back=Ok(promise),
                )
            )


def invoke_callback(
    promise: Promise[Any],
    awaitables: list[Awaiting[Any]],
    runnables: list[Runnable[Any]],
    value: Result[Any, Exception],
) -> None:
    promise.set_result(value)
    _unblock_promise(promise=promise, awaitables=awaitables, runnables=runnables)


def call_callback(
    awaitable: Awaiting[Any],
    awaitables: list[Awaiting[Any]],
    runnables: list[Runnable[Any]],
    value: Result[Any, Exception],
) -> None:
    awaitables.remove(awaitable)
    awaitable.value_promise.set_result(value)
    runnables.append(
        Runnable(
            coroutine_promise_pair=awaitable.coroutine_promise_pair,
            value_to_yield_back=awaitable.value_promise.result(),
        )
    )
    _unblock_promise(
        promise=awaitable.coroutine_promise_pair.promise,
        awaitables=awaitables,
        runnables=runnables,
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
