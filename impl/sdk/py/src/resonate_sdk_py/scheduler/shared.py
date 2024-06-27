from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from asyncio import iscoroutinefunction
from concurrent.futures import Future
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Generic, Union, cast

from result import Err, Ok, Result
from typing_extensions import Concatenate, ParamSpec, TypeAlias, TypeVar, assert_never

from resonate_sdk_py.context import Context
from resonate_sdk_py.processor import IAsyncCommand, ICommand

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator


T = TypeVar("T")
P = ParamSpec("P")


class CoroScheduler(ABC):
    @abstractmethod
    def add(self, coros: Generator[Yieldable, Any, T]) -> Promise[T]: ...

    @abstractmethod
    def run(self) -> None: ...


class FnCmd(ICommand[T]):
    def __init__(
        self,
        ctx: Context,
        fn: Callable[Concatenate[Context, P], T],
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


class AsyncFnCmd(IAsyncCommand[T]):
    def __init__(
        self,
        ctx: Context,
        fn: Callable[Concatenate[Context, P], Coroutine[Any, Any, Any]],
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


def wrap_fn_into_cmd(
    ctx: Context,
    fn: Callable[Concatenate[Context, P], T | Coroutine[Any, Any, T]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> FnCmd[T] | AsyncFnCmd[T]:
    cmd: AsyncFnCmd[T] | FnCmd[T]
    if iscoroutinefunction(func=fn):
        cmd = AsyncFnCmd(ctx, fn, *args, **kwargs)
    else:
        cmd = FnCmd(
            ctx, cast(Callable[Concatenate[Context, P], T], fn), *args, **kwargs
        )
    return cmd


class Call:
    def __init__(
        self,
        ctx: Context,
        fn: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.ctx = ctx
        self.args = args
        self.kwargs = kwargs


class Invoke:
    def __init__(
        self,
        ctx: Context,
        fn: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.ctx = ctx
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
