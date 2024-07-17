from __future__ import annotations

import asyncio
from asyncio import iscoroutinefunction
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Callable, Generic, cast

from result import Err, Ok, Result
from typing_extensions import Concatenate, ParamSpec, TypeVar, assert_never

from resonate.context import Context
from resonate.processor import IAsyncCommand, ICommand

if TYPE_CHECKING:
    from collections.abc import Coroutine


T = TypeVar("T")
P = ParamSpec("P")


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

    def success(self) -> bool:
        if not self.done():
            return False
        try:
            self.result()
        except Exception:  # noqa: BLE001
            return False
        else:
            return True

    def failure(self) -> bool:
        return not self.success()
