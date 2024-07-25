from __future__ import annotations

import asyncio
import contextlib
import queue
from asyncio import iscoroutinefunction
from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

from typing_extensions import Concatenate, ParamSpec

from resonate.context import Context
from resonate.processor import IAsyncCommand, ICommand
from resonate.result import Err, Ok, Result

if TYPE_CHECKING:
    from collections.abc import Coroutine


T = TypeVar("T")
P = ParamSpec("P")


class _FnCmd(ICommand[T]):
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


class _AsyncCmd(IAsyncCommand[T]):
    def __init__(
        self,
        ctx: Context,
        fn: Callable[Concatenate[Context, P], Coroutine[Any, Any, T]],
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
) -> _FnCmd[T] | _AsyncCmd[T]:
    cmd: _AsyncCmd[T] | _FnCmd[T]
    if iscoroutinefunction(func=fn):
        cmd = _AsyncCmd(ctx, fn, *args, **kwargs)
    else:
        cmd = _FnCmd(
            ctx, cast(Callable[Concatenate[Context, P], T], fn), *args, **kwargs
        )
    return cmd


def dequeue_batch(q: queue.Queue[T], batch_size: int) -> list[T] | None:
    elements: list[T] = []
    with contextlib.suppress(queue.Empty):
        for _ in range(batch_size):
            e = q.get_nowait()
            elements.append(e)
            q.task_done()
    if len(elements) == 0:
        return None
    return elements


def dequeue(q: queue.Queue[T], timeout: float | None = None) -> T:
    qe = q.get(timeout=timeout)
    q.task_done()
    return qe
