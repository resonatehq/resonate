from __future__ import annotations

import asyncio
import contextlib
import hashlib
import queue
from inspect import iscoroutinefunction, isfunction
from typing import TYPE_CHECKING, Generic, TypeVar
from uuid import UUID

from typing_extensions import ParamSpec

from resonate.result import Err, Ok, Result

if TYPE_CHECKING:
    from resonate.context import Context
    from resonate.typing import DurableAsyncFn, DurableFn, DurableSyncFn

T = TypeVar("T")
P = ParamSpec("P")


class FnWrapper(Generic[T]):
    def __init__(
        self,
        ctx: Context,
        fn: DurableSyncFn[P, T],
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


class AsyncFnWrapper(Generic[T]):
    def __init__(
        self,
        ctx: Context,
        fn: DurableAsyncFn[P, T],
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


def wrap_fn(
    ctx: Context,
    fn: DurableFn[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> FnWrapper[T] | AsyncFnWrapper[T]:
    cmd: FnWrapper[T] | AsyncFnWrapper[T]
    if isfunction(fn):
        cmd = FnWrapper(ctx, fn, *args, **kwargs)
    else:
        assert iscoroutinefunction(fn)
        cmd = AsyncFnWrapper(ctx, fn, *args, **kwargs)
    return cmd


def dequeue_batch(q: queue.Queue[T], batch_size: int) -> list[T]:
    elements: list[T] = []
    with contextlib.suppress(queue.Empty):
        for _ in range(batch_size):
            e = q.get_nowait()
            elements.append(e)
            q.task_done()
    return elements


def dequeue(q: queue.Queue[T], timeout: float | None = None) -> T:
    qe = q.get(timeout=timeout)
    q.task_done()
    return qe


def string_to_ikey(string: str) -> str:
    return UUID(bytes=hashlib.sha1(string.encode("utf-8")).digest()[:16]).hex[-4:]  # noqa: S324


def get_parent_promise_id_from_ctx(ctx: Context) -> str | None:
    return ctx.parent_ctx.ctx_id if ctx.parent_ctx is not None else None
