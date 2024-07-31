from __future__ import annotations

import contextlib
import queue
from asyncio import iscoroutinefunction
from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

from typing_extensions import Concatenate, ParamSpec

from resonate.context import Context
from resonate.typing import AsyncCmd, FnCmd

if TYPE_CHECKING:
    from collections.abc import Coroutine


T = TypeVar("T")
P = ParamSpec("P")


def wrap_fn_into_cmd(
    ctx: Context,
    fn: Callable[Concatenate[Context, P], T | Coroutine[Any, Any, T]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> FnCmd[T] | AsyncCmd[T]:
    cmd: AsyncCmd[T] | FnCmd[T]
    if iscoroutinefunction(func=fn):
        cmd = AsyncCmd(ctx, fn, *args, **kwargs)
    else:
        cmd = FnCmd(
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
