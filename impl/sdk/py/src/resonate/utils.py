from __future__ import annotations

import contextlib
import hashlib
import queue
from typing import TYPE_CHECKING, TypeVar
from uuid import UUID

if TYPE_CHECKING:
    from resonate.context import Context

T = TypeVar("T")


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
