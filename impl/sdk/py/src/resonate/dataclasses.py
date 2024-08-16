from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.promise import Promise
    from resonate.result import Result
    from resonate.typing import (
        DurableCoro,
        DurableFn,
        Yieldable,
    )

T = TypeVar("T")
P = ParamSpec("P")


@dataclass(frozen=True)
class CoroAndPromise(Generic[T]):
    coro: Generator[Yieldable, Any, T]
    prom: Promise[T]
    ctx: Context


@dataclass(frozen=True)
class Runnable(Generic[T]):
    coro_and_promise: CoroAndPromise[T]
    next_value: Result[Any, Exception] | None


class FnOrCoroutine:
    def __init__(
        self,
        exec_unit: DurableCoro[P, Any] | DurableFn[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.exec_unit = exec_unit
        self.args = args
        self.kwargs = kwargs


class Command:
    def __call__(self, ctx: Context) -> None:
        # This is not meant to be call. We are making the type system happy.
        _ = ctx
        msg = "You should never be here!"
        raise AssertionError(msg)
