from __future__ import annotations

from dataclasses import dataclass
from inspect import isgenerator
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import ParamSpec, assert_never

from resonate.actions import LFI
from resonate.commands import CreateDurablePromiseReq
from resonate.promise import all_promises_are_done
from resonate.result import Err, Ok

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


class RouteInfo:
    def __init__(
        self,
        ctx: Context,
        promise: Promise[Any],
        retry_attempt: int,
    ) -> None:
        assert isinstance(promise.action, LFI)
        assert isinstance(promise.action.exec_unit, FnOrCoroutine)
        self.ctx = ctx
        self.promise = promise
        self.fn_or_coroutine = promise.action.exec_unit
        self.retry_attempt = retry_attempt
        self.retry_policy = promise.action.opts.retry_policy


class ResonateCoro(Generic[T]):
    def __init__(
        self,
        route_info: RouteInfo,
        coro: Generator[Yieldable, Any, T],
    ) -> None:
        assert isgenerator(coro)
        self.route_info = route_info
        self._coro = coro
        self._coro_active = True
        self._final_value: Result[T, Exception] | None = None
        self._first_error_used = False
        self._next_child_to_yield = 0

    def _pending_children(self) -> list[Promise[Any]]:
        return self.route_info.promise.children_promises[self._next_child_to_yield :]

    def next(self) -> Yieldable:
        assert self._coro_active
        return next(self._coro)

    def send(self, v: T) -> Yieldable:
        if self._coro_active:
            try:
                return self._coro.send(v)
            except StopIteration as e:
                assert self._final_value is None, "return value can only be set once."
                self._final_value = Ok(e.value)
                self._coro_active = False

        for child in self._pending_children():
            self._next_child_to_yield += 1
            if child.done():
                continue
            return child

        assert all_promises_are_done(
            self.route_info.promise.children_promises
        ), "All children promise must have been resolved."
        assert not self._coro_active
        assert isinstance(self._final_value, Ok)
        raise StopIteration(self._final_value.unwrap())

    def throw(self, error: Exception) -> Yieldable:
        if self._coro_active:
            try:
                return self._coro.throw(error)
            except StopIteration as e:
                assert self._final_value is None, "return value can only be set once."
                self._final_value = Ok(e.value)
                self._coro_active = False

        if not self._first_error_used:
            self._final_value = Err(error)
            self._first_error_used = True

        for child in self._pending_children():
            self._next_child_to_yield += 1
            if child.done():
                continue
            return child
        assert all_promises_are_done(
            self.route_info.promise.children_promises
        ), "All children promise must have been resolved."

        assert not self._coro_active
        assert self._final_value is not None
        if isinstance(self._final_value, Ok):
            raise StopIteration(self._final_value)
        if isinstance(self._final_value, Err):
            raise self._final_value.err()
        assert_never(self._final_value)


@dataclass(frozen=True)
class Runnable(Generic[T]):
    coro: ResonateCoro[T]
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

    def to_req(
        self, promise_id: str | None, func_name: str, tags: dict[str, str]
    ) -> CreateDurablePromiseReq:
        return CreateDurablePromiseReq(
            promise_id=promise_id,
            data={
                "func": func_name,
                "args": self.args,
                "kwargs": self.kwargs,
            },
            tags=tags,
        )
