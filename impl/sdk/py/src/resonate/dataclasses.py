from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, final, overload

from typing_extensions import ParamSpec, assert_never

from resonate.promise import Promise
from resonate.result import Err, Ok, Result

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.handle import Handle
    from resonate.record import Record
    from resonate.scheduler.traits import IScheduler
    from resonate.typing import Data, DurableCoro, DurableFn, Headers, Tags, Yieldable

T = TypeVar("T")
P = ParamSpec("P")


@final
@dataclass(frozen=True)
class SQE(Generic[T]):
    thunk: Callable[[], T]
    id: str


@final
class RegisteredFn(Generic[P, T]):
    def __init__(
        self, scheduler: IScheduler, func: DurableCoro[P, T] | DurableFn[P, T], /
    ) -> None:
        self.fn = func
        self._scheduler = scheduler

    def run(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[T]:
        return self._scheduler.run(id, self.fn, *args, **kwargs)

    def get(self, id: str) -> Handle[T]:
        return self._scheduler.get(id)


@final
@dataclass(frozen=True)
class FinalValue(Generic[T]):
    v: Result[T, Exception]


class ResonateCoro(Generic[T]):
    def __init__(self, record: Record[T], coro: Generator[Yieldable, Any, T]) -> None:
        self.id = record.id
        self._coro = coro
        self._coro_active: bool = True
        self._final_value: Result[T, Exception] | None = None
        self._first_error_used = False
        self._next_child_to_yield = 0
        self._record = record

    def _pending_children(self) -> list[Record[Any]]:
        return self._record.children[self._next_child_to_yield :]

    def advance(
        self, next_value: Result[Any, Exception] | None
    ) -> Yieldable | FinalValue[T]:
        yielded: Yieldable
        try:
            if next_value is None:
                # `None` means to initialize the coroutine
                assert self._coro_active
                yielded = next(self._coro)
            elif isinstance(next_value, Ok):
                # `Ok[T]` means send a value
                yielded = self._send(next_value.unwrap())
            elif isinstance(next_value, Err):
                # `Err[Exception]` means throw and Exception
                yielded = self._throw(next_value.err())
            else:
                assert_never(next_value)
        except StopIteration as e:
            return FinalValue(Ok(e.value))
        except Exception as e:  # noqa: BLE001
            return FinalValue(Err(e))
        return yielded

    def _send(self, v: T) -> Yieldable:
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
            return Promise[Any](child.id)

        assert all(child.done() for child in self._record.children)
        assert not self._coro_active
        assert isinstance(self._final_value, Ok)
        raise StopIteration(self._final_value.unwrap())

    def _throw(self, error: Exception) -> Yieldable:
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
            return Promise[Any](child.id)
        assert all(
            child.done() for child in self._record.children
        ), "All children promise must have been resolved."

        assert not self._coro_active
        assert self._final_value is not None
        if isinstance(self._final_value, Ok):
            raise StopIteration(self._final_value)
        if isinstance(self._final_value, Err):
            raise self._final_value.err()
        assert_never(self._final_value)


class Invocation(Generic[T]):
    @overload
    def __init__(
        self,
        fn: str,
        /,
        *args: Any,  # noqa: ANN401
        **kwargs: Any,  # noqa: ANN401
    ) -> None: ...
    @overload
    def __init__(
        self,
        fn: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None: ...
    def __init__(
        self,
        fn: DurableCoro[P, T] | DurableFn[P, T] | str,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs


class DurablePromise:
    def __init__(
        self,
        id: str | None = None,
        data: Data = None,
        headers: Headers = None,
        tags: Tags = None,
        timeout: int = sys.maxsize,
    ) -> None:
        self.id = id
        self.data = data
        self.headers = headers
        self.tags = tags
        self.timeout = timeout
