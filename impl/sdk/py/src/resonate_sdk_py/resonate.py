from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, Union

from result import Err, Ok, Result
from typing_extensions import ParamSpec, TypeAlias, assert_never

if TYPE_CHECKING:
    from collections.abc import Generator

ReturnType = TypeVar("ReturnType")
SendType = TypeVar("SendType")
T = TypeVar("T")
P = ParamSpec("P")


@dataclass
class Promise(Generic[T]):
    result: Result[T, Exception] | None = None

    def resolve(self, value: T) -> None:
        if self.is_pending():
            self.result = Ok(value)
        else:
            msg = "Not possible to resolve a non pending promise"
            raise RuntimeError(msg)

    def reject(self, error: Exception) -> None:
        if self.is_pending():
            self.result = Err(error)
        else:
            msg = "Not possible to reject a non pending promise"
            raise RuntimeError(msg)

    def is_pending(self) -> bool:
        return self.result is None

    def is_completed(self) -> bool:
        return not self.is_pending()

    def is_resolved(self) -> bool:
        return isinstance(self.result, Ok)

    def is_rejected(self) -> bool:
        return isinstance(self.result, Err)


class Invocation:
    def __init__(
        self,
        func: Callable[P, ReturnType | Generator[Yieldable, SendType, ReturnType]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs


class Call:
    def __init__(
        self,
        func: Callable[P, ReturnType | Generator[Yieldable, SendType, ReturnType]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs


Yieldable: TypeAlias = Union[Invocation, Promise, Call]


@dataclass(frozen=True)
class FinalValue(Generic[T]):
    value: T


@dataclass(frozen=True)
class CoroutineAndAssociatedPromise(Generic[SendType, ReturnType]):
    coro: Generator[Yieldable, SendType, ReturnType]
    promise: Promise[ReturnType]


@dataclass(frozen=True)
class Next(Generic[ReturnType]):
    """
    Next represents the value to be yielded back to coroutine.

    If value is None, it means is not yet computed and will be yielded
    back in the future.

    """

    result: Result[ReturnType, Exception] | None = None


@dataclass(frozen=True)
class Runnable(Generic[SendType, ReturnType]):
    coro_with_promise: CoroutineAndAssociatedPromise[SendType, ReturnType]
    yield_back_value: Next[SendType]


@dataclass(frozen=True)
class Awaiting(Generic[SendType, ReturnType]):
    coro_with_promise: CoroutineAndAssociatedPromise[SendType, ReturnType]
    prom: Promise[SendType]


def _advance_span(
    coro: Generator[Yieldable, SendType, ReturnType], resv: Next[SendType]
) -> Yieldable:
    advance_value: Yieldable

    if resv.result is None:
        advance_value = next(coro)
    elif isinstance(resv.result, Ok):
        advance_value = coro.send(resv.result.unwrap())
    elif isinstance(resv.result, Err):
        advance_value = coro.throw(resv.result.err())
    else:
        assert_never(resv.result)

    return advance_value


class Scheduler:
    def __init__(self) -> None:
        self.runnables: list[Runnable] = []
        self.awaitings: list[Awaiting] = []

    def add(
        self,
        func: Callable[P, Generator[Yieldable, SendType, ReturnType]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.runnables.append(
            Runnable(
                coro_with_promise=CoroutineAndAssociatedPromise(
                    coro=func(*args, **kwargs),
                    promise=Promise[ReturnType](),
                ),
                yield_back_value=Next[SendType](),
            )
        )

    def _add_to_runnables(
        self,
        coro_with_promise: CoroutineAndAssociatedPromise[SendType, ReturnType],
        value: Result[SendType, Exception] | None,
    ) -> None:
        self.runnables.append(
            Runnable[SendType, ReturnType](
                coro_with_promise=coro_with_promise,
                yield_back_value=Next[SendType](value),
            )
        )

    def _add_to_awaitables(
        self,
        coro_with_promise: CoroutineAndAssociatedPromise[SendType, ReturnType],
        prom: Promise[SendType],
    ) -> None:
        self.awaitings.append(
            Awaiting[SendType, ReturnType](
                coro_with_promise=coro_with_promise,
                prom=prom,
            )
        )

    def _process_invocation(self, invocation: Invocation, runnable: Runnable) -> None:
        next_promise = Promise()
        if inspect.isgeneratorfunction(invocation.func):
            self._add_to_runnables(
                coro_with_promise=CoroutineAndAssociatedPromise(
                    coro=invocation.func(*invocation.args, **invocation.kwargs),
                    promise=next_promise,
                ),
                value=None,
            )
            self._add_to_runnables(
                runnable.coro_with_promise,
                Ok(next_promise),
            )

        else:
            try:
                value = retry(
                    invocation.func,
                    *invocation.args,
                    **invocation.kwargs,
                )
                next_promise.resolve(value)
                self._add_to_runnables(
                    runnable.coro_with_promise,
                    Ok(next_promise),
                )
            except Exception as e:  # noqa: BLE001
                return self._handle_error(
                    error=e,
                    promise=next_promise,
                    runnable=runnable,
                )

        return None

    def _handle_error(
        self, error: Exception, promise: Promise[T], runnable: Runnable
    ) -> None:
        promise.reject(error)
        self._add_to_runnables(
            runnable.coro_with_promise,
            promise.result,
        )

    def _process_call(self, call: Call, runnable: Runnable) -> None:
        next_promise = Promise()
        if inspect.isgeneratorfunction(call.func):
            self._add_to_runnables(
                coro_with_promise=CoroutineAndAssociatedPromise(
                    call.func(*call.args, **call.kwargs),
                    next_promise,
                ),
                value=None,
            )
            self._add_to_awaitables(
                coro_with_promise=runnable.coro_with_promise,
                prom=next_promise,
            )
        else:
            try:
                value = retry(
                    call.func,
                    *call.args,
                    **call.kwargs,
                )
                next_promise.resolve(value)
                self._add_to_runnables(
                    runnable.coro_with_promise,
                    next_promise.result,
                )
            except Exception as e:  # noqa: BLE001
                return self._handle_error(
                    error=e,
                    promise=next_promise,
                    runnable=runnable,
                )
        return None

    def _process_promise(
        self, promise: Promise[ReturnType], runnable: Runnable
    ) -> None:
        if promise.is_completed():
            self._add_to_runnables(
                coro_with_promise=runnable.coro_with_promise,
                value=promise.result,
            )

        else:
            self._add_to_awaitables(
                coro_with_promise=runnable.coro_with_promise,
                prom=promise,
            )

    def run(self) -> Any:  # noqa: ANN401
        generator_final_value: FinalValue[Any] | None = None
        while len(self.runnables) > 0:
            runnable = self.runnables.pop()
            try:
                next_yieldable = _advance_span(
                    coro=runnable.coro_with_promise.coro,
                    resv=runnable.yield_back_value,
                )
            except StopIteration as e:
                if isinstance(e.value, Promise):
                    assert isinstance(  # noqa: PT017
                        e.value.result, Ok
                    ), "Final promise should be resolved."

                    e.value = e.value.result.unwrap()

                generator_final_value = FinalValue(value=e.value)
                runnable.coro_with_promise.promise.resolve(e.value)
                for idx, awaiting in enumerate(self.awaitings):
                    if awaiting.prom == runnable.coro_with_promise.promise:
                        awaiting_to_move = self.awaitings.pop(idx)
                        self._add_to_runnables(
                            awaiting_to_move.coro_with_promise,
                            value=Ok(e.value),
                        )

                continue

            if isinstance(next_yieldable, Promise):
                self._process_promise(promise=next_yieldable, runnable=runnable)

            elif isinstance(next_yieldable, Invocation):
                self._process_invocation(invocation=next_yieldable, runnable=runnable)

            elif isinstance(next_yieldable, Call):
                self._process_call(call=next_yieldable, runnable=runnable)
            else:
                assert_never(next_yieldable)

        if generator_final_value is None:
            msg = "No coroutine was added before running."
            raise RuntimeError(msg)
        return generator_final_value.value


def retry(func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    i = 0
    max_tries = 3
    while True:
        try:
            return func(*args, **kwargs)
        except Exception:  # noqa: BLE001, PERF203
            if i == max_tries:
                raise
            i += 1
