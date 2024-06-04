from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, Union

from result import Err, Ok, Result
from typing_extensions import ParamSpec, TypeAlias, assert_never

if TYPE_CHECKING:
    from collections.abc import Generator

T = TypeVar("T")
K = TypeVar("K")
P = ParamSpec("P")


class Promise(Generic[T]):
    def __init__(self) -> None:
        self.result: Result[T, Exception] | None = None

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
    def __init__(self, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs


class Call:
    def __init__(self, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs


Yieldable: TypeAlias = Union[Invocation, Promise, Call]


@dataclass(frozen=True)
class Next(Generic[T]):
    """Next is what gets yield back to the generator."""

    result: Result[T, Exception] | None = None


@dataclass(frozen=True)
class Runnable(Generic[K, T]):
    coro: Generator[Yieldable, K, T]
    yield_back_value: Next[K]
    resv: Promise[T]


@dataclass(frozen=True)
class Awaiting(Generic[K, T]):
    coro: Generator[Yieldable, K, T]
    prom: Promise[K]
    resv: Promise[T]


class Scheduler(Generic[K, T]):
    def __init__(self) -> None:
        self.runnables: list[Runnable[K, T]] = []
        self.awaitings: list[Awaiting[K, T]] = []

    def add(
        self,
        func: Callable[P, Generator[Yieldable, K, T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.runnables.append(
            Runnable[K, T](
                coro=func(*args, **kwargs),
                resv=Promise[T](),
                yield_back_value=Next[K](),
            )
        )

    def _dump(self) -> None:
        print(self.runnables)  # noqa: T201

    def run(self) -> Any:
        while len(self.runnables) > 0:
            self._dump()
            runnable = self.runnables.pop()
            try:
                next_yieldable = advance(
                    coro=runnable.coro, resv=runnable.yield_back_value
                )
            except StopIteration as e:
                generator_final_value = e.value
                runnable.resv.resolve(e.value)
                for idx, awaiting in enumerate(self.awaitings):
                    if awaiting.prom == runnable.resv:
                        awaiting_to_move = self.awaitings.pop(idx)
                        self.runnables.append(
                            Runnable(
                                coro=awaiting_to_move.coro,
                                yield_back_value=Next(Ok(e.value)),
                                resv=awaiting_to_move.resv,
                            )
                        )
                continue

            if isinstance(next_yieldable, Promise):
                if next_yieldable.is_completed():
                    next_value = Next(result=next_yieldable.result)
                    self.runnables.append(
                        Runnable(
                            runnable.coro,
                            yield_back_value=next_value,
                            resv=runnable.resv,
                        )
                    )

                else:
                    self.awaitings.append(
                        Awaiting(
                            coro=runnable.coro, prom=next_yieldable, resv=runnable.resv
                        )
                    )
            elif isinstance(next_yieldable, Invocation):
                next_promise = Promise()

                value = next_yieldable.func(
                    *next_yieldable.args, **next_yieldable.kwargs
                )
                if inspect.isgenerator(value):
                    self.runnables.append(
                        Runnable(
                            coro=value,
                            yield_back_value=Next(result=None),
                            resv=next_promise,
                        )
                    )
                    self.runnables.append(
                        Runnable(
                            coro=runnable.coro,
                            yield_back_value=Next(result=Ok(next_promise)),
                            resv=runnable.resv,
                        )
                    )
                else:
                    next_promise.resolve(value)
                    self.runnables.append(
                        Runnable(
                            coro=runnable.coro,
                            yield_back_value=Next(Ok(next_promise)),
                            resv=runnable.resv,
                        )
                    )
            elif isinstance(next_yieldable, Call):
                print(next_yieldable)  # noqa: T201
            else:
                assert_never(next_yieldable)

        return generator_final_value


def bar(name: str) -> str:
    return f"Hi {name}"


def buzz(a: int, b: int) -> int:
    return a // b


def gen_buzz() -> Generator[Yieldable, Any, int]:
    p: Promise[int] = yield Invocation(buzz, a=15, b=1)
    v: int = yield p
    return v


def foo() -> Generator[Yieldable, Any, str]:
    p1: Promise[str] = yield Invocation(bar, name="tomas")
    p2: Promise[int] = yield Invocation(buzz, a=10, b=1)
    v1: str = yield p1
    v2: int = yield p2
    p3: Promise[int] = yield Invocation(gen_buzz)
    v3: int = yield p3
    return f"{v1} {v2} {v3}"


def advance(coro: Generator[Yieldable, K, T], resv: Next[K]) -> Yieldable:
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
