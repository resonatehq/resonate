from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Generic, TypeVar, final

from typing_extensions import assert_never

from resonate.result import Err, Ok, Result

T = TypeVar("T")


@final
@dataclass(frozen=True)
class Handle(Generic[T]):
    id: str
    f: Future[T] = field(repr=False, default_factory=Future, init=False)

    def result(self, timeout: float | None = None) -> T:
        return self.f.result(timeout=timeout)

    def set_result(self, result: Result[T, Exception]) -> None:
        if isinstance(result, Ok):
            self.f.set_result(result.unwrap())
        elif isinstance(result, Err):
            self.f.set_exception(result.err())
        else:
            assert_never(result)
