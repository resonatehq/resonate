from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, final

if TYPE_CHECKING:
    from threading import Event

    from resonate.result import Result

T = TypeVar("T")


@final
@dataclass(frozen=True)
class SQE(Generic[T]):
    thunk: Callable[[], T]
    callback: Callable[[Result[T, Exception]], None]


@final
@dataclass(frozen=True)
class CQE(Generic[T]):
    result: Result[T, Exception]
    callback: Callable[[Result[T, Exception]], None]


class IProcessor(ABC):
    @abstractmethod
    def start(self, event: Event) -> None: ...

    @abstractmethod
    def enqueue(self, sqe: SQE[Any]) -> None: ...

    @abstractmethod
    def dequeue(self) -> list[CQE[Any]]: ...
