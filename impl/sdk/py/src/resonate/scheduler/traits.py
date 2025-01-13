from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, TypeVar

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from resonate.handle import Handle
    from resonate.typing import DurableCoro, DurableFn

P = ParamSpec("P")
T = TypeVar("T")


class IScheduler(ABC):
    @abstractmethod
    def start(self) -> None: ...

    @abstractmethod
    def stop(self) -> None: ...
    @abstractmethod
    def run(
        self,
        id: str,
        func: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]: ...

    @abstractmethod
    def get(self, id: str) -> Handle[Any]: ...
