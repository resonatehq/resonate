from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, final

from typing_extensions import ParamSpec

from resonate.dataclasses import FnOrCoroutine

if TYPE_CHECKING:
    from resonate.typing import DurableCoro, DurableFn, ExecutionUnit

P = ParamSpec("P")


class _ToInvoke(ABC):
    @abstractmethod
    def to_invoke(self) -> Invoke: ...


@final
class TopLevelInvoke(_ToInvoke):
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

    def to_invoke(self) -> Invoke:
        return Invoke(FnOrCoroutine(self.exec_unit, *self.args, **self.kwargs))


@final
class Call(_ToInvoke):
    def __init__(self, exec_unit: ExecutionUnit) -> None:
        self.exec_unit = exec_unit

    def to_invoke(self) -> Invoke:
        return Invoke(self.exec_unit)


@final
class Invoke:
    def __init__(self, exec_unit: ExecutionUnit) -> None:
        self.exec_unit = exec_unit
