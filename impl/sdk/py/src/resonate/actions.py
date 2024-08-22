from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from resonate.typing import ExecutionUnit

P = ParamSpec("P")


@final
class Call:
    def __init__(self, exec_unit: ExecutionUnit) -> None:
        self.exec_unit = exec_unit

    def to_invoke(self) -> Invoke:
        return Invoke(self.exec_unit)


@final
class Invoke:
    def __init__(self, exec_unit: ExecutionUnit) -> None:
        self.exec_unit = exec_unit


@final
class Sleep:
    def __init__(self, seconds: int) -> None:
        self.seconds = seconds
