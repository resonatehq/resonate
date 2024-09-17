from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from resonate.options import Options
    from resonate.typing import ExecutionUnit

P = ParamSpec("P")


@final
@dataclass(frozen=True)
class Call:
    exec_unit: ExecutionUnit
    opts: Options

    def to_invoke(self) -> Invoke:
        return Invoke(self.exec_unit, opts=self.opts)


@final
@dataclass(frozen=True)
class Invoke:
    exec_unit: ExecutionUnit
    opts: Options


@final
@dataclass(frozen=True)
class Sleep:
    seconds: int
