from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, final

from typing_extensions import ParamSpec, Self

from resonate.options import Options

if TYPE_CHECKING:
    from resonate.retry_policy import RetryPolicy
    from resonate.typing import ExecutionUnit

P = ParamSpec("P")


@final
@dataclass
class Call:
    exec_unit: ExecutionUnit
    opts: Options = field(default=Options())

    def with_options(
        self,
        *,
        durable: bool = True,
        promise_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Self:
        self.opts = Options(
            durable=durable, promise_id=promise_id, retry_policy=retry_policy
        )
        return self

    def to_invoke(self) -> Invoke:
        return Invoke(self.exec_unit, opts=self.opts)


@final
@dataclass
class Invoke:
    exec_unit: ExecutionUnit
    opts: Options = field(default=Options())

    def with_options(
        self,
        *,
        durable: bool = True,
        promise_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Self:
        self.opts = Options(
            durable=durable, promise_id=promise_id, retry_policy=retry_policy
        )
        return self


@final
@dataclass(frozen=True)
class Sleep:
    seconds: int
