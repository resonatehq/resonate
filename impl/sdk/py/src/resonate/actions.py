from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar, final

from typing_extensions import Self

from resonate.dataclasses import DurablePromise
from resonate.options import Options

if TYPE_CHECKING:
    from resonate import retry_policy
    from resonate.dataclasses import Invocation


T = TypeVar("T")
_ASSERT_MSG = (
    "Creating durable promises directly does not support adding options this way"
)


@final
@dataclass
class RFI:
    unit: Invocation[Any] | DurablePromise
    opts: Options = field(default_factory=Options)

    def options(
        self,
        *,
        id: str | None = None,
        send_to: str | None = None,
        execute_here: bool = False,
    ) -> Self:
        assert not isinstance(self.unit, DurablePromise), _ASSERT_MSG
        self.opts = Options(id=id, send_to=send_to, execute_here=execute_here)
        return self


@final
@dataclass
class RFC:
    unit: Invocation[Any] | DurablePromise
    opts: Options = field(default_factory=Options)

    def options(
        self,
        *,
        id: str | None = None,
        send_to: str | None = None,
        execute_here: bool = False,
    ) -> Self:
        assert not isinstance(self.unit, DurablePromise), _ASSERT_MSG
        self.opts = Options(id=id, send_to=send_to, execute_here=execute_here)
        return self

    def to_rfi(self) -> RFI:
        return RFI(self.unit, self.opts)


@final
@dataclass
class LFC:
    unit: Invocation[Any]
    opts: Options = field(default_factory=Options)

    def options(
        self,
        *,
        id: str | None = None,
        durable: bool = True,
        retry_policy: retry_policy.RetryPolicy | None = None,
    ) -> Self:
        self.opts = Options(durable=durable, id=id, retry_policy=retry_policy)
        return self

    def to_lfi(self) -> LFI:
        return LFI(self.unit, opts=self.opts)


@final
@dataclass
class DI:
    """
    Dataclass that contains all required information to do a
    detached invocation.
    """

    id: str
    unit: Invocation[Any]
    opts: Options = field(default_factory=Options)

    def options(self) -> Self:
        self.opts = Options(durable=True, id=self.id)
        return self


@final
@dataclass
class LFI:
    unit: Invocation[Any]
    opts: Options = field(default_factory=Options)

    def options(
        self,
        *,
        id: str | None = None,
        durable: bool = True,
        retry_policy: retry_policy.RetryPolicy | None = None,
    ) -> Self:
        self.opts = Options(durable=durable, id=id, retry_policy=retry_policy)
        return self
