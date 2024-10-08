from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar, final

from typing_extensions import ParamSpec, Self

from resonate.commands import Command
from resonate.options import Options
from resonate.retry_policy import never

if TYPE_CHECKING:
    from resonate.dataclasses import FnOrCoroutine
    from resonate.promise import Promise
    from resonate.retry_policy import RetryPolicy
    from resonate.typing import ExecutionUnit


P = ParamSpec("P")
T = TypeVar("T")


@final
@dataclass
class RFI:
    exec_unit: ExecutionUnit
    promise_id: str | None = None

    def with_options(self, promise_id: str) -> Self:
        assert not isinstance(
            self.exec_unit, Command
        ), "Options must be set on the command."
        assert self.promise_id is None, "promise ID has already been set"
        self.promise_id = promise_id
        return self


@final
@dataclass
class RFC:
    exec_unit: ExecutionUnit
    promise_id: str | None = None

    def with_options(self, promise_id: str) -> Self:
        assert not isinstance(
            self.exec_unit, Command
        ), "Options must be set on the command."
        assert self.promise_id is None, "promise ID has already been set"
        self.promise_id = promise_id
        return self

    def to_invocation(self) -> RFI:
        return RFI(self.exec_unit, self.promise_id)


@final
@dataclass
class LFC:
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

    def to_invocation(self) -> LFI:
        return LFI(self.exec_unit, opts=self.opts)


@final
@dataclass
class DeferredInvocation:
    """
    Dataclass that contains all required information to do a
    deferred invocation.
    """

    promise_id: str
    coro: FnOrCoroutine
    opts: Options = field(default=Options())

    def with_options(self, *, retry_policy: RetryPolicy | None = None) -> Self:
        self.opts = Options(
            durable=True, promise_id=self.promise_id, retry_policy=retry_policy
        )
        return self


@final
@dataclass
class LFI:
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
class All:
    """
    A combinator that waits for all promises to complete.

    Attributes:
        promises (list[Promise[Any]]): A list of promises to be combined.
    """

    def __init__(self, promises: list[Promise[Any]]) -> None:
        self.opts = Options(retry_policy=never())
        self.promises = promises

    def with_options(
        self,
        *,
        durable: bool = True,
        promise_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Self:
        """
        Set options for the combinator.

        Args:
            durable (bool): Whether the promise is durable. Defaults to True.
            promise_id (str | None): An optional identifier for the promise.
            retry_policy (RetryPolicy | None): An optional retry policy for the promise.

        Returns:
            Self: The combinator instance with updated options.
        """
        self.opts = Options(
            durable=durable,
            promise_id=promise_id,
            retry_policy=retry_policy if retry_policy is not None else never(),
        )
        return self


@final
class AllSettled:
    """
    A combinator that waits for all promises to complete and returns a list of results
    or Errors.

    Attributes:
        promises (list[Promise[Any]]): A list of promises to be combined.
    """

    def __init__(self, promises: list[Promise[Any]]) -> None:
        self.opts = Options(retry_policy=never())
        self.promises = promises

    def with_options(
        self,
        *,
        durable: bool = True,
        promise_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Self:
        """
        Set options for the combinator.

        Args:
            durable (bool): Whether the promise is durable. Defaults to True.
            promise_id (str | None): An optional identifier for the promise.
            retry_policy (RetryPolicy | None): An optional retry policy for the promise.

        Returns:
            Self: The combinator instance with updated options.
        """
        self.opts = Options(
            durable=durable,
            promise_id=promise_id,
            retry_policy=retry_policy if retry_policy is not None else never(),
        )
        return self


@final
class Race:
    """
    A combinator that completes when any of the promises completes.

    Attributes:
        promises (list[Promise[Any]]): A list of promises to race.
    """

    def __init__(self, promises: list[Promise[Any]]) -> None:
        assert (
            len(promises) > 0
        ), "Race combinator requires a non empty list of promises"
        self.opts = Options(retry_policy=never())
        self.promises = promises

    def with_options(
        self,
        *,
        durable: bool = True,
        promise_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Self:
        """
        Set options for the combinator.

        Args:
            durable (bool): Whether the promise is durable. Defaults to True.
            promise_id (str | None): An optional identifier for the promise.
            retry_policy (RetryPolicy | None): An optional retry policy for the promise.

        Returns:
            Self: The combinator instance with updated options.
        """
        self.opts = Options(
            durable=durable,
            promise_id=promise_id,
            retry_policy=retry_policy if retry_policy is not None else never(),
        )
        return self
