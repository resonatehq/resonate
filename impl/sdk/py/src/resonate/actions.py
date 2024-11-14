from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar, final

from typing_extensions import ParamSpec, Self

from resonate.commands import Command, CreateDurablePromiseReq
from resonate.options import LOptions, ROptions
from resonate.retry_policy import never

if TYPE_CHECKING:
    from resonate.dataclasses import FnOrCoroutine
    from resonate.promise import Promise
    from resonate.retry_policy import RetryPolicy


P = ParamSpec("P")
T = TypeVar("T")


@final
@dataclass
class RFI:
    exec_unit: (
        FnOrCoroutine
        | tuple[str, tuple[Any, ...], dict[str, Any]]
        | CreateDurablePromiseReq
    )
    opts: ROptions = field(default=ROptions())

    def options(self, id: str | None = None, target: str | None = None) -> Self:
        assert not isinstance(
            self.exec_unit, CreateDurablePromiseReq
        ), "Options must be set on the cmd."
        self.opts = ROptions(id=id, target=target)
        return self


@final
@dataclass
class RFC:
    exec_unit: (
        FnOrCoroutine
        | tuple[str, tuple[Any, ...], dict[str, Any]]
        | CreateDurablePromiseReq
    )
    opts: ROptions = field(default=ROptions())

    def options(self, id: str | None = None, target: str | None = None) -> Self:
        assert not isinstance(
            self.exec_unit, CreateDurablePromiseReq
        ), "Options must be set on the cmd."
        self.opts = ROptions(id=id, target=target)
        return self

    def to_invocation(self) -> RFI:
        return RFI(self.exec_unit, self.opts)


@final
@dataclass
class LFC:
    exec_unit: FnOrCoroutine | Command
    opts: LOptions = field(default=LOptions())

    def options(
        self,
        id: str | None = None,
        retry_policy: RetryPolicy | None = None,
        *,
        durable: bool = True,
    ) -> Self:
        if retry_policy is not None:
            assert not isinstance(
                self.exec_unit, Command
            ), "Retry policies on batching are set when registering command handlers."
        self.opts = LOptions(durable=durable, id=id, retry_policy=retry_policy)
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

    id: str
    coro: FnOrCoroutine
    opts: LOptions = field(default=LOptions())

    def options(self, *, retry_policy: RetryPolicy | None = None) -> Self:
        self.opts = LOptions(durable=True, id=self.id, retry_policy=retry_policy)
        return self


@final
@dataclass
class LFI:
    exec_unit: FnOrCoroutine | Command
    opts: LOptions = field(default=LOptions())

    def options(
        self,
        id: str | None = None,
        retry_policy: RetryPolicy | None = None,
        *,
        durable: bool = True,
    ) -> Self:
        if retry_policy is not None:
            assert not isinstance(
                self.exec_unit, Command
            ), "Retry policies on batching are set when registering command handlers."
        self.opts = LOptions(durable=durable, id=id, retry_policy=retry_policy)
        return self


@final
class All:
    """
    A combinator that waits for all promises to complete.

    Attributes: promises (list[Promise[Any]]): A list of promises to be combined.
    """

    def __init__(self, promises: list[Promise[Any]]) -> None:
        self.opts = LOptions(retry_policy=never())
        self.promises = promises

    def options(
        self,
        *,
        durable: bool = True,
        id: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Self:
        """
        Set options for the combinator.

        Args: durable (bool): Whether the promise is durable. Defaults to True.
        id (str | None): An optional identifier for the promise.
        retry_policy (RetryPolicy | None): An optional retry policy for the promise.

        Returns: Self: The combinator instance with updated options.
        """
        self.opts = LOptions(
            durable=durable,
            id=id,
            retry_policy=retry_policy if retry_policy is not None else never(),
        )
        return self


@final
class AllSettled:
    """
    A combinator that waits for all promises to complete and returns a list of results
    or Errors.

    Attributes: promises (list[Promise[Any]]): A list of promises to be combined.
    """

    def __init__(self, promises: list[Promise[Any]]) -> None:
        self.opts = LOptions(retry_policy=never())
        self.promises = promises

    def options(
        self,
        *,
        durable: bool = True,
        id: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Self:
        """
        Set options for the combinator.

        Args: durable (bool): Whether the promise is durable. Defaults to True.
        id (str | None): An optional identifier for the promise.
        retry_policy (RetryPolicy | None): An optional retry policy for the promise.

        Returns: Self: The combinator instance with updated options.
        """
        self.opts = LOptions(
            durable=durable,
            id=id,
            retry_policy=retry_policy if retry_policy is not None else never(),
        )
        return self


@final
class Race:
    """
    A combinator that completes when any of the promises completes.

    Attributes: promises (list[Promise[Any]]): A list of promises to race.
    """

    def __init__(self, promises: list[Promise[Any]]) -> None:
        assert (
            len(promises) > 0
        ), "Race combinator requires a non empty list of promises"
        self.opts = LOptions(retry_policy=never())
        self.promises = promises

    def options(
        self,
        *,
        durable: bool = True,
        id: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Self:
        """
        Set options for the combinator.

        Args: durable (bool): Whether the promise is durable. Defaults to True.
        id (str | None): An optional identifier for the promise.
        retry_policy (RetryPolicy | None): An optional retry policy for the promise.

        Returns: Self: The combinator instance with updated options.
        """
        self.opts = LOptions(
            durable=durable,
            id=id,
            retry_policy=retry_policy if retry_policy is not None else never(),
        )
        return self
