from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, final

from typing_extensions import ParamSpec

from resonate.actions import (
    LFC,
    LFI,
    RFC,
    RFI,
    All,
    AllSettled,
    DeferredInvocation,
    Race,
)
from resonate.commands import Command, CreateDurablePromiseReq
from resonate.dataclasses import FnOrCoroutine
from resonate.dependency_injection import Dependencies
from resonate.promise import Promise

if TYPE_CHECKING:
    from resonate.typing import (
        DurableCoro,
        DurableFn,
        ExecutionUnit,
        Invokable,
        Promise,
    )

P = ParamSpec("P")
T = TypeVar("T")


def _wrap_into_execution_unit(
    invokable: Invokable[P],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ExecutionUnit:
    if isinstance(invokable, Command):
        return invokable
    return FnOrCoroutine(invokable, *args, **kwargs)


@final
class Context:
    def __init__(
        self,
        seed: int | None,
        deps: Dependencies | None = None,
    ) -> None:
        self.seed = seed
        self.deps = deps if deps is not None else Dependencies()

    def assert_statement(self, stmt: bool, msg: str) -> None:  # noqa: FBT001
        if self.seed is None:
            return
        assert stmt, msg

    def get_dependency(self, key: str) -> Any:  # noqa: ANN401
        return self.deps.get(key)

    def rfc(
        self,
        invokable: Invokable[P],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC:
        promise_id: str | None = None
        if isinstance(invokable, Command):
            assert isinstance(
                invokable, CreateDurablePromiseReq
            ), f"The only command allowed for rfc is {CreateDurablePromiseReq.__name__}"
            promise_id = invokable.promise_id
        return RFC(
            _wrap_into_execution_unit(invokable, *args, **kwargs), promise_id=promise_id
        )

    def rfi(
        self,
        invokable: Invokable[P],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI:
        return self.rfc(invokable, *args, **kwargs).to_invocation()

    def lfi(
        self,
        invokable: Invokable[P],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI:
        """
        Local function invocation.

        Invoke and immediatelly receive a `Promise[T]` that
        represents the future result of the execution.

        The `Promise` can be yielded later in the execution to await
        for the result.
        """
        return self.lfc(invokable, *args, **kwargs).to_invocation()

    def lfc(
        self,
        invokable: Invokable[P],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC:
        """
        Local function call.

        LFC and await for the result of the execution. It's syntax
        sugar for `yield (yield ctx.lfi(...))`
        """
        if isinstance(invokable, Command):
            assert not isinstance(
                invokable, CreateDurablePromiseReq
            ), f"Command {CreateDurablePromiseReq.__name__} is reserved for lfc."
        return LFC(_wrap_into_execution_unit(invokable, *args, **kwargs))

    def deferred(
        self,
        promise_id: str,
        coro: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> DeferredInvocation:
        """
        Deferred invocation.

        Invoke as a root invocation. Is equivalent to do `Scheduler.run(...)`
        invoked execution will be retried and managed from the server.
        """
        return DeferredInvocation(
            promise_id=promise_id, coro=FnOrCoroutine(coro, *args, **kwargs)
        )

    def all(self, promises: list[Promise[Any]]) -> All:
        """
        Aggregates multiple promises into a single Promise that resolves when all of the
        promises in the input list have resolved.

        Args:
            promises (list[Promise[Any]]): An iterable of promises to be aggregated.

        Returns:
            All: A new Promise that resolves with a list of the resolved values from
            each promise in the input list, or rejects with the reason of the first
            promise that rejects.
        """
        return All(promises)

    def race(self, promises: list[Promise[Any]]) -> Race:
        """
        Aggregates multiple promises and returns a new Promise that resolves or rejects
        as soon as one of the promises in the input list resolves or rejects.

        Args:
            promises (list[Promise[Any]]): An iterable of promises to be raced.

        Returns:
            Race: A new Promise that resolves or rejects with the value/reason of the
            first promise in the list that resolves or rejects.
        """
        return Race(promises)

    def all_settled(self, promises: list[Promise[Any]]) -> AllSettled:
        """
        Aggregates multiple promises and returns a new Promise that resolves when all of
        the promises in the input list have either resolved or rejected.

        Args:
            promises (list[Promise[Any]]): An iterable of promises to be aggregated.

        Returns:
            AllSettled: A new Promise that resolves with a list of objects, each with a
            `status` property of either `'fulfilled'` or `'rejected'`, and a `value` or
            `reason` property depending on the outcome of the corresponding promise.
        """
        return AllSettled(promises)
