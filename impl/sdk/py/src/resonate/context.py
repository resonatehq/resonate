from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, final

from typing_extensions import ParamSpec

from resonate.actions import (
    All,
    AllSettled,
    Call,
    DeferredInvocation,
    Invocation,
    Race,
    Sleep,
)
from resonate.dataclasses import Command, FnOrCoroutine
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
        ctx_id: str,
        seed: int | None,
        parent_ctx: Context | None = None,
        deps: Dependencies | None = None,
    ) -> None:
        self.ctx_id = ctx_id
        self.seed = seed
        self.parent_ctx = parent_ctx
        self.deps = deps if deps is not None else Dependencies()
        self._num_children = 0

    def parent_promise_id(self) -> str | None:
        return self.parent_ctx.ctx_id if self.parent_ctx is not None else None

    def new_child(self, ctx_id: str | None) -> Context:
        self._num_children += 1
        if ctx_id is None:
            ctx_id = f"{self.ctx_id}.{self._num_children}"
        return Context(
            seed=self.seed,
            parent_ctx=self,
            deps=self.deps,
            ctx_id=ctx_id,
        )

    def assert_statement(self, stmt: bool, msg: str) -> None:  # noqa: FBT001
        if self.seed is None:
            return
        assert stmt, msg

    def get_dependency(self, key: str) -> Any:  # noqa: ANN401
        return self.deps.get(key)

    def lfi(
        self,
        invokable: Invokable[P],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Invocation:
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
    ) -> Call:
        """
        Local function call.

        Call and await for the result of the execution. It's syntax
        sugar for `yield (yield ctx.lfi(...))`
        """
        return Call(_wrap_into_execution_unit(invokable, *args, **kwargs))

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

    def sleep(self, seconds: int) -> Sleep:
        return Sleep(seconds)

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
