from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, final

from typing_extensions import ParamSpec

from resonate.actions import Call, DeferredInvocation, Invocation, Sleep
from resonate.dataclasses import Command, FnOrCoroutine
from resonate.dependency_injection import Dependencies

if TYPE_CHECKING:
    from resonate.typing import DurableCoro, DurableFn, ExecutionUnit, Invokable

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
