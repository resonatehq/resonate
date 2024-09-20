from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, final

from typing_extensions import ParamSpec

from resonate.actions import Call, Invoke, Sleep
from resonate.dataclasses import Command, FnOrCoroutine
from resonate.dependency_injection import Dependencies

if TYPE_CHECKING:
    from resonate.typing import ExecutionUnit, Invokable

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
    ) -> Invoke:
        return self.lfc(invokable, *args, **kwargs).to_invoke()

    def lfc(
        self,
        invokable: Invokable[P],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Call:
        return Call(_wrap_into_execution_unit(invokable, *args, **kwargs))

    def sleep(self, seconds: int) -> Sleep:
        return Sleep(seconds)
