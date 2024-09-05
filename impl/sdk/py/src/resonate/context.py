from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar

from typing_extensions import ParamSpec

from resonate.actions import Call, Invoke, Sleep
from resonate.dataclasses import Command, FnOrCoroutine
from resonate.dependency_injection import Dependencies

if TYPE_CHECKING:
    from resonate.options import Options
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


def _new_deps() -> Dependencies:
    return Dependencies()


@dataclass
class Context:
    ctx_id: str
    seed: int | None
    parent_ctx: Context | None = None
    deps: Dependencies = field(default_factory=_new_deps)
    _num_children: int = field(init=False, default=0)

    def new_child(self) -> Context:
        self._num_children += 1
        return Context(
            seed=self.seed,
            parent_ctx=self,
            deps=self.deps,
            ctx_id=f"{self.ctx_id}.{self._num_children}",
        )

    def assert_statement(self, stmt: bool, msg: str) -> None:  # noqa: FBT001
        if self.seed is None:
            return
        assert stmt, msg

    def get_dependency(self, key: str) -> Any:  # noqa: ANN401
        return self.deps.get(key)

    def invoke(
        self,
        invokable: Invokable[P],
        opts: Options | None = None,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Invoke:
        return Invoke(
            _wrap_into_execution_unit(invokable, *args, **kwargs),
            opts=opts,
        )

    def call(
        self,
        invokable: Invokable[P],
        opts: Options | None = None,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Call:
        return Call(_wrap_into_execution_unit(invokable, *args, **kwargs), opts=opts)

    def sleep(self, seconds: int) -> Sleep:
        return Sleep(seconds)
