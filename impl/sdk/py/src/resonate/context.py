from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, TypeVar, Union

from typing_extensions import Concatenate, ParamSpec, TypeAlias

from resonate.dependency_injection import Dependencies

if TYPE_CHECKING:
    from collections.abc import Coroutine

P = ParamSpec("P")
T = TypeVar("T")


class Command:
    def __call__(self, ctx: Context) -> None:
        # This is not meant to be call. We are making the type system happy.
        _ = ctx
        msg = "You should never be here!"
        raise AssertionError(msg)


class FnOrCoroutine:
    def __init__(
        self,
        fn: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs


ExecutionUnit: TypeAlias = Union[Command, FnOrCoroutine]


def _wrap_into_execution_unit(
    fn: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ExecutionUnit:
    if isinstance(fn, Command):
        return fn
    return FnOrCoroutine(fn, *args, **kwargs)


class Call:
    def __init__(self, exec_unit: ExecutionUnit) -> None:
        self.exec_unit = exec_unit

    def to_invoke(self) -> Invoke:
        return Invoke(self.exec_unit)


class Invoke:
    def __init__(self, exec_unit: ExecutionUnit) -> None:
        self.exec_unit = exec_unit


class Context:
    def __init__(
        self,
        ctx_id: str,
        parent_ctx: Context | None = None,
        deps: Dependencies | None = None,
        *,
        dst: bool = False,
    ) -> None:
        self.parent_ctx = parent_ctx
        self.dst = dst
        self.deps = deps or Dependencies()
        self.ctx_id = ctx_id
        self._num_children: int = 0

    def new_child(self) -> Context:
        self._num_children += 1
        return Context(
            parent_ctx=self,
            dst=self.dst,
            deps=self.deps,
            ctx_id=f"{self.ctx_id}.{self._num_children}",
        )

    def assert_statement(self, stmt: bool, msg: str) -> None:  # noqa: FBT001
        if not self.dst:
            return
        assert stmt, msg

    def invoke(
        self,
        fn: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Invoke:
        exec_unit = _wrap_into_execution_unit(fn, *args, **kwargs)
        return Invoke(exec_unit)

    def call(
        self,
        fn: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Call:
        exec_unit = _wrap_into_execution_unit(fn, *args, **kwargs)
        return Call(exec_unit)
