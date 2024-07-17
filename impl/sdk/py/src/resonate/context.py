from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, TypeVar

from typing_extensions import Concatenate, ParamSpec

from resonate.dependency_injection import Dependencies

if TYPE_CHECKING:
    from collections.abc import Coroutine

P = ParamSpec("P")
T = TypeVar("T")


class Call:
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


class Invoke:
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


class Context:
    def __init__(
        self,
        parent_ctx: Context | None = None,
        deps: Dependencies | None = None,
        *,
        dst: bool = False,
    ) -> None:
        self.parent_ctx = parent_ctx
        self.dst = dst
        self.deps = deps or Dependencies()

    def new_child(self) -> Context:
        return Context(parent_ctx=self, dst=self.dst, deps=self.deps)

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
        return Invoke(fn, *args, **kwargs)

    def call(
        self,
        fn: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Call:
        return Call(fn, *args, **kwargs)
