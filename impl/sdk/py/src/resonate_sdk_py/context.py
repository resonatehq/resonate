from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, TypeVar

from typing_extensions import Concatenate, ParamSpec

if TYPE_CHECKING:
    from collections.abc import Coroutine

P = ParamSpec("P")
T = TypeVar("T")


class Call:
    def __init__(
        self,
        ctx: Context,
        fn: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.ctx = ctx
        self.args = args
        self.kwargs = kwargs


class Invoke:
    def __init__(
        self,
        ctx: Context,
        fn: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.fn = fn
        self.ctx = ctx
        self.args = args
        self.kwargs = kwargs


class Context:
    def __init__(
        self,
        parent_ctx: Context | None = None,
        deps: dict[str, Any] | None = None,
        *,
        dst: bool = False,
    ) -> None:
        self.parent_ctx = parent_ctx
        self.dst = dst
        self._deps = deps or {}

    def new_child(self) -> Context:
        return Context(parent_ctx=self, dst=self.dst, deps=self._deps)

    def set_dependency(self, key: str, obj: Any) -> None:  # noqa: ANN401
        if key in self._deps:
            msg = f"There's already an object under key: {key}"
            raise RuntimeError(msg)
        self._deps[key] = obj

    def get_dependency(self, key: str) -> Any:  # noqa: ANN401
        return self._deps[key]

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
        return Invoke(self.new_child(), fn, *args, **kwargs)

    def call(
        self,
        fn: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Call:
        return Call(self.new_child(), fn, *args, **kwargs)
