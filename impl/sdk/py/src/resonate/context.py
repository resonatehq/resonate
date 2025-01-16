from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, TypeVar, final, overload

from typing_extensions import Concatenate, ParamSpec

from resonate.actions import (
    DI,
    LFC,
    LFI,
    RFC,
    RFI,
)
from resonate.dataclasses import DurablePromise, Invocation, RegisteredFn
from resonate.time import now

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator

    from resonate.dependencies import Dependencies
    from resonate.typing import DurableCoro, DurableFn, Yieldable

P = ParamSpec("P")
T = TypeVar("T")


@final
class Context:
    def __init__(
        self,
        deps: Dependencies,
    ) -> None:
        self._deps = deps

    def get_dependency(self, key: str) -> Any:  # noqa: ANN401
        return self._deps.get(key)

    def sleep(self, secs: int) -> RFC:
        return self.rfc(
            DurablePromise(
                tags={"resonate:timeout": "true"}, timeout=now() + (secs * 1_000)
            )
        )

    @overload
    def rfc(self, cmd: DurablePromise, /) -> RFC: ...
    @overload
    def rfc(
        self,
        func: RegisteredFn[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC: ...
    @overload
    def rfc(self, func: str, /, *args: Any, **kwargs: Any) -> RFC: ...  # noqa: ANN401
    @overload
    def rfc(
        self,
        func: Callable[Concatenate[Context, P], Generator[Yieldable, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC: ...
    @overload
    def rfc(
        self,
        func: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC: ...
    def rfc(
        self,
        func_or_cmd: DurableCoro[P, T]
        | DurableFn[P, T]
        | str
        | DurablePromise
        | RegisteredFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC:
        unit: Invocation[Any] | DurablePromise
        if isinstance(func_or_cmd, str):
            unit = Invocation(func_or_cmd, *args, **kwargs)
        elif isinstance(func_or_cmd, DurablePromise):
            unit = func_or_cmd
        elif isinstance(func_or_cmd, RegisteredFn):
            unit = Invocation(func_or_cmd.fn, *args, **kwargs)
        else:
            unit = Invocation(func_or_cmd, *args, **kwargs)
        return RFC(unit)

    @overload
    def rfi(self, cmd: DurablePromise, /) -> RFI: ...
    @overload
    def rfi(
        self,
        func: RegisteredFn[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI: ...
    @overload
    def rfi(self, func: str, /, *args: Any, **kwargs: Any) -> RFI: ...  # noqa: ANN401
    @overload
    def rfi(
        self,
        func: Callable[Concatenate[Context, P], Generator[Yieldable, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI: ...
    @overload
    def rfi(
        self,
        func: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI: ...
    def rfi(
        self,
        func_or_cmd: DurableCoro[P, T]
        | DurableFn[P, T]
        | str
        | DurablePromise
        | RegisteredFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI:
        return self.rfc(func_or_cmd, *args, **kwargs).to_rfi()

    @overload
    def lfi(
        self,
        func: RegisteredFn[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI: ...
    @overload
    def lfi(
        self,
        func: Callable[Concatenate[Context, P], Generator[Yieldable, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI: ...
    @overload
    def lfi(
        self,
        func: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI: ...
    def lfi(
        self,
        func_or_cmd: DurableCoro[P, T] | DurableFn[P, T] | RegisteredFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI:
        """
        Local function invocation.

        Invoke and immediatelly receive a ``Promise[T]`` that
        represents the future result of the execution.

        The `Promise` can be yielded later in the execution to await
        for the result.
        """
        return self.lfc(func_or_cmd, *args, **kwargs).to_lfi()

    @overload
    def lfc(
        self,
        func: RegisteredFn[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC: ...
    @overload
    def lfc(
        self,
        func: Callable[Concatenate[Context, P], Generator[Yieldable, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC: ...
    @overload
    def lfc(
        self,
        func: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC: ...
    def lfc(
        self,
        func_or_cmd: DurableCoro[P, T] | DurableFn[P, T] | RegisteredFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC:
        """
        Local function call.

        LFC and await for the result of the execution. It's syntax
        sugar for ``yield (yield ctx.lfi(...))``
        """
        unit: Invocation[Any]
        if isinstance(func_or_cmd, RegisteredFn):
            unit = Invocation(func_or_cmd.fn, *args, **kwargs)
        else:
            unit = Invocation(func_or_cmd, *args, **kwargs)
        return LFC(unit)

    @overload
    def deferred(
        self,
        id: str,
        coro: Callable[Concatenate[Context, P], Generator[Yieldable, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> DI: ...
    @overload
    def deferred(
        self,
        id: str,
        coro: Callable[Concatenate[Context, P], Any | Coroutine[Any, Any, Any]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> DI: ...
    def deferred(
        self,
        id: str,
        coro: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> DI:
        """
        Deferred invocation.

        Invoke as a root invocation. Is equivalent to do ``Scheduler.run(...)``
        invoked execution will be retried and managed from the server.
        """
        return DI(id=id, unit=Invocation(coro, *args, **kwargs))
