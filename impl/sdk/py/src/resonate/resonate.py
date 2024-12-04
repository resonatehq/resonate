from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, TypedDict, TypeVar, overload
from uuid import uuid4

from typing_extensions import Concatenate, ParamSpec

from resonate.collections import FunctionRegistry
from resonate.dataclasses import RegisteredFn
from resonate.dependencies import Dependencies
from resonate.options import Options
from resonate.scheduler.scheduler import Scheduler
from resonate.scheduler.traits import IScheduler
from resonate.stores.remote import RemoteStore
from resonate.task_sources.poller import Poller
from resonate.task_sources.traits import ITaskSource

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator

    from resonate import retry_policy
    from resonate.context import Context
    from resonate.record import Handle
    from resonate.scheduler.traits import IScheduler
    from resonate.stores.local import LocalStore
    from resonate.task_sources.traits import ITaskSource
    from resonate.typing import DurableCoro, DurableFn, Yieldable

P = ParamSpec("P")
T = TypeVar("T")


class _RunOptions(TypedDict):
    version: int


class Resonate:
    def __init__(
        self,
        pid: str | None = None,
        store: LocalStore | RemoteStore | None = None,
        scheduler: IScheduler | None = None,
        task_source: ITaskSource | None = None,
    ) -> None:
        self._deps = Dependencies()
        self._fn_registry = FunctionRegistry()
        self.pid = pid if pid is not None else uuid4().hex

        self._task_source: ITaskSource = task_source or Poller(
            url="http://localhost:8002",
            group="default",
        )

        self._scheduler: IScheduler = (
            scheduler
            if scheduler is not None
            else Scheduler(
                fn_registry=self._fn_registry,
                deps=self._deps,
                pid=self.pid,
                store=store
                if store is not None
                else RemoteStore("http://localhost:8001"),
                task_source=self._task_source,
            )
        )

        self._task_source.set_sync_event(self._scheduler.get_sync_event())
        self._scheduler.set_default_recv(self._task_source.default_recv())

        self._task_source.start(self.pid)

    def set_dependency(self, key: str, obj: Any) -> None:  # noqa: ANN401
        self._deps.set(key, obj)

    @overload
    def register(
        self,
        func: Callable[
            Concatenate[Context, P],
            Generator[Yieldable, Any, T],
        ],
        name: str | None = None,
        version: int = 1,
        retry_policy: retry_policy.RetryPolicy | None = None,
    ) -> RegisteredFn[P, T]: ...
    @overload
    def register(
        self,
        func: Callable[Concatenate[Context, P], Coroutine[Any, Any, T]],
        name: str | None = None,
        version: int = 1,
        retry_policy: retry_policy.RetryPolicy | None = None,
    ) -> RegisteredFn[P, T]: ...
    @overload
    def register(
        self,
        func: Callable[Concatenate[Context, P], T],
        name: str | None = None,
        version: int = 1,
        retry_policy: retry_policy.RetryPolicy | None = None,
    ) -> RegisteredFn[P, T]: ...
    def register(
        self,
        func: DurableCoro[P, T] | DurableFn[P, T],
        name: str | None = None,
        version: int = 1,
        retry_policy: retry_policy.RetryPolicy | None = None,
    ) -> RegisteredFn[P, T]:
        if name is None:
            name = func.__name__
        self._fn_registry.add(
            name,
            (func, Options(version=version, durable=True, retry_policy=retry_policy)),
        )
        return RegisteredFn[P, T](self._scheduler, func)

    @overload
    def run(
        self,
        id: str,
        func: Callable[
            Concatenate[Context, P],
            Generator[Yieldable, Any, T],
        ]
        | Callable[Concatenate[Context, P], T]
        | Callable[Concatenate[Context, P], Coroutine[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]: ...
    @overload
    def run(
        self,
        id: str,
        func: tuple[
            Callable[
                Concatenate[Context, P],
                Generator[Yieldable, Any, T],
            ]
            | Callable[Concatenate[Context, P], T]
            | Callable[Concatenate[Context, P], Coroutine[Any, Any, T]],
            _RunOptions,
        ],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]: ...
    def run(
        self,
        id: str,
        func: Callable[
            Concatenate[Context, P],
            Generator[Yieldable, Any, T],
        ]
        | Callable[Concatenate[Context, P], T]
        | Callable[Concatenate[Context, P], Coroutine[Any, Any, T]]
        | tuple[
            Callable[
                Concatenate[Context, P],
                Generator[Yieldable, Any, T],
            ]
            | Callable[Concatenate[Context, P], T]
            | Callable[Concatenate[Context, P], Coroutine[Any, Any, T]],
            _RunOptions,
        ],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]:
        if isinstance(func, tuple):
            raise NotImplementedError
        return self._scheduler.run(id, func, *args, **kwargs)
