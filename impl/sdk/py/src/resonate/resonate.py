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
    from resonate.handle import Handle
    from resonate.scheduler.traits import IScheduler
    from resonate.stores.local import LocalStore
    from resonate.task_sources.traits import ITaskSource
    from resonate.typing import DurableCoro, DurableFn, Yieldable

P = ParamSpec("P")
T = TypeVar("T")


class _RunOptions(TypedDict):
    version: int


class Resonate:
    """
    The Resonate class serves as the main API interface for Resonate Application Nodes.

    Object attributes:
        - _deps (Dependencies): Manages application-level dependencies.
        - _registry (FunctionRegistry): Stores and manages registered functions.
        - _scheduler (IScheduler): Manages coroutine and function executions.

    """

    def __init__(
        self,
        pid: str | None = None,
        store: LocalStore | RemoteStore | None = None,
        task_source: ITaskSource | None = None,
    ) -> None:
        """
        Initialization args:
            - pid (str | None): Optional process ID for the scheduler.
                Defaults to a generated UUID.
            - store (LocalStore | RemoteStore | None): Optional store for promises.
                Defaults to RemoteStore.
            - task_source (TaskSource | None): Optional task source for obtaining tasks.
                Defaults to Poller.

        Example::

            from resonate import Resonate
            # ...
            resonate = Resonate()

        Example with custom store and task source::

            from resonate import Resonate
            from resonate.stores import RemoteStore
            from resonate.task_sources import Poller
            # ...
            resonate = Resonate(
                store=RemoteStore(url="http://localhost:8001"),
                task_source=Poller(url="http://localhost:8002"),
            )
        """
        self._deps = Dependencies()
        self._registry = FunctionRegistry()

        self._scheduler: IScheduler = Scheduler(
            deps=self._deps,
            pid=pid or uuid4().hex,
            registry=self._registry,
            store=store or RemoteStore(),
            task_source=task_source or Poller(),
        )

        # start the scheduler
        self._scheduler.start()

    def stop(self) -> None:
        """
        Stops the scheduler and halts all executions.
        """
        self._scheduler.stop()

    def set_dependency(self, key: str, obj: Any) -> None:  # noqa: ANN401
        """
        Sets a dependency to be used by the Application Node.

        Args:
            key: The identifier for the dependency.
            obj: The dependency object to associate with the key.
        """
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
        self._registry.add(
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
