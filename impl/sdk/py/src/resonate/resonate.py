from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, TypeVar, overload
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

    from resonate.context import Context
    from resonate.handle import Handle
    from resonate.retry_policy import RetryPolicy
    from resonate.scheduler.traits import IScheduler
    from resonate.stores.local import LocalStore
    from resonate.stores.traits import IPromiseStore
    from resonate.task_sources.traits import ITaskSource
    from resonate.typing import Yieldable

P = ParamSpec("P")
T = TypeVar("T")


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

        self._store = store or RemoteStore()

        self._scheduler: IScheduler = Scheduler(
            deps=self._deps,
            pid=pid or uuid4().hex,
            registry=self._registry,
            store=self._store,
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
        /,
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> RegisteredFn[P, T]: ...
    @overload
    def register(
        self,
        func: Callable[Concatenate[Context, P], Coroutine[Any, Any, T]],
        /,
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> RegisteredFn[P, T]: ...
    @overload
    def register(
        self,
        func: Callable[Concatenate[Context, P], T],
        /,
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> RegisteredFn[P, T]: ...
    @overload
    def register(
        self,
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[
        [Callable[Concatenate[Context, P], Any]],
        RegisteredFn[P, Any],
    ]: ...
    def register(
        self, *args: Any, **kwargs: Any
    ) -> (
        RegisteredFn[P, T]
        | Callable[
            [Callable[Concatenate[Context, P], Any]],
            RegisteredFn[P, T],
        ]
    ):
        name: str | None = kwargs.get("name")
        version: int = kwargs.get("version", 1)
        retry_policy: RetryPolicy | None = kwargs.get("retry_policy")

        def wrapper(
            func: Callable[Concatenate[Context, P], Any],
        ) -> RegisteredFn[P, Any]:
            self._registry.add(
                name or func.__name__,
                (
                    func,
                    Options(version=version, durable=True, retry_policy=retry_policy),
                ),
            )
            return RegisteredFn(self._scheduler, func)

        if args and callable(args[0]):
            return wrapper(args[0])

        return wrapper

    @overload
    def run(
        self,
        id: str,
        func: RegisteredFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]: ...
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
    def run(
        self,
        id: str,
        func: RegisteredFn[P, T]
        | Callable[
            Concatenate[Context, P],
            Generator[Yieldable, Any, T],
        ]
        | Callable[Concatenate[Context, P], T]
        | Callable[Concatenate[Context, P], Coroutine[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]:
        if isinstance(func, tuple):
            raise NotImplementedError
        if isinstance(func, RegisteredFn):
            func = func.fn
        return self._scheduler.run(id, func, *args, **kwargs)

    @property
    def promises(self) -> IPromiseStore:
        return self._store.promises
