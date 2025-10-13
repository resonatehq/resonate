from __future__ import annotations

import copy
import functools
import inspect
import logging
import random
import time
import uuid
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Concatenate, Literal, ParamSpec, TypeVar, TypeVarTuple, overload

from resonate.bridge import Bridge
from resonate.conventions import Base, Local, Remote, Sleep
from resonate.coroutine import LFC, LFI, RFC, RFI, Promise
from resonate.dependencies import Dependencies
from resonate.loggers import ContextLogger
from resonate.message_sources import LocalMessageSource, Poller
from resonate.models.handle import Handle
from resonate.models.message_source import MessageSource
from resonate.models.store import Store
from resonate.options import Options
from resonate.registry import Registry
from resonate.stores import LocalStore, RemoteStore

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Sequence

    from resonate.models.context import Info
    from resonate.models.encoder import Encoder
    from resonate.models.logger import Logger
    from resonate.models.retry_policy import RetryPolicy
    from resonate.models.store import PromiseStore, ScheduleStore

ALLOWED_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


class Resonate:
    """Resonate client."""

    def __init__(
        self,
        *,
        dependencies: Dependencies | None = None,
        group: str = "default",
        log_level: int | Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = logging.NOTSET,
        message_source: MessageSource | None = None,
        pid: str | None = None,
        registry: Registry | None = None,
        store: Store | None = None,
        ttl: int = 10,
        workers: int | None = None,
    ) -> None:
        """Create a Resonate client."""
        # dependencies
        if dependencies is not None and not isinstance(dependencies, Dependencies):
            msg = f"dependencies must be `Dependencies | None`, got {type(dependencies).__name__}"
            raise TypeError(msg)

        # group
        if not isinstance(group, str):
            msg = f"group must be `str`, got {type(group).__name__}"
            raise TypeError(msg)

        # log level
        if not isinstance(log_level, (int, str)):
            msg = f"log_level must be an int or a str, got {type(log_level).__name__}"
            raise TypeError(msg)
        if isinstance(log_level, str) and log_level not in ALLOWED_LOG_LEVELS:
            msg = f"string log_level must be one of {ALLOWED_LOG_LEVELS}, got {log_level!r}"
            raise ValueError(msg)

        # message source
        if message_source is not None and not isinstance(message_source, MessageSource):
            msg = f"message_source must be `MessageSource | None`, got {type(dependencies).__name__}"
            raise TypeError(msg)

        # pid
        if pid is not None and not isinstance(pid, str):
            msg = f"pid must be `str | None`, got {type(pid).__name__}"
            raise TypeError(msg)

        # registry
        if registry is not None and not isinstance(registry, Registry):
            msg = f"registry must be `Registry | None`, got {type(registry).__name__}"
            raise TypeError(msg)

        # store
        if store is not None and not isinstance(store, Store):
            msg = f"store must be `Store | None`, got {type(registry).__name__}"
            raise TypeError(msg)
        if isinstance(store, LocalStore) and not isinstance(message_source, LocalMessageSource):
            msg = "message source must be LocalMessageSource when store is LocalStore"
            raise TypeError(msg)
        if isinstance(store, RemoteStore) and isinstance(message_source, LocalMessageSource):
            msg = "message source must not be LocalMessageSource when store is RemoteStore"
            raise TypeError(msg)

        # ttl
        if not isinstance(ttl, int):
            msg = f"ttl must be `int`, got {type(ttl).__name__}"
            raise TypeError(msg)

        # store / message source
        if (store is None) != (message_source is None):
            msg = "store and message source must both be set or both be unset"
            raise ValueError(msg)

        self._dependencies = dependencies or Dependencies()
        self._group = group
        self._log_level = log_level
        self._opts = Options()
        self._pid = pid or uuid.uuid4().hex
        self._registry = registry or Registry()
        self._started = False
        self._ttl = ttl
        self._workers = workers

        if store and message_source:
            self._store = store
            self._message_source = message_source
        else:
            self._store = LocalStore()
            self._message_source = self._store.message_source(self._group, self._pid)

        self._bridge = Bridge(
            ctx=lambda id, cid, info: Context(id, cid, info, self._registry, self._dependencies, ContextLogger(cid, id, self._log_level)),
            pid=self._pid,
            ttl=self._ttl,
            opts=self._opts,
            workers=self._workers,
            unicast=self._message_source.unicast,
            anycast=self._message_source.anycast,
            registry=self._registry,
            store=self._store,
            message_source=self._message_source,
        )

    @classmethod
    def local(
        cls,
        dependencies: Dependencies | None = None,
        group: str = "default",
        log_level: int | Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = logging.INFO,
        pid: str | None = None,
        registry: Registry | None = None,
        ttl: int = 10,
        workers: int | None = None,
    ) -> Resonate:
        """Initialize a Resonate client instance for local development.

        Initializes and returns a Resonate Client with zero dependencies.
        There is no external persistence — all state is stored in local memory,
        so there is no need to connect to a network. This client enables rapid
        API testing and experimentation before connecting to a Resonate Server.

        Args:
            dependencies (Dependencies | None): Optional dependency injection container.
            group (str): Worker group name. Defaults to ``default``.
            log_level (int | Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]):
                Logging verbosity level. Defaults to ``logging.INFO``.
            pid (str | None): Optional process identifier for the worker.
            registry (Registry | None): Optional registry to manage in-memory objects.
            ttl (int): Time-to-live (in seconds) for claimed tasks. Defaults to ``10``.
            workers (int | None): Optional number of worker threads or processes
                for function execution.

        Returns:
            Resonate: A Resonate Client instance.

        Example:
            Creating a local client and running a registered function::

                resonate = Resonate.local(ttl=30, log_level="DEBUG")
                resonate.register(foo)
                resonate.run("foo.1", foo, ...)

        """
        pid = pid or uuid.uuid4().hex
        store = LocalStore()

        return cls(
            pid=pid,
            ttl=ttl,
            group=group,
            registry=registry,
            dependencies=dependencies,
            log_level=log_level,
            store=store,
            message_source=store.message_source(group=group, id=pid),
            workers=workers,
        )

    @classmethod
    def remote(
        cls,
        auth: tuple[str, str] | None = None,
        dependencies: Dependencies | None = None,
        group: str = "default",
        host: str | None = None,
        log_level: int | Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = logging.INFO,
        message_source_port: str | None = None,
        pid: str | None = None,
        registry: Registry | None = None,
        store_port: str | None = None,
        ttl: int = 10,
        workers: int | None = None,
    ) -> Resonate:
        """Initialize a Resonate client with remote configuration.

        This method initializes and returns a Resonate Client that has
        dependencies on a Resonate Server and/or additional message sources.
        These dependencies enable distributed durable workers to work together via durable RPCs.
        The default remote configuration expects to connect to a Resonate Server on your localhost network as both a promise store and message source.

        Args:
            auth (tuple[str, str] | None): Optional authentication credentials for
                connecting to the remote store. Expected format is `(username, password)`.
            dependencies (Dependencies | None): Optional dependency injection container.
            group (str): Client group name. Defaults to `"default"`.
            host (str | None): Host address of the remote store service.
            log_level (int | Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]):
                Logging verbosity level. Defaults to `logging.INFO`.
            message_source_port (str | None): Port used for message source communication.
            pid (str | None): Optional process identifier for the client.
            registry (Registry | None): Optional registry to manage remote object mappings.
            store_port (str | None): Port used for the remote store service.
            ttl (int): Time-to-live (in seconds) for claimed tasks. Defaults to `10`.
            workers (int | None): Optional number of worker threads or processes for function execution.

        Returns:
            Resonate: A Resonate Client instance

        Example:
            Creating a remote client and running a registered function::

                resonate = Resonate.local(ttl=30, log_level="DEBUG")
                resonate.register(foo)
                resonate.run("foo.1", foo, ...)

        """
        pid = pid or uuid.uuid4().hex

        return cls(
            pid=pid,
            ttl=ttl,
            group=group,
            registry=registry,
            dependencies=dependencies,
            log_level=log_level,
            store=RemoteStore(host=host, port=store_port, auth=auth),
            message_source=Poller(group=group, id=pid, host=host, port=message_source_port, auth=auth),
            workers=workers,
        )

    @property
    def promises(self) -> PromiseStore:
        """Promises low level client."""
        return self._store.promises

    @property
    def schedules(self) -> ScheduleStore:
        """Schedules low level client."""
        return self._store.schedules

    def start(self) -> None:
        """Explicitly start Resonate threads.

        This happens automatically when calling .run() or .rpc(),
        but is generally recommended for all workers that have
        functions registered with Resonate.
        """
        if not self._started:
            self._bridge.start()

    def stop(self) -> None:
        """Stop resonate."""
        self._started = False
        self._bridge.stop()

    def options(
        self,
        *,
        encoder: Encoder[Any, str | None] | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None = None,
        tags: dict[str, str] | None = None,
        target: str | None = None,
        timeout: float | None = None,
        version: int | None = None,
    ) -> Resonate:
        """Configure options for the function.

        - encoder: Configure your own data encoder.
        - idempotency_key: Define the idempotency key invocation or a function that
            receives the promise id and creates an idempotency key.
        - retry_policy: Define the retry policy: exponential | constant | linear | never
        - tags: Add custom tags to the durable promise representing the invocation.
        - target: Target to distribute the invocation.
        - timeout: Number of seconds before the invocation timesout.
        - version: Version of the function to invoke.
        """
        copied: Resonate = copy.copy(self)
        copied._opts = self._opts.merge(
            encoder=encoder,
            idempotency_key=idempotency_key,
            retry_policy=retry_policy,
            tags=tags,
            target=target,
            timeout=timeout,
            version=version,
        )

        return copied

    @overload
    def register[**P, R](
        self,
        func: Callable[Concatenate[Context, P], R],
        /,
        *,
        name: str | None = None,
        version: int = 1,
    ) -> Function[P, R]: ...
    @overload
    def register[**P, R](
        self,
        *,
        name: str | None = None,
        version: int = 1,
    ) -> Callable[[Callable[Concatenate[Context, P], R]], Function[P, R]]: ...
    def register[**P, R](
        self,
        *args: Callable[Concatenate[Context, P], R] | None,
        name: str | None = None,
        version: int = 1,
    ) -> Function[P, R] | Callable[[Callable[Concatenate[Context, P], R]], Function[P, R]]:
        """Register a function with Resonate for execution and version control.

        This method makes a function available for top-level execution under a
        specific name and version. It can be used either directly by passing the
        target function, or as a decorator for more concise registration.

        When used as a decorator without arguments, the function is registered
        automatically using its own name and the default version. Providing explicit
        `name` or `version` values allows precise control over function identification
        and versioning for repeatable, distributed invocation.

        Args:
            *args (Callable[Concatenate[Context, P], R] | None): The function to register,
                        or `None` when used as a decorator.
            name (str | None): Optional explicit name for the registered function.
                Defaults to the function's own name.
            version (int): Version number for the registered function. Defaults to `1`.

        Returns:
            Function[P, R] | Callable[[Callable[Concatenate[Context, P], R]], Function[P, R]]:
                If called directly, returns a `Function` wrapper for the registered
                function. If used as a decorator, returns a decorator that registers
                the target function upon definition.

        Example:
            Registering a function directly::

                def greet(ctx: Context, name: str) -> str:
                    return f"Hello, {name}!"

                resonate.register(greet, name="greet_user", version=2)

            Using as a decorator::

                @resonate.register(name="process_data", version=2)
                def process(ctx: Context, data: dict) -> dict:
                    ...

        """
        if name is not None and not isinstance(name, str):
            msg = f"name must be `str | None`, got {type(name).__name__}"
            raise TypeError(msg)

        if not isinstance(version, int):
            msg = f"version must be `int`, got {type(version).__name__}"
            raise TypeError(msg)

        def wrapper(func: Callable[..., Any]) -> Function[P, R]:
            if not callable(func):
                msg = "func must be Callable"
                raise TypeError(msg)

            if isinstance(func, Function):
                func = func.func

            self._registry.add(func, name, version)
            return Function(self, name or func.__name__, func, self._opts.merge(version=version))

        if args and args[0] is not None:
            return wrapper(args[0])

        return wrapper

    @overload
    def run[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R: ...
    @overload
    def run(
        self,
        id: str,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...
    def run[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Run a registered function through Resonate and waits for its result.

        This method executes a registered function by its identifier or reference
        and returns the computed result. If a durable promise with the same `id`
        already exists, Resonate will reuse the existing result or subscribe to its
        completion rather than executing the function again. This ensures
        idempotent and deterministic behavior across distributed executions.

        Resonate guarantees that duplicate executions for the same `id` are prevented.

        Notes:
            - The target function must be registered with Resonate.
            - All function arguments and keyword arguments must be serializable.
            - This is a **blocking** operation that waits for completion.

        Args:
            id (str): Unique identifier for this function invocation. Used to
                deduplicate and resume durable results.
            func (Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str):
                The function to execute, either as a callable or the registered name of the function.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.

        Returns:
            R: The result of the executed function.

        Example:
            Running a function directly::

                result = resonate.run("task-42", my_function, 10, flag=True)

            Running by registered name::

                result = resonate.run("task-42", "process_data", records)

        """
        return self.begin_run(id, func, *args, **kwargs).result()

    @overload
    def begin_run[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]: ...
    @overload
    def begin_run(
        self,
        id: str,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> Handle[Any]: ...
    def begin_run[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]:
        """Begin execution of a registered function through Resonate.

        This method starts the execution of a registered function and returns a
        `Handle` that can be used to track progress, await completion, or retrieve
        results later. If a durable promise with the same `id` already exists,
        Resonate will reuse the existing execution state or subscribe to its result
        rather than starting a new run. This ensures idempotent and fault-tolerant
        execution across distributed systems.

        Resonate guarantees that duplicate executions for the same `id` are prevented.

        Notes:
            - The target function must be registered with Resonate.
            - All function arguments and keyword arguments must be serializable.
            - This operation is **non-blocking**; it returns immediately with a handle.

        Args:
            id (str): Unique identifier for this function invocation. Used to
                deduplicate and resume durable results.
            func (Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str):
                The function to execute, either as a callable or the registered name of the function.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.

        Returns:
            Handle[R]: A handle object that can be used to monitor, await, or
            retrieve the function's result.

        Example:
            Starting a function run asynchronously::

                handle = resonate.begin_run("job-123", process_data, records)
                # Do other work...
                result = handle.result()

            Using a registered function name::

                handle = resonate.begin_run("job-123", "aggregate_metrics", dataset)

            Non-blocking awaiting::

                result = await resonate.begin_run("job-123", "aggregate_metrics", dataset)

        """
        # id
        if not isinstance(id, str):
            msg = f"id must be `str`, got {type(id).__name__}"
            raise TypeError(msg)
        # func
        if not (callable(func) or isinstance(func, str)):
            msg = f"func must be `Callable | str`, got {type(func).__name__}"
            raise TypeError(msg)
        # tuple
        if not isinstance(args, tuple):
            msg = f"args must be `tuple`, got {type(args).__name__}"
            raise TypeError(args)
        # dict
        if not isinstance(kwargs, dict):
            msg = f"kwargs must be `dict`, got {type(kwargs).__name__}"
            raise TypeError(kwargs)

        self.start()
        future = Future[R]()

        name, func, version = self._registry.get(func, self._opts.version)
        opts = self._opts.merge(version=version)

        self._bridge.run(Remote(id, id, id, name, args, kwargs, opts), func, args, kwargs, opts, future)
        return Handle(id, future, self._bridge.subscribe)

    @overload
    def rpc[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R: ...
    @overload
    def rpc(
        self,
        id: str,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...
    def rpc[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Execute a registered function remotely through Resonate and waits for its result.

        This method invokes a registered function on a remote Resonate worker and
        blocks until the result is available. If a durable promise with the same `id`
        already exists, Resonate will reuse the existing execution or subscribe to its
        completion instead of executing it again, ensuring idempotent and consistent
        remote execution.

        Resonate guarantees that duplicate remote executions for the same `id` are prevented.

        Notes:
            - The target function must be registered with Resonate.
            - All function arguments and keyword arguments must be serializable.
            - This is a **blocking** remote operation that waits for completion.

        Args:
            id (str): Unique identifier for this function invocation. Used to
                deduplicate and resume durable remote results.
            func (Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str):
                The function to execute remotely, either as a callable or the
                registered name of the function.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.

        Returns:
            R: The result of the remote function execution.

        Example:
            Running a function remotely by reference::

                result = resonate.rpc("remote-task-7", my_function, data)

            Running a registered function by name::

                result = resonate.rpc("remote-task-7", "process_order", order_id)

        """
        return self.begin_rpc(id, func, *args, **kwargs).result()

    @overload
    def begin_rpc[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]: ...
    @overload
    def begin_rpc(
        self,
        id: str,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> Handle[Any]: ...
    def begin_rpc[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]:
        """Begin remote execution of a registered function through Resonate.

        This method initiates a remote execution on a Resonate worker and returns a
        `Handle` that can be used to track progress, await completion, or retrieve
        the result later. If a durable promise with the same `id` already exists,
        Resonate will reuse the existing execution state or subscribe to its result
        instead of starting a new one. This ensures consistent, idempotent, and
        fault-tolerant behavior in distributed environments.

        Resonate guarantees that duplicate remote executions for the same `id` are prevented.

        Notes:
            - The target function must be registered with Resonate.
            - All function arguments and keyword arguments must be serializable.
            - This operation is **non-blocking** and returns immediately with a handle.

        Args:
            id (str): Unique identifier for this remote invocation. Used to
                deduplicate and resume durable results.
            func (Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str):
                The function to execute remotely, either as a callable or the
                registered name of the function.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.

        Returns:
            Handle[R]: A handle object representing the remote execution,
            which can be used to monitor, await, or retrieve results.

        Example:
            Starting a remote run asynchronously::

                handle = resonate.begin_rpc("job-987", process_data, records)
                # Do other work...
                result = handle.result()

            Using a registered function name::

                handle = resonate.begin_rpc("job-987", "aggregate_metrics", dataset)

            Non-blocking awaiting::

                result = await resonate.begin_rpc("job-987", "aggregate_metrics", dataset)

        """
        # id
        if not isinstance(id, str):
            msg = f"id must be `str`, got {type(id).__name__}"
            raise TypeError(msg)
        # func
        if not (callable(func) or isinstance(func, str)):
            msg = f"func must be `Callable | str`, got {type(func).__name__}"
            raise TypeError(msg)
        # tuple
        if not isinstance(args, tuple):
            msg = f"args must be `tuple`, got {type(args).__name__}"
            raise TypeError(args)
        # dict
        if not isinstance(kwargs, dict):
            msg = f"kwargs must be `dict`, got {type(kwargs).__name__}"
            raise TypeError(kwargs)

        self.start()
        future = Future[R]()

        if isinstance(func, str):
            name = func
            version = self._registry.latest(func)
        else:
            name, _, version = self._registry.get(func, self._opts.version)

        opts = self._opts.merge(version=version)
        self._bridge.rpc(Remote(id, id, id, name, args, kwargs, opts), opts, future)
        return Handle(id, future, self._bridge.subscribe)

    def get(self, id: str) -> Handle[Any]:
        """Retrieve or subscribe to an existing execution by ID.

        This method attaches to an existing durable promise identified by `id`.
        If the associated execution is still in progress, it returns a `Handle`
        that can be used to await or observe its completion. If the execution has
        already completed, the handle is resolved immediately with the stored result.

        Notes:
            - A durable promise with the given `id` must already exist.
            - This operation is **non-blocking**; awaiting the handle will block only if the execution is still running.

        Args:
            id (str): Unique identifier of the target execution or durable promise.

        Returns:
            Handle[Any]: A handle representing the existing execution, which can
            be used to await or retrieve the result.

        Example:
            Attaching to an existing execution::

                handle = resonate.get("job-42")
                result = handle.result()

            Non-blocking awaiting::

                handle = resonate.get("job-42")
                result = await handle

        """
        # id
        if not isinstance(id, str):
            msg = f"id must be `str`, got {type(id).__name__}"
            raise TypeError(msg)

        self.start()
        future = Future()

        self._bridge.get(id, self._opts, future)
        return Handle(id, future, self._bridge.subscribe)

    def set_dependency(self, name: str, obj: Any) -> None:
        """Register a named dependency for use within function execution contexts.

        Registers a dependency object that will be made available to all registered
        functions through their ``Context``. Dependencies are typically shared resources
        such as database clients, configuration objects, or service interfaces that
        functions can access during execution.

        Args:
            name (str): The name under which the dependency is registered. This
                name is used to retrieve the dependency within a function's ``Context``.
            obj (Any): The dependency instance to register.

        Returns:
            None

        Example:
            Registering and using a dependency::

                client.set_dependency("db", DatabaseClient())

                @client.register()
                def fetch_user(ctx: Context, user_id: str):
                    db = ctx.dependencies["db"]
                    return db.get_user(user_id)

        """
        # name
        if not isinstance(name, str):
            msg = f"name must be `str`, got {type(name).__name__}"
            raise TypeError(msg)

        self._dependencies.add(name, obj)


class Context:
    def __init__(self, id: str, cid: str, info: Info, registry: Registry, dependencies: Dependencies, logger: Logger) -> None:
        self._id = id
        self._cid = cid
        self._info = info
        self._registry = registry
        self._dependencies = dependencies
        self._logger = logger
        self._random = Random(self)
        self._time = Time(self)
        self._counter = 0

    def __repr__(self) -> str:
        return f"Context(id={self._id}, cid={self._cid}, info={self._info})"

    @property
    def id(self) -> str:
        """Id for the current execution."""
        return self._id

    @property
    def info(self) -> Info:
        """Information for the current execution."""
        return self._info

    @property
    def logger(self) -> Logger:
        return self._logger

    @property
    def random(self) -> Random:
        """Deterministic random methods."""
        return self._random

    @property
    def time(self) -> Time:
        """Deterministic time methods."""
        return self._time

    def get_dependency[T](self, key: str, default: T = None) -> Any | T:
        """Retrieve a registered dependency by name.

        This method returns the dependency object registered under the given key.
        If no dependency exists for that key, the provided `default` value is returned.
        A `TypeError` is raised if `key` is not a string.

        Args:
            key (str): The name of the dependency to retrieve.
            default (T, optional): The value to return if no dependency is found.
                Defaults to `None`.

        Returns:
            Any | T: The registered dependency if it exists, otherwise the
            specified `default` value.

        Raises:
            TypeError: If `key` is not a string.

        Example:
            Retrieving dependencies from a context::

                db = ctx.get_dependency("db")
                cache = ctx.get_dependency("cache", default=NullCache())

        """
        if not isinstance(key, str):
            msg = f"key must be `str`, got {type(key).__name__}"
            raise TypeError(msg)

        return self._dependencies.get(key, default)

    def run[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC[R]:
        """Schedules a function for an immediate effectively-once local execution and awaits its result.

        This method initiates the immediate effectively-once execution of the given function within the current process.
        By default, this method checkpoints at the invocation and the result of the called function.
        This method is an alias of `ctx.lfc()`.

        Args:
            func (Callable[Concatenate[Context, P], Generator[Any, Any, R] | R]):
                The function to execute locally.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.

        Returns:
            LFC[R]: An object representing the scheduled execution
            and its eventual result.

        """
        return self.lfc(func, *args, **kwargs)

    def begin_run[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI[R]:
        """Schedules a function for an immediate effectively-once local execution and returns a promise.

        This method initiates the immediate asynchronous execution of the given function within the current
        process, returning a promise — yield the promise to get a result. This method checkpoints at the invocation
        and result of the function execution. This method is an alias of ctx.lfi().

        By default, execution is durable, but non-durable behavior can be configured
        if desired.

        Args:
            func (Callable[Concatenate[Context, P], Generator[Any, Any, R] | R]):
                The function to execute locally.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.

        Returns:
            LFI[R]: An object representing the scheduled local execution, which can
            be awaited for the final result.

        """
        return self.lfi(func, *args, **kwargs)

    @overload
    def rpc[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC[R]: ...
    @overload
    def rpc(
        self,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> RFC: ...
    def rpc(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> RFC:
        """Schedules a function for remote execution and awaits its result.

        This method executes the given function remotely through the global event loop.
        The function may run in a different process or worker, depending on the
        configured Resonate environment. It serves as an alias for `ctx.rfc`, providing
        a simplified interface for blocking remote calls.

        The function must be registered with Resonate before invocation, and all
        arguments must be serializable.

        Args:
            func (Callable | str): The function to execute remotely, or the registered
                name of the function.
            *args (Any): Positional arguments to pass to the function.
            **kwargs (Any): Keyword arguments to pass to the function.

        Returns:
            RFC: An object representing the execution and its result.

        """
        return self.rfc(func, *args, **kwargs)

    @overload
    def begin_rpc[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI[R]: ...
    @overload
    def begin_rpc(
        self,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI: ...
    def begin_rpc(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI:
        """Schedules a function for remote execution and returns a promise.

        This method schedules the given function for remote execution through the
        global event loop. The function may run in a different process or worker,
        depending on the configured Resonate environment. It serves as an alias for
        `ctx.rfi`, providing a simplified interface for non-blocking remote calls.

        The returned promise can be awaited to obtain the final result once
        the remote execution completes.

        The function must be registered with Resonate before invocation, and all
        arguments must be serializable.

        Args:
            func (Callable | str): The function to execute remotely, or the registered
                name of the function.
            *args (Any): Positional arguments to pass to the function.
            **kwargs (Any): Keyword arguments to pass to the function.

        Returns:
            RFI: An object representing the scheduled
            execution, which can be awaited for the final result.

        """
        return self.rfi(func, *args, **kwargs)

    def lfi[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI[R]:
        if isinstance(func, Function):
            func = func.func

        if not inspect.isfunction(func):
            msg = "provided callable must be a function"
            raise ValueError(msg)

        opts = Options(version=self._registry.latest(func))
        return LFI(Local(self._next(), self._cid, self._id, opts), func, args, kwargs, opts)

    def lfc[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC[R]:
        if isinstance(func, Function):
            func = func.func

        if not inspect.isfunction(func):
            msg = "provided callable must be a function"
            raise ValueError(msg)

        opts = Options(version=self._registry.latest(func))
        return LFC(Local(self._next(), self._cid, self._id, opts), func, args, kwargs, opts)

    @overload
    def rfi[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI[R]: ...
    @overload
    def rfi(
        self,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI: ...
    def rfi(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI:
        name, _, version = (func, None, self._registry.latest(func)) if isinstance(func, str) else self._registry.get(func)
        return RFI(Remote(self._next(), self._cid, self._id, name, args, kwargs, Options(version=version)))

    @overload
    def rfc[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC[R]: ...
    @overload
    def rfc(
        self,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> RFC: ...
    def rfc(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> RFC:
        name, _, version = (func, None, self._registry.latest(func)) if isinstance(func, str) else self._registry.get(func)
        return RFC(Remote(self._next(), self._cid, self._id, name, args, kwargs, Options(version=version)))

    @overload
    def detached[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI[R]: ...
    @overload
    def detached(
        self,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI: ...
    def detached(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI:
        """Schedules a function for immediate effectively-once execution that is completely detached from the current Call Graph, starting a new Call Graph.

        This method schedules the given function for remote execution through the
        global event loop. The function may run in a separate process or worker
        without being tied to the callers lifecycle. It is typically used for
        background or fire-and-forget workloads.

        The returned promise can still be awaited if the caller wishes to
        retrieve the final result upon completion.

        The function must be registered with Resonate before invocation, and all
        arguments must be serializable.

        Args:
            func (Callable | str): The function to execute remotely, or the registered
                name of the function.
            *args (Any): Positional arguments to pass to the function.
            **kwargs (Any): Keyword arguments to pass to the function.

        Returns:
            RFI: A remote function invocation handle representing the detached
            execution, which can optionally be awaited for the final result.

        """
        name, _, version = (func, None, self._registry.latest(func)) if isinstance(func, str) else self._registry.get(func)
        return RFI(Remote(self._next(), self._cid, self._id, name, args, kwargs, Options(version=version)), mode="detached")

    @overload
    def typesafe[T](self, cmd: LFI[T] | RFI[T]) -> Generator[LFI[T] | RFI[T], Promise[T], Promise[T]]: ...
    @overload
    def typesafe[T](self, cmd: LFC[T] | RFC[T] | Promise[T]) -> Generator[LFC[T] | RFC[T] | Promise[T], T, T]: ...
    def typesafe(self, cmd: LFI | RFI | LFC | RFC | Promise) -> Generator[LFI | RFI | LFC | RFC | Promise, Any, Any]:
        """Optionally provide type safety for your coroutine definition."""
        return (yield cmd)

    def sleep(self, secs: float) -> RFC[None]:
        """Suspends execution within a Resonate function for a specified duration.

        This method pauses the current function for the given number of seconds. It can
        be safely used within both local and remote Resonate-managed functions to
        simulate delays, throttle execution, or coordinate timing between tasks.

        There is no maximum sleep duration. Fractional (floating-point) values are
        supported for sub-second precision.

        Args:
            secs (float): The number of seconds to sleep. Can be a fractional value.

        Returns:
            RFC[None]: A handle representing the sleep operation, which resolves when
            the specified duration has elapsed.

        """
        if not isinstance(secs, int | float):
            msg = f"secs must be `float`, got {type(secs).__name__}"
            raise TypeError(msg)

        return RFC(Sleep(self._next(), secs))

    def promise(
        self,
        *,
        id: str | None = None,
        timeout: float | None = None,
        idempotency_key: str | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> RFI:
        """Get or creates a durable promise that can be awaited.

        If no `id` is provided, a new promise is created with a generated identifier.
        If an `id` is provided and a promise with that ID already exists, the existing
        promise is returned (provided the `idempotency_key` matches). This ensures safe,
        idempotent reuse of promises across invocations or retries.

        Promises are especially useful for **Human-In-The-Loop (HITL)** workflows, where
        execution needs to pause until a human provides input or takes an action. The
        created promise can later be fulfilled using `Resonate.promise.resolve()`.

        Args:
            id (str, optional): The unique identifier of the promise. If omitted, a new ID
                is generated automatically.
            timeout (float, optional): The duration in seconds before the promise expires.
                If `None`, the promise does not expire.
            idempotency_key (str, optional): A key used to ensure idempotent creation.
                When specified, an existing promise with the same key will be reused.
            data (Any, optional): Optional initial data to associate with the promise.
            tags (dict[str, str], optional): Metadata tags for categorizing or tracking
                the promise.

        Returns:
            RFI: An object representing the created or retrieved promise. The handle can be
            awaited to block until the promise is resolved.

        """
        if id is not None and not isinstance(id, str):
            msg = f"id must be `str | None`, got {type(id).__name__}"
            raise TypeError(msg)

        if timeout is not None and isinstance(timeout, int | float):
            msg = f"timeout must be `float`, got {type(timeout).__name__}"
            raise TypeError(msg)

        if idempotency_key is not None and not isinstance(idempotency_key, str):
            msg = f"idempotency_key must be `str | None`, got {type(idempotency_key).__name__}"
            raise TypeError(msg)

        if tags is not None and not isinstance(tags, dict):
            msg = f"tags must be `dict | None`, got {type(tags).__name__}"
            raise TypeError(tags)

        default_id = self._next()
        id = id or default_id

        return RFI(
            Base(
                id,
                timeout or 31536000,
                idempotency_key or id,
                data,
                tags,
            ),
        )

    def _next(self) -> str:
        self._counter += 1
        return f"{self._id}.{self._counter}"


class Time:
    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx

    def strftime(self, format: str) -> LFC[str]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:time", time).strftime(format))

    def time(self) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:time", time).time())


class Random:
    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx

    def betavariate(self, alpha: float, beta: float) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).betavariate(alpha, beta))

    def choice[T](self, seq: Sequence[T]) -> LFC[T]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).choice(seq))

    def expovariate(self, lambd: float = 1) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).expovariate(lambd))

    def getrandbits(self, k: int) -> LFC[int]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).getrandbits(k))

    def randint(self, a: int, b: int) -> LFC[int]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).randint(a, b))

    def random(self) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).random())

    def randrange(self, start: int, stop: int | None = None, step: int = 1) -> LFC[int]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).randrange(start, stop, step))

    def triangular(self, low: float = 0, high: float = 1, mode: float | None = None) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).triangular(low, high, mode))


class Function[**P, R]:
    __name__: str
    __type_params__: tuple[TypeVar | ParamSpec | TypeVarTuple, ...] = ()

    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], R], opts: Options) -> None:
        # updates the following attributes:
        # __module__
        # __name__
        # __qualname__
        # __doc__
        # __annotations__
        # __type_params__
        # __dict__
        functools.update_wrapper(self, func)

        self._resonate = resonate
        self._name = name
        self._func = func
        self._opts = opts

    @property
    def name(self) -> str:
        return self._name

    @property
    def func(self) -> Callable[Concatenate[Context, P], R]:
        return self._func

    def __call__(self, ctx: Context, *args: P.args, **kwargs: P.kwargs) -> R:
        return self._func(ctx, *args, **kwargs)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Function):
            return self._func == other._func
        if callable(other):
            return self._func == other
        return NotImplemented

    def __hash__(self) -> int:
        # Helpful for ensuring proper registry lookups, a function and an instance of Function
        # that wraps the same function has the same identity.
        return self._func.__hash__()

    def options(
        self,
        *,
        encoder: Encoder[Any, str | None] | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None = None,
        tags: dict[str, str] | None = None,
        target: str | None = None,
        timeout: float | None = None,
        version: int | None = None,
    ) -> Function[P, R]:
        """Configure invocation options for a registered function.

        This method allows fine-grained control over how a function is invoked,
        including encoding, idempotency behavior, retry strategy, timeout policy,
        and targeting.

        These options modify the metadata and runtime behavior of the function
        without altering its implementation. They can be applied directly or
        through decorators to customize invocation semantics for specific workloads.

        Args:
            encoder (Encoder[Any, str | None], optional): Custom data encoder used for
                serializing inputs or outputs.
            idempotency_key (str | Callable[[str], str], optional): Explicit key or a
                callable that derives an idempotency key from the promise ID.
                Ensures repeated invocations with the same key reuse prior results.
            retry_policy (RetryPolicy | Callable[[Callable], RetryPolicy], optional):
                Retry strategy defining how failed invocations are retried. Common
                options include exponential, constant, linear, or never.
            tags (dict[str, str], optional): Metadata tags applied to the durable
                promise representing the invocation. Useful for tracing and filtering.
            target (str, optional): The target node, worker group, or endpoint to route
                the invocation to.
            timeout (float, optional): Maximum number of seconds before the invocation
                times out. If `None`, no timeout is enforced.
            version (int, optional): Specific version of the function to invoke. Useful
                for staged rollouts or backward compatibility.

        Returns:
            Function[P, R]: The function configured with the specified invocation options.

        """
        self._opts = self._opts.merge(
            encoder=encoder,
            idempotency_key=idempotency_key,
            retry_policy=retry_policy,
            tags=tags,
            target=target,
            timeout=timeout,
            version=version,
        )
        return self

    def run[T](self: Function[P, Generator[Any, Any, T] | T], id: str, *args: P.args, **kwargs: P.kwargs) -> T:
        """Run a registered function with Resonate and waits for the result.

        This method executes the specified function under a durable promise identified
        by the given `id`. If a promise with the same `id` already exists, Resonate
        subscribes to its result or returns it immediately if it has completed.

        Duplicate executions for the same `id` are automatically prevented, ensuring
        idempotent and consistent behavior across distributed runs.

        This is a **blocking operation**—execution will not continue until the function
        result is available.

        Args:
            id (str): The unique identifier of the durable promise. Reusing an ID
                ensures idempotent execution.
            *args (P.args): Positional arguments passed to the function.
            **kwargs (P.kwargs): Keyword arguments passed to the function.

        Returns:
            T: The final result of the executed function.

        """
        return self.begin_run(id, *args, **kwargs).result()

    def begin_run[T](self: Function[P, Generator[Any, Any, T] | T], id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[T]:
        """Run a registered function asynchronously with Resonate.

        This method schedules the function for execution under a durable promise
        identified by the given `id`. If a promise with the same `id` already exists,
        Resonate subscribes to its result or returns it immediately if it has completed.

        Unlike `Function.run`, this method is **non-blocking** and returns a handle
        (`Handle[T]`) that can be awaited or queried later for the final result.

        Duplicate executions for the same `id` are automatically prevented, ensuring
        idempotent and consistent behavior across distributed runs.

        Args:
            id (str): The unique identifier of the durable promise. Reusing an ID
                ensures idempotent execution.
            *args (P.args): Positional arguments passed to the function.
            **kwargs (P.kwargs): Keyword arguments passed to the function.

        Returns:
            Handle[T]: A handle representing the asynchronous execution. The handle
            can be awaited or inspected for status and results.

        """
        resonate = self._resonate.options(
            encoder=self._opts.encoder,
            idempotency_key=self._opts.idempotency_key,
            retry_policy=self._opts.retry_policy,
            tags=self._opts.tags,
            target=self._opts.target,
            timeout=self._opts.timeout,
            version=self._opts.version,
        )
        return resonate.begin_run(id, self._func, *args, **kwargs)

    def rpc[T](self: Function[P, Generator[Any, Any, T] | T], id: str, *args: P.args, **kwargs: P.kwargs) -> T:
        """Run a registered function remotely with Resonate and waits for the result.

        This method executes the specified function on a remote worker or process under
        a durable promise identified by the given `id`. If a promise with the same `id`
        already exists, Resonate subscribes to its result or returns it immediately if
        it has already completed.

        This is a **blocking operation**, and execution will not continue until the
        remote function completes and returns its result.

        Duplicate executions for the same `id` are automatically prevented, ensuring
        idempotent and consistent behavior across distributed runs.

        Args:
            id (str): The unique identifier of the durable promise. Reusing an ID
                ensures idempotent remote execution.
            *args (P.args): Positional arguments passed to the function.
            **kwargs (P.kwargs): Keyword arguments passed to the function.

        Returns:
            T: The final result returned from the remote function execution.

        """
        return self.begin_rpc(id, *args, **kwargs).result()

    def begin_rpc[T](self: Function[P, Generator[Any, Any, T] | T], id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[T]:
        """Run a registered function remotely with Resonate and returns a handle.

        This method schedules the specified function for remote execution under a
        durable promise identified by the given `id`. The function runs on a remote
        worker or process as part of the distributed execution environment.

        Unlike `Function.rpc`, this method is **non-blocking** and immediately returns
        a `Handle[T]` object that can be awaited or queried later to retrieve the final
        result once remote execution completes.

        If a durable promise with the same `id` already exists, Resonate subscribes to
        its result or returns it immediately if it has already completed. Duplicate
        executions for the same `id` are automatically prevented, ensuring idempotent
        and consistent behavior.

        Args:
            id (str): The unique identifier of the durable promise. Reusing an ID
                ensures idempotent remote execution.
            *args (P.args): Positional arguments passed to the function.
            **kwargs (P.kwargs): Keyword arguments passed to the function.

        Returns:
            Handle[T]: A handle representing the asynchronous remote execution. The
            handle can be awaited or inspected for completion and results.

        """
        resonate = self._resonate.options(
            encoder=self._opts.encoder,
            idempotency_key=self._opts.idempotency_key,
            retry_policy=self._opts.retry_policy,
            tags=self._opts.tags,
            target=self._opts.target,
            timeout=self._opts.timeout,
            version=self._opts.version,
        )
        return resonate.begin_rpc(id, self._func, *args, **kwargs)
