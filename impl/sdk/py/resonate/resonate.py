from __future__ import annotations

import copy
import random
import time
import uuid
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Concatenate, overload

from resonate.bridge import Bridge
from resonate.conventions import Base, Local, Remote, Sleep
from resonate.coroutine import LFC, LFI, RFC, RFI
from resonate.dependencies import Dependencies
from resonate.message_sources import LocalMessageSource, Poller
from resonate.models.handle import Handle
from resonate.options import Options
from resonate.registry import Registry
from resonate.stores import LocalStore, RemoteStore

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from resonate.models.context import Info
    from resonate.models.message_source import MessageSource
    from resonate.models.retry_policy import RetryPolicy
    from resonate.models.store import PromiseStore, Store


class Resonate:
    def __init__(
        self,
        *,
        pid: str | None = None,
        ttl: int = 10,
        group: str = "default",
        registry: Registry | None = None,
        dependencies: Dependencies | None = None,
        store: Store | None = None,
        message_source: MessageSource | None = None,
    ) -> None:
        # enforce mutual inclusion/exclusion of store and message source
        assert (store is None) == (message_source is None), "store and message source must both be set or both be unset"
        assert not isinstance(store, LocalStore) or isinstance(message_source, LocalMessageSource), "message source must be local message source"
        assert not isinstance(store, RemoteStore) or not isinstance(message_source, LocalMessageSource), "message source must not be local message source"

        self._started = False
        self._pid = pid or uuid.uuid4().hex
        self._ttl = ttl
        self._group = group
        self._opts = Options()
        self._registry = registry or Registry()
        self._dependencies = dependencies or Dependencies()

        if store and message_source:
            self._store = store
            self._message_source = message_source
        else:
            self._store = LocalStore()
            self._message_source = self._store.message_source(self._group, self._pid)

        self._bridge = Bridge(
            ctx=lambda id, info: Context(id, info, self._opts, self._registry, self._dependencies),
            pid=self._pid,
            ttl=ttl,
            anycast=self._message_source.anycast,
            unicast=self._message_source.unicast,
            registry=self._registry,
            store=self._store,
            message_source=self._message_source,
        )

    @classmethod
    def local(
        cls,
        pid: str | None = None,
        ttl: int = 10,
        group: str = "default",
        registry: Registry | None = None,
        dependencies: Dependencies | None = None,
    ) -> Resonate:
        pid = pid or uuid.uuid4().hex
        store = LocalStore()

        return cls(
            pid=pid,
            ttl=ttl,
            group=group,
            registry=registry,
            dependencies=dependencies,
            store=store,
            message_source=store.message_source(group=group, id=pid),
        )

    @classmethod
    def remote(
        cls,
        host: str | None = None,
        store_port: str | None = None,
        message_source_port: str | None = None,
        pid: str | None = None,
        ttl: int = 10,
        group: str = "default",
        registry: Registry | None = None,
        dependencies: Dependencies | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Resonate:
        pid = pid or uuid.uuid4().hex

        return cls(
            pid=pid,
            ttl=ttl,
            group=group,
            registry=registry,
            dependencies=dependencies,
            store=RemoteStore(host=host, port=store_port, retry_policy=retry_policy),
            message_source=Poller(group=group, id=pid, host=host, port=message_source_port),
        )

    @property
    def promises(self) -> PromiseStore:
        return self._store.promises

    def start(self) -> None:
        if not self._started:
            self._bridge.start()

    def stop(self) -> None:
        self._started = False
        self._bridge.stop()

    def options(
        self,
        *,
        idempotency_key: str | Callable[[str], str] | None = None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None = None,
        send_to: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Resonate:
        copied: Resonate = copy.copy(self)
        copied._opts = self._opts.merge(idempotency_key=idempotency_key, retry_policy=retry_policy, send_to=send_to, tags=tags, timeout=timeout, version=version)
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
    ) -> Callable[[Callable], Function[P, Any]]: ...
    def register[**P, R](
        self,
        *args: Callable | None,
        name: str | None = None,
        version: int = 1,
    ) -> Callable[[Callable], Function[P, R]] | Function[P, R]:
        def wrapper(func: Callable) -> Function[P, R]:
            self._registry.add(func.func if isinstance(func, Function) else func, name or func.__name__, version)
            return Function(self, name or func.__name__, func, self._opts.merge(version=version))

        if args and callable(args[0]):
            return wrapper(args[0])

        return wrapper

    @overload
    def run[**P, R](self, id: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def run[**P, R](self, id: str, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def run(self, id: str, func: str, *args: Any, **kwargs: Any) -> Handle[Any]: ...
    def run[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]:
        self.start()

        future = Future[R]()
        name, func, version = self._registry.get(func, self._opts.version)
        opts = self._opts.merge(version=version)
        self._bridge.run(Remote(id, name, args, kwargs, opts), func, args, kwargs, opts, future)

        return Handle(future)

    @overload
    def rpc[**P, R](self, id: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def rpc[**P, R](self, id: str, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> Handle[R]: ...
    @overload
    def rpc(self, id: str, func: str, *args: Any, **kwargs: Any) -> Handle[Any]: ...
    def rpc[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]:
        self.start()

        future = Future[R]()
        name, _, version = (func, None, self._registry.latest(func)) if isinstance(func, str) else self._registry.get(func, self._opts.version)
        self._bridge.rpc(Remote(id, name, args, kwargs, self._opts.merge(version=version)), future)

        return Handle(future)

    def get(self, id: str) -> Handle[Any]:
        self.start()

        future = Future()
        self._bridge.get(id, future)

        return Handle(future)

    def set_dependency(self, name: str, obj: Any) -> None:
        self._dependencies.add(name, obj)


class Context:
    def __init__(self, id: str, info: Info, opts: Options, registry: Registry, dependencies: Dependencies) -> None:
        self._id = id
        self._info = info
        self._opts = opts
        self._registry = registry
        self._dependencies = dependencies
        self._random = Random(self)
        self._counter = 0

    @property
    def id(self) -> str:
        return self._id

    @property
    def info(self) -> Info:
        return self._info

    @property
    def random(self) -> Random:
        return self._random

    def get_dependency[T](self, key: str, default: T = None) -> Any | T:
        return self._dependencies.get(key, default)

    @overload
    def lfi[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> LFI: ...
    @overload
    def lfi[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> LFI: ...
    def lfi(self, func: Callable, *args: Any, **kwargs: Any) -> LFI:
        self._counter += 1
        opts = Options(timeout=self._opts.timeout, version=self._registry.latest(func))
        return LFI(Local(f"{self.id}.{self._counter}", opts), func, args, kwargs, opts)

    @overload
    def lfc[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> LFC: ...
    @overload
    def lfc[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> LFC: ...
    def lfc(self, func: Callable, *args: Any, **kwargs: Any) -> LFC:
        self._counter += 1
        opts = Options(timeout=self._opts.timeout, version=self._registry.latest(func))
        return LFC(Local(f"{self.id}.{self._counter}", opts), func, args, kwargs, opts)

    @overload
    def rfi[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def rfi[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def rfi(self, func: str, *args: Any, **kwargs: Any) -> RFI: ...
    def rfi(self, func: Callable | str, *args: Any, **kwargs: Any) -> RFI:
        assert self._opts.version == 0

        self._counter += 1
        name, _, version = (func, None, self._registry.latest(func)) if isinstance(func, str) else self._registry.get(func)
        return RFI(Remote(f"{self.id}.{self._counter}", name, args, kwargs, Options(timeout=self._opts.timeout, version=version)))

    @overload
    def rfc[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> RFC: ...
    @overload
    def rfc[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> RFC: ...
    @overload
    def rfc(self, func: str, *args: Any, **kwargs: Any) -> RFC: ...
    def rfc(self, func: Callable | str, *args: Any, **kwargs: Any) -> RFC:
        assert self._opts.version == 0

        self._counter += 1
        name, _, version = (func, None, self._registry.latest(func)) if isinstance(func, str) else self._registry.get(func)
        return RFC(Remote(f"{self.id}.{self._counter}", name, args, kwargs, Options(timeout=self._opts.timeout, version=version)))

    @overload
    def detached[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def detached[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def detached(self, func: str, *args: Any, **kwargs: Any) -> RFI: ...
    def detached(self, func: Callable | str, *args: Any, **kwargs: Any) -> RFI:
        assert self._opts.version == 0

        self._counter += 1
        name, _, version = (func, None, self._registry.latest(func)) if isinstance(func, str) else self._registry.get(func)
        return RFI(Remote(f"{self.id}.{self._counter}", name, args, kwargs, Options(version=version)), mode="detached")

    def sleep(self, secs: int) -> RFC:
        self._counter += 1
        return RFC(Sleep(f"{self.id}.{self._counter}", int((time.time() + secs) * 1000)))

    def promise(
        self,
        *,
        id: str | None = None,
        idempotency_key: str | None = None,
        headers: dict[str, str] | None = None,
        data: Any = None,
        timeout: int | None = None,
        tags: dict[str, str] | None = None,
    ) -> RFI:
        self._counter += 1

        id = id or self._opts.id or f"{self.id}.{self._counter}"
        ikey = idempotency_key or self._opts.idempotency_key
        timeout = timeout or self._opts.timeout
        tags = tags or self._opts.tags
        return RFI(Base(id, ikey(id) if callable(ikey) else ikey, headers, data, timeout, tags))


class Random:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx

    def random(self) -> LFC:
        return self.ctx.lfc(lambda _: self.ctx.get_dependency("resonate:random", random).random())

    def betavariate(self, alpha: float, beta: float) -> LFC:
        return self.ctx.lfc(lambda _: self.ctx.get_dependency("resonate:random", random).betavariate(alpha, beta))

    def randrange(self, start: int, stop: int | None = None, step: int = 1) -> LFC:
        return self.ctx.lfc(lambda _: self.ctx.get_dependency("resonate:random", random).randrange(start, stop, step))

    def randint(self, a: int, b: int) -> LFC:
        return self.ctx.lfc(lambda _: self.ctx.get_dependency("resonate:random", random).randint(a, b))

    def getrandbits(self, k: int) -> LFC:
        return self.ctx.lfc(lambda _: self.ctx.get_dependency("resonate:random", random).getrandbits(k))

    def triangular(self, low: float = 0, high: float = 1, mode: float | None = None) -> LFC:
        return self.ctx.lfc(lambda _: self.ctx.get_dependency("resonate:random", random).triangular(low, high, mode))

    def expovariate(self, lambd: float = 1) -> LFC:
        return self.ctx.lfc(lambda _: self.ctx.get_dependency("resonate:random", random).expovariate(lambd))


class Function[**P, R]:
    @overload
    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], opts: Options) -> None: ...
    @overload
    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], R], opts: Options) -> None: ...
    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R], opts: Options) -> None:
        self._resonate = resonate
        self._name = name
        self._func = func
        self._opts = opts

    @property
    def name(self) -> str:
        return self._name

    @property
    def func(self) -> Callable:
        return self._func

    @property
    def __name__(self) -> str:
        return self._name

    def __call__(self, ctx: Context, *args: P.args, **kwargs: P.kwargs) -> Generator[Any, Any, R] | R:
        return self._func(ctx, *args, **kwargs)

    def options(
        self,
        *,
        idempotency_key: str | Callable[[str], str] | None = None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None = None,
        send_to: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Function[P, R]:
        self._opts = self._opts.merge(
            idempotency_key=idempotency_key,
            retry_policy=retry_policy,
            send_to=send_to,
            tags=tags,
            timeout=timeout,
            version=version,
        )
        return self

    def run(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        resonate = self._resonate.options(
            idempotency_key=self._opts.idempotency_key,
            retry_policy=self._opts.retry_policy,
            send_to=self._opts.send_to,
            tags=self._opts.tags,
            timeout=self._opts.timeout,
            version=self._opts.version,
        )
        return resonate.run(id, self._name, *args, **kwargs)

    def rpc(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        resonate = self._resonate.options(
            idempotency_key=self._opts.idempotency_key,
            retry_policy=self._opts.retry_policy,
            send_to=self._opts.send_to,
            tags=self._opts.tags,
            timeout=self._opts.timeout,
            version=self._opts.version,
        )
        return resonate.rpc(id, self._name, *args, **kwargs)
