from __future__ import annotations

import copy
import random
import uuid
from collections.abc import Callable
from concurrent.futures import Future
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Concatenate, overload

from resonate.bridge import Bridge
from resonate.conventions.base import Base
from resonate.conventions.default import Default
from resonate.conventions.sleep import Sleep
from resonate.coroutine import LFC, LFI, RFC, RFI
from resonate.dependencies import Dependencies
from resonate.message_sources.poller import Poller
from resonate.models.commands import Invoke, Listen
from resonate.models.durable_promise import DurablePromise
from resonate.models.handle import Handle
from resonate.options import Options
from resonate.registry import Registry
from resonate.retry_policies import Exponential, Never
from resonate.stores.local import LocalStore
from resonate.stores.remote import RemoteStore

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.models.context import Info
    from resonate.models.encoder import Encoder
    from resonate.models.message_source import MessageSource
    from resonate.models.retry_policy import RetryPolicy
    from resonate.models.store import PromiseStore, Store


class Resonate:
    def __init__(
        self,
        *,
        url: str | None = None,
        pid: str | None = None,
        ttl: int = 10,
        anycast: str | None = None,
        unicast: str | None = None,
        store: Store | None = None,
        message_source: MessageSource | None = None,
        encoder: Encoder[Any, str | None] | None = None,
        registry: Registry | None = None,
        dependencies: Dependencies | None = None,
    ) -> None:
        self._started = False

        self._pid = pid or uuid.uuid4().hex
        self._opts = Options()

        self._registry = registry or Registry()
        self._dependencies = dependencies or Dependencies()

        self._store = store or LocalStore() if url is None else RemoteStore(url)
        assert not isinstance(self._store, LocalStore) or message_source is None

        message_source = message_source or self._store.as_msg_source() if isinstance(self._store, LocalStore) else Poller()

        # TODO(dfarr): grab default addresses from message source
        self._unicast = unicast or f"poll://default/{self._pid}"
        self._anycast = anycast or f"poll://default/{self._pid}"

        self._bridge = Bridge(
            ctx=lambda id, info: Context(id, info, self._opts, self._registry, self._dependencies),
            pid=self._pid,
            ttl=ttl,
            anycast=self._anycast,
            unicast=self._unicast,
            store=self._store,
            message_source=message_source,
            registry=self._registry,
        )

    def start(self) -> None:
        if not self._started:
            self._bridge.start()

    def stop(self) -> None:
        self._started = False
        self._bridge.stop()

    def options(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        send_to: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Resonate:
        copied: Resonate = copy.copy(self)
        copied._opts = self._opts.merge(send_to=send_to, timeout=timeout, version=version, tags=tags, retry_policy=retry_policy)
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
        match func:
            case str():
                name = func
                func, version = self._registry.get(func, self._opts.version)
            case Callable():
                func = func.func if isinstance(func, Function) else func
                name, version = self._registry.get(func, self._opts.version)

        fp, fv = Future[DurablePromise](), Future[R]()
        self._bridge.invoke(Invoke(id, name, func, args, kwargs, self._opts.merge(version=version)), futures=(fp, fv))

        fp.result()
        return Handle(fv)

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
        match func:
            case str():
                name, version = func, self._registry.latest(func)
            case Callable():
                name, version = self._registry.get(func.func if isinstance(func, Function) else func, self._opts.version)

        # For rpc make version = 1 as the default instead of 0
        version = version or 1

        fp, fv = Future[DurablePromise](), Future[R]()
        self._bridge.invoke(Invoke(id, name, None, args, kwargs, self._opts.merge(version=version)), futures=(fp, fv))

        fp.result()
        return Handle(fv)

    def get(self, id: str) -> Handle[Any]:
        self.start()
        fp, fv = Future[DurablePromise](), Future[Any]()
        self._bridge.listen(Listen(id), futures=(fp, fv))

        fp.result()
        return Handle(fv)

    def set_dependency(self, name: str, obj: Any) -> None:
        self._dependencies.add(name, obj)

    @property
    def promises(self) -> PromiseStore:
        return self._store.promises


# Context
class Context:
    def __init__(self, id: str, info: Info, opts: Options, registry: Registry, dependencies: Dependencies) -> None:
        self._id = id
        self._info = info
        self._opts = opts
        self._registry = registry
        self._dependencies = dependencies
        self._counter = 0

    @property
    def id(self) -> str:
        return self._id

    @property
    def info(self) -> Info:
        return self._info

    @overload
    def lfi[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> LFI: ...
    @overload
    def lfi[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> LFI: ...
    @overload
    def lfi(self, func: str, *args: Any, **kwargs: Any) -> LFI: ...
    def lfi(self, func: Callable | str, *args: Any, **kwargs: Any) -> LFI:
        self._counter += 1
        func, version, versions = self._lfi_func(func)
        retry_policy = Never() if isgeneratorfunction(func) else Exponential()
        return LFI(f"{self.id}.{self._counter}", func, args, kwargs, Options(version=version, retry_policy=retry_policy, timeout=self._opts.timeout), versions)

    @overload
    def lfc[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> LFC: ...
    @overload
    def lfc[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> LFC: ...
    @overload
    def lfc(self, func: str, *args: Any, **kwargs: Any) -> LFC: ...
    def lfc(self, func: Callable | str, *args: Any, **kwargs: Any) -> LFC:
        self._counter += 1
        retry_policy = Never() if isgeneratorfunction(func) else Exponential()
        func, version, versions = self._lfi_func(func)
        return LFC(f"{self.id}.{self._counter}", func, args, kwargs, Options(version=version, retry_policy=retry_policy, timeout=self._opts.timeout), versions)

    @overload
    def rfi[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def rfi[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def rfi(self, func: str, *args: Any, **kwargs: Any) -> RFI: ...
    def rfi(self, func: Callable | str, *args: Any, **kwargs: Any) -> RFI:
        self._counter += 1
        func, version, versions = self._rfi_func(func)
        return RFI(f"{self.id}.{self._counter}", Default(func, args, kwargs, versions, self._registry, Options(version=version, timeout=self._opts.timeout)))

    @overload
    def rfc[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> RFC: ...
    @overload
    def rfc[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> RFC: ...
    @overload
    def rfc(self, func: str, *args: Any, **kwargs: Any) -> RFC: ...
    def rfc(self, func: Callable | str, *args: Any, **kwargs: Any) -> RFC:
        self._counter += 1
        func, version, versions = self._rfi_func(func)
        return RFC(f"{self.id}.{self._counter}", Default(func, args, kwargs, versions, self._registry, Options(version=version, timeout=self._opts.timeout)))

    @overload
    def detached[**P, R](self, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def detached[**P, R](self, func: Callable[Concatenate[Context, P], R], *args: P.args, **kwargs: P.kwargs) -> RFI: ...
    @overload
    def detached(self, func: str, *args: Any, **kwargs: Any) -> RFI: ...
    def detached(self, func: Callable | str, *args: Any, **kwargs: Any) -> RFI:
        self._counter += 1
        func, version, versions = self._rfi_func(func)
        return RFI(f"{self.id}.{self._counter}", Default(func, args, kwargs, versions, self._registry, Options(version=version)), mode="detached")

    def sleep(self, secs: int) -> RFC:
        self._counter += 1
        return RFC(f"{self.id}.{self._counter}", Sleep(secs))

    def promise(self, data: Any = None, headers: dict[str, str] | None = None) -> RFI:
        self._counter += 1
        return RFI(f"{self.id}.{self._counter}", Base(data, headers))

    def random(self, a: int, b: int) -> LFC:
        return self.lfc(lambda _, a, b: random.randint(a, b), a, b)

    def get_dependency(self, name: str) -> Any:
        return self._dependencies.get(name)

    def _lfi_func(self, f: str | Callable) -> tuple[Callable, int, dict[int, Callable] | None]:
        match f:
            case str():
                return *self._registry.get(f), self._registry.all(f)
            case Callable():
                return f, self._registry.latest(f), None

    def _rfi_func(self, f: str | Callable) -> tuple[str, int, set[int] | None]:
        match f:
            case str():
                return f, self._registry.latest(f), None
            case Callable():
                return *self._registry.get(f), self._registry.all(f)


# Function


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
        retry_policy: RetryPolicy | None = None,
        send_to: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Function[P, Generator[Any, Any, R] | R]:
        return Function(
            self._resonate,
            self._name,
            self._func,
            self._opts.merge(retry_policy=retry_policy, send_to=send_to, tags=tags, timeout=timeout, version=version),
        )

    def run(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        return self._resonate.options(**self._opts.to_dict()).run(id, self._name, *args, **kwargs)

    def rpc(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        return self._resonate.options(**self._opts.to_dict()).rpc(id, self._name, *args, **kwargs)
