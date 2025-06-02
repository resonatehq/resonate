from __future__ import annotations

import logging
import sys
import time
import uuid
from concurrent.futures import Future
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Protocol

from resonate.conventions import Base, Local
from resonate.coroutine import LFC, LFI, RFC, RFI
from resonate.encoders import JsonEncoder, NoopEncoder, PairEncoder
from resonate.models.commands import (
    CancelPromiseReq,
    CancelPromiseRes,
    Command,
    CreateCallbackReq,
    CreatePromiseReq,
    CreatePromiseRes,
    CreatePromiseWithTaskReq,
    CreatePromiseWithTaskRes,
    Function,
    Invoke,
    Network,
    Receive,
    RejectPromiseReq,
    RejectPromiseRes,
    ResolvePromiseReq,
    ResolvePromiseRes,
    Resume,
    Return,
)
from resonate.models.result import Ko, Ok
from resonate.models.task import Task
from resonate.options import Options
from resonate.resonate import Remote
from resonate.scheduler import Scheduler
from resonate.stores import LocalStore

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.models.context import Info
    from resonate.models.logger import Logger
    from resonate.registry import Registry


# Context
class Context:
    def __init__(self, id: str, cid: str) -> None:
        self.id = id
        self.cid = cid

    @property
    def info(self) -> Info:
        raise NotImplementedError

    @property
    def logger(self) -> Logger:
        return logging.getLogger("test")

    def get_dependency(self, key: str, default: Any = None) -> Any:
        return default

    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(Local(uuid.uuid4().hex, self.cid, self.id), func, args, kwargs)

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        assert not isinstance(func, str)
        return LFC(Local(uuid.uuid4().hex, self.cid, self.id), func, args, kwargs)

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(uuid.uuid4().hex, self.cid, self.id, func.__name__, args, kwargs))

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        assert not isinstance(func, str)
        return RFC(Remote(uuid.uuid4().hex, self.cid, self.id, func.__name__, args, kwargs))

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(uuid.uuid4().hex, self.cid, self.id, func.__name__, args, kwargs), mode="detached")


class LocalContext:
    def __init__(self, id: str, cid: str) -> None:
        self.id = id
        self.cid = cid

    @property
    def info(self) -> Info:
        raise NotImplementedError

    @property
    def logger(self) -> Logger:
        return logging.getLogger("test")

    def get_dependency(self, key: str, default: Any = None) -> Any:
        return default

    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(Local(uuid.uuid4().hex, self.cid, self.id), func, args, kwargs)

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        assert not isinstance(func, str)
        return LFC(Local(uuid.uuid4().hex, self.cid, self.id), func, args, kwargs)

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(Local(uuid.uuid4().hex, self.cid, self.id), func, args, kwargs)

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFC:
        assert not isinstance(func, str)
        return LFC(Local(uuid.uuid4().hex, self.cid, self.id), func, args, kwargs)

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> LFI:
        assert not isinstance(func, str)
        return LFI(Local(uuid.uuid4().hex, self.cid, self.id), func, args, kwargs)


class RemoteContext:
    def __init__(self, id: str, cid: str) -> None:
        self.id = id
        self.cid = cid

    @property
    def info(self) -> Info:
        raise NotImplementedError

    @property
    def logger(self) -> Logger:
        return logging.getLogger("test")

    def get_dependency(self, key: str, default: Any = None) -> Any:
        return default

    def lfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(uuid.uuid4().hex, self.cid, self.id, func.__name__, args, kwargs))

    def lfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        assert not isinstance(func, str)
        return RFC(Remote(uuid.uuid4().hex, self.cid, self.id, func.__name__, args, kwargs))

    def rfi(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(uuid.uuid4().hex, self.cid, self.id, func.__name__, args, kwargs))

    def rfc(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFC:
        assert not isinstance(func, str)
        return RFC(Remote(uuid.uuid4().hex, self.cid, self.id, func.__name__, args, kwargs))

    def detached(self, func: str | Callable, *args: Any, **kwargs: Any) -> RFI:
        assert not isinstance(func, str)
        return RFI(Remote(uuid.uuid4().hex, self.cid, self.id, func.__name__, args, kwargs), mode="detached")


# Runners


class Runner(Protocol):
    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R: ...


class SimpleRunner:
    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        return self._run(id, func, args, kwargs)

    def _run(self, id: str, func: Callable, args: tuple, kwargs: dict) -> Any:
        if not isgeneratorfunction(func):
            return func(None, *args, **kwargs)

        g = func(LocalContext(id, id), *args, **kwargs)
        v = None

        try:
            while True:
                match g.send(v):
                    case LFI(conv, func, args, kwargs):
                        v = (conv.id, func, args, kwargs)
                    case LFC(conv, func, args, kwargs):
                        v = self._run(conv.id, func, args, kwargs)
                    case (id, func, args, kwargs):
                        v = self._run(id, func, args, kwargs)
        except StopIteration as e:
            return e.value


class ResonateRunner:
    def __init__(self, registry: Registry) -> None:
        # registry
        self.registry = registry

        # store
        self.store = LocalStore()

        # encoder
        self.encoder = PairEncoder(NoopEncoder(), JsonEncoder())

        # create scheduler and connect store
        self.scheduler = Scheduler(ctx=lambda id, cid, *_: Context(id, cid))

    def run[**P, R](self, id: str, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        cmds: list[Command] = []
        init = True
        conv = Remote(id, id, id, func.__name__, args, kwargs)
        future = Future[R]()

        headers, data = self.encoder.encode(conv.data)
        assert headers is None

        promise, _ = self.store.promises.create_with_task(
            id=conv.id,
            ikey=conv.idempotency_key,
            timeout=int((time.time() + conv.timeout) * 1000),
            data=data,
            tags=conv.tags,
            pid=self.scheduler.pid,
            ttl=sys.maxsize,
        )

        cmds.append(Invoke(id, conv, promise.abs_timeout, func, args, kwargs, promise=promise))

        while cmds:
            next = self.scheduler.step(cmds.pop(0), future if init else None)
            init = False

            for req in next.reqs:
                match req:
                    case Function(_id, cid, f):
                        try:
                            r = Ok(f())
                        except Exception as e:
                            r = Ko(e)
                        cmds.append(Return(_id, cid, r))

                    case Network(_id, cid, CreatePromiseReq(id, timeout, ikey, strict, headers, data, tags)):
                        promise = self.store.promises.create(
                            id=id,
                            timeout=timeout,
                            ikey=ikey,
                            strict=strict,
                            headers=headers,
                            data=data,
                            tags=tags,
                        )
                        cmds.append(Receive(_id, cid, CreatePromiseRes(promise)))

                    case Network(_id, cid, CreatePromiseWithTaskReq(id, timeout, pid, ttl, ikey, strict, headers, data, tags)):
                        promise, task = self.store.promises.create_with_task(
                            id=id,
                            timeout=timeout,
                            pid=pid,
                            ttl=ttl,
                            ikey=ikey,
                            strict=strict,
                            headers=headers,
                            data=data,
                            tags=tags,
                        )
                        cmds.append(Receive(_id, cid, CreatePromiseWithTaskRes(promise, task)))

                    case Network(_id, cid, ResolvePromiseReq(id, ikey, strict, headers, data)):
                        promise = self.store.promises.resolve(
                            id=id,
                            ikey=ikey,
                            strict=strict,
                            headers=headers,
                            data=data,
                        )
                        cmds.append(Receive(_id, cid, ResolvePromiseRes(promise)))

                    case Network(_id, cid, RejectPromiseReq(id, ikey, strict, headers, data)):
                        promise = self.store.promises.reject(
                            id=id,
                            ikey=ikey,
                            strict=strict,
                            headers=headers,
                            data=data,
                        )
                        cmds.append(Receive(_id, cid, RejectPromiseRes(promise)))

                    case Network(_id, cid, CancelPromiseReq(id, ikey, strict, headers, data)):
                        promise = self.store.promises.cancel(
                            id=id,
                            ikey=ikey,
                            strict=strict,
                            headers=headers,
                            data=data,
                        )
                        cmds.append(Receive(_id, cid, CancelPromiseRes(promise)))

                    case Network(_id, cid, CreateCallbackReq(promise_id, root_promise_id, timeout, recv)):
                        promise, callback = self.store.promises.callback(
                            promise_id=promise_id,
                            root_promise_id=root_promise_id,
                            recv=recv,
                            timeout=timeout,
                        )
                        if promise.completed:
                            assert not callback
                            cmds.append(Resume(_id, cid, promise))

                    case _:
                        raise NotImplementedError

            for _, msg in self.store.step():
                match msg:
                    case {"type": "invoke", "task": {"id": id, "counter": counter}}:
                        task = Task(id=id, counter=counter, store=self.store)
                        root, leaf = task.claim(pid=self.scheduler.pid, ttl=sys.maxsize)
                        assert root.pending
                        assert not leaf

                        data = self.encoder.decode(root.param.to_tuple())
                        assert isinstance(data, dict)
                        assert "func" in data
                        assert "args" in data
                        assert "kwargs" in data

                        _, func, version = self.registry.get(data["func"])

                        cmds.append(
                            Invoke(
                                root.id,
                                Base(
                                    root.id,
                                    root.rel_timeout,
                                    root.ikey_for_create,
                                    root.param.data,
                                    root.tags,
                                ),
                                root.abs_timeout,
                                func,
                                data["args"],
                                data["kwargs"],
                                Options(version=version),
                                root,
                            )
                        )

                    case {"type": "resume", "task": {"id": id, "counter": counter}}:
                        task = Task(id=id, counter=counter, store=self.store)
                        root, leaf = task.claim(pid=self.scheduler.pid, ttl=sys.maxsize)
                        assert root.pending
                        assert leaf
                        assert leaf.completed

                        cmds.append(
                            Resume(
                                id=leaf.id,
                                cid=root.id,
                                promise=leaf,
                            )
                        )

                    case _:
                        raise NotImplementedError

        return future.result()


class ResonateLFXRunner(ResonateRunner):
    def __init__(self, registry: Registry) -> None:
        self.registry = registry

        # create store
        self.store = LocalStore()

        # create encoder
        self.encoder = PairEncoder(NoopEncoder(), JsonEncoder())

        # create scheduler
        self.scheduler = Scheduler(ctx=lambda id, cid, *_: LocalContext(id, cid))


class ResonateRFXRunner(ResonateRunner):
    def __init__(self, registry: Registry) -> None:
        self.registry = registry

        # create store
        self.store = LocalStore()

        # create encoder
        self.encoder = PairEncoder(NoopEncoder(), JsonEncoder())

        # create scheduler and connect store
        self.scheduler = Scheduler(ctx=lambda id, cid, *_: RemoteContext(id, cid))
