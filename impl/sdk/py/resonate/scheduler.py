from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Final, Literal

from resonate import utils
from resonate.conventions import Base
from resonate.coroutine import AWT, LFI, RFI, TRM, Coroutine
from resonate.graph import Graph, Node
from resonate.models.clock import Clock
from resonate.models.commands import (
    CancelPromiseReq,
    CancelPromiseRes,
    Command,
    CreateCallbackReq,
    CreatePromiseReq,
    CreatePromiseRes,
    CreateSubscriptionReq,
    Delayed,
    Function,
    Invoke,
    Listen,
    Network,
    Notify,
    Receive,
    RejectPromiseReq,
    RejectPromiseRes,
    ResolvePromiseReq,
    ResolvePromiseRes,
    Resume,
    Retry,
    Return,
)
from resonate.models.result import Ko, Ok, Result
from resonate.options import Options

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future

    from resonate.errors import ResonateShutdownError
    from resonate.models.context import Context
    from resonate.models.convention import Convention
    from resonate.models.durable_promise import DurablePromise
    from resonate.models.encoder import Encoder
    from resonate.models.retry_policy import RetryPolicy


class Scheduler:
    def __init__(
        self,
        ctx: Callable[[str, str, Info], Context],
        pid: str | None = None,
        unicast: str | None = None,
        anycast: str | None = None,
    ) -> None:
        # ctx
        self.ctx = ctx

        # pid
        self.pid = pid or uuid.uuid4().hex

        # unicast / anycast
        self.unicast = unicast or f"poll://uni@default/{pid}"
        self.anycast = anycast or f"poll://any@default/{pid}"

        # computations
        self.computations: dict[str, Computation] = {}

    def __repr__(self) -> str:
        return f"Scheduler(pid={self.pid}, computations={list(self.computations.values())})"

    def step(self, cmd: Command, future: Future | None = None) -> More | Done:
        computation = self.computations.setdefault(cmd.cid, Computation(cmd.cid, self.ctx, self.pid, self.unicast, self.anycast))

        # subscribe
        if future:
            computation.subscribe(future)

        # apply
        computation.apply(cmd)

        # eval
        return computation.eval()

    def shutdown(self, e: ResonateShutdownError) -> None:
        # reject all futures for all computations
        for computation in self.computations.values():
            while future := computation.futures.spop():
                assert not future.done()
                future.set_exception(e)


class Info:
    def __init__(self, func: Lfnc | Coro) -> None:
        self._func = func

    def __repr__(self) -> str:
        return f"Info(attempt={self.attempt}, ikey={self.idempotency_key} tags={self.tags}, timeout={self.timeout})"

    @property
    def attempt(self) -> int:
        return self._func.attempt

    @property
    def idempotency_key(self) -> str | None:
        return self._func.promise.ikey_for_create if self._func.promise else self._func.conv.idempotency_key

    @property
    def tags(self) -> dict[str, str] | None:
        return self._func.promise.tags if self._func.promise else self._func.conv.tags

    @property
    def timeout(self) -> float:
        return self._func.timeout

    @property
    def version(self) -> int:
        return self._func.opts.version


type State = Enabled[Running[Init | Lfnc | Coro]] | Enabled[Suspended[Init | Rfnc | Coro]] | Enabled[Completed[Lfnc | Rfnc | Coro]] | Blocked[Running[Init | Lfnc | Coro]]


@dataclass
class Enabled[T: Running[Init | Lfnc | Coro] | Suspended[Init | Rfnc | Coro] | Completed[Lfnc | Rfnc | Coro]]:
    value: Final[T]

    def __init__(self, value: T) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"Enabled::{self.value}"

    def bind[U: Running | Suspended | Completed](self, func: Callable[[T], U]) -> Enabled[U]:
        return Enabled(func(self.value))

    @property
    def running(self) -> bool:
        return self.value.running

    @property
    def suspended(self) -> bool:
        return self.value.suspended

    @property
    def exec(self) -> T:
        return self.value

    @property
    def func(self) -> Init | Lfnc | Rfnc | Coro:
        return self.value.value


@dataclass
class Blocked[T: Running[Init | Lfnc | Coro]]:
    value: Final[T]

    def __init__(self, value: T) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"Blocked::{self.value}"

    def bind[U: Running](self, func: Callable[[T], U]) -> Blocked[U]:
        return Blocked(func(self.value))

    @property
    def running(self) -> bool:
        return False

    @property
    def suspended(self) -> bool:
        return False

    @property
    def exec(self) -> T:
        return self.value

    @property
    def func(self) -> Init | Lfnc | Coro:
        return self.value.value


@dataclass
class Running[T: Init | Lfnc | Coro]:
    value: Final[T]

    def __init__(self, value: T) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"Running::{self.value}"

    def bind[U: Init | Lfnc | Coro](self, func: Callable[[T], U]) -> Running[U]:
        return Running(func(self.value))

    @property
    def running(self) -> bool:
        return True

    @property
    def suspended(self) -> bool:
        return False

    @property
    def exec(self) -> Running[T]:
        return self

    @property
    def func(self) -> T:
        return self.value


@dataclass
class Suspended[T: Init | Rfnc | Coro]:
    value: Final[T]

    def __init__(self, value: T) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"Suspended::{self.value}"

    def bind[U: Init | Rfnc | Coro](self, func: Callable[[T], U]) -> Suspended[U]:
        return Suspended(func(self.value))

    @property
    def running(self) -> bool:
        return False

    @property
    def suspended(self) -> bool:
        return True

    @property
    def exec(self) -> Suspended[T]:
        return self

    @property
    def func(self) -> T:
        return self.value


@dataclass
class Completed[T: Lfnc | Rfnc | Coro]:
    value: Final[T]

    def __init__(self, value: T) -> None:
        self.value = value

    def __post_init__(self) -> None:
        assert self.value.result is not None, "Result must be set."

    def __repr__(self) -> str:
        return f"Completed::{self.value}"

    def bind[U: Lfnc | Rfnc | Coro](self, func: Callable[[T], U]) -> Completed[U]:
        return Completed(func(self.value))

    @property
    def running(self) -> bool:
        return False

    @property
    def suspended(self) -> bool:
        return True

    @property
    def exec(self) -> Completed[T]:
        return self

    @property
    def func(self) -> T:
        return self.value


@dataclass
class Init:
    next: Lfnc | Rfnc | Coro | None = None
    suspends: list[Node[State]] = field(default_factory=list)

    @property
    def id(self) -> str:
        return self.next.id if self.next else ""

    def map(self, *, suspends: Node[State] | None = None) -> Init:
        if suspends:
            self.suspends.append(suspends)
        return self

    def __repr__(self) -> str:
        s = ", ".join([s.value.func.id for s in self.suspends])
        return f"Init(id={self.id}, next={self.next}, suspends=[{s}])"


@dataclass
class Lfnc:
    id: str
    cid: str
    conv: Convention
    timeout: float  # absolute time in seconds
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: Options
    ccls: Callable[[str, str, Info], Context]

    attempt: int = field(default=1, init=False)
    ctx: Context = field(init=False)
    encoder: Encoder[Any, tuple[dict[str, str] | None, str | None]] = field(init=False)
    promise: DurablePromise | None = field(default=None, init=False)
    result: Result[Any] | None = field(default=None, init=False)
    retry_policy: RetryPolicy = field(init=False)
    suspends: list[Node[State]] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self.ctx = self.ccls(self.id, self.cid, Info(self))
        self.encoder = self.opts.get_encoder()
        self.retry_policy = self.opts.get_retry_policy(self.func)

    def map(
        self,
        *,
        attempt: int | None = None,
        promise: DurablePromise | None = None,
        result: Result[Any] | None = None,
        suspends: Node[State] | None = None,
    ) -> Lfnc:
        if attempt:
            assert attempt == self.attempt + 1, "Attempt must be monotonically incremented."
            self.attempt = attempt
        if promise:
            assert self.opts.durable, "Func must be durable."
            self.promise = promise
            self.timeout = promise.abs_timeout
        if result:
            self.result = result
        if suspends:
            self.suspends.append(suspends)
        return self

    def __repr__(self) -> str:
        s = ", ".join([s.value.func.id for s in self.suspends])
        return f"Lfnc(id={self.id}, suspends=[{s}], result={self.result})"


@dataclass
class Rfnc:
    id: str
    cid: str
    conv: Convention
    timeout: float  # absolute time in seconds
    opts: Options

    attempt: int = field(default=1, init=False)
    encoder: Encoder[Any, tuple[dict[str, str] | None, str | None]] = field(init=False)
    promise: DurablePromise | None = field(default=None, init=False)
    result: Result[Any] | None = field(default=None, init=False)
    suspends: list[Node[State]] = field(default_factory=list, init=False)

    def map(
        self,
        *,
        promise: DurablePromise | None = None,
        result: Result[Any] | None = None,
        suspends: Node[State] | None = None,
    ) -> Rfnc:
        if promise:
            self.promise = promise
            self.timeout = promise.abs_timeout
        if result:
            self.result = result
        if suspends:
            self.suspends.append(suspends)
        return self

    def __repr__(self) -> str:
        s = ", ".join([s.value.func.id for s in self.suspends])
        return f"Rfnc(id={self.id}, suspends=[{s}], result={self.result})"

    def __post_init__(self) -> None:
        self.encoder = self.opts.get_encoder()


@dataclass
class Coro:
    id: str
    cid: str
    conv: Convention
    timeout: float  # absolute time in seconds
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: Options
    ccls: Callable[[str, str, Info], Context]

    coro: Coroutine = field(init=False)
    next: None | AWT | Result[Any] = field(default=None, init=False)

    attempt: int = field(default=1, init=False)
    ctx: Context = field(init=False)
    encoder: Encoder[Any, tuple[dict[str, str] | None, str | None]] = field(init=False)
    promise: DurablePromise | None = field(default=None, init=False)
    result: Result[Any] | None = field(default=None, init=False)
    suspends: list[Node[State]] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self.ctx = self.ccls(self.id, self.cid, Info(self))
        self.coro = Coroutine(self.id, self.cid, self.func(self.ctx, *self.args, **self.kwargs))
        self.encoder = self.opts.get_encoder()
        self.retry_policy = self.opts.get_retry_policy(self.func)

    def map(
        self,
        *,
        attempt: int | None = None,
        next: AWT | Result[Any] | None = None,
        promise: DurablePromise | None = None,
        result: Result[Any] | None = None,
        suspends: Node[State] | None = None,
        timeout: float | None = None,
    ) -> Coro:
        if attempt:
            assert attempt == self.attempt + 1, "Attempt must be monotonically incremented."
            self.attempt = attempt
            self.coro = Coroutine(self.id, self.cid, self.func(self.ctx, *self.args, **self.kwargs))
            self.next = None
        if next:
            self.next = next
        if promise:
            assert self.opts.durable, "Func must be durable."
            self.promise = promise
            self.timeout = promise.abs_timeout
        if result:
            self.result = result
        if suspends:
            self.suspends.append(suspends)
        if timeout:
            assert timeout >= 0, "Timeout must be greater than or equal to 0."
            self.timeout = timeout
        return self

    def __repr__(self) -> str:
        s = ", ".join([s.value.func.id for s in self.suspends])
        return f"Coro(id={self.id}, next={self.next}, suspends=[{s}], result={self.result})"


class PoppableList[T](list[T]):
    def spop(self) -> T | None:
        return self.pop() if self else None


@dataclass
class More:
    reqs: list[Function | Delayed[Function | Retry] | Network[CreatePromiseReq | ResolvePromiseReq | RejectPromiseReq | CancelPromiseReq]]
    done: Literal[False] = False


@dataclass
class Done:
    reqs: list[Network[CreateCallbackReq | CreateSubscriptionReq]]
    done: Literal[True] = True


class Computation:
    def __init__(self, id: str, ctx: Callable[[str, str, Info], Context], pid: str, unicast: str, anycast: str) -> None:
        self.id = id
        self.ctx = ctx
        self.pid = pid

        self.unicast = unicast
        self.anycast = anycast

        self.graph = Graph[State](id, Enabled(Suspended(Init())))
        self.futures: PoppableList[Future] = PoppableList()
        self.max_len: Final[int] = 100

    def __repr__(self) -> str:
        return f"Computation(id={self.id}, runnable={self.runnable()}, suspendable={self.suspendable()})"

    def runnable(self) -> bool:
        return any(n.value.running for n in self.graph.traverse())

    def suspendable(self) -> bool:
        return all(n.value.suspended for n in self.graph.traverse())

    def subscribe(self, future: Future) -> None:
        self.futures.append(future)

    def result(self) -> Result[Any] | None:
        match self.graph.root.value.exec:
            case Completed(Lfnc(result=result) | Rfnc(result=result) | Coro(result=result)):
                assert result, "Result must be set."
                return result
            case _:
                return None

    def apply(self, cmd: Command) -> None:
        match cmd, self.graph.root.value.func:
            case Invoke(id, conv, timeout, func, args, kwargs, opts, promise), Init(next=None):
                assert id == conv.id == self.id == (promise.id if promise else id), "Ids must match."
                assert (promise is not None) == opts.durable, "Promise must be set iff durable."

                cls = Coro if isgeneratorfunction(func) else Lfnc
                func = cls(id, self.id, conv, timeout, func, args, kwargs, opts, self.ctx)

                if promise and promise.completed:
                    self.graph.root.transition(Enabled(Completed(func.map(promise=promise, result=promise.result(opts.get_encoder())))))
                else:
                    self.graph.root.transition(Enabled(Running(func.map(promise=promise))))

            case Invoke(), _:
                # first invoke "wins", computation will be joined
                pass

            case Resume(invoke=Invoke() as invoke), Init(next=None):
                self.apply(invoke)

            case Resume(id, promise=promise), _:
                node = self.graph.find(lambda n: n.id == id)

                if node:
                    self._apply_resume(node, promise)

            case Return(id, res=res), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."
                self._apply_return(node, res)

            case Receive(id, res=CreatePromiseRes(promise) | ResolvePromiseRes(promise) | RejectPromiseRes(promise) | CancelPromiseRes(promise)), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."

                self._apply_receive(node, promise)

            case Retry(id), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."

                self._apply_retry(node)

            case Listen(id), _:
                assert id == self.id, "Id must match computation id."

            case Notify(id, promise), _:
                assert id == promise.id == self.id, "Id must match promise and computation id."

                self._apply_notify(self.graph.root, promise)

            case _:
                msg = f"Command {cmd} not supported"
                raise NotImplementedError(msg)

    def _apply_resume(self, node: Node[State], promise: DurablePromise) -> None:
        assert promise.completed, "Promise must be completed."

        match node.value, promise:
            case Blocked(Running(Init(Rfnc(id=id, encoder=encoder) as next, suspends))), promise:
                assert not next.suspends, "Suspends must be initially empty."
                node.transition(Enabled(Completed(next.map(promise=promise, result=promise.result(encoder)))))

                # unblock waiting[p]
                self._unblock(suspends, AWT(id))

            case Enabled(Suspended(Rfnc(encoder=encoder, suspends=suspends) as f)), promise:
                result = promise.result(encoder)
                node.transition(Enabled(Completed(f.map(promise=promise, result=result))))

                # unblock waiting[v]
                self._unblock(suspends, result)

            case Enabled(Completed()), _:
                # On resume, it is possible that the node is already completed.
                pass

            case _:
                raise NotImplementedError

    def _apply_return(self, node: Node[State], result: Result[Any]) -> None:
        match node.value:
            case Blocked(Running(Lfnc() as f)):
                node.transition(Enabled(Running(f.map(result=result))))
            case _:
                raise NotImplementedError

    def _apply_receive(self, node: Node[State], promise: DurablePromise) -> None:
        match node.value, promise:
            case Blocked(Running(Init(Lfnc(id) | Coro(id) as next, suspends))), promise if promise.pending:
                assert not next.suspends, "Suspends must be initially empty."
                node.transition(Enabled(Running(next.map(promise=promise))))

                # unblock waiting[p]
                self._unblock(suspends, AWT(id))

            case Blocked(Running(Init(Rfnc(id) as next, suspends))), promise if promise.pending:
                assert not next.suspends, "Next suspends must be initially empty."
                assert promise.tags.get("resonate:scope") != "local", "Scope must not be local."
                node.transition(Enabled(Suspended(next.map(promise=promise))))

                # unblock waiting[p]
                self._unblock(suspends, AWT(id))

            case Blocked(Running(Init(next, suspends))), promise if promise.completed:
                assert next, "Next must be set."
                assert not next.suspends, "Suspends must be initially empty."
                node.transition(Enabled(Completed(next.map(promise=promise, result=promise.result(next.encoder)))))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id))

            case Blocked(Running(Lfnc(encoder=encoder, suspends=suspends) | Coro(encoder=encoder, suspends=suspends) as f)), promise if promise.completed:
                result = promise.result(encoder)
                node.transition(Enabled(Completed(f.map(promise=promise, result=result))))

                # unblock waiting[v]
                self._unblock(suspends, result)

            case Enabled(Completed()), _:
                # On resume, it is possible that the node is already completed.
                pass

            case _:
                raise NotImplementedError

    def _apply_retry(self, node: Node[State]) -> None:
        match node.value:
            case Blocked(Running(Coro() as c)):
                assert c.next is None, "Next must be None."
                node.transition(Enabled(Running(c)))

            case _:
                raise NotImplementedError

    def _apply_notify(self, node: Node[State], promise: DurablePromise) -> None:
        assert promise.completed, "Promise must be completed."

        match node.value:
            case Enabled(Suspended(Init(next=None))):
                func = Rfnc(
                    promise.id,
                    self.id,
                    Base(
                        promise.id,
                        promise.rel_timeout,
                        promise.ikey_for_create,
                        promise.param.data,
                        promise.tags,
                    ),
                    promise.abs_timeout,
                    Options(),
                )
                node.transition(Enabled(Completed(func.map(promise=promise, result=promise.result(func.encoder)))))

            case _:
                # Note: we could implement notify in a way that takes precedence over a locally
                # running computation, however, it is easier for now to ignore the notify and let
                # the computation finish.
                pass

    def eval(self) -> More | Done:
        more: list[Function | Delayed[Function | Retry] | Network[CreatePromiseReq | ResolvePromiseReq | RejectPromiseReq | CancelPromiseReq]] = []
        done: list[Network[CreateCallbackReq | CreateSubscriptionReq]] = []

        while self.runnable():
            for node in self.graph.traverse():
                more.extend(self._eval(node))

        if self.suspendable():
            assert not more, "More requests must be empty."

            for node in self.graph.traverse():
                match node.value:
                    case Enabled(Suspended(Init(id=id, next=next))):
                        assert node == self.graph.root, "Node must be root node."
                        assert not next, "Next must be None."

                        # TODO(dfarr): subscriptions should probably have small
                        # timeout values and we can rely on recreating them
                        # from time to time on the sdk. For the time being we
                        # are simply setting timeout to a very large value.
                        #
                        # 253402329599000
                        # == (datetime.max.timestamp() - 1) * 1000
                        # == 9999-12-31T23:59:59Z
                        timeout = 253402329599000

                        done.append(
                            Network(
                                self.id,
                                self.id,
                                CreateSubscriptionReq(
                                    self.pid,
                                    self.id,
                                    timeout,
                                    self.unicast,
                                ),
                            )
                        )

                    case Enabled(Suspended(Rfnc(id=id, suspends=suspends))) if suspends:
                        assert node is not self.graph.root, "Node must not be root node."
                        assert isinstance(self.graph.root.value.func, (Lfnc, Coro)), "Root node must be Lfnc or Coro."
                        assert self.graph.root.value.func.promise, "Promise must be set."

                        done.append(
                            Network(
                                id,
                                self.id,
                                CreateCallbackReq(
                                    id,
                                    self.id,
                                    self.graph.root.value.func.promise.timeout,
                                    self.anycast,
                                ),
                            )
                        )

        if result := self.result():
            while future := self.futures.spop():
                assert not future.done()
                match result:
                    case Ok(v):
                        future.set_result(v)
                    case Ko(e):
                        future.set_exception(e)

        return Done(done) if self.suspendable() else More(more)

    def _eval(self, node: Node[State]) -> list[Function | Delayed[Function | Retry] | Network[CreatePromiseReq | ResolvePromiseReq | RejectPromiseReq | CancelPromiseReq]]:
        match node.value:
            case Enabled(Running(Init(Lfnc(id=id, conv=conv, timeout=timeout, opts=opts) | Coro(id=id, conv=conv, timeout=timeout, opts=opts) as func, suspends))):
                assert id == conv.id == node.id, "Id must match convention id and node id."
                assert node is not self.graph.root, "Node must not be root node."
                node.transition(Blocked(Running(Init(func, suspends))) if opts.durable else Enabled(Running(func)))

                if not opts.durable:
                    assert len(suspends) == 1, "Only the parent couroutine can be blocked."
                    self._unblock(suspends, AWT(id))
                    return []

                headers, data = func.encoder.encode(conv.data)
                return [
                    Network(
                        id,
                        self.id,
                        CreatePromiseReq(
                            id=id,
                            timeout=int(timeout * 1000),
                            ikey=conv.idempotency_key,
                            headers=headers,
                            data=data,
                            tags=conv.tags,
                        ),
                    ),
                ]

            case Enabled(Running(Init(Rfnc(id=id, conv=conv, timeout=timeout, encoder=encoder))) as exec):
                assert id == conv.id == node.id, "Id must match convention id and node id."
                node.transition(Blocked(exec))
                headers, data = encoder.encode(conv.data)

                return [
                    Network(
                        id,
                        self.id,
                        CreatePromiseReq(
                            id=id,
                            timeout=int(timeout * 1000),
                            ikey=conv.idempotency_key,
                            headers=headers,
                            data=data,
                            tags=conv.tags,
                        ),
                    ),
                ]

            case Enabled(Running(Lfnc(id=id, timeout=timeout, func=func, args=args, kwargs=kwargs, opts=opts) as f)):
                assert id == node.id, "Id must match node id."

                clock = f.ctx.get_dependency("resonate:time", time)
                assert isinstance(clock, Clock), "resonate:time must be an instance of clock"

                match f.result, opts.durable, f.retry_policy.next(f.attempt):
                    case None, _, _:
                        f.ctx.logger.debug(
                            "Running function %s(%s)",
                            f.func.__name__,
                            utils.truncate(utils.format_args_and_kwargs(f.args, f.kwargs), self.max_len),
                        )
                        node.transition(Blocked(Running(f)))

                        return [
                            Function(id, self.id, lambda: func(f.ctx, *args, **kwargs)),
                        ]

                    case Ok(v), True, _:
                        f.ctx.logger.debug(
                            "Function %s(%s) succeeded with %s",
                            f.func.__name__,
                            utils.truncate(utils.format_args_and_kwargs(f.args, f.kwargs), self.max_len),
                            utils.truncate(repr(v), self.max_len),
                        )
                        node.transition(Blocked(Running(f)))
                        headers, data = f.encoder.encode(v)

                        return [
                            Network(id, self.id, ResolvePromiseReq(id=id, ikey=id, headers=headers, data=data)),
                        ]

                    case Ok(v), False, _:
                        f.ctx.logger.debug(
                            "Function %s(%s) succeeded with %s",
                            f.func.__name__,
                            utils.truncate(utils.format_args_and_kwargs(f.args, f.kwargs), self.max_len),
                            utils.truncate(repr(v), self.max_len),
                        )
                        node.transition(Enabled(Completed(f)))
                        self._unblock(f.suspends, f.result)
                        return []

                    case Ko(e), True, d if d is None or clock.time() + d > timeout or type(e) in opts.non_retryable_exceptions:
                        f.ctx.logger.error(
                            "Function %s(%s) failed with %s",
                            f.func.__name__,
                            utils.truncate(utils.format_args_and_kwargs(f.args, f.kwargs), self.max_len),
                            utils.truncate(repr(e), self.max_len),
                        )
                        node.transition(Blocked(Running(f)))
                        headers, data = f.encoder.encode(e)

                        return [
                            Network(id, self.id, RejectPromiseReq(id=id, ikey=id, headers=headers, data=data)),
                        ]

                    case Ko(e), False, d if d is None or clock.time() + d > timeout or type(e) in opts.non_retryable_exceptions:
                        f.ctx.logger.error(
                            "Function %s(%s) failed with %s",
                            f.func.__name__,
                            utils.truncate(utils.format_args_and_kwargs(f.args, f.kwargs), self.max_len),
                            utils.truncate(repr(e), self.max_len),
                        )
                        node.transition(Enabled(Completed(f)))
                        self._unblock(f.suspends, f.result)
                        return []

                    case Ko(e), _, d:
                        assert d is not None, "Delay must be set."
                        f.ctx.logger.warning(
                            "Function %s(%s) failed with %s (retrying in %.1fs)",
                            f.func.__name__,
                            utils.truncate(utils.format_args_and_kwargs(f.args, f.kwargs), self.max_len),
                            utils.truncate(repr(e), self.max_len),
                            d,
                        )
                        node.transition(Blocked(Running(f.map(attempt=f.attempt + 1))))

                        return [
                            Delayed(Function(id, self.id, lambda: func(f.ctx, *args, **kwargs)), d),
                        ]

            case Enabled(Running(Coro(id=id, timeout=timeout, coro=coro, next=next, opts=opts) as c)):
                if next is None:
                    c.ctx.logger.debug(
                        "Running function %s(%s)%s",
                        c.func.__name__,
                        utils.truncate(utils.format_args_and_kwargs(c.args, c.kwargs), self.max_len),
                        f" (attempt {c.attempt})" if c.attempt > 1 else "",
                    )

                cmd = coro.send(next)
                child = self.graph.find(lambda n: n.id == cmd.id) or Node(cmd.id, Enabled(Suspended(Init())))

                clock = c.ctx.get_dependency("resonate:time", time)
                assert isinstance(clock, Clock), "resonate:time must be an instance of clock"

                match cmd, child.value:
                    case LFI(conv, func, args, kwargs, opts), Enabled(Suspended(Init(next=None))):
                        cls = Coro if isgeneratorfunction(func) else Lfnc
                        next = cls(conv.id, self.id, conv, min(clock.time() + conv.timeout, c.timeout), func, args, kwargs, opts, self.ctx)
                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(Init(next, suspends=[node]))))
                        return []

                    case RFI(conv, opts, mode=mode), Enabled(Suspended(Init(next=None))):
                        next = Rfnc(
                            conv.id,
                            self.id,
                            conv,
                            min(clock.time() + conv.timeout, c.timeout) if mode == "attached" else clock.time() + conv.timeout,
                            opts,
                        )
                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(Init(next, suspends=[node]))))
                        return []

                    case LFI(conv) | RFI(conv), Enabled(Running(Init() as f)):
                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(f.map(suspends=node))))
                        return []

                    case LFI(conv) | RFI(conv), Blocked(Running(Init() as f)):
                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Blocked(Running(f.map(suspends=node))))
                        return []

                    case LFI(conv) | RFI(conv), _:
                        node.add_edge(child)
                        node.transition(Enabled(Running(c.map(next=AWT(conv.id)))))
                        return []

                    case AWT(), Enabled(Running(f)):
                        node.add_edge(child, "waiting[v]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(f.map(suspends=node))))
                        return []

                    case AWT(), Blocked(Running(f)):
                        node.add_edge(child, "waiting[v]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Blocked(Running(f.map(suspends=node))))
                        return []

                    case AWT(), Enabled(Suspended(f)):
                        node.add_edge(child, "waiting[v]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Suspended(f.map(suspends=node))))
                        return []

                    case AWT(), Enabled(Completed(Lfnc(result=result) | Rfnc(result=result) | Coro(result=result))):
                        assert result is not None, "Completed result must be set."
                        node.transition(Enabled(Running(c.map(next=result))))
                        return []

                    case TRM(id, result), _:
                        assert id == node.id, "Id must match node id."

                        match result, opts.durable, c.retry_policy.next(c.attempt):
                            case Ok(v), True, _:
                                c.ctx.logger.debug(
                                    "Function %s(%s) succeeded with %s",
                                    c.func.__name__,
                                    utils.truncate(utils.format_args_and_kwargs(c.args, c.kwargs), self.max_len),
                                    utils.truncate(repr(v), self.max_len),
                                )
                                node.transition(Blocked(Running(c)))
                                headers, data = c.encoder.encode(v)

                                return [
                                    Network(id, self.id, ResolvePromiseReq(id=id, ikey=id, headers=headers, data=data)),
                                ]

                            case Ok(v), False, _:
                                c.ctx.logger.debug(
                                    "Function %s(%s) succeeded with %s",
                                    c.func.__name__,
                                    utils.truncate(utils.format_args_and_kwargs(c.args, c.kwargs), self.max_len),
                                    utils.truncate(repr(v), self.max_len),
                                )
                                node.transition(Enabled(Completed(c.map(result=result))))
                                self._unblock(c.suspends, result)
                                return []

                            case Ko(e), True, d if d is None or clock.time() + d > timeout or type(e) in opts.non_retryable_exceptions:
                                c.ctx.logger.error(
                                    "Function %s(%s) failed with %s",
                                    c.func.__name__,
                                    utils.truncate(utils.format_args_and_kwargs(c.args, c.kwargs), self.max_len),
                                    utils.truncate(repr(e), self.max_len),
                                )
                                node.transition(Blocked(Running(c)))
                                headers, data = c.encoder.encode(e)

                                return [
                                    Network(id, self.id, RejectPromiseReq(id=id, ikey=id, headers=headers, data=data)),
                                ]

                            case Ko(e), False, d if d is None or clock.time() + d > timeout or type(e) in opts.non_retryable_exceptions:
                                c.ctx.logger.error(
                                    "Function %s(%s) failed with %s",
                                    c.func.__name__,
                                    utils.truncate(utils.format_args_and_kwargs(c.args, c.kwargs), self.max_len),
                                    utils.truncate(repr(e), self.max_len),
                                )
                                node.transition(Enabled(Completed(c.map(result=result))))
                                self._unblock(c.suspends, result)
                                return []

                            case Ko(e), _, d:
                                assert d is not None, "Delay must be set."
                                c.ctx.logger.warning(
                                    "Function %s(%s) failed with %s (retrying in %.1fs)",
                                    c.func.__name__,
                                    utils.truncate(utils.format_args_and_kwargs(c.args, c.kwargs), self.max_len),
                                    utils.truncate(repr(e), self.max_len),
                                    d,
                                )
                                node.transition(Blocked(Running(c.map(attempt=c.attempt + 1))))

                                return [
                                    Delayed(Retry(id, self.id), d),
                                ]

                    case _:
                        raise NotImplementedError

            case Enabled(Suspended() | Completed()) | Blocked(Running()):
                # valid states
                return []

            case _:
                # invalid states
                raise NotImplementedError

    def _unblock(self, nodes: list[Node[State]], next: AWT | Result[Any]) -> None:
        for node in nodes:
            match node.value:
                case Enabled(Suspended(Coro() as c)):
                    node.transition(Enabled(Running(c.map(next=next))))
                case _:
                    raise NotImplementedError
