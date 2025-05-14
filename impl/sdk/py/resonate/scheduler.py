from __future__ import annotations

import logging
import sys
import time
import uuid
from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Final, Literal

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

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future

    from resonate.errors import ResonateShutdownError
    from resonate.models.context import Context
    from resonate.models.convention import Convention
    from resonate.models.durable_promise import DurablePromise
    from resonate.models.retry_policy import RetryPolicy
    from resonate.options import Options

logger = logging.getLogger(__package__)


class Scheduler:
    def __init__(
        self,
        ctx: Callable[[str, Info], Context],
        pid: str | None = None,
        unicast: str | None = None,
        anycast: str | None = None,
    ) -> None:
        # ctx
        self.ctx = ctx

        # pid
        self.pid = pid or uuid.uuid4().hex

        # unicast / anycast
        self.unicast = unicast or f"poll://default/{pid}"
        self.anycast = anycast or f"poll://default/{pid}"

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
    ccls: Callable[[str, Info], Context]

    attempt: int = field(default=1, init=False)
    ctx: Context = field(init=False)
    promise: DurablePromise | None = field(default=None, init=False)
    result: Result | None = field(default=None, init=False)
    retry_policy: RetryPolicy = field(init=False)
    suspends: list[Node[State]] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self.ctx = self.ccls(self.id, Info(self))
        self.retry_policy = self.opts.retry_policy(self.func) if callable(self.opts.retry_policy) else self.opts.retry_policy

    def map(
        self,
        *,
        attempt: int | None = None,
        promise: DurablePromise | None = None,
        result: Result | None = None,
        suspends: Node[State] | None = None,
    ) -> Lfnc:
        if attempt:
            assert attempt == self.attempt + 1, "Attempt must be monotonically incremented."
            self.attempt = attempt
        if promise:
            assert self.opts.durable, "Func must be durable."
            self.promise = promise
            self.result = promise.result if promise.completed else None
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

    attempt: int = field(default=1, init=False)
    promise: DurablePromise | None = field(default=None, init=False)
    result: Result | None = field(default=None, init=False)
    suspends: list[Node[State]] = field(default_factory=list, init=False)

    def map(
        self,
        *,
        promise: DurablePromise | None = None,
        result: Result | None = None,
        suspends: Node[State] | None = None,
    ) -> Rfnc:
        if promise:
            self.promise = promise
            self.result = promise.result if promise.completed else None
            self.timeout = promise.abs_timeout
        if result:
            self.result = result
        if suspends:
            self.suspends.append(suspends)
        return self

    def __repr__(self) -> str:
        s = ", ".join([s.value.func.id for s in self.suspends])
        return f"Rfnc(id={self.id}, suspends=[{s}], result={self.result})"


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
    ccls: Callable[[str, Info], Context]

    coro: Coroutine = field(init=False)
    next: None | AWT | Result = field(default=None, init=False)

    attempt: int = field(default=1, init=False)
    ctx: Context = field(init=False)
    promise: DurablePromise | None = field(default=None, init=False)
    result: Result | None = field(default=None, init=False)
    suspends: list[Node[State]] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self.ctx = self.ccls(self.id, Info(self))
        self.coro = Coroutine(self.id, self.cid, self.func(self.ctx, *self.args, **self.kwargs))
        self.retry_policy = self.opts.retry_policy(self.func) if callable(self.opts.retry_policy) else self.opts.retry_policy

    def map(
        self,
        *,
        attempt: int | None = None,
        next: AWT | Result | None = None,
        promise: DurablePromise | None = None,
        result: Result | None = None,
        suspends: Node[State] | None = None,
        timeout: int | None = None,
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
            self.result = promise.result if promise.completed else None
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
    def __init__(self, id: str, ctx: Callable[[str, Info], Context], pid: str, unicast: str, anycast: str) -> None:
        self.id = id
        self.ctx = ctx
        self.pid = pid

        self.unicast = unicast
        self.anycast = anycast

        self.graph = Graph[State](id, Enabled(Suspended(Init())))
        self.futures: PoppableList[Future] = PoppableList()
        self.history: list[Command | Function | Delayed | Network | tuple[str, None | AWT | Result, LFI | RFI | AWT | TRM]] = []

    def __repr__(self) -> str:
        return f"Computation(id={self.id}, runnable={self.runnable()}, suspendable={self.suspendable()})"

    def runnable(self) -> bool:
        return any(n.value.running for n in self.graph.traverse())

    def suspendable(self) -> bool:
        return all(n.value.suspended for n in self.graph.traverse())

    def subscribe(self, future: Future) -> None:
        self.futures.append(future)

    def result(self) -> Result | None:
        match self.graph.root.value.exec:
            case Completed(Lfnc(result=result) | Rfnc(result=result) | Coro(result=result)):
                assert result, "Result must be set."
                return result
            case _:
                return None

    def apply(self, cmd: Command) -> None:
        self.history.append(cmd)
        match cmd, self.graph.root.value.func:
            case Invoke(id, conv, timeout, func, args, kwargs, opts, promise), Init(next=None):
                assert id == conv.id == self.id == (promise.id if promise else id), "Ids must match."
                assert (promise is not None) == opts.durable, "Promise must be set iff durable."

                cls = Coro if isgeneratorfunction(func) else Lfnc
                self.graph.root.transition(Enabled(Running(cls(id, self.id, conv, timeout, func, args, kwargs, opts, self.ctx).map(promise=promise))))

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
            case Enabled(Suspended(Rfnc(suspends=suspends) as f)) | Blocked(Running(Init(next=Rfnc(suspends=suspends) as f))), promise:
                node.transition(Enabled(Completed(f.map(promise=promise))))

                # unblock waiting[v]
                self._unblock(suspends, promise.result)

            case Enabled(Completed()), _:
                # On resume, it is possible that the node is already completed.
                pass

            case _:
                raise NotImplementedError

    def _apply_return(self, node: Node[State], result: Result) -> None:
        match node.value:
            case Blocked(Running(Lfnc() as f)):
                node.transition(Enabled(Running(f.map(result=result))))
            case _:
                raise NotImplementedError

    def _apply_receive(self, node: Node[State], promise: DurablePromise) -> None:
        match node.value, promise:
            case Blocked(Running(Init(Lfnc() | Coro() as next, suspends))), promise if promise.pending:
                assert not next.suspends, "Suspends must be initially empty."
                node.transition(Enabled(Running(next.map(promise=promise))))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id))

            case Blocked(Running(Init(Rfnc() as next, suspends))), promise if promise.pending:
                assert not next.suspends, "Next suspends must be initially empty."
                assert promise.tags.get("resonate:scope") != "local", "Scope must not be local."
                node.transition(Enabled(Suspended(next.map(promise=promise))))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id))

            case Blocked(Running(Init(next, suspends))), promise if promise.completed:
                assert next, "Next must be set."
                assert not next.suspends, "Suspends must be initially empty."
                node.transition(Enabled(Completed(next.map(promise=promise))))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id))

            case Blocked(Running(Lfnc(suspends=suspends) | Coro(suspends=suspends) as f)), promise if promise.completed:
                node.transition(Enabled(Completed(f.map(promise=promise))))

                # unblock waiting[v]
                self._unblock(suspends, promise.result)

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
        match node.value:
            case Enabled(Suspended(Init(next=None))):
                func = Rfnc(
                    promise.id,
                    self.id,
                    Base(
                        promise.id,
                        promise.rel_timeout,
                        promise.ikey_for_create,
                        promise.param.headers,
                        promise.param.data,
                        promise.tags,
                    ),
                    promise.abs_timeout,
                )
                node.transition(Enabled(Completed(func.map(promise=promise))))

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

                # Enabled::Suspended::Rfnc requires a callback
                if isinstance(node.value, Enabled) and isinstance(node.value.exec, Suspended) and isinstance(node.value.func, Rfnc) and node.value.func.suspends:
                    assert node is not self.graph.root, "Node must not be root node."
                    assert isinstance(self.graph.root.value.func, (Lfnc, Coro)), "Root node must be Lfnc or Coro."
                    assert self.graph.root.value.func.promise, "Promise must be set."

                    done.append(
                        Network(
                            node.id,
                            self.id,
                            CreateCallbackReq(
                                f"{self.id}:{node.id}",
                                node.id,
                                self.id,
                                self.graph.root.value.func.promise.timeout,
                                self.anycast,
                            ),
                        )
                    )

        # Enabled::Suspended::Init requires a subscription
        if isinstance(self.graph.root.value, Enabled) and isinstance(self.graph.root.value.exec, Suspended) and isinstance(self.graph.root.value.func, Init):
            assert self.suspendable(), "Computation must be suspendable."
            done.append(
                Network(
                    self.id,
                    self.id,
                    CreateSubscriptionReq(
                        f"{self.pid}:{self.id}",
                        self.id,
                        sys.maxsize,  # TODO(dfarr): evaluate default setting for subscription timeout
                        self.unicast,
                    ),
                )
            )

        if self.suspendable():
            assert not more, "More requests must be empty."
            self.history.extend(done)
        else:
            self.history.extend(more)

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

                match opts.durable:
                    case True:
                        return [
                            Network(
                                id,
                                self.id,
                                CreatePromiseReq(
                                    id=id,
                                    timeout=int(timeout * 1000),
                                    ikey=conv.idempotency_key,
                                    headers=conv.headers,
                                    data=conv.data,
                                    tags=conv.tags,
                                ),
                            ),
                        ]
                    case False:
                        assert len(suspends) == 1, "Only the parent couroutine can be blocked."
                        self._unblock(suspends, AWT(id))
                        return []

            case Enabled(Running(Init(Rfnc(id=id, conv=conv, timeout=timeout))) as exec):
                assert id == conv.id == node.id, "Id must match convention id and node id."
                node.transition(Blocked(exec))

                return [
                    Network(
                        id,
                        self.id,
                        CreatePromiseReq(
                            id=id,
                            timeout=int(timeout * 1000),
                            ikey=conv.idempotency_key,
                            headers=conv.headers,
                            data=conv.data,
                            tags=conv.tags,
                        ),
                    ),
                ]

            case Enabled(Running(Lfnc(id=id, func=func, args=args, kwargs=kwargs, opts=opts, attempt=attempt, ctx=ctx, result=result, suspends=suspends, timeout=timeout) as f)):
                assert id == node.id, "Id must match node id."

                match result, opts.durable, f.retry_policy.next(attempt):
                    case None, _, _:
                        logger.info("enqueued", extra={"computation_id": self.id, "id": id})
                        node.transition(Blocked(Running(f)))
                        return [
                            Function(id, self.id, lambda: func(ctx, *args, **kwargs)),
                        ]
                    case Ok(v), True, _:
                        logger.info("completed successfully", extra={"computation_id": self.id, "id": id})

                        node.transition(Blocked(Running(f)))
                        return [
                            Network(id, self.id, ResolvePromiseReq(id=id, ikey=id, data=v)),
                        ]
                    case Ok(), False, _:
                        logger.info("completed successfully", extra={"computation_id": self.id, "id": id})

                        node.transition(Enabled(Completed(f.map(result=result))))
                        self._unblock(suspends, result)
                        return []
                    case Ko(e), True, d if d is None or time.time() + d > timeout or type(e) in opts.non_retryable_exceptions:
                        logger.info("completed unsuccessfully", extra={"computation_id": self.id, "id": id})

                        node.transition(Blocked(Running(f)))
                        return [
                            Network(id, self.id, RejectPromiseReq(id=id, ikey=id, data=e)),
                        ]
                    case Ko(e), False, d if d is None or time.time() + d > timeout or type(e) in opts.non_retryable_exceptions:
                        logger.info("completed unsuccessfully", extra={"computation_id": self.id, "id": id})

                        node.transition(Enabled(Completed(f.map(result=result))))
                        self._unblock(suspends, result)
                        return []
                    case Ko(), _, d:
                        assert d is not None, "Delay must be set."
                        logger.info("enqueued(attempt=%s)", attempt + 1, extra={"computation_id": self.id, "id": id})
                        node.transition(Blocked(Running(f.map(attempt=attempt + 1))))
                        return [
                            Delayed(Function(id, self.id, lambda: func(ctx, *args, **kwargs)), d),
                        ]

            case Enabled(Running(Coro(id=id, coro=coro, next=next, opts=opts, attempt=attempt, ctx=parent_ctx, timeout=timeout) as c)):
                match next:
                    case None:
                        logger.info("spawned", extra={"computation_id": self.id, "id": id, "attempt": attempt})

                    case Ok() | Ko():
                        logger.info("resumed", extra={"computation_id": self.id, "id": id, "attempt": attempt})

                    case AWT():
                        pass

                cmd = coro.send(next)
                child = self.graph.find(lambda n: n.id == cmd.id) or Node(cmd.id, Enabled(Suspended(Init())))
                self.history.append((id, next, cmd))

                clock = parent_ctx.get_dependency("resonate:time", time)
                assert isinstance(clock, Clock), "resonate:time must be an instance of clock"

                match cmd, child.value:
                    case LFI(conv, func, args, kwargs, opts), Enabled(Suspended(Init(next=None))):
                        logger.info("invoked %s", conv.id, extra={"computation_id": self.id, "id": id})

                        cls = Coro if isgeneratorfunction(func) else Lfnc
                        next = cls(conv.id, self.id, conv, min(clock.time() + conv.timeout, c.timeout), func, args, kwargs, opts, self.ctx)
                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(Init(next, suspends=[node]))))
                        return []

                    case RFI(conv, mode=mode), Enabled(Suspended(Init(next=None))):
                        logger.info("invoked %s", conv.id, extra={"computation_id": self.id, "id": id})

                        next = Rfnc(conv.id, self.id, conv, (min(clock.time() + conv.timeout, c.timeout) if mode == "attached" else clock.time() + conv.timeout))
                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(Init(next, suspends=[node]))))
                        return []

                    case LFI(conv) | RFI(conv), Enabled(Running(Init() as f)):
                        logger.info("invoked %s", conv.id, extra={"computation_id": self.id, "id": id})

                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(f.map(suspends=node))))
                        return []

                    case LFI(conv) | RFI(conv), Blocked(Running(Init() as f)):
                        logger.info("invoked %s", conv.id, extra={"computation_id": self.id, "id": id})

                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Blocked(Running(f.map(suspends=node))))
                        return []

                    case LFI(conv) | RFI(conv), _:
                        logger.info("invoked %s", conv.id, extra={"computation_id": self.id, "id": id})

                        node.add_edge(child)
                        node.transition(Enabled(Running(c.map(next=AWT(conv.id)))))
                        return []

                    case AWT(aid), Enabled(Running(f)):
                        logger.info("awaiting %s", aid, extra={"computation_id": self.id, "id": id})

                        node.add_edge(child, "waiting[v]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(f.map(suspends=node))))
                        return []

                    case AWT(aid), Blocked(Running(f)):
                        logger.info("awaiting %s", aid, extra={"computation_id": self.id, "id": id})
                        node.add_edge(child, "waiting[v]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Blocked(Running(f.map(suspends=node))))
                        return []

                    case AWT(aid), Enabled(Suspended(f)):
                        logger.info("awaiting %s", aid, extra={"computation_id": self.id, "id": id})
                        node.add_edge(child, "waiting[v]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Suspended(f.map(suspends=node))))
                        return []

                    case AWT(aid), Enabled(Completed(Lfnc(result=result) | Rfnc(result=result) | Coro(result=result))):
                        assert result is not None, "Completed result must be set."
                        logger.info("awaiting %s", aid, extra={"computation_id": self.id, "id": id})
                        node.transition(Enabled(Running(c.map(next=result))))
                        return []

                    case TRM(id, result), _:
                        assert id == node.id, "Id must match node id."

                        match result, opts.durable, c.retry_policy.next(attempt):
                            case Ok(v), True, _:
                                logger.info("completed successfully", extra={"computation_id": self.id, "id": id})

                                node.transition(Blocked(Running(c)))
                                return [
                                    Network(id, self.id, ResolvePromiseReq(id=id, ikey=id, data=v)),
                                ]
                            case Ok(), False, _:
                                logger.info("completed successfully", extra={"computation_id": self.id, "id": id})

                                node.transition(Enabled(Completed(c.map(result=result))))
                                self._unblock(c.suspends, result)
                                return []
                            case Ko(e), True, d if d is None or time.time() + d > timeout or type(e) in opts.non_retryable_exceptions:
                                logger.info("completed unsuccessfully", extra={"computation_id": self.id, "id": id})

                                node.transition(Blocked(Running(c)))
                                return [
                                    Network(id, self.id, RejectPromiseReq(id=id, ikey=id, data=e)),
                                ]
                            case Ko(e), False, d if d is None or time.time() + d > timeout or type(e) in opts.non_retryable_exceptions:
                                logger.info("completed unsuccessfully", extra={"computation_id": self.id, "id": id})

                                node.transition(Enabled(Completed(c.map(result=result))))
                                self._unblock(c.suspends, result)
                                return []
                            case Ko(), _, d:
                                assert d is not None, "Delay must be set."

                                node.transition(Blocked(Running(c.map(attempt=attempt + 1))))
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

    def _unblock(self, nodes: list[Node[State]], next: AWT | Result) -> None:
        for node in nodes:
            match node.value:
                case Enabled(Suspended(Coro() as c)):
                    node.transition(Enabled(Running(c.map(next=next))))
                case _:
                    raise NotImplementedError

    def print(self) -> None:
        format = """
========================================
Computation: %s
========================================

Graph:
  %s

History:
  %s
"""

        logger.info(
            format,
            self.id,
            "\n  ".join(f"{'  ' * level}{node}" for node, level in self.graph.traverse_with_level()),
            "\n  ".join(str(event) for event in self.history) if self.history else "None",
        )
