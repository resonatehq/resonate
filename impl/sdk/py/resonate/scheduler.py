from __future__ import annotations

import sys
import uuid
from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Final, Literal

from resonate.coroutine import AWT, LFI, RFI, TRM, Coroutine
from resonate.graph import Graph, Node
from resonate.logging import logger
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

    from resonate.models.context import Context
    from resonate.models.durable_promise import DurablePromise


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

    def step(self, cmd: Command, futures: tuple[Future, Future] | None = None) -> More | Done:
        computation = self.computations.setdefault(cmd.cid, Computation(cmd.cid, self.ctx, self.pid, self.unicast, self.anycast))

        # subscribe
        if futures:
            computation.subscribe(*futures)

        # apply
        computation.apply(cmd)

        # eval
        return computation.eval()


class Info:
    def __init__(self) -> None:
        self._attempt = 1

    @property
    def attempt(self) -> int:
        return self._attempt

    def increment_attempt(self) -> None:
        self._attempt += 1


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
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: Options = field(default_factory=Options)
    info: Info = field(default_factory=Info)

    suspends: list[Node[State]] = field(default_factory=list)
    result: Result | None = None

    def map(self, *, suspends: Node[State] | None = None, result: Result | None = None) -> Lfnc:
        if suspends:
            self.suspends.append(suspends)
        if result:
            self.result = result
        return self

    def __repr__(self) -> str:
        s = ", ".join([s.value.func.id for s in self.suspends])
        return f"Lfnc(id={self.id}, suspends=[{s}], result={self.result})"


@dataclass
class Rfnc:
    id: str
    timeout: int
    ikey: str | None = None
    headers: dict[str, str] | None = None
    data: Any = None
    tags: dict[str, str] | None = None

    suspends: list[Node[State]] = field(default_factory=list)
    result: Result | None = None

    def map(self, *, suspends: Node[State] | None = None, result: Result | None = None) -> Rfnc:
        if suspends:
            self.suspends.append(suspends)
        if result:
            self.result = result
        return self

    def __repr__(self) -> str:
        s = ", ".join([s.value.func.id for s in self.suspends])
        return f"Rfnc(id={self.id}, suspends=[{s}], result={self.result})"


@dataclass
class Coro:
    id: str
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: Options

    cid: str
    ctx: Callable[[str, Info], Context]
    info: Info = field(default_factory=Info)

    coro: Coroutine = field(init=False)
    next: None | AWT | Result = None

    suspends: list[Node[State]] = field(default_factory=list)
    result: Result | None = None

    def __post_init__(self) -> None:
        self.coro = Coroutine(self.id, self.cid, self.func(self.ctx(self.id, self.info), *self.args, **self.kwargs))

    def reset(self) -> None:
        self.next = None
        self.coro = Coroutine(self.id, self.cid, self.func(self.ctx(self.id, self.info), *self.args, **self.kwargs))

    def map(self, *, next: None | AWT | Result = None, suspends: Node[State] | None = None, result: Result | None = None) -> Coro:
        if next:
            self.next = next
        if suspends:
            self.suspends.append(suspends)
        if result:
            self.result = result
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
        self.futures: tuple[PoppableList[Future], PoppableList[Future]] = (PoppableList(), PoppableList())
        self.history: list[Command | Function | Delayed | Network | tuple[str, None | AWT | Result, LFI | RFI | AWT | TRM]] = []

    def __repr__(self) -> str:
        return f"Computation(id={self.id}, runnable={self.runnable()}, suspendable={self.suspendable()})"

    def runnable(self) -> bool:
        return any(n.value.running for n in self.graph.traverse())

    def suspendable(self) -> bool:
        return all(n.value.suspended for n in self.graph.traverse())

    def subscribe(self, fp: Future, fv: Future) -> None:
        self.futures[0].append(fp)
        self.futures[1].append(fv)

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
            case Invoke(id, _, func, args, kwargs, opts), Init(next=None):
                if isgeneratorfunction(func):
                    self.graph.root.transition(Enabled(Running(Coro(id, func, args, kwargs, opts, self.id, self.ctx))))
                elif func is None:
                    self.graph.root.transition(Enabled(Suspended(Rfnc(id, opts.timeout))))
                else:
                    self.graph.root.transition(Enabled(Running(Lfnc(id, func, args, kwargs, opts))))

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
                node.transition(Enabled(Completed(f.map(result=promise.result))))

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
                node.transition(Enabled(Running(next)))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id))

            case Blocked(Running(Init(Rfnc() as next, suspends))), promise if promise.pending:
                assert not next.suspends, "Next suspends must be initially empty."
                assert promise.tags.get("resonate:scope") == "global", "Scope must be global."
                node.transition(Enabled(Suspended(next)))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id))

            case Blocked(Running(Init(next, suspends))), promise if promise.completed:
                assert next, "Next must be set."
                assert not next.suspends, "Suspends must be initially empty."
                node.transition(Enabled(Completed(next.map(result=promise.result))))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id))

            case Blocked(Running(Lfnc(suspends=suspends) | Coro(suspends=suspends) as f)), promise if promise.completed:
                node.transition(Enabled(Completed(f.map(result=promise.result))))

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
                node.transition(
                    Enabled(
                        Completed(
                            Rfnc(
                                promise.id,
                                promise.timeout,
                                promise.ikey_for_create,
                                promise.param.headers,
                                promise.param.data,
                                promise.tags,
                                result=promise.result,
                            )
                        )
                    )
                )

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
                    assert isinstance(self.graph.root.value.func, (Lfnc, Coro))

                    done.append(
                        Network(
                            node.id,
                            self.id,
                            CreateCallbackReq(
                                f"{self.id}:{node.id}",
                                node.id,
                                self.id,
                                self.graph.root.value.func.opts.timeout,
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
            while future := self.futures[1].spop():
                match result:
                    case Ok(v):
                        future.set_result(v)
                    case Ko(e):
                        future.set_exception(e)

        return Done(done) if self.suspendable() else More(more)

    def _eval(self, node: Node[State]) -> list[Function | Delayed[Function | Retry] | Network[CreatePromiseReq | ResolvePromiseReq | RejectPromiseReq | CancelPromiseReq]]:
        match node.value:
            case Enabled(Running(Init(Lfnc(id, opts=opts) | Coro(id, opts=opts) as func, suspends)) as exec):
                assert id == node.id, "Id must match node id."
                assert node is not self.graph.root, "Node must not be root node."

                match opts.durable:
                    case True:
                        node.transition(Blocked(exec))
                        return [
                            Network(
                                id,
                                self.id,
                                CreatePromiseReq(
                                    id=id,
                                    ikey=id,
                                    timeout=opts.timeout,
                                    tags={**opts.tags, "resonate:scope": "local"},
                                ),
                            ),
                        ]
                    case False:
                        assert len(suspends) == 1, "Nothing should be blocked"
                        node.transition(Enabled(Running(func)))
                        self._unblock(suspends, AWT(id))
                        return []

            case Enabled(Running(Init(Rfnc(id, timeout, ikey, headers, data, tags))) as exec):
                assert id == node.id, "Id must match node id."
                node.transition(Blocked(exec))

                return [
                    Network(
                        id,
                        self.id,
                        CreatePromiseReq(
                            id=id,
                            ikey=ikey,
                            timeout=timeout,
                            headers=headers,
                            data=data,
                            tags={**(tags or {}), "resonate:scope": "global"},
                        ),
                    ),
                ]

            case Enabled(Running(Lfnc(id, func, args, kwargs, opts, info, result=result, suspends=suspends) as f)):
                assert id == node.id, "Id must match node id."
                assert func is not None, "Func is required for local function."

                match result, opts.retry_policy.next(info.attempt), opts.durable:
                    case None, _, _:
                        node.transition(Blocked(Running(f)))
                        return [
                            Function(id, self.id, lambda: func(self.ctx(id, info), *args, **kwargs)),
                        ]
                    case Ok(v), _, True:
                        node.transition(Blocked(Running(f)))
                        return [
                            Network(id, self.id, ResolvePromiseReq(id=id, ikey=id, data=v)),
                        ]
                    case Ko(v), None, True:
                        node.transition(Blocked(Running(f)))
                        return [
                            Network(id, self.id, RejectPromiseReq(id=id, ikey=id, data=v)),
                        ]
                    case Ok(v), _, False:
                        node.transition(Enabled(Completed(f.map(result=result))))
                        self._unblock(suspends, result)
                        return []
                    case Ko(v), None, False:
                        node.transition(Enabled(Completed(f.map(result=result))))
                        self._unblock(suspends, result)
                        return []
                    case Ko(), delay, _:
                        node.transition(Blocked(Running(f)))
                        info.increment_attempt()
                        return [
                            Delayed(Function(id, self.id, lambda: func(self.ctx(id, info), *args, **kwargs)), delay),
                        ]

            case Enabled(Running(Coro(id=id, coro=coro, next=next, opts=opts, info=info) as c)):
                cmd = coro.send(next)
                child = self.graph.find(lambda n: n.id == cmd.id) or Node(cmd.id, Enabled(Suspended(Init())))
                self.history.append((id, next, cmd))

                match cmd, child.value:
                    case LFI(id, func, args, kwargs, opts), Enabled(Suspended(Init(next=None))):
                        next = Coro(id, func, args, kwargs, opts, self.id, self.ctx) if isgeneratorfunction(func) else Lfnc(id, func, args, kwargs, opts)

                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(Init(next, suspends=[node]))))
                        return []

                    case RFI(id, convention), Enabled(Suspended(Init(next=None))):
                        data, tags, timeout, headers = convention.format()
                        next = Rfnc(id=id, timeout=timeout, ikey=id, data=data, tags=tags, headers=headers)

                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(Init(next, suspends=[node]))))
                        return []

                    case LFI() | RFI(), Enabled(Running(Init() as f)):
                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(f.map(suspends=node))))
                        return []

                    case LFI() | RFI(), Blocked(Running(Init() as f)):
                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Blocked(Running(f.map(suspends=node))))
                        return []

                    case LFI(id) | RFI(id), _:
                        node.add_edge(child)
                        node.transition(Enabled(Running(c.map(next=AWT(id)))))
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
                        match result, opts.retry_policy.next(info.attempt), opts.durable:
                            case Ok(v), _, True:
                                node.transition(Blocked(Running(c)))
                                return [
                                    Network(id, self.id, ResolvePromiseReq(id=id, ikey=id, data=v)),
                                ]
                            case Ko(v), None, True:
                                node.transition(Blocked(Running(c)))
                                return [
                                    Network(id, self.id, RejectPromiseReq(id=id, ikey=id, data=v)),
                                ]
                            case Ok(v), _, False:
                                node.transition(Enabled(Completed(c.map(result=result))))
                                self._unblock(c.suspends, result)
                                return []
                            case Ko(v), None, False:
                                node.transition(Enabled(Completed(c.map(result=result))))
                                self._unblock(c.suspends, result)
                                return []
                            case Ko(), delay, _:
                                node.transition(Blocked(Running(c)))
                                c.reset()
                                info.increment_attempt()
                                return [
                                    Delayed(Retry(id, self.id), delay),
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
