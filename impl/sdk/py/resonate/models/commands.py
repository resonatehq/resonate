from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from resonate.options import Options

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.models.callback import Callback
    from resonate.models.convention import Convention
    from resonate.models.durable_promise import DurablePromise
    from resonate.models.result import Result
    from resonate.models.task import Task


# Commands

type Command = Invoke | Resume | Return | Receive | Retry | Listen | Notify


@dataclass
class Invoke:
    id: str
    conv: Convention
    timeout: float  # absolute time in seconds
    func: Callable[..., Any] = field(repr=False)
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    opts: Options = field(default_factory=Options, repr=False)
    promise: DurablePromise | None = None

    @property
    def cid(self) -> str:
        return self.id


@dataclass
class Resume:
    id: str
    cid: str
    promise: DurablePromise
    invoke: Invoke | None = None


@dataclass
class Return:
    id: str
    cid: str
    res: Result


@dataclass
class Receive:
    id: str
    cid: str
    res: CreatePromiseRes | CreatePromiseWithTaskRes | ResolvePromiseRes | RejectPromiseRes | CancelPromiseRes | CreateCallbackRes


@dataclass
class Listen:
    id: str

    @property
    def cid(self) -> str:
        return self.id


@dataclass
class Notify:
    id: str
    promise: DurablePromise

    @property
    def cid(self) -> str:
        return self.id


@dataclass
class Retry:
    id: str
    cid: str


# Requests

type Request = Network | Function | Delayed


@dataclass
class Network[T: CreatePromiseReq | ResolvePromiseReq | RejectPromiseReq | CancelPromiseReq | CreateCallbackReq | CreateSubscriptionReq]:
    id: str
    cid: str
    req: T


@dataclass
class Function:
    id: str
    cid: str
    func: Callable[[], Any]


@dataclass
class Delayed[T: Function | Retry]:
    item: T
    delay: float

    @property
    def id(self) -> str:
        return self.item.id


@dataclass
class CreatePromiseReq:
    id: str
    timeout: int
    ikey: str | None = None
    strict: bool = False
    headers: dict[str, str] | None = None
    data: str | None = None
    tags: dict[str, str] | None = None


@dataclass
class CreatePromiseRes:
    promise: DurablePromise


@dataclass
class CreatePromiseWithTaskReq:
    id: str
    timeout: int
    pid: str
    ttl: int
    ikey: str | None = None
    strict: bool = False
    headers: dict[str, str] | None = None
    data: str | None = None
    tags: dict[str, str] | None = None


@dataclass
class CreatePromiseWithTaskRes:
    promise: DurablePromise
    task: Task | None


@dataclass
class ResolvePromiseReq:
    id: str
    ikey: str | None = None
    strict: bool = False
    headers: dict[str, str] | None = None
    data: str | None = None


@dataclass
class ResolvePromiseRes:
    promise: DurablePromise


@dataclass
class RejectPromiseReq:
    id: str
    ikey: str | None = None
    strict: bool = False
    headers: dict[str, str] | None = None
    data: str | None = None


@dataclass
class RejectPromiseRes:
    promise: DurablePromise


@dataclass
class CancelPromiseReq:
    id: str
    ikey: str | None = None
    strict: bool = False
    headers: dict[str, str] | None = None
    data: str | None = None


@dataclass
class CancelPromiseRes:
    promise: DurablePromise


@dataclass
class CreateCallbackReq:
    promise_id: str
    root_promise_id: str
    timeout: int
    recv: str


@dataclass
class CreateCallbackRes:
    promise: DurablePromise
    callback: Callback | None


@dataclass
class CreateSubscriptionReq:
    id: str
    promise_id: str
    timeout: int
    recv: str


@dataclass
class CreateSubscriptionRes:
    promise: DurablePromise
    callback: Callback | None


@dataclass
class ClaimTaskReq:
    id: str
    counter: int
    pid: str
    ttl: int


@dataclass
class ClaimTaskRes:
    root: DurablePromise
    leaf: DurablePromise | None
    task: Task


@dataclass
class CompleteTaskReq:
    id: str
    counter: int


@dataclass
class CompleteTaskRes:
    pass


@dataclass
class HeartbeatTasksReq:
    pid: str


@dataclass
class HeartbeatTasksRes:
    affected: int
