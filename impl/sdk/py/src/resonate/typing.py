from __future__ import annotations

from collections import deque
from collections.abc import Coroutine, Generator
from typing import Any, Callable, Literal, TypeVar, Union

from typing_extensions import Concatenate, ParamSpec, TypeAlias

from resonate.actions import (
    LFC,
    LFI,
    RFC,
    RFI,
    All,
    AllSettled,
    DeferredInvocation,
    Race,
)
from resonate.commands import Command
from resonate.context import Context
from resonate.dataclasses import Runnable
from resonate.promise import Promise

T = TypeVar("T")
P = ParamSpec("P")


Combinator: TypeAlias = Union[All, AllSettled, Race]

PromiseActions: TypeAlias = Union[Combinator, LFI, RFI]

Yieldable: TypeAlias = Union[LFC, Promise[Any], PromiseActions, DeferredInvocation, RFC]

DurableCoro: TypeAlias = Callable[Concatenate[Context, P], Generator[Yieldable, Any, T]]
DurableSyncFn: TypeAlias = Callable[Concatenate[Context, P], T]
DurableAsyncFn: TypeAlias = Callable[Concatenate[Context, P], Coroutine[Any, Any, T]]
DurableFn: TypeAlias = Union[DurableSyncFn[P, T], DurableAsyncFn[P, T]]


AwaitingFor: TypeAlias = Literal["local", "remote"]
Runnables: TypeAlias = deque[tuple[Runnable[Any], bool]]


Headers: TypeAlias = Union[dict[str, str], None]
Tags: TypeAlias = Union[dict[str, str], None]
IdempotencyKey: TypeAlias = Union[str, None]
Data: TypeAlias = Union[str, None]
State: TypeAlias = Literal[
    "PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"
]


C = TypeVar("C", bound=Command)

CmdHandlerResult: TypeAlias = Union[list[Union[T, Exception]], T, None]
CmdHandler: TypeAlias = Callable[[Context, list[C]], CmdHandlerResult[Any]]
