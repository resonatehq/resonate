from __future__ import annotations

from collections.abc import Coroutine, Generator
from typing import Any, Callable, Literal, TypeVar, Union

from typing_extensions import Concatenate, ParamSpec, TypeAlias

from resonate.actions import (
    DI,
    LFC,
    LFI,
    RFC,
    RFI,
)
from resonate.context import Context
from resonate.promise import Promise

T = TypeVar("T")
P = ParamSpec("P")


Yieldable: TypeAlias = Union[
    LFI,
    LFC,
    RFI,
    RFC,
    DI,
    Promise[Any],
]

DurableCoro: TypeAlias = Callable[Concatenate[Context, P], Generator[Yieldable, Any, T]]
DurableSyncFn: TypeAlias = Callable[Concatenate[Context, P], T]
DurableAsyncFn: TypeAlias = Callable[Concatenate[Context, P], Coroutine[Any, Any, T]]
DurableFn: TypeAlias = Union[DurableSyncFn[P, T], DurableAsyncFn[P, T]]

Headers: TypeAlias = Union[dict[str, str], None]
Tags: TypeAlias = Union[dict[str, str], None]
IdempotencyKey: TypeAlias = Union[str, None]
Data: TypeAlias = Union[str, None]
State: TypeAlias = Literal[
    "PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"
]
