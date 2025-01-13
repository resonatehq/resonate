from __future__ import annotations

from dataclasses import dataclass
from queue import Queue
from typing import TYPE_CHECKING, Any, Union

from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from resonate.dataclasses import Invocation
    from resonate.handle import Handle
    from resonate.result import Result
    from resonate.stores.record import TaskRecord


@dataclass(frozen=True)
class Invoke:
    id: str


@dataclass(frozen=True)
class ForkOrJoin:
    id: str
    handle: Handle[Any]
    invocation: Invocation[Any]


@dataclass(frozen=True)
class Subscribe:
    id: str
    handle: Handle[Any]


@dataclass(frozen=True)
class Notify:
    id: str
    value: Result[Any, Exception]


@dataclass(frozen=True)
class Resume:
    id: str
    next_value: Result[Any, Exception]


@dataclass(frozen=True)
class Complete:
    id: str
    result: Result[Any, Exception]


@dataclass(frozen=True)
class Claim:
    record: TaskRecord


Command: TypeAlias = Union[
    Invoke, Resume, Complete, Claim, Subscribe, Notify, ForkOrJoin
]
CommandQ: TypeAlias = Queue[Union[Command, None]]
