"""Execution events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Union

from typing_extensions import ParamSpec, TypeAlias

if TYPE_CHECKING:
    from resonate.result import Result

P = ParamSpec("P")


# Promise Events
@dataclass(frozen=True)
class PromiseCreated:
    promise_id: str
    tick: int


@dataclass(frozen=True)
class PromiseCompleted:
    promise_id: str
    tick: int
    value: Result[Any, Exception]


# Execution Events


@dataclass(frozen=True)
class ExecutionInvoked:
    promise_id: str
    tick: int
    fn_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(frozen=True)
class ExecutionTerminated:
    promise_id: str
    tick: int


@dataclass(frozen=True)
class ExecutionResumed:
    promise_id: str
    tick: int


@dataclass(frozen=True)
class ExecutionAwaited:
    promise_id: str
    tick: int


SchedulerEvents: TypeAlias = Union[
    PromiseCreated,
    PromiseCompleted,
    ExecutionInvoked,
    ExecutionTerminated,
    ExecutionResumed,
    ExecutionAwaited,
]
