"""Execution events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Union

from typing_extensions import ParamSpec, TypeAlias

P = ParamSpec("P")


@dataclass(frozen=True)
class PromiseCreated:
    promise_id: str
    tick: int
    fn_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(frozen=True)
class ExecutionStarted:
    promise_id: str
    tick: int
    fn_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(frozen=True)
class PromiseResolved:
    promise_id: str
    tick: int


@dataclass(frozen=True)
class SuspendedForPromise:
    promise_id: str
    tick: int


@dataclass(frozen=True)
class AwaitedForPromise:
    promise_id: str
    tick: int


@dataclass(frozen=True)
class Resummend:
    promise_id: str
    tick: int


SchedulerEvents: TypeAlias = Union[
    PromiseCreated,
    ExecutionStarted,
    PromiseResolved,
    SuspendedForPromise,
    AwaitedForPromise,
    Resummend,
]
