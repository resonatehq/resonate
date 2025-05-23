from __future__ import annotations

from typing import Literal, TypedDict

type Mesg = InvokeMesg | ResumeMesg | NotifyMesg


class InvokeMesg(TypedDict):
    type: Literal["invoke"]
    task: TaskMesg


class ResumeMesg(TypedDict):
    type: Literal["resume"]
    task: TaskMesg


class NotifyMesg(TypedDict):
    type: Literal["notify"]
    promise: DurablePromiseMesg


class TaskMesg(TypedDict):
    id: str
    counter: int


class DurablePromiseMesg(TypedDict):
    id: str
    state: str
    timeout: int
    idempotencyKeyForCreate: str | None
    idempotencyKeyForComplete: str | None
    param: DurablePromiseValueMesg
    value: DurablePromiseValueMesg
    tags: dict[str, str] | None
    createdOn: int
    completedOn: int | None


class DurablePromiseValueMesg(TypedDict):
    headers: dict[str, str] | None
    data: str | None
