from __future__ import annotations

from typing import Any, Literal, TypedDict

type Mesg = InvokeMesg | ResumeMesg | NotifyMesg


class InvokeMesg(TypedDict):
    type: Literal["invoke"]
    task: TaskMesg


class ResumeMesg(TypedDict):
    type: Literal["resume"]
    task: TaskMesg


class NotifyMesg(TypedDict):
    type: Literal["notify"]
    promise: Any


class TaskMesg(TypedDict):
    id: str
    counter: int
