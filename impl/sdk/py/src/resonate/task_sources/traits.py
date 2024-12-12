from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from resonate.stores.record import TaskRecord

if TYPE_CHECKING:
    from threading import Event

    from resonate.stores.record import TaskRecord


class ITaskSource(ABC):
    @abstractmethod
    def start(self, event: Event, pid: str) -> None: ...

    @abstractmethod
    def dequeue(self) -> list[TaskRecord]: ...

    @abstractmethod
    def default_recv(self, pid: str) -> dict[str, Any]: ...
