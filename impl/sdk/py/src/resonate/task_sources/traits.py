from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from threading import Event

    from resonate.stores.record import TaskRecord


class ITaskSource(ABC):
    @abstractmethod
    def start(self, pid: str) -> None: ...

    @abstractmethod
    def default_recv(self) -> dict[str, Any]: ...

    @abstractmethod
    def get_tasks(self) -> list[TaskRecord]: ...

    @abstractmethod
    def set_sync_event(self, event: Event) -> None: ...
