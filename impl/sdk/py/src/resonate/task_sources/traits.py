from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from resonate.traits import Subsystem


class ITaskSource(Subsystem, ABC):
    @abstractmethod
    def set_pid(self, pid: str) -> None: ...
    @abstractmethod
    def default_recv(self) -> dict[str, Any]: ...
