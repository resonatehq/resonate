from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from resonate.traits import Subsystem


class ITaskSource(Subsystem, ABC):
    @abstractmethod
    def default_recv(self, pid: str) -> dict[str, Any]: ...
