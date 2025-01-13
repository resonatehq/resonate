from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resonate.cmd_queue import CommandQ


class Subsystem(ABC):
    @abstractmethod
    def start(self, cmd_queue: CommandQ, pid: str) -> None: ...

    @abstractmethod
    def stop(self) -> None: ...
