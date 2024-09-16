from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resonate.events import (
        SchedulerEvents,
    )


class IAdapter(ABC):
    @abstractmethod
    def process_event(self, event: SchedulerEvents) -> None: ...
