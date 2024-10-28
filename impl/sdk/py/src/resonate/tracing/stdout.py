from __future__ import annotations

from typing import TYPE_CHECKING, final

from resonate.logging import logger
from resonate.tracing import IAdapter

if TYPE_CHECKING:
    from resonate.events import SchedulerEvents


@final
class StdOutAdapter(IAdapter):
    def process_event(self, event: SchedulerEvents) -> None:
        logger.info(event)
