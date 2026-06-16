from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Protocol

from resonate.send import TaskRef

if TYPE_CHECKING:
    from resonate.send import Sender

logger = logging.getLogger(__name__)


class Heartbeat(Protocol):
    """Protocol for keeping task leases alive.

    Implementations track a set of active tasks and periodically send heartbeat
    requests to the server for all of them.
    """

    def start(self, task_id: str, task_version: int) -> None:
        """Add a task to the heartbeat set.

        Starts the heartbeat loop if this is the first tracked task.
        """

    def stop(self, task_id: str) -> None:
        """Remove a task from the heartbeat set.

        Stops the heartbeat loop if no tasks remain.
        """

    def shutdown(self) -> None:
        """Shut down the heartbeat entirely.

        Clears all tracked tasks and aborts the loop. Called on graceful shutdown.
        """


class NoopHeartbeat:
    """No-op heartbeat for local mode."""

    def start(self, task_id: str, task_version: int) -> None: ...
    def stop(self, task_id: str) -> None: ...
    def shutdown(self) -> None: ...


class AsyncHeartbeat:
    """Sends ``task.heartbeat`` requests at regular intervals for tracked tasks.

    Uses :class:`~resonate.send.Sender` (not the raw transport) so the request
    goes through the standard protocol envelope with corrId, version header, etc.

    The loop runs as an asyncio task; ``active_tasks`` needs no lock because
    asyncio is single-threaded and the sync methods never mutate it across an
    ``await``.
    """

    def __init__(self, pid: str, interval_ms: int, sender: Sender) -> None:
        self.pid = pid
        self.interval_ms = interval_ms
        self.sender = sender
        self.active_tasks: dict[str, int] = {}
        self._handle: asyncio.Task[None] | None = None

    def _ensure_loop_running(self) -> None:
        """Spawn the heartbeat loop if not already running."""
        if self._handle is not None:
            return
        self._handle = asyncio.create_task(self._run())

    async def _run(self) -> None:
        interval = self.interval_ms / 1000
        # Send the first heartbeat immediately, then one every interval.
        first = True
        with contextlib.suppress(asyncio.CancelledError):
            while True:
                if first:
                    first = False
                else:
                    await asyncio.sleep(interval)

                # Snapshot active tasks.
                tasks = [
                    TaskRef(id=task_id, version=version)
                    for task_id, version in self.active_tasks.items()
                ]

                if not tasks:
                    continue

                try:
                    await self.sender.task_heartbeat(self.pid, tasks)
                except Exception as exc:  # log and keep heartbeating
                    logger.warning("heartbeat failed: %s", exc)

    def start(self, task_id: str, task_version: int) -> None:
        self.active_tasks[task_id] = task_version
        self._ensure_loop_running()

    def stop(self, task_id: str) -> None:
        self.active_tasks.pop(task_id, None)
        if not self.active_tasks:
            handle = self._handle
            self._handle = None
            if handle is not None:
                handle.cancel()

    def shutdown(self) -> None:
        self.active_tasks.clear()
        handle = self._handle
        self._handle = None
        if handle is not None:
            handle.cancel()
