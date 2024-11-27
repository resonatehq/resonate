from __future__ import annotations

import time
from threading import Event, Thread
from typing import Any

import requests

from resonate.encoders import JsonEncoder
from resonate.queue import Queue
from resonate.stores.record import TaskRecord
from resonate.task_sources.traits import ITaskSource


class Poller(ITaskSource):
    def __init__(self, url: str, group: str) -> None:
        self._url = url
        self._group = group
        self._encoder = JsonEncoder()
        self._sync_event: Event | None = None
        self._pid: str | None = None
        self._tasks: Queue[TaskRecord] = Queue()
        self._worket_thread = Thread(target=self._run, daemon=True)

    def get_tasks(self) -> list[TaskRecord]:
        return self._tasks.dequeue_all()

    def set_sync_event(self, event: Event) -> None:
        assert self._sync_event is None
        self._sync_event = event

    def _run(self) -> None:
        assert self._pid is not None
        assert self._sync_event is not None
        try:
            while True:
                response = requests.get(  # noqa: S113
                    url=f"{self._url}/{self._group}/{self._pid}",
                    headers={"Accept": "text/event-stream"},
                    stream=True,
                )
                for line in response.iter_lines(chunk_size=None, decode_unicode=True):
                    if not line:
                        continue
                    stripped: str = line.strip()
                    assert stripped.startswith("data:")
                    info = self._encoder.decode(stripped[5:])
                    task = TaskRecord.decode(info["task"], encoder=self._encoder)
                    self._tasks.put_nowait(task)
                    self._sync_event.set()
        except requests.exceptions.ConnectionError:
            time.sleep(2)

    def start(self, pid: str) -> None:
        assert self._pid is None
        self._pid = pid
        self._worket_thread.start()

    def default_recv(self) -> dict[str, Any]:
        return {"type": "poll", "data": {"group": self._group, "id": self._pid}}
