from __future__ import annotations

import time
from threading import Event, Thread
from typing import Any

import requests

from resonate.encoders import JsonEncoder
from resonate.logging import logger
from resonate.queue import Queue
from resonate.stores.record import TaskRecord
from resonate.task_sources.traits import ITaskSource


class Poller(ITaskSource):
    def __init__(
        self,
        url: str = "http://localhost:8002",
        group: str = "default",
    ) -> None:
        self._url = url
        self._group = group
        self._encoder = JsonEncoder()
        self._tasks: Queue[TaskRecord] = Queue()

    def start(self, event: Event, pid: str) -> None:
        t = Thread(target=self._run, args=(event, pid), daemon=True)
        t.start()

    def dequeue(self) -> list[TaskRecord]:
        return self._tasks.dequeue_all()

    def _run(self, event: Event, pid: str) -> None:
        url = f"{self._url}/{self._group}/{pid}"

        while True:
            try:
                with requests.get(url, stream=True) as res:  # noqa: S113
                    if not res.ok:
                        break

                    for line in res.iter_lines(chunk_size=None, decode_unicode=True):
                        if not line:
                            continue

                        stripped = line.strip()
                        if not stripped.startswith("data:"):
                            continue

                        info = self._encoder.decode(stripped[5:])
                        if "task" not in info:
                            continue

                        # extract the task
                        task = TaskRecord.decode(info["task"], encoder=self._encoder)

                        # enqueue the task
                        self._tasks.put_nowait(task)

                        # raise event so scheduler knows there is something to process
                        event.set()

            except requests.exceptions.ConnectionError:
                logger.warning("Connection to poller failed, reconnecting")

            time.sleep(1)

    def default_recv(self, pid: str) -> dict[str, Any]:
        return {"type": "poll", "data": {"group": self._group, "id": pid}}
