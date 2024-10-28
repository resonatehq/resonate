from __future__ import annotations

import json
from threading import Thread
from typing import TYPE_CHECKING

import requests

from resonate.encoders import JsonEncoder
from resonate.record import TaskRecord

if TYPE_CHECKING:
    from resonate.scheduler import Scheduler


class LongPoller:
    def __init__(
        self,
        scheduler: Scheduler,
        url: str = "http://localhost",
    ) -> None:
        self._url = url
        self._scheduler = scheduler
        self._encoder = JsonEncoder()
        self._worket_thread = Thread(target=self._run, daemon=True)
        self._worket_thread.start()

    def _run(self) -> None:
        while True:
            poll_url = (
                f"{self._url}:8002/{self._scheduler.logic_group}/{self._scheduler.pid}"
            )
            response = requests.get(  # noqa: S113
                url=poll_url,
                headers={"Accept": "text/event-stream"},
                stream=True,
            )
            for line in response.iter_lines(chunk_size=None, decode_unicode=True):
                if not line:
                    continue
                stripped: str = line.strip()
                assert stripped.startswith("data:")
                info = json.loads(stripped[5:])
                self._scheduler.enqueue_task_record(
                    TaskRecord.decode(info["task"], encoder=self._encoder)
                )
