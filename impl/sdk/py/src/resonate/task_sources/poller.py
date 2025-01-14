from __future__ import annotations

import os
import time
from threading import Thread
from typing import Any

import requests

from resonate import utils
from resonate.cmd_queue import Claim, CommandQ
from resonate.encoders import JsonEncoder
from resonate.logging import logger
from resonate.stores.record import TaskRecord
from resonate.task_sources.traits import ITaskSource


class Poller(ITaskSource):
    def __init__(
        self,
        url: str | None = None,
        group: str | None = None,
    ) -> None:
        self._url = (
            url
            if url is not None
            else os.getenv("RESONATE_POLLER", "http://localhost:8002")
        )
        self._group = (
            group if group is not None else os.getenv("RESONATE_GROUP", "default")
        )
        self._encoder = JsonEncoder()
        self._t: Thread | None = None

    def start(self, cmd_queue: CommandQ, pid: str) -> None:
        assert self._t is None
        self._t = Thread(target=self._run, args=(cmd_queue, pid), daemon=True)
        self._t.start()

    def stop(self) -> None:
        raise NotImplementedError

    @utils.exit_on_exception
    def _run(self, cmd_queue: CommandQ, pid: str) -> None:
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
                        cmd_queue.put(Claim(task))

            except requests.exceptions.ConnectionError:
                logger.warning("Connection to poller failed, reconnecting")

            time.sleep(1)

    def default_recv(self, pid: str) -> dict[str, Any]:
        return {"type": "poll", "data": {"group": self._group, "id": pid}}
