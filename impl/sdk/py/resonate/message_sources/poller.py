from __future__ import annotations

import logging
import os
import time
from threading import Thread
from typing import TYPE_CHECKING, Any

import requests

from resonate.encoders import JsonEncoder
from resonate.models.message import InvokeMesg, NotifyMesg, ResumeMesg
from resonate.utils import exit_on_exception

if TYPE_CHECKING:
    from collections.abc import Generator

    from requests.models import Response

    from resonate.models.encoder import Encoder
    from resonate.models.message import Mesg
    from resonate.models.message_source import MessageQ

logger = logging.getLogger(f"{__package__}.poller")


class Poller:
    def __init__(
        self,
        group: str,
        id: str,
        host: str | None = None,
        port: str | None = None,
        timeout: int | None = None,
        encoder: Encoder[Any, str] | None = None,
    ) -> None:
        self._group = group
        self._id = id
        self._host = host or os.getenv("RESONATE_HOST_MESSAGE_SOURCE", os.getenv("RESONATE_HOST", "http://localhost"))
        self._port = port or os.getenv("RESONATE_PORT_MESSAGE_SOURCE", "8002")
        self._timeout = timeout
        self._encoder = encoder or JsonEncoder()
        self._thread: Thread | None = None
        self._shutdown = False

    @property
    def url(self) -> str:
        return f"{self._host}:{self._port}/{self._group}/{self._id}"

    @property
    def unicast(self) -> str:
        return f"poll://{self._group}/{self._id}"

    @property
    def anycast(self) -> str:
        return f"poll://{self._group}/{self._id}"

    def start(self, mq: MessageQ) -> None:
        if self._thread is not None:
            return

        self._thread = Thread(name="poller-thread", target=self.loop, args=(mq,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        # TODO(avillega): Couldn't come up with a nice way of stoping this thread
        # iter_lines is blocking and request.get is also blocking, this makes it so
        # the only way to stop it is waiting for a timeout on the request itself
        # which could never happen.

        # This shutdown is only respected when the poller is instantiated with a timeout
        # value, which is not the default. This is still useful for tests.
        self._shutdown = True

    @exit_on_exception("mesg_source.poller")
    def loop(self, mq: MessageQ) -> None:
        while True:
            if self._shutdown:
                break

            try:
                with requests.get(self.url, stream=True, timeout=self._timeout) as res:
                    res.raise_for_status()

                    for msg in self._step(res):
                        mq.enqueue(msg)

            except requests.exceptions.Timeout:
                logger.warning("Polling request timed out for group %s. Retrying after delay...", self._group)
                time.sleep(0.5)
                continue
            except requests.exceptions.RequestException as e:
                logger.warning("Polling network error for group %s: %s. Retrying after delay...", self._group, str(e))
                time.sleep(0.5)
                continue
            except Exception as e:
                logger.warning("Unexpected error in poller loop for group %s: %s. Retrying after delay...", self._group, e)
                time.sleep(0.5)
                continue

    def step(self) -> list[Mesg]:
        with requests.get(self.url, stream=True, timeout=self._timeout) as res:
            msg = next(self._step(res))
            return [msg]

    def _step(self, res: Response) -> Generator[Mesg, None, None]:
        for line in res.iter_lines(chunk_size=None, decode_unicode=True):
            assert isinstance(line, str), "line must be a string"
            if msg := self._process_line(line):
                yield msg

    def _process_line(self, line: str) -> Mesg | None:
        if not line:
            return None

        stripped = line.strip()
        if not stripped.startswith("data:"):
            return None

        d = self._encoder.decode(stripped[5:])
        match d["type"]:
            case "invoke":
                return InvokeMesg(type="invoke", task=d["task"])
            case "resume":
                return ResumeMesg(type="resume", task=d["task"])
            case "notify":
                return NotifyMesg(type="notify", promise=d["promise"])
            case _:
                # Unknown message type
                return None
