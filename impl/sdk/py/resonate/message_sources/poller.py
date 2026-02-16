from __future__ import annotations

import logging
import queue
import time
from threading import Thread
from typing import TYPE_CHECKING, Any

import requests

from resonate.encoders import JsonEncoder
from resonate.models.message import InvokeMesg, Mesg, NotifyMesg, ResumeMesg
from resonate.utils import exit_on_exception

if TYPE_CHECKING:
    from resonate.models.encoder import Encoder

logger = logging.getLogger(__name__)


class Poller:
    def __init__(
        self,
        group: str,
        id: str,
        url: str | None = None,
        auth: tuple[str, str] | None = None,
        token: str | None = None,
        timeout: float | None = None,
        encoder: Encoder[Any, str] | None = None,
    ) -> None:
        self._messages = queue.Queue[Mesg | None]()
        self._group = group
        self._id = id
        self._url = url or "http://localhost:8001"
        self._auth = auth
        self._token = token
        self._timeout = timeout
        self._encoder = encoder or JsonEncoder()
        self._thread = Thread(name="message-source::poller", target=self.loop, daemon=True)
        self._stopped = False

    @property
    def url(self) -> str:
        return f"{self._url}/poll/{self._group}/{self._id}"

    @property
    def unicast(self) -> str:
        return f"poll://uni@{self._group}/{self._id}"

    @property
    def anycast(self) -> str:
        return f"poll://any@{self._group}/{self._id}"

    def start(self) -> None:
        if not self._thread.is_alive():
            self._thread.start()

    def stop(self) -> None:
        # signal to consumer to disconnect
        self._messages.put(None)

        # TODO(avillega): Couldn't come up with a nice way of stoping this thread
        # iter_lines is blocking and request.get is also blocking, this makes it so
        # the only way to stop it is waiting for a timeout on the request itself
        # which could never happen.

        # This shutdown is only respected when the poller is instantiated with a timeout
        # value, which is not the default. This is still useful for tests.
        self._stopped = True

    def enqueue(self, mesg: Mesg) -> None:
        self._messages.put(mesg)

    def next(self) -> Mesg | None:
        return self._messages.get()

    @exit_on_exception
    def loop(self) -> None:
        delay = 5
        while not self._stopped:
            try:
                headers: dict[str, str] = {}
                auth = None
                if self._token:
                    headers["Authorization"] = f"Bearer {self._token}"
                elif self._auth:
                    auth = self._auth

                with requests.get(self.url, auth=auth, headers=headers, stream=True, timeout=self._timeout) as res:
                    res.raise_for_status()

                    for line in res.iter_lines(chunk_size=None, decode_unicode=True):
                        assert isinstance(line, str), "line must be a string"
                        if msg := self._process_line(line):
                            self._messages.put(msg)

            except requests.exceptions.Timeout:
                logger.warning("Networking. Cannot connect to %s. Retrying in %s sec.", self._url, delay)
                time.sleep(delay)
                continue
            except requests.exceptions.RequestException:
                logger.warning("Networking. Cannot connect to %s. Retrying in %s sec.", self._url, delay)
                time.sleep(delay)
                continue
            except Exception:
                logger.warning("Networking. Cannot connect to %s. Retrying in %s sec.", self._url, delay)
                time.sleep(delay)
                continue

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
