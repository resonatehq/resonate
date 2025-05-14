from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from resonate.utils import exit_on_exception

if TYPE_CHECKING:
    from resonate.models.message import Mesg
    from resonate.models.message_source import MessageQ
    from resonate.stores import LocalStore


class LocalMessageSource:
    def __init__(self, group: str, id: str, store: LocalStore) -> None:
        self._group = group
        self._id = id
        self._store = store
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    @property
    def unicast(self) -> str:
        return f"poll://{self._group}/{self._id}"

    @property
    def anycast(self) -> str:
        return f"poll://{self._group}/{self._id}"

    def start(self, mq: MessageQ) -> None:
        if self._thread is not None:
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            args=(mq,),
            name="local_msg_source",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        if self._thread is not None:
            self._stop_event.set()
            self._thread.join()
            self._thread = None
            self._stop_event.clear()

    @exit_on_exception("mesg_source.local")
    def _loop(self, mq: MessageQ) -> None:
        while not self._stop_event.is_set():
            for msg in self.step():
                mq.enqueue(msg)
            self._stop_event.wait(0.1)

    def step(self) -> list[Mesg]:
        return [m for _, m in self._store.step()]
