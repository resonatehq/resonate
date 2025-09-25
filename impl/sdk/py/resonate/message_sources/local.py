from __future__ import annotations

import queue
from typing import TYPE_CHECKING

from resonate.models.message import Mesg

if TYPE_CHECKING:
    from resonate.stores import LocalStore


class LocalMessageSource:
    def __init__(self, store: LocalStore, group: str, id: str, scheme: str = "local") -> None:
        self._messages = queue.Queue[Mesg | None]()
        self._store = store
        self._scheme = scheme
        self._group = group
        self._id = id

    @property
    def group(self) -> str:
        return self._group

    @property
    def id(self) -> str:
        return self._id

    @property
    def unicast(self) -> str:
        return f"{self._scheme}://uni@{self._group}/{self._id}"

    @property
    def anycast(self) -> str:
        return f"{self._scheme}://any@{self._group}/{self._id}"

    def start(self) -> None:
        # idempotently connect to the store
        self._store.connect(self)

        # idempotently start the store
        self._store.start()

    def stop(self) -> None:
        # disconnect from the store
        self._store.disconnect(self)

        # signal to consumers to disconnect
        self._messages.put(None)

    def enqueue(self, mesg: Mesg) -> None:
        self._messages.put(mesg)

    def next(self) -> Mesg | None:
        return self._messages.get()
