from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from resonate.models.message import Mesg


class MessageSource(Protocol):
    def start(self, cq: MessageQ, pid: str) -> None: ...
    def stop(self) -> None: ...


class MessageQ(Protocol):
    def enqueue(self, mesg: Mesg) -> None: ...
