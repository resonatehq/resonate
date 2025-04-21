from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from resonate.models.enqueueable import Enqueueable
    from resonate.models.message import Mesg


class MessageSource(Protocol):
    def start(self, cq: Enqueueable[Mesg], pid: str) -> None: ...
    def stop(self) -> None: ...
