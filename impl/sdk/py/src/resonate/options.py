from __future__ import annotations

from typing import final


@final
class Options:
    def __init__(
        self,
        *,
        id: str | None = None,
        durable: bool = True,
        send_to: str | None = None,
        version: int = 1,
    ) -> None:
        self.durable = durable
        self.id = id
        self.send_to = send_to
        self.version = version
