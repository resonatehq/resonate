from __future__ import annotations

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resonate.typing import Data, Headers, Tags


class Command: ...


class DurablePromise:
    def __init__(
        self,
        id: str | None = None,
        data: Data = None,
        headers: Headers = None,
        tags: Tags = None,
        timeout: int = sys.maxsize,
    ) -> None:
        self.id = id
        self.data = data
        self.headers = headers
        self.tags = tags
        self.timeout = timeout
