from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from resonate.options import Options


@dataclass
class Sleep:
    secs: int
    opts: Options = field(default_factory=Options)

    def format(self) -> tuple[Any, dict[str, str], int, dict[str, str] | None]:
        return (None, {"resonate:timeout": "true"}, int((time.time() + self.secs) * 1000), None)

    def options(self, send_to: str | None, tags: dict[str, str] | None, timeout: int | None, version: int | None) -> None:
        return
