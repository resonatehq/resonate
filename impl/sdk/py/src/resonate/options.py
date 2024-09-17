from __future__ import annotations

from dataclasses import dataclass, field
from typing import final


@final
@dataclass(frozen=True)
class Options:
    durable: bool = field(default=True)
    promise_id: str | None = None
