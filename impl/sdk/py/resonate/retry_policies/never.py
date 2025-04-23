from __future__ import annotations

from dataclasses import dataclass
from typing import final


@final
@dataclass(frozen=True)
class Never:
    def next(self, attempt: int) -> float | None:
        assert attempt >= 0, "attempt must be greater or equal than 0"
        return None
