from __future__ import annotations

from dataclasses import dataclass
from typing import final


@final
@dataclass(frozen=True)
class Constant:
    delay: float = 1
    max_retries: int = -1

    def next(self, attempt: int) -> float | None:
        assert attempt >= 0, "attempt must be greater or equal than 0"
        if attempt > self.max_retries and self.max_retries >= 0:
            return None
        return self.delay
