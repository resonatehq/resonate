from __future__ import annotations

from dataclasses import dataclass
from typing import final


@final
@dataclass(frozen=True)
class Exponential:
    delay: float = 1
    factor: float = 2
    max_delay: float = 30
    max_retries: int = -1

    def next(self, attempt: int) -> float | None:
        assert attempt >= 0, "attempt must be greater or equal than 0"
        if attempt > self.max_retries and self.max_retries >= 0:
            return None
        return min(self.delay * (self.factor**attempt), self.max_delay)
