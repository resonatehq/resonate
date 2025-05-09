from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import final


@final
@dataclass(frozen=True)
class Exponential:
    delay: float = 1
    max_retries: int = sys.maxsize
    factor: float = 2
    max_delay: float = 30

    def next(self, attempt: int) -> float | None:
        assert attempt >= 0, "attempt must be greater than or equal to 0"

        if attempt > self.max_retries:
            return None

        if attempt == 0:
            return 0

        return min(self.delay * (self.factor**attempt), self.max_delay)
