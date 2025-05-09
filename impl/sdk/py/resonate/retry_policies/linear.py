from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import final


@final
@dataclass(frozen=True)
class Linear:
    delay: float = 1
    max_retries: int = sys.maxsize

    def next(self, attempt: int) -> float | None:
        assert attempt >= 0, "attempt must be greater than or equal to 0"

        if attempt > self.max_retries:
            return None

        return self.delay * attempt
