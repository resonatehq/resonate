from __future__ import annotations

from dataclasses import dataclass
from typing import final


@final
@dataclass(frozen=True)
class Never:
    def next(self, attempt: int) -> float | None:
        assert attempt >= 0, "attempt must be greater than or equal to 0"
        return 0 if attempt == 0 else None
