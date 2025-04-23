from __future__ import annotations

from typing import Protocol


class RetryPolicy(Protocol):
    def next(self, attempt: int) -> float | None: ...
