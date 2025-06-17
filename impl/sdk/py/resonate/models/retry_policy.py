from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class RetryPolicy(Protocol):
    def next(self, attempt: int) -> float | None: ...
