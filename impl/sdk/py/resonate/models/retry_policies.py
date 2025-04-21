from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, final


class Retriable(Protocol):
    def next(self, attempt: int) -> float | None: ...


type RetryPolicy = Exponential | Linear | Constant | Never


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


@final
@dataclass(frozen=True)
class Linear:
    delay: float = 1
    max_retries: int = -1

    def next(self, attempt: int) -> float | None:
        assert attempt >= 0, "attempt must be greater or equal than 0"
        if attempt > self.max_retries and self.max_retries >= 0:
            return None
        return self.delay * attempt


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


@final
@dataclass(frozen=True)
class Never:
    def next(self, attempt: int) -> float | None:
        assert attempt >= 0, "attempt must be greater or equal than 0"
        return None
