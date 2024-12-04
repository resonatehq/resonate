from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Union, final

from typing_extensions import TypeAlias, assert_never


class Retriable(ABC):
    @abstractmethod
    def calculate_delay(self, attempt: int) -> float: ...

    @abstractmethod
    def should_retry(self, attempt: int) -> bool: ...


@final
@dataclass(frozen=True)
class Exponential(Retriable):
    """A retry policy where the delay between retries grows exponentially."""

    base_delay: float
    factor: float
    max_retries: int

    def calculate_delay(self, attempt: int) -> float:
        assert attempt > 0, "Attempt must be positive."
        return self.base_delay * (self.factor**attempt)

    def should_retry(self, attempt: int) -> bool:
        assert attempt > 0, "Attempt must be positive."
        if self.max_retries < 0:
            return True
        return attempt <= self.max_retries


@final
@dataclass(frozen=True)
class Linear(Retriable):
    """A retry policy where the delay between retries grows linearly."""

    delay: float
    max_retries: int

    def calculate_delay(self, attempt: int) -> float:
        assert attempt > 0, "Attempt must be positive."
        return self.delay * attempt

    def should_retry(self, attempt: int) -> bool:
        assert attempt > 0, "Attempt must be positive."
        if self.max_retries < 0:
            return True
        return attempt <= self.max_retries


@final
@dataclass(frozen=True)
class Constant(Retriable):
    """A retry policy where the delay between retries is constant."""

    delay: float
    max_retries: int

    def calculate_delay(self, attempt: int) -> float:
        assert attempt > 0, "Attempt must be positive."
        return self.delay

    def should_retry(self, attempt: int) -> bool:
        assert attempt > 0, "Attempt must be positive."
        if self.max_retries < 0:
            return True
        return attempt <= self.max_retries


@final
@dataclass(frozen=True)
class Never:
    """A retry policy where there's no retry."""


RetryPolicy: TypeAlias = Union[Exponential, Linear, Constant, Never]


def linear(delay: float, max_retries: int) -> Linear:
    return Linear(delay=delay, max_retries=max_retries)


def exponential(base_delay: float, factor: float, max_retries: int) -> Exponential:
    return Exponential(base_delay=base_delay, factor=factor, max_retries=max_retries)


def constant(delay: float, max_retries: int) -> Constant:
    return Constant(delay=delay, max_retries=max_retries)


def never() -> Never:
    return Never()


def default_policy() -> RetryPolicy:
    return exponential(base_delay=1, factor=2, max_retries=10)


def calculate_total_possible_delay(policy: Exponential | Linear | Constant) -> float:
    total_possible_delay: float
    if isinstance(policy, Exponential):
        # Geometric series sum formula
        total_possible_delay = policy.base_delay * (
            (policy.factor * (1 - policy.factor**policy.max_retries))
            / (1 - policy.factor)
        )
    elif isinstance(policy, Linear):
        total_possible_delay = (
            policy.delay * (policy.max_retries * (policy.max_retries + 1)) / 2
        )
    elif isinstance(policy, Constant):
        total_possible_delay = policy.delay * policy.max_retries
    else:
        assert_never(policy)
    return total_possible_delay
