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
        return self.base_delay * (self.factor**attempt)

    def should_retry(self, attempt: int) -> bool:
        return attempt <= self.max_retries


def exponential(base_delay: float, factor: float, max_retries: int) -> Exponential:
    return Exponential(base_delay=base_delay, factor=factor, max_retries=max_retries)


@final
@dataclass(frozen=True)
class Linear(Retriable):
    """A retry policy where the delay between retries grows linearly."""

    delay: float
    max_retries: int

    def calculate_delay(self, attempt: int) -> float:
        return self.delay * attempt

    def should_retry(self, attempt: int) -> bool:
        return attempt <= self.max_retries


def linear(delay: float, max_retries: int) -> Linear:
    return Linear(delay=delay, max_retries=max_retries)


@final
@dataclass(frozen=True)
class Constant(Retriable):
    """A retry policy where the delay between retries is constant."""

    delay: float
    max_retries: int

    def calculate_delay(self, attempt: int) -> float:
        _ = attempt
        return self.delay

    def should_retry(self, attempt: int) -> bool:
        return attempt <= self.max_retries


def constant(delay: float, max_retries: int) -> Constant:
    return Constant(delay=delay, max_retries=max_retries)


@final
@dataclass(frozen=True)
class Never:
    """A retry policy where there's no retry."""


def never() -> Never:
    return Never()


RetryPolicy: TypeAlias = Union[Exponential, Linear, Constant, Never]


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
