from __future__ import annotations

import pytest

from resonate.retry import (
    Constant,
    Exponential,
    Linear,
    Never,
)


class TestExponential:
    def test_default_schedule_and_cutoff(self) -> None:
        policy = Exponential(delay=1, factor=2, max_delay=60, max_retries=5)

        assert policy.next(1) == 2
        assert policy.next(2) == 4
        assert policy.next(5) == 32
        assert policy.next(6) is None

    def test_caps_at_max_delay(self) -> None:
        policy = Exponential(delay=10, factor=3, max_delay=25, max_retries=5)

        assert policy.next(1) == 25
        assert policy.next(2) == 25
        assert policy.next(5) == 25
        assert policy.next(6) is None

    @pytest.mark.parametrize(
        ("attempt", "expected"),
        [
            (0, 1),
            (1, 2),
            (3, 8),
        ],
    )
    def test_returns_expected_delay_for_attempts_within_limit(
        self, attempt: int, expected: int
    ) -> None:

        policy = Exponential(delay=1, factor=2, max_delay=60, max_retries=5)
        assert policy.next(attempt) == expected


class TestLinear:
    @pytest.mark.parametrize(
        ("max_retries", "delay", "attempt", "expected"),
        [
            (3, 5, 0, 0),
            (3, 5, 1, 5),
            (3, 5, 2, 10),
            (3, 5, 3, 15),
        ],
    )
    def test_returns_multiples_up_to_max_retries(
        self, max_retries: int, delay: int, attempt: int, expected: int
    ) -> None:
        policy = Linear(max_retries=max_retries, delay=delay)
        assert policy.next(attempt) == expected

    def test_returns_none_after_max_retries(self) -> None:
        policy = Linear(max_retries=3, delay=5)

        assert policy.next(4) is None
        assert policy.next(10) is None


class TestConstant:
    @pytest.mark.parametrize(
        ("max_retries", "delay", "attempt"),
        [
            (0, 7, 0),
            (3, 7, 1),
            (3, 7, 3),
        ],
    )
    def test_returns_constant_delay_up_to_max_retries(
        self, max_retries: int, delay: int, attempt: int
    ) -> None:
        policy = Constant(max_retries=max_retries, delay=delay)
        assert policy.next(attempt) == delay

    def test_returns_none_after_max_retries(self) -> None:
        policy = Constant(max_retries=3, delay=7)

        assert policy.next(4) is None
        assert policy.next(100) is None


class TestNever:
    @pytest.mark.parametrize("attempt", [-1, 0, 1, 10, 999])
    def test_always_returns_none(self, attempt: int) -> None:
        assert Never().next(attempt) is None
