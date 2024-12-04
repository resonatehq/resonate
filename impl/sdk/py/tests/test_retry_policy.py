from __future__ import annotations

from resonate.retry_policy import (
    calculate_total_possible_delay,
    constant,
    exponential,
    linear,
)


def test_infinite_retry() -> None:
    exponential_policy = exponential(base_delay=1, factor=2, max_retries=-1)
    constant_policy = constant(delay=1, max_retries=-1)
    linear_policy = linear(delay=1, max_retries=-1)
    for i in range(10, 1000, 10):
        assert exponential_policy.should_retry(i)
        assert constant_policy.should_retry(i)
        assert linear_policy.should_retry(i)


def test_exponential() -> None:
    policy = exponential(base_delay=1, factor=2, max_retries=3)
    expected_delays = (2, 4, 8)
    assert calculate_total_possible_delay(policy) == sum(expected_delays)
    assert policy.should_retry(attempt=1)
    assert policy.calculate_delay(attempt=1) == expected_delays[0]
    assert policy.should_retry(attempt=2)
    assert policy.calculate_delay(attempt=2) == expected_delays[1]
    assert policy.should_retry(attempt=3)
    assert policy.calculate_delay(attempt=3) == expected_delays[2]
    assert not policy.should_retry(attempt=4)


def test_linear() -> None:
    policy = linear(delay=2, max_retries=3)
    expected_delays = (2, 4, 6)
    assert calculate_total_possible_delay(policy) == sum(expected_delays)
    assert policy.should_retry(attempt=1)
    assert policy.calculate_delay(attempt=1) == expected_delays[0]
    assert policy.should_retry(attempt=2)
    assert policy.calculate_delay(attempt=2) == expected_delays[1]
    assert policy.should_retry(attempt=3)
    assert policy.calculate_delay(attempt=3) == expected_delays[2]
    assert not policy.should_retry(attempt=4)


def test_constant() -> None:
    policy = constant(delay=2, max_retries=3)
    expected_delays = (2, 2, 2)
    assert calculate_total_possible_delay(policy) == sum(expected_delays)
    assert policy.should_retry(attempt=1)
    assert policy.calculate_delay(attempt=1) == expected_delays[0]
    assert policy.should_retry(attempt=2)
    assert policy.calculate_delay(attempt=2) == expected_delays[1]
    assert policy.should_retry(attempt=3)
    assert policy.calculate_delay(attempt=3) == expected_delays[2]
    assert not policy.should_retry(attempt=4)
