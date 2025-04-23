from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from resonate.retry_policies import Constant, Exponential, Linear, Never

if TYPE_CHECKING:
    from resonate.models.retry_policy import RetryPolicy


@pytest.mark.parametrize(
    ("policy", "progression"),
    [
        (Never(), None),
        (
            Constant(delay=1, max_retries=2),
            [1, 1, None],
        ),
        (
            Linear(delay=1, max_retries=2),
            [1, 2, None],
        ),
        (
            Exponential(delay=1, factor=2, max_retries=5, max_delay=8),
            [2, 4, 8, 8, 8, None],
        ),
    ],
)
def test_delay_progression(policy: RetryPolicy, progression: list[float | None] | None) -> None:
    if isinstance(policy, Never):
        return

    i: int = 1
    delays: list[float | None] = []
    while True:
        delays.append(policy.next(i))
        i += 1
        if delays[-1] is None:
            break

    assert delays == progression
