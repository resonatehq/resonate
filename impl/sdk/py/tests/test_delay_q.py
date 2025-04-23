from __future__ import annotations

from resonate.delay_q import DelayQ


def test_delay_queue() -> None:
    dq = DelayQ[int]()
    dq.add(1, 10)
    dq.add(1, 10)
    dq.add(1, 10)
    assert dq.get(11)[0] == [1, 1, 1]
    assert dq.get(11)[0] == []
    dq.add(1, 2)
    dq.add(2, 1)
    item, next_time = dq.get(1)
    assert item == [2]
    assert next_time == 2

    item, next_time = dq.get(2)
    assert item == [1]
    assert next_time == 0

    dq.add(1, 2)
    dq.add(1, 2)
    dq.add(1, 2)
    assert dq.get(2)[0] == [1, 1, 1]
