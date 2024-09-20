from __future__ import annotations

from resonate.queue import DelayQueue


def test_delay_queue() -> None:
    queue = DelayQueue[int](caller_event=None)
    queue.put_nowait(item=3, delay=0.002)
    assert queue.dequeue() == 3  # noqa: PLR2004
    assert queue.items_in_delay() == 0
    queue.put_nowait(item=10, delay=0.01)
    queue.put_nowait(item=3, delay=0.002)
    assert queue.dequeue() == 3  # noqa: PLR2004
    assert queue.items_in_delay() == 1
    assert queue.dequeue() == 10  # noqa: PLR2004
    assert queue.items_in_delay() == 0
