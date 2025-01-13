from __future__ import annotations

from resonate.cmd_queue import CommandQ, Invoke
from resonate.delay_queue import DelayQueue


def test_delay_queue() -> None:
    queue = DelayQueue()
    cmd_queue = CommandQ()
    queue.start(cmd_queue, "")
    queue.enqueue(item=Invoke("3"), delay=0.002)
    assert cmd_queue.get() == Invoke("3")
    cmd_queue.task_done()
    queue.enqueue(item=Invoke("10"), delay=0.01)
    queue.enqueue(item=Invoke("2"), delay=0.002)
    assert cmd_queue.get() == Invoke("2")
    cmd_queue.task_done()
    assert cmd_queue.get() == Invoke("10")
    cmd_queue.task_done()
    queue.stop()
