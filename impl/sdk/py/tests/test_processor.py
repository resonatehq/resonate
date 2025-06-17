from __future__ import annotations

from queue import Queue

import pytest

from resonate.models.result import Ok, Result
from resonate.processor import Processor


def greet(name: str) -> str:
    return f"Hi {name}"


def callback(q: Queue[tuple[str, str]], expected: str, result: Result[str]) -> None:
    assert isinstance(result, Ok)
    q.put((result.value, expected))


@pytest.mark.parametrize("workers", [1, 2, 3])
def test_processor(workers: int) -> None:
    q = Queue[tuple[str, str]]()

    p = Processor(workers)
    assert len(p.threads) == workers

    names = ["A", "B"]
    expected_greet = [greet("A"), greet("B")]
    p.start()
    for name, expected in zip(names, expected_greet, strict=False):
        p.enqueue(lambda name=name: greet(name), lambda r, expected=expected: callback(q, expected, r))

    p.stop()
    assert q.qsize() == len(names)

    for _ in range(q.qsize()):
        actual, expected = q.get()
        assert actual == expected
        q.task_done()

    q.join()
    assert q.empty()
