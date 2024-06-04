from __future__ import annotations

from typing import TYPE_CHECKING

from resonate_sdk_py.resonate import Scheduler
from resonate_sdk_py.scheduler import foo

if TYPE_CHECKING:
    from collections.abc import Generator


def test_a_function() -> None:
    assert True


def echo_round() -> Generator[int, float, str]:
    sent = yield 0
    while sent >= 0:
        sent = yield round(sent)
    return "Done"


def test_generator() -> None:
    scheduler = Scheduler()
    scheduler.add(foo)
    scheduler.run()
