from __future__ import annotations

from resonate_sdk_py.scheduler import Scheduler, foo


def test_exploration() -> None:
    scheduler = Scheduler()
    scheduler.add(foo)
    assert scheduler.run() == "Hi tomas 10 15"
