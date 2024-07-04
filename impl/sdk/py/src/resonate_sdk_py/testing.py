from __future__ import annotations

import random

from typing_extensions import assert_never

from resonate_sdk_py.scheduler.dst import DSTScheduler


def dst(seed: range | int) -> list[DSTScheduler]:
    schedulers: list[DSTScheduler] = []
    if isinstance(seed, range):
        for _ in seed:
            seed_to_use = int(random.random())  # noqa: S311
            schedulers.append(DSTScheduler(seed=seed_to_use))
    elif isinstance(seed, int):
        schedulers.append(DSTScheduler(seed=seed))
    else:
        assert_never(seed)

    return schedulers
