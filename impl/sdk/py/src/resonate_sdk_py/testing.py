from __future__ import annotations

import random

from typing_extensions import assert_never

from resonate_sdk_py.scheduler.dst import DSTScheduler


def dst(seeds: list[range | int]) -> list[DSTScheduler]:
    schedulers: list[DSTScheduler] = []
    for seed in seeds:
        if isinstance(seed, range):
            for _ in seed:
                seed_to_use = random.randint(0, 1_000)  # noqa: S311
                schedulers.append(DSTScheduler(seed=seed_to_use))
        elif isinstance(seed, int):
            schedulers.append(DSTScheduler(seed=seed))
        else:
            assert_never(seed)

    return schedulers
