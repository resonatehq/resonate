from __future__ import annotations

from typing_extensions import assert_never

from resonate.scheduler.dst import DSTScheduler


def dst(seeds: list[range | int]) -> list[DSTScheduler]:
    schedulers: list[DSTScheduler] = []
    for seed in seeds:
        if isinstance(seed, range):
            schedulers.extend(DSTScheduler(i) for i in seed)
        elif isinstance(seed, int):
            schedulers.append(DSTScheduler(seed=seed))
        else:
            assert_never(seed)

    return schedulers
