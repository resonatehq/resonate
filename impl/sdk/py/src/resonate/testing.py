from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Callable

from typing_extensions import Concatenate, assert_never

from resonate.contants import ENV_VARIABLE_PIN_SEED
from resonate.scheduler.dst import DSTScheduler

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from resonate.context import Context


def dst(
    seeds: list[range | int],
    mocks: dict[
        Callable[Concatenate[Context, ...], Any | Coroutine[None, None, Any]],
        Callable[[], Any],
    ]
    | None = None,
) -> list[DSTScheduler]:
    schedulers: list[DSTScheduler] = []

    pin_seed = os.environ.get(ENV_VARIABLE_PIN_SEED)
    if pin_seed is not None:
        seeds = [int(pin_seed)]

    for seed in seeds:
        if isinstance(seed, range):
            schedulers.extend(DSTScheduler(i, mocks=mocks) for i in seed)
        elif isinstance(seed, int):
            schedulers.append(DSTScheduler(seed=seed, mocks=mocks))
        else:
            assert_never(seed)

    return schedulers


def dsds(s: DSTScheduler) -> str:
    return f"{s.seed} cause the test to fail. Re run with `PIN_RANDOM_SEED={s.seed}`"
