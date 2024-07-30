from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Callable

from typing_extensions import Concatenate, assert_never

from resonate.contants import ENV_VARIABLE_PIN_SEED
from resonate.dst.scheduler import DSTScheduler, Mode

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from resonate.context import Context
    from resonate.dependency_injection import Dependencies


def dst(  # noqa: PLR0913
    seeds: list[range | int],
    mocks: dict[
        Callable[Concatenate[Context, ...], Any | Coroutine[None, None, Any]],
        Callable[[], Any],
    ]
    | None = None,
    log_file: str | None = None,
    mode: Mode = "concurrent",
    failure_chance: float = 0,
    max_failures: int = 0,
    probe: Callable[[Dependencies, int], Any] | None = None,
) -> list[DSTScheduler]:
    schedulers: list[DSTScheduler] = []

    pin_seed = os.environ.get(ENV_VARIABLE_PIN_SEED)
    if pin_seed is not None:
        seeds = [int(pin_seed)]

    for seed in seeds:
        if isinstance(seed, range):
            schedulers.extend(
                DSTScheduler(
                    i,
                    mocks=mocks,
                    mode=mode,
                    failure_chance=failure_chance,
                    max_failures=max_failures,
                    log_file=log_file,
                    probe=probe,
                )
                for i in seed
            )
        elif isinstance(seed, int):
            schedulers.append(
                DSTScheduler(
                    seed=seed,
                    mocks=mocks,
                    mode=mode,
                    failure_chance=failure_chance,
                    max_failures=max_failures,
                    log_file=log_file,
                    probe=probe,
                )
            )
        else:
            assert_never(seed)

    return schedulers


def dsds(s: DSTScheduler) -> str:
    return f"{s.seed} cause the test to fail. Re run with `PIN_RANDOM_SEED={s.seed}`"
