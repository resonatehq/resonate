from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from typing_extensions import Concatenate, assert_never

from resonate.contants import ENV_VARIABLE_PIN_SEED
from resonate.scheduler.dst import DSTScheduler, Mode

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from resonate.context import Context


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
) -> list[DSTScheduler]:
    if log_file is not None:
        lf_path = Path().cwd() / log_file
        if not lf_path.exists() and not lf_path.is_file():
            lf_path.mkdir(exist_ok=False)
        for content in os.listdir(lf_path):
            c_path = lf_path / content
            if c_path.is_file():
                c_path.unlink()
            else:
                shutil.rmtree(c_path)

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
                )
            )
        else:
            assert_never(seed)

    return schedulers


def dsds(s: DSTScheduler) -> str:
    return f"{s.seed} cause the test to fail. Re run with `PIN_RANDOM_SEED={s.seed}`"
