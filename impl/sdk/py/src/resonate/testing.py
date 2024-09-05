from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Callable

from typing_extensions import ParamSpec, assert_never

from resonate import random
from resonate.contants import ENV_VARIABLE_PIN_SEED
from resonate.dst.scheduler import DSTScheduler, Mode
from resonate.storage import IPromiseStore, LocalPromiseStore

if TYPE_CHECKING:
    from resonate.dependency_injection import Dependencies
    from resonate.typing import DurableCoro, DurableFn, MockFn

P = ParamSpec("P")


def dst(  # noqa: PLR0913
    seeds: list[range | int],
    mocks: dict[DurableCoro[P, Any] | DurableFn[P, Any], MockFn[Any]] | None = None,
    log_file: str | None = None,
    mode: Mode = "concurrent",
    failure_chance: float = 0,
    max_failures: int = 0,
    probe: Callable[[Dependencies, int], Any] | None = None,
    assert_always: Callable[[Dependencies, int, int], Any] | None = None,
    assert_eventually: Callable[[Dependencies, int], Any] | None = None,
    durable_promise_storage: IPromiseStore | None = None,
) -> list[DSTScheduler]:
    def _new_dst_scheduler(seed: int) -> DSTScheduler:
        return DSTScheduler(
            random=random.Random(seed),
            mocks=mocks,
            mode=mode,
            failure_chance=failure_chance,
            max_failures=max_failures,
            log_file=log_file,
            probe=probe,
            assert_always=assert_always,
            assert_eventually=assert_eventually,
            durable_promise_storage=durable_promise_storage
            if durable_promise_storage
            else LocalPromiseStore(),
        )

    schedulers: list[DSTScheduler] = []
    pin_seed = os.environ.get(ENV_VARIABLE_PIN_SEED)
    if pin_seed is not None:
        seeds = [int(pin_seed)]

    for seed in seeds:
        if isinstance(seed, range):
            schedulers.extend(_new_dst_scheduler(i) for i in seed)
        elif isinstance(seed, int):
            schedulers.append(_new_dst_scheduler(seed))
        else:
            assert_never(seed)

    return schedulers
