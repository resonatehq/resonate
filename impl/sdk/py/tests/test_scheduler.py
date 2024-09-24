from __future__ import annotations

import dataclasses
import os
import time
from functools import cache
from typing import TYPE_CHECKING, Any

import pytest
from resonate import scheduler
from resonate.retry_policy import (
    Linear,
    calculate_total_possible_delay,
    constant,
    never,
)
from resonate.storage import (
    IPromiseStore,
    LocalPromiseStore,
    MemoryStorage,
    RemotePromiseStore,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.promise import Promise
    from resonate.typing import Yieldable


def foo(ctx: Context, name: str, sleep_time: float) -> str:  # noqa: ARG001
    time.sleep(sleep_time)
    return name


def baz(ctx: Context, name: str, sleep_time: float) -> Generator[Yieldable, Any, str]:
    p = yield ctx.lfi(foo, name=name, sleep_time=sleep_time).with_options(
        retry_policy=never()
    )
    return (yield p)


def bar(
    ctx: Context, name: str, sleep_time: float
) -> Generator[Yieldable, Any, Promise[str]]:
    p: Promise[str] = yield ctx.lfi(foo, name=name, sleep_time=sleep_time).with_options(
        retry_policy=never()
    )
    return p


@cache
def _promise_storages() -> list[IPromiseStore]:
    stores: list[IPromiseStore] = [LocalPromiseStore(MemoryStorage())]
    if os.getenv("RESONATE_STORE_URL") is not None:
        stores.append(RemotePromiseStore(url=os.environ["RESONATE_STORE_URL"]))
    return stores


@pytest.mark.skip()
@pytest.mark.parametrize("store", _promise_storages())
def test_coro_return_promise(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(
        processor_threads=1,
        durable_promise_storage=store,
    )
    p: Promise[Promise[str]] = s.with_options(retry_policy=never()).run(
        "bar", bar, name="A", sleep_time=0.01
    )
    assert p.result(timeout=2) == "A"


@pytest.mark.parametrize("store", _promise_storages())
def test_scheduler(store: IPromiseStore) -> None:
    p = scheduler.Scheduler(
        durable_promise_storage=store,
    )

    promise: Promise[str] = p.run("baz-1", baz, name="A", sleep_time=0.02)
    assert promise.result(timeout=4) == "A"

    promise = p.run("foo-1", foo, name="B", sleep_time=0.02)
    assert promise.result(timeout=4) == "B"


@pytest.mark.parametrize("store", _promise_storages())
def test_multithreading_capabilities(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(
        processor_threads=3,
        durable_promise_storage=store,
    )

    time_per_process: float = 0.5
    start = time.time()
    p1: Promise[str] = s.run(
        "multi-threaded-1",
        baz,
        name="A",
        sleep_time=time_per_process,
    )
    p2: Promise[str] = s.run(
        "multi-threaded-2",
        baz,
        name="B",
        sleep_time=time_per_process,
    )
    p3: Promise[str] = s.run(
        "multi-threaded-3",
        baz,
        name="C",
        sleep_time=time_per_process,
    )

    assert p1.result() == "A"
    assert p2.result() == "B"
    assert p3.result() == "C"
    total_time = time.time() - start
    assert total_time == pytest.approx(
        time_per_process, rel=1e-1
    ), f"I should have taken about {time_per_process} seconds to process all coroutines"


def sleep_coroutine(
    ctx: Context, sleep_time: int, name: str
) -> Generator[Yieldable, Any, str]:
    yield ctx.sleep(sleep_time)
    return name


@pytest.mark.skip()
@pytest.mark.parametrize("store", _promise_storages())
def test_sleep_on_coroutines(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(
        processor_threads=1,
        durable_promise_storage=store,
    )
    start = time.time()
    sleep_time = 4
    p1: Promise[str] = s.run(
        "sleeping-coro-1",
        sleep_coroutine,
        sleep_time=sleep_time,
        name="A",
    )
    p2: Promise[str] = s.run(
        "sleeping-coro-2",
        sleep_coroutine,
        sleep_time=sleep_time,
        name="B",
    )
    p3: Promise[str] = s.run(
        "sleeping-coro-3",
        sleep_coroutine,
        sleep_time=sleep_time,
        name="C",
    )
    assert p1.result() == "A"
    assert p2.result() == "B"
    assert p3.result() == "C"

    assert time.time() - start == pytest.approx(
        sleep_time, rel=1e-1
    ), f"I should have taken about {sleep_time} seconds to process all coroutines"


def _failing(ctx: Context, error: type[Exception]) -> None:  # noqa: ARG001, RUF100
    raise error


def coro(ctx: Context, policy_info: dict[str, Any]) -> Generator[Yieldable, Any, None]:
    policy = Linear(delay=policy_info["delay"], max_retries=policy_info["max_retries"])
    yield ctx.lfc(_failing, error=NotImplementedError).with_options(
        durable=False, retry_policy=policy
    )


@pytest.mark.parametrize("store", _promise_storages())
def test_retry(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(durable_promise_storage=store)
    policy = Linear(delay=0.005, max_retries=2)

    start = time.time()
    p: Promise[None] = s.with_options(retry_policy=never()).run(
        "retry-coro", coro, dataclasses.asdict(policy)
    )
    with pytest.raises(NotImplementedError):
        assert p.result()

    total_possible_delay = calculate_total_possible_delay(policy)
    assert (
        time.time() - start <= total_possible_delay + 0.1
    ), f"It should have taken about {total_possible_delay} + 0.1 secs (for API calling) to finish"  # noqa: E501


def coro_that_triggers_structure_concurrency(
    ctx: Context,
) -> Generator[Yieldable, Any, int]:
    yield ctx.lfi(foo, name="a", sleep_time=0.3)
    return 1


@pytest.mark.parametrize("store", _promise_storages())
def test_structure_concurrency(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(durable_promise_storage=store)
    start = time.time()
    p: Promise[int] = s.with_options(retry_policy=never()).run(
        "structure-concurrency", coro_that_triggers_structure_concurrency
    )
    assert p.result() == 1
    max_expected_delay = 0.3 + 0.1
    assert (
        time.time() - start <= max_expected_delay
    ), f"It should have taken about {max_expected_delay} to finish"


def coro_that_triggers_structure_concurrency_and_fails(
    ctx: Context,
) -> Generator[Yieldable, Any, int]:
    yield ctx.lfi(_failing, error=NotImplementedError).with_options(
        retry_policy=constant(delay=0.03, max_retries=2), durable=False
    )
    return 1


@pytest.mark.parametrize("store", _promise_storages())
def test_structure_concurrency_with_failure(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(durable_promise_storage=store)
    p: Promise[int] = s.with_options(retry_policy=never()).run(
        "structure-concurrency-with-failure",
        coro_that_triggers_structure_concurrency_and_fails,
    )
    with pytest.raises(NotImplementedError):
        p.result()


def coro_that_trigger_structure_concurrency_and_multiple_errors(
    ctx: Context,
) -> Generator[Yieldable, Any, int]:
    yield ctx.lfi(_failing, error=TypeError).with_options(
        retry_policy=never(), durable=False
    )
    yield ctx.lfi(_failing, error=NotImplementedError).with_options(
        durable=False, retry_policy=never()
    )
    return 1


@pytest.mark.parametrize("store", _promise_storages())
def test_structure_concurrency_with_multiple_failures(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(durable_promise_storage=store)
    p: Promise[int] = s.with_options(retry_policy=never()).run(
        "structure-concurrency-with-many-failure",
        coro_that_trigger_structure_concurrency_and_multiple_errors,
    )
    with pytest.raises(TypeError):
        p.result()
