from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import time
from concurrent.futures import TimeoutError
from functools import cache
from typing import TYPE_CHECKING, Any

import pytest

from resonate import scheduler
from resonate.commands import CreateDurablePromiseReq
from resonate.retry_policy import (
    Linear,
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


@pytest.mark.skip
@pytest.mark.parametrize("store", _promise_storages())
def test_coro_return_promise(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(
        processor_threads=1,
        durable_promise_storage=store,
    )
    s.register(bar, "bar-function", retry_policy=never())
    p: Promise[Promise[str]] = s.run("bar", bar, name="A", sleep_time=0.01)
    assert p.result(timeout=2) == "A"


@pytest.mark.parametrize("store", _promise_storages())
def test_scheduler(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(
        durable_promise_storage=store,
    )

    s.register(baz, "baz-function", retry_policy=never())
    s.register(foo, "foo-function", retry_policy=never())

    promise: Promise[str] = s.run("baz-1", baz, name="A", sleep_time=0.02)
    assert promise.result(timeout=4) == "A"

    promise = s.run("foo-1", foo, name="B", sleep_time=0.02)
    assert promise.result(timeout=4) == "B"


@pytest.mark.parametrize("store", _promise_storages())
def test_multithreading_capabilities(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(
        processor_threads=3,
        durable_promise_storage=store,
    )
    s.register(baz, "baz")

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
    assert time.time() - start <= 1


@pytest.mark.parametrize("store", _promise_storages())
def test_retry(store: IPromiseStore) -> None:
    tries = 0

    def _failing(ctx: Context, error: type[Exception]) -> None:  # noqa: ARG001, RUF100
        nonlocal tries
        tries += 1
        raise error

    def coro(
        ctx: Context, policy_info: dict[str, Any]
    ) -> Generator[Yieldable, Any, None]:
        policy = Linear(
            delay=policy_info["delay"], max_retries=policy_info["max_retries"]
        )
        yield ctx.lfc(_failing, error=NotImplementedError).with_options(
            durable=False, retry_policy=policy
        )

    s = scheduler.Scheduler(durable_promise_storage=store)
    policy = Linear(delay=0, max_retries=2)

    s.register(coro, "coro-func", retry_policy=never())

    p: Promise[None] = s.run("retry-coro", coro, dataclasses.asdict(policy))
    with pytest.raises(NotImplementedError):
        assert p.result()
    assert tries == 3  # noqa: PLR2004


@pytest.mark.parametrize("store", _promise_storages())
def test_structure_concurrency(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(durable_promise_storage=store)

    child_p: Promise[str] | None = None

    def coro_that_triggers_structure_concurrency(
        ctx: Context,
    ) -> Generator[Yieldable, Any, int]:
        nonlocal child_p
        p: Promise[str] = yield ctx.lfi(foo, name="a", sleep_time=0)
        child_p = p
        return 1

    s.register(
        coro_that_triggers_structure_concurrency,
        "coro-that-triggers-structured-concurrency",
        retry_policy=never(),
    )

    p: Promise[int] = s.run(
        "structure-concurrency",
        coro_that_triggers_structure_concurrency,
    )
    assert p.result()
    assert child_p is not None
    assert child_p.done()


@pytest.mark.parametrize("store", _promise_storages())
def test_structure_concurrency_with_failure(store: IPromiseStore) -> None:
    tries = 0

    def _failing(ctx: Context, error: type[Exception]) -> None:  # noqa: ARG001, RUF100
        nonlocal tries
        tries += 1
        raise error

    def coro_that_triggers_structure_concurrency_and_fails(
        ctx: Context,
    ) -> Generator[Yieldable, Any, int]:
        yield ctx.lfi(_failing, error=NotImplementedError).with_options(
            retry_policy=constant(delay=0.03, max_retries=2), durable=False
        )
        return 1

    s = scheduler.Scheduler(durable_promise_storage=store)

    s.register(
        coro_that_triggers_structure_concurrency_and_fails,
        "coro-structured-concurrency-and-failure",
        retry_policy=never(),
    )
    p: Promise[int] = s.run(
        "structure-concurrency-with-failure",
        coro_that_triggers_structure_concurrency_and_fails,
    )
    with pytest.raises(NotImplementedError):
        p.result()

    assert tries == 3  # noqa: PLR2004


@pytest.mark.parametrize("store", _promise_storages())
def test_structure_concurrency_with_multiple_failures(store: IPromiseStore) -> None:
    def _failing(ctx: Context, error: type[Exception]) -> None:  # noqa: ARG001, RUF100
        raise error

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

    s = scheduler.Scheduler(durable_promise_storage=store)

    s.register(
        coro_that_trigger_structure_concurrency_and_multiple_errors,
        "ss-and-multiple-errors",
        retry_policy=never(),
    )
    p: Promise[int] = s.run(
        "structure-concurrency-with-many-failure",
        coro_that_trigger_structure_concurrency_and_multiple_errors,
    )
    with pytest.raises(TypeError):
        p.result()


@pytest.mark.parametrize("store", _promise_storages())
def test_deferred_invoke(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(durable_promise_storage=store)

    deferred_p: Promise[str] | None = None

    def coro_with_deferred_invoke(ctx: Context) -> Generator[Yieldable, Any, int]:
        nonlocal deferred_p
        p: Promise[str] = yield ctx.deferred(
            "deferred-invoke", foo, name="A", sleep_time=0.02
        )
        deferred_p = p

        return 1

    s.register(coro_with_deferred_invoke, "coro-with-deferred-invoked")
    s.register(foo, "foo")

    p: Promise[int] = s.run("test-deferred-invoke", coro_with_deferred_invoke)
    assert p.result() == 1
    assert deferred_p is not None
    assert not deferred_p.done()

    def_p = s.run("deferred-invoke", foo, name="B", sleep_time=120)
    assert def_p.result() == "A"


def _fn_sleep(_ctx: Context, wait: float, result: str) -> str:
    time.sleep(wait)
    return result


def _fn_raise_or_ret(_ctx: Context, val: Any) -> Any:  # noqa: ANN401
    if isinstance(val, Exception):
        raise val

    return val


def race_coro(
    ctx: Context, waits_results: list[tuple[float, str]]
) -> Generator[Yieldable, Any, str]:
    ps = []
    for wt, result in waits_results:
        p = yield ctx.lfi(_fn_sleep, wait=wt, result=result)
        ps.append(p)

    p_race = yield ctx.race(ps)

    res = yield p_race
    return res


def all_coro(
    ctx: Context, waits_results: list[tuple[float, str]]
) -> Generator[Yieldable, Any, list[str]]:
    ps = []
    for wt, result in waits_results:
        p = yield ctx.lfi(_fn_sleep, wait=wt, result=result)
        ps.append(p)

    p_all = yield ctx.all(ps)

    res = yield p_all
    return res


def all_settled_coro(
    ctx: Context, vals: list[Any]
) -> Generator[Yieldable, Any, list[str]]:
    ps = []
    for val in vals:
        p = yield ctx.lfi(_fn_raise_or_ret, val=val).with_options(retry_policy=never())
        ps.append(p)

    p_all_settled = yield ctx.all_settled(ps)

    res = yield p_all_settled
    return res


@pytest.mark.parametrize("store", _promise_storages())
def test_all_combinator(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(durable_promise_storage=store)
    # Test case 1
    waits_results = [(0.02, "A"), (0.03, "B"), (0.01, "C"), (0.02, "D"), (0.02, "E")]
    expected = ["A", "B", "C", "D", "E"]
    s.register(all_coro, "all-coro")
    p_all: Promise[list[str]] = s.run(
        "all-coro-0", all_coro, waits_results=waits_results
    )
    assert p_all.result() == expected

    # Test case 2
    waits_results = [(0.05, "A"), (0.04, "B"), (0.03, "C"), (0.02, "D"), (0.01, "E")]
    expected = ["A", "B", "C", "D", "E"]
    p_all = s.run("all-coro-1", all_coro, waits_results=waits_results)
    assert p_all.result() == expected

    # Test case 3
    waits_results = [(0.01, "A"), (0.01, "B"), (0.01, "C")]
    expected = ["A", "B", "C"]
    p_all = s.run("all-coro-2", all_coro, waits_results=waits_results)
    assert p_all.result() == expected

    # Test case 4
    waits_results = [(0.1, "A")]
    expected = ["A"]
    p_all = s.run("all-coro-3", all_coro, waits_results=waits_results)
    assert p_all.result() == expected

    # Test case - empty list
    waits_results = []
    expected = []
    p_all = s.run("all-coro-4", all_coro, waits_results=waits_results)
    # failure of this test will most likely appear as a timeout
    assert p_all.result(timeout=1) == expected


@pytest.mark.parametrize("store", _promise_storages())
def test_all_settled_combinator(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(durable_promise_storage=store)
    # Test case 1
    vals = ["A", "B", "C", "D", "E"]
    expected = ["A", "B", "C", "D", "E"]
    s.register(all_settled_coro, "all-settled-coro")
    p_all_settled: Promise[list[Any]] = s.run(
        "all-settled-coro-0", all_settled_coro, vals=vals
    )
    assert p_all_settled.result() == expected

    # Test case 2
    vals = ["A"]
    expected = ["A"]
    p_all_settled = s.run("all-settled-coro-1", all_settled_coro, vals=vals)
    assert p_all_settled.result() == expected

    # Test case 3
    vals_err = ["A", ValueError()]
    expected_err = ["A", ValueError()]
    p_all_settled = s.run("all-settled-coro-2", all_settled_coro, vals=vals_err)
    for val, exp in zip(p_all_settled.result(timeout=1), expected_err):
        if isinstance(val, Exception):
            assert isinstance(val, type(exp))
        else:
            assert val == exp

    # Test case - empty list
    vals = []
    expected = []
    p_all_settled = s.run("all-settled-coro-3", all_settled_coro, vals=vals)
    # failure of this test will most likely appear as a timeout
    assert p_all_settled.result(timeout=1) == expected


@pytest.mark.parametrize("store", _promise_storages())
def test_race_combinator(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(durable_promise_storage=store, processor_threads=16)

    # Test case 1
    waits_results = [(0.03, "A"), (0.03, "B"), (0.01, "C"), (0.03, "D"), (0.03, "E")]
    expected = "C"
    s.register(race_coro, "race-coro", retry_policy=never())
    p_race: Promise[str] = s.run("race-coro-0", race_coro, waits_results=waits_results)
    assert p_race.result() == expected

    # Test case 2
    waits_results = [(0.05, "A"), (0.04, "B"), (0.03, "C"), (0.02, "D"), (0.01, "E")]
    expected = "E"
    p_race = s.run("race-coro-1", race_coro, waits_results=waits_results)
    assert p_race.result() == expected

    # # Test case 3
    waits_results = [(0.010, "A"), (0.02, "B"), (0.02, "C")]
    expected = "A"
    p_race = s.run("race-coro-2", race_coro, waits_results=waits_results)
    assert p_race.result() == expected

    # Test case 4
    waits_results = [(0.1, "A")]
    expected = "A"
    p_race = s.run("race-coro-3", race_coro, waits_results=waits_results)
    assert p_race.result() == expected

    # Test case 5
    waits_results = []
    p_race = s.run("race-coro-4", race_coro, waits_results=waits_results)
    with pytest.raises(AssertionError):
        p_race.result()


@pytest.mark.parametrize("store", _promise_storages())
def test_coro_retry(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(durable_promise_storage=store)

    tries = 0

    def _coro_to_retry(ctx: Context) -> Generator[Yieldable, Any, int]:
        nonlocal tries
        tries += 1
        yield ctx.lfc(foo, name="A", sleep_time=0)
        raise ValueError

    s.register(_coro_to_retry, "coro-to-retry", constant(delay=0, max_retries=3))
    p: Promise[int] = s.run("coro-to-retry.1", _coro_to_retry)
    with pytest.raises(ValueError):  # noqa: PT011
        p.result()

    assert tries == 4  # noqa: PLR2004


def factorial(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
    if n in (0, 1):
        return 1
    return n * (yield ctx.rfc(factorial, n - 1))


@pytest.mark.parametrize("store", _promise_storages())
def test_rfc(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(durable_promise_storage=store)
    s.register(factorial, tags={"a": "1"})
    p: Promise[int] = s.run("factorial-n", factorial, 4)
    with contextlib.suppress(TimeoutError):
        p.result(timeout=0.1)

    child_promise_record = store.get(promise_id="factorial-n.1")
    assert child_promise_record.promise_id == "factorial-n.1"
    assert child_promise_record.param is not None
    assert child_promise_record.param.data is not None
    data = json.loads(child_promise_record.param.data)
    assert data["func"] == "factorial"
    assert data["args"] == [3]
    assert child_promise_record.tags is not None
    assert child_promise_record.tags.keys() == {"a", "resonate:invoke"}


def _raw_rfc(ctx: Context) -> Generator[Yieldable, Any, None]:
    yield ctx.rfc(
        CreateDurablePromiseReq(
            promise_id="abc",
            data={
                "func": "func",
                "args": (1, 2),
            },
            tags={"demo": "test"},
        )
    )


@pytest.mark.parametrize("store", _promise_storages())
def test_rfc_raw(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(durable_promise_storage=store)
    s.register(_raw_rfc)
    p: Promise[None] = s.run("test-raw-rfc", _raw_rfc)
    with contextlib.suppress(TimeoutError):
        p.result(timeout=0.2)

    child_promise_record = store.get(promise_id="abc")
    assert child_promise_record.promise_id == "abc"
    assert child_promise_record.state == "PENDING"
    assert child_promise_record.tags == {"demo": "test"}
