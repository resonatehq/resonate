from __future__ import annotations

import dataclasses
import os
import time
from functools import cache
from typing import TYPE_CHECKING, Any

import pytest

from resonate import scheduler
from resonate.commands import Command, CreateDurablePromiseReq
from resonate.result import Ok
from resonate.retry_policy import (
    Linear,
    constant,
    never,
)
from resonate.storage.local_store import (
    LocalStore,
    MemoryStorage,
)
from resonate.storage.resonate_server import RemoteServer

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.promise import Promise
    from resonate.storage.traits import IPromiseStore
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
    stores: list[IPromiseStore] = [LocalStore(MemoryStorage())]
    if os.getenv("RESONATE_STORE_URL") is not None:
        stores.append(RemoteServer(url=os.environ["RESONATE_STORE_URL"]))
    return stores


@pytest.mark.parametrize("store", _promise_storages())
def test_scheduler(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(
        durable_promise_storage=store,
    )

    s.register(baz, "baz-function", retry_policy=never())
    s.register(foo, "foo-function", retry_policy=never())

    promise: Promise[str] = s.run("baz-1", baz, name="A", sleep_time=0.02)
    s.wait_until_blocked()
    assert promise.done()
    assert promise.result() == "A"

    promise = s.run("foo-1", foo, name="B", sleep_time=0.02)
    s.wait_until_blocked()
    assert promise.done()
    assert promise.result() == "B"


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
    s.wait_until_blocked()
    assert p1.done()
    assert p2.done()
    assert p2.done()
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
        yield ctx.lfc(_failing, error=RuntimeError).with_options(
            durable=False, retry_policy=policy
        )

    s = scheduler.Scheduler(durable_promise_storage=store)
    policy = Linear(delay=0, max_retries=2)

    s.register(coro, "coro-func", retry_policy=never())

    p: Promise[None] = s.run("retry-coro", coro, dataclasses.asdict(policy))
    s.wait_until_blocked()
    assert p.done()
    with pytest.raises(RuntimeError):
        assert p.result()
    assert tries == 3  # noqa: PLR2004


@pytest.mark.parametrize("store", _promise_storages())
def test_async_fibonacci(store: IPromiseStore) -> None:
    def fibonacci(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n
        p1: Promise[Any] = yield ctx.lfi(fibonacci, n - 1).with_options(
            promise_id=f"async-fibonacci-{n-1}"
        )
        p2: Promise[Any] = yield ctx.lfi(fibonacci, n - 2).with_options(
            promise_id=f"async-fibonacci-{n-2}"
        )
        v2 = yield p2
        v1 = yield p1
        return v1 + v2

    s = scheduler.Scheduler(durable_promise_storage=store)
    s.register(fibonacci, "async-fibonacci")
    n = 40
    p: Promise[int] = s.run(f"async-fibonacci-{n}", fibonacci, n)
    s.wait_until_blocked()
    assert p.result() == 102334155  # noqa: PLR2004


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
    s.wait_until_blocked()
    assert p.done()
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
        yield ctx.lfi(_failing, error=RuntimeError).with_options(
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
    s.wait_until_blocked()
    assert p.done()
    with pytest.raises(RuntimeError):
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
        yield ctx.lfi(_failing, error=RuntimeError).with_options(
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
    s.wait_until_blocked()
    assert p.done()
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
    s.wait_until_blocked()
    assert p.done()
    assert p.result() == 1
    assert deferred_p is not None
    assert deferred_p.done()

    def_p = s.run("deferred-invoke", foo, name="B", sleep_time=120)
    assert def_p.done()
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
    s.wait_until_blocked()
    assert p_all.done()
    assert p_all.result() == expected

    # Test case 2
    waits_results = [(0.05, "A"), (0.04, "B"), (0.03, "C"), (0.02, "D"), (0.01, "E")]
    expected = ["A", "B", "C", "D", "E"]
    p_all = s.run("all-coro-1", all_coro, waits_results=waits_results)
    s.wait_until_blocked()
    assert p_all.done()
    assert p_all.result() == expected

    # Test case 3
    waits_results = [(0.01, "A"), (0.01, "B"), (0.01, "C")]
    expected = ["A", "B", "C"]
    p_all = s.run("all-coro-2", all_coro, waits_results=waits_results)
    s.wait_until_blocked()
    assert p_all.done()
    assert p_all.result() == expected

    # Test case 4
    waits_results = [(0.1, "A")]
    expected = ["A"]
    p_all = s.run("all-coro-3", all_coro, waits_results=waits_results)
    s.wait_until_blocked()
    assert p_all.done()
    assert p_all.result() == expected

    # Test case - empty list
    waits_results = []
    expected = []
    p_all = s.run("all-coro-4", all_coro, waits_results=waits_results)
    s.wait_until_blocked()
    assert p_all.done()
    assert p_all.result() == expected


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
    s.wait_until_blocked()
    assert p_all_settled.done()
    assert p_all_settled.result() == expected

    # Test case 2
    vals = ["A"]
    expected = ["A"]
    p_all_settled = s.run("all-settled-coro-1", all_settled_coro, vals=vals)
    s.wait_until_blocked()
    assert p_all_settled.done()
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
    s.wait_until_blocked()
    assert p_all_settled.done()
    assert p_all_settled.result() == expected


@pytest.mark.parametrize("store", _promise_storages())
def test_race_combinator(store: IPromiseStore) -> None:
    s = scheduler.Scheduler(durable_promise_storage=store, processor_threads=16)

    # Test case 1
    waits_results = [(0.1, "A"), (0.1, "B"), (0.01, "C"), (0.1, "D"), (0.1, "E")]
    expected = "C"
    s.register(race_coro, "race-coro", retry_policy=never())
    p_race: Promise[str] = s.run("race-coro-0", race_coro, waits_results=waits_results)
    s.wait_until_blocked()
    assert p_race.done()
    assert p_race.result() == expected

    # Test case 2
    waits_results = [(0.1, "A"), (0.1, "B"), (0.1, "C"), (0.1, "D"), (0.01, "E")]
    expected = "E"
    p_race = s.run("race-coro-1", race_coro, waits_results=waits_results)
    s.wait_until_blocked()
    assert p_race.done()
    assert p_race.result() == expected

    # # Test case 3
    waits_results = [(0.01, "A"), (0.1, "B"), (0.1, "C")]
    expected = "A"
    p_race = s.run("race-coro-2", race_coro, waits_results=waits_results)
    s.wait_until_blocked()
    assert p_race.done()
    assert p_race.result() == expected

    # Test case 4
    waits_results = [(0.1, "A")]
    expected = "A"
    p_race = s.run("race-coro-3", race_coro, waits_results=waits_results)
    s.wait_until_blocked()
    assert p_race.done()
    assert p_race.result() == expected

    # Test case 5
    waits_results = []
    p_race = s.run("race-coro-4", race_coro, waits_results=waits_results)
    s.wait_until_blocked()
    assert p_race.done()
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
    s.wait_until_blocked()
    assert p.done()
    with pytest.raises(ValueError):  # noqa: PT011
        p.result()

    assert tries == 4  # noqa: PLR2004


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


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_rfc_raw() -> None:
    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])

    s = scheduler.Scheduler(durable_promise_storage=store)
    s.register(_raw_rfc)
    p: Promise[None] = s.run("test-raw-rfc", _raw_rfc)
    s.wait_until_blocked()
    assert not p.done()

    child_promise_record = store.get(promise_id="abc")
    assert child_promise_record.promise_id == "abc"
    assert child_promise_record.state == "PENDING"
    assert child_promise_record.tags == {"demo": "test"}


@pytest.mark.parametrize("store", _promise_storages())
def test_dedup(store: IPromiseStore) -> None:
    def factorial(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return 1

        p: Promise[int] = yield ctx.lfi(factorial, n - 1).with_options(
            promise_id=f"factorial-dedup-{n-1}", retry_policy=never()
        )
        return n * (yield p)

    s = scheduler.Scheduler(durable_promise_storage=store)
    s.register(factorial, retry_policy=never())
    n = 5
    p1: Promise[int] = s.run(f"factorial-dedup-{n}", factorial, n)
    p2: Promise[int] = s.run(f"factorial-dedup-{n}", factorial, n)
    s.wait_until_blocked()
    assert p1.done()
    assert p1 == p2
    assert p2.result() == 120  # noqa: PLR2004


@pytest.mark.parametrize("store", _promise_storages())
def test_batching(store: IPromiseStore) -> None:
    @dataclasses.dataclass(frozen=True)
    class GreetCommand(Command):
        name: str

    def command_handler(ctx: Context, cmds: list[GreetCommand]) -> list[str]:  # noqa: ARG001
        return [cmd.name for cmd in cmds]

    def greet(ctx: Context, name: str) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.lfi(GreetCommand(name=name))
        v: str = yield p
        return v

    s = scheduler.Scheduler(store)
    s.register_command_handler(GreetCommand, command_handler, maxlen=10)
    s.register(greet)

    characters = [
        "Gon Freecss",
        "Killua Zoldyck",
        "Kurapika",
        "Leorio Paradinight",
        "Hisoka Morow",
        "Chrollo Lucilfer",
        "Meruem",
        "Isaac Netero",
        "Biscuit Krueger",
        "Feitan Portor",
        "Shalnark",
        "Shizuku Murasaki",
        "Nobunaga Hazama",
        "Pakunoda",
        "Phinks Magcub",
        "Uvogin",
        "Machi Komacine",
        "Kite",
        "Knuckle Bine",
        "Morel Mackernasey",
    ]
    promises: list[Promise[str]] = []
    for name in characters:
        p: Promise[str] = s.run(f"test-batching-greet-{name}", greet, name)
        promises.append(p)

    s.wait_until_blocked()

    for p, name in zip(promises, characters):
        assert p.done()
        assert p.result() == name


@pytest.mark.parametrize("store", _promise_storages())
def test_batching_with_failing_func(store: IPromiseStore) -> None:
    @dataclasses.dataclass(frozen=True)
    class ACommand(Command):
        n: int

    def command_handler(ctx: Context, cmds: list[ACommand]) -> list[str]:  # noqa: ARG001
        raise RuntimeError

    def do_something(ctx: Context, n: int) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.lfi(ACommand(n))
        v: str = yield p
        return v

    s = scheduler.Scheduler(store)

    s.register_command_handler(
        ACommand,
        command_handler,
        maxlen=10,
        retry_policy=never(),
    )
    s.register(do_something, retry_policy=never())

    promises: list[Promise[str]] = []
    for n in range(10):
        p: Promise[str] = s.run(f"do-something-{n}", do_something, n)
        promises.append(p)
    s.wait_until_blocked()

    for p in promises:
        assert p.done()
        with pytest.raises(RuntimeError):
            p.result()


@pytest.mark.parametrize("store", _promise_storages())
def test_batching_with_no_result(store: IPromiseStore) -> None:
    @dataclasses.dataclass(frozen=True)
    class ACommand(Command):
        n: int

    def command_handler(ctx: Context, cmds: list[ACommand]) -> None:  # noqa: ARG001
        _ = cmds

    def do_something(ctx: Context, n: int) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.lfi(ACommand(n))
        v: str = yield p
        return v

    s = scheduler.Scheduler(store)
    s.register_command_handler(ACommand, command_handler, maxlen=10)
    s.register(do_something, retry_policy=never())

    promises: list[Promise[str]] = []
    for n in range(10):
        p: Promise[str] = s.run(f"do-something-with-no-result-{n}", do_something, n)
        promises.append(p)
    s.wait_until_blocked()

    for p in promises:
        assert p.done()
        assert p.result() is None


@pytest.mark.parametrize("store", _promise_storages())
def test_batching_with_single_result(store: IPromiseStore) -> None:
    @dataclasses.dataclass(frozen=True)
    class ACommand(Command):
        n: int

    def command_handler(ctx: Context, cmds: list[ACommand]) -> str:  # noqa: ARG001
        _ = cmds
        return "Ok"

    def do_something(ctx: Context, n: int) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.lfi(ACommand(n))
        v: str = yield p
        return v

    s = scheduler.Scheduler(store)
    s.register_command_handler(ACommand, command_handler, maxlen=10)
    s.register(do_something, retry_policy=never())

    promises: list[Promise[str]] = []
    for n in range(10):
        p: Promise[str] = s.run(f"do-something-with-single-result-{n}", do_something, n)
        promises.append(p)

    s.wait_until_blocked()

    for p in promises:
        assert p.done()
        assert p.result() == "Ok"


@pytest.mark.parametrize("store", _promise_storages())
def test_batching_with_failing_func_and_retries(store: IPromiseStore) -> None:
    @dataclasses.dataclass(frozen=True)
    class ACommand(Command):
        n: int

    retries: int = -1

    def command_handler(ctx: Context, cmds: list[ACommand]) -> list[str]:  # noqa: ARG001
        nonlocal retries
        retries += 1
        raise RuntimeError

    def do_something(ctx: Context, n: int) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.lfi(ACommand(n))
        v: str = yield p
        return v

    s = scheduler.Scheduler(store)

    max_retries = 10
    s.register_command_handler(
        ACommand,
        command_handler,
        maxlen=10,
        retry_policy=constant(delay=0, max_retries=max_retries),
    )
    s.register(do_something, retry_policy=never())

    n = 1
    p: Promise[str] = s.run(f"test-batch-retries{n}", do_something, n)
    s.wait_until_blocked()
    assert p.done()

    with pytest.raises(RuntimeError):
        p.result()

    assert retries == max_retries


@pytest.mark.parametrize("store", _promise_storages())
def test_batching_with_element_level_exception(store: IPromiseStore) -> None:
    @dataclasses.dataclass(frozen=True)
    class ACommand(Command):
        n: int

    should_fail: bool = False

    def command_handler(ctx: Context, cmds: list[ACommand]) -> list[str | Exception]:  # noqa: ARG001
        nonlocal should_fail
        results: list[str | Exception] = []
        for _ in range(len(cmds)):
            if should_fail:
                results.append(RuntimeError("something bad happened"))
            else:
                results.append("something good happened")
            should_fail = not should_fail
        return results

    def do_something(ctx: Context, n: int) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.lfi(ACommand(n))
        v: str = yield p
        return v

    s = scheduler.Scheduler(store)

    s.register_command_handler(
        ACommand,
        command_handler,
        retry_policy=never(),
    )
    s.register(do_something, retry_policy=never())

    promises: list[Promise[str]] = []
    for i in range(10):
        promises.append(s.run(f"test-batch-element-level-failure{i}", do_something, i))  # noqa: PERF401

    s.wait_until_blocked()

    successes = 0
    failures = 0
    for p in promises:
        assert p.done()
        result = p.safe_result()
        if isinstance(result, Ok):
            successes += 1
        else:
            failures += 1

    assert successes == failures


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_remote_call_same_node() -> None:
    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])

    def _number_from_other_node(ctx: Context) -> int:  # noqa: ARG001
        return 1

    def _remotely(ctx: Context) -> Generator[Yieldable, Any, int]:
        n = yield ctx.rfc(_number_from_other_node).with_options(target="call-same-node")
        return n

    s = scheduler.Scheduler(store, logic_group="call-same-node")
    s.register(_remotely, retry_policy=never())
    s.register(_number_from_other_node, retry_policy=never())
    p: Promise[int] = s.run("test-remote-call-same-node", _remotely)
    s.wait_until_blocked()
    assert not p.done()
    assert p.result() == 1


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_remote_invocation_same_node() -> None:
    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])

    def _number_from_other_node(ctx: Context) -> int:  # noqa: ARG001
        return 1

    def _remotely(ctx: Context) -> Generator[Yieldable, Any, int]:
        p = yield ctx.rfi(_number_from_other_node).with_options(
            target="invocation-same-node"
        )
        n = yield p
        return n

    s = scheduler.Scheduler(store, logic_group="invocation-same-node")
    s.register(_remotely, retry_policy=never())
    s.register(_number_from_other_node, retry_policy=never())
    p: Promise[int] = s.run("test-remote-invocation-same-node", _remotely)
    s.wait_until_blocked()
    assert not p.done()
    assert p.result() == 1


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_remote_invocation_other_node() -> None:
    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])

    def _number_from_other_node(ctx: Context) -> int:  # noqa: ARG001
        return 1

    def _remotely(ctx: Context) -> Generator[Yieldable, Any, int]:
        p = yield ctx.rfi(_number_from_other_node).with_options(
            target="invocation-other-node"
        )
        n = yield p
        return n

    s = scheduler.Scheduler(store, logic_group="invocation-this-node")
    s.register(_remotely, retry_policy=never())
    s.register(_number_from_other_node, retry_policy=never())

    s_other = scheduler.Scheduler(store, logic_group="invocation-other-node")
    s_other.register(_remotely, retry_policy=never())
    s_other.register(_number_from_other_node, retry_policy=never())

    p: Promise[int] = s.run("test-remote-invocation-other-node", _remotely)
    s.wait_until_blocked()
    assert not p.done()
    assert p.result() == 1
