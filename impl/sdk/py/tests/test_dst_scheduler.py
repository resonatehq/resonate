from __future__ import annotations

import os
import random
import tempfile
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import resonate
from resonate.contants import ENV_VARIABLE_PIN_SEED
from resonate.context import Command
from resonate.dst.scheduler import DSTScheduler
from resonate.events import (
    ExecutionAwaited,
    ExecutionInvoked,
    ExecutionResumed,
    ExecutionTerminated,
    PromiseCompleted,
    PromiseCreated,
)
from resonate.options import Options
from resonate.result import Ok
from resonate.storage import (
    IPromiseStore,
    LocalPromiseStore,
    MemoryStorage,
    RemotePromiseStore,
)
from resonate.testing import dst
from resonate.time import now
from typing_extensions import TypeVar

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.dependency_injection import Dependencies
    from resonate.dst.scheduler import DSTScheduler
    from resonate.promise import Promise
    from resonate.typing import Yieldable

T = TypeVar("T")


def number(ctx: Context, n: int) -> int:  # noqa: ARG001
    return n


def failing_function(ctx: Context) -> None:  # noqa: ARG001
    msg = "An error happenned"
    raise RuntimeError(msg)


def coro_that_fails_call(ctx: Context) -> Generator[Yieldable, Any, None]:
    return (yield ctx.call(failing_function))


def coro_that_fails_invoke(ctx: Context) -> Generator[Yieldable, Any, None]:
    return (yield (yield ctx.invoke(failing_function)))


def raise_inmediately(ctx: Context) -> Generator[Yieldable, Any, int]:  # noqa: ARG001
    msg = "First thing we do is fail."
    raise RuntimeError(msg)


def only_call(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
    x: int = yield ctx.call(number, n=n)
    return x


def only_invocation(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
    xp: Promise[int] = yield ctx.invoke(number, n=n)
    x: int = yield xp
    return x


def failing_asserting(ctx: Context) -> Generator[Yieldable, Any, int]:
    x: int = yield ctx.call(only_invocation, n=3)
    ctx.assert_statement(x < 0, f"{x} should be negative")
    return x


def _promise_result(promises: list[Promise[T]]) -> list[T]:
    return [x.result() for x in promises]


def mocked_number() -> int:
    return 23


@dataclass(frozen=True)
class GreetCommand(Command):
    name: str


def batch_greeting(cmds: list[GreetCommand]) -> list[str]:
    return [f"Hello {cmd.name}" for cmd in cmds]


def greet_with_batching(ctx: Context, name: str) -> Generator[Yieldable, Any, str]:
    p: Promise[str] = yield ctx.invoke(GreetCommand(name=name))
    g: str = yield p
    return g


def greet_with_batching_but_with_call(
    ctx: Context, name: str
) -> Generator[Yieldable, Any, str]:
    g: str = yield ctx.call(GreetCommand(name=name))
    return g


@cache
def _promise_storages() -> list[IPromiseStore]:
    stores: list[IPromiseStore] = [LocalPromiseStore(MemoryStorage())]
    if os.getenv("RESONATE_STORE_URL") is not None:
        stores.append(RemotePromiseStore(url=os.environ["RESONATE_STORE_URL"]))
    return stores


@pytest.mark.parametrize("store", _promise_storages())
def test_raise_inmediately(store: IPromiseStore) -> None:
    s = dst(seeds=[1], durable_promise_storage=store)[0]
    s.add("raise-inmediately-1", Options(durable=True), raise_inmediately)
    p = s.run()[0]
    assert p.failure()
    with pytest.raises(RuntimeError):
        p.result()


@pytest.mark.parametrize("store", _promise_storages())
def test_failing_call(store: IPromiseStore) -> None:
    s = dst(seeds=[1], durable_promise_storage=store)[0]
    s.add("failing-call-1", Options(durable=True), coro_that_fails_call)
    promises = s.run()
    assert (p.failure() for p in promises)
    assert (not p.success() for p in promises)

    s = dst(seeds=[1])[0]
    s.add("failing-invoke-1", Options(durable=True), coro_that_fails_invoke)
    promises = s.run()
    assert (p.failure() for p in promises)
    assert (not p.success() for p in promises)


@pytest.mark.skip()
@pytest.mark.parametrize("store", _promise_storages())
def test_batching_using_call(store: IPromiseStore) -> None:
    s = dst(seeds=[1], durable_promise_storage=store)[0]
    s.register_command(cmd=GreetCommand, handler=batch_greeting, max_batch=2)

    s.add(
        "batching-using-call-1", Options(durable=True), greet_with_batching, name="Ging"
    )
    s.add(
        "batching-using-call-2",
        Options(durable=True),
        greet_with_batching,
        name="Razor",
    )
    s.add(
        "batching-using-call-3",
        Options(durable=True),
        greet_with_batching_but_with_call,
        name="Eta",
    )
    s.add(
        "batching-using-call-4",
        Options(durable=True),
        greet_with_batching,
        name="Elena",
    )
    s.add(
        "batching-using-call-5", Options(durable=True), greet_with_batching, name="Dwun"
    )
    greetings_promises = s.run()
    assert all(p.success() for p in greetings_promises)
    assert [p.result() for p in greetings_promises] == [
        "Hello Ging",
        "Hello Razor",
        "Hello Eta",
        "Hello Elena",
        "Hello Dwun",
    ]


@pytest.mark.skip()
@pytest.mark.parametrize("store", _promise_storages())
def test_batching(store: IPromiseStore) -> None:
    s: DSTScheduler = dst(seeds=[1], durable_promise_storage=store)[0]

    s.register_command(cmd=GreetCommand, handler=batch_greeting, max_batch=2)
    s.add("batching-1", Options(durable=True), greet_with_batching, name="Ging")
    s.add("batching-2", Options(durable=True), greet_with_batching, name="Razor")
    s.add("batching-3", Options(durable=True), greet_with_batching, name="Eta")
    s.add("batching-4", Options(durable=True), greet_with_batching, name="Elena")
    s.add("batching-5", Options(durable=True), greet_with_batching, name="Dwun")
    greetings_promises = s.run()
    assert all(p.success() for p in greetings_promises)
    assert [p.result() for p in greetings_promises] == [
        "Hello Ging",
        "Hello Razor",
        "Hello Eta",
        "Hello Elena",
        "Hello Dwun",
    ]


@pytest.mark.dst()
@pytest.mark.parametrize("store", _promise_storages())
def test_pin_seed(store: IPromiseStore) -> None:
    s = dst(seeds=[1], durable_promise_storage=store)[0]
    assert s.seed == 1

    os.environ[ENV_VARIABLE_PIN_SEED] = "32"
    s = dst(seeds=[1])[0]

    assert s.seed == int(os.environ.pop(ENV_VARIABLE_PIN_SEED))


@pytest.mark.dst()
@pytest.mark.parametrize("store", _promise_storages())
def test_mock_function(store: IPromiseStore) -> None:
    s = dst(seeds=[1], durable_promise_storage=store)[0]
    s.add("mock-1", Options(durable=True), only_call, n=3)
    s.add("mock-2", Options(durable=True), only_invocation, n=3)
    promises = s.run()
    assert all(p.result() == 3 for p in promises)  # noqa: PLR2004
    s = dst(seeds=[1], mocks={number: mocked_number})[0]
    promises = s.run()
    assert all(p.result() == 23 for p in promises)  # noqa: PLR2004


@pytest.mark.dst()
@pytest.mark.parametrize("store", _promise_storages())
def test_dst_scheduler(store: IPromiseStore) -> None:
    for _ in range(100):
        seed = random.randint(0, 1000000)  # noqa: S311
        s = dst(seeds=[seed], durable_promise_storage=store)[0]

        s.add("only-call-1", Options(durable=True), only_call, n=1)
        s.add("only-call-2", Options(durable=True), only_call, n=2)
        s.add("only-call-3", Options(durable=True), only_call, n=3)
        s.add("only-call-4", Options(durable=True), only_call, n=4)
        s.add("only-call-5", Options(durable=True), only_call, n=5)

        promises = s.run()
        values = _promise_result(promises=promises)
        assert values == [
            1,
            2,
            3,
            4,
            5,
        ], f"Test fails when seed it {seed}"


@pytest.mark.dst()
@pytest.mark.parametrize("store", _promise_storages())
def test_dst_determinitic(store: IPromiseStore) -> None:
    seed = random.randint(1, 100)  # noqa: S311
    s = dst(seeds=[seed], durable_promise_storage=store)[0]
    s.add("dst-deterministic-1", Options(durable=True), only_call, n=1)
    s.add("dst-deterministic-2", Options(durable=True), only_call, n=2)
    s.add("dst-deterministic-3", Options(durable=True), only_call, n=3)
    s.add("dst-deterministic-4", Options(durable=True), only_call, n=4)
    s.add("dst-deterministic-5", Options(durable=True), only_call, n=5)
    promises = s.run()
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004
    expected_events = s.get_events()

    same_seed_s = dst(seeds=[seed])[0]
    same_seed_s.add("dst-deterministic-1", Options(durable=True), only_call, n=1)
    same_seed_s.add("dst-deterministic-2", Options(durable=True), only_call, n=2)
    same_seed_s.add("dst-deterministic-3", Options(durable=True), only_call, n=3)
    same_seed_s.add("dst-deterministic-4", Options(durable=True), only_call, n=4)
    same_seed_s.add("dst-deterministic-5", Options(durable=True), only_call, n=5)
    promises = same_seed_s.run()
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004
    assert expected_events == same_seed_s.get_events()

    different_seed_s = dst(seeds=[seed + 10])[0]
    different_seed_s.add("dst-deterministic-1", Options(durable=True), only_call, n=1)
    different_seed_s.add("dst-deterministic-2", Options(durable=True), only_call, n=2)
    different_seed_s.add("dst-deterministic-3", Options(durable=True), only_call, n=3)
    different_seed_s.add("dst-deterministic-4", Options(durable=True), only_call, n=4)
    different_seed_s.add("dst-deterministic-5", Options(durable=True), only_call, n=5)
    promises = different_seed_s.run()
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004
    assert expected_events != different_seed_s.get_events()


@pytest.mark.dst()
@pytest.mark.parametrize("store", _promise_storages())
def test_failing_asserting(store: IPromiseStore) -> None:
    s = dst(seeds=[1], durable_promise_storage=store)[0]
    s.add("failing-asserting-1", Options(durable=True), failing_asserting)
    p = s.run()
    with pytest.raises(AssertionError):
        p[0].result()


@pytest.mark.dst()
@pytest.mark.parametrize("scheduler", resonate.testing.dst([range(10)]))
def test_dst_framework(scheduler: DSTScheduler) -> None:
    scheduler.add("dst-framework-1", Options(durable=True), only_call, n=1)
    scheduler.add("dst-framework-2", Options(durable=True), only_call, n=2)
    scheduler.add("dst-framework-3", Options(durable=True), only_call, n=3)
    scheduler.add("dst-framework-4", Options(durable=True), only_call, n=4)
    scheduler.add("dst-framework-5", Options(durable=True), only_call, n=5)
    promises = scheduler.run()
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004


@pytest.mark.dst()
@pytest.mark.parametrize("store", _promise_storages())
def test_failure(store: IPromiseStore) -> None:
    scheduler = dst(
        seeds=[1],
        max_failures=3,
        failure_chance=100,
        durable_promise_storage=store,
    )[0]
    scheduler.add("failure-1", Options(durable=True), only_call, n=1)
    p = scheduler.run()
    assert p[0].done()
    assert p[0].result() == 1
    assert scheduler.tick == 6  # noqa: PLR2004
    assert scheduler.current_failures == 3  # noqa: PLR2004

    scheduler = dst(seeds=[1], max_failures=2, failure_chance=0)[0]
    scheduler.add("failure-1", Options(durable=True), only_call, n=1)
    p = scheduler.run()
    assert p[0].done()
    assert p[0].result() == 1
    assert scheduler.tick == 3  # noqa: PLR2004
    assert scheduler.current_failures == 0


@pytest.mark.dst()
@pytest.mark.parametrize("store", _promise_storages())
def test_sequential_retry(store: IPromiseStore) -> None:
    seq_scheduler = dst(seeds=[1], mode="sequential", durable_promise_storage=store)[0]
    seq_scheduler.add("execution-seq-with-retry", Options(durable=True), only_call, n=1)
    promises = seq_scheduler.run()
    assert [p.result() for p in promises] == [1]
    assert seq_scheduler.get_events() == [
        PromiseCreated(
            promise_id="execution-seq-with-retry", parent_promise_id=None, tick=0
        ),
        ExecutionInvoked(
            promise_id="execution-seq-with-retry",
            parent_promise_id=None,
            tick=0,
            fn_name="only_call",
            args=(),
            kwargs={"n": 1},
        ),
        PromiseCreated(
            promise_id="execution-seq-with-retry.1",
            parent_promise_id="execution-seq-with-retry",
            tick=1,
        ),
        ExecutionInvoked(
            promise_id="execution-seq-with-retry.1",
            parent_promise_id="execution-seq-with-retry",
            tick=1,
            fn_name="number",
            args=(),
            kwargs={"n": 1},
        ),
        ExecutionAwaited(
            promise_id="execution-seq-with-retry",
            parent_promise_id=None,
            tick=1,
        ),
        ExecutionTerminated(
            promise_id="execution-seq-with-retry.1",
            parent_promise_id="execution-seq-with-retry",
            tick=2,
        ),
        PromiseCompleted(
            promise_id="execution-seq-with-retry.1",
            parent_promise_id="execution-seq-with-retry",
            tick=2,
            value=Ok(1),
        ),
        ExecutionResumed(
            promise_id="execution-seq-with-retry",
            parent_promise_id=None,
            tick=3,
        ),
        ExecutionTerminated(
            promise_id="execution-seq-with-retry",
            parent_promise_id=None,
            tick=3,
        ),
        PromiseCompleted(
            promise_id="execution-seq-with-retry",
            parent_promise_id=None,
            tick=3,
            value=Ok(1),
        ),
    ]

    promises = seq_scheduler.run()
    assert [p.result() for p in promises] == [1]
    assert seq_scheduler.get_events() == [
        PromiseCreated(
            promise_id="execution-seq-with-retry", parent_promise_id=None, tick=0
        ),
        PromiseCompleted(
            promise_id="execution-seq-with-retry",
            parent_promise_id=None,
            tick=0,
            value=Ok(1),
        ),
    ]


@pytest.mark.dst()
@pytest.mark.parametrize("store", _promise_storages())
def test_sequential(store: IPromiseStore) -> None:
    seq_scheduler = dst(seeds=[1], mode="sequential", durable_promise_storage=store)[0]
    seq_scheduler.add("execution-seq-1", Options(durable=True), only_call, n=1)
    seq_scheduler.add("execution-seq-2", Options(durable=True), only_call, n=2)
    seq_scheduler.add("execution-seq-3", Options(durable=True), only_call, n=3)
    seq_scheduler.add("execution-seq-4", Options(durable=True), only_call, n=4)
    seq_scheduler.add("execution-seq-5", Options(durable=True), only_call, n=5)
    promises = seq_scheduler.run()
    assert [p.result() for p in promises] == [1, 2, 3, 4, 5]

    con_scheduler = dst(seeds=[1], max_failures=2, durable_promise_storage=store)[0]
    con_scheduler.add("execution-con-1", Options(durable=True), only_call, n=1)
    con_scheduler.add("execution-con-2", Options(durable=True), only_call, n=2)
    con_scheduler.add("execution-con-3", Options(durable=True), only_call, n=3)
    con_scheduler.add("execution-con-4", Options(durable=True), only_call, n=4)
    con_scheduler.add("execution-con-5", Options(durable=True), only_call, n=5)
    promises = con_scheduler.run()
    assert [p.result() for p in promises] == [1, 2, 3, 4, 5]
    assert len(con_scheduler.get_events()) == len(seq_scheduler.get_events())


@pytest.mark.parametrize("store", _promise_storages())
def test_dump_events(store: IPromiseStore) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        log_file_path = Path(temp_dir) / "cool_log_%s.txt"
        s = dst(
            seeds=[1], log_file=log_file_path.as_posix(), durable_promise_storage=store
        )[0]
        formatted_file_path = Path(log_file_path.as_posix() % (s.seed))
        assert not formatted_file_path.exists()
        s.add("dump-events-1", Options(durable=True), only_call, n=1)
        s.add("dump-events-2", Options(durable=True), only_call, n=2)
        s.add("dump-events-3", Options(durable=True), only_call, n=3)
        s.add("dump-events-4", Options(durable=True), only_call, n=4)
        s.add("dump-events-5", Options(durable=True), only_call, n=5)
        s.run()

        assert formatted_file_path.exists()
        assert formatted_file_path.read_text() == "".join(
            f"{e}\n" for e in s.get_events()
        )


def _probe_function(deps: Dependencies, tick: int) -> int:  # noqa: ARG001
    return now()


@pytest.mark.parametrize("store", _promise_storages())
def test_probe(store: IPromiseStore) -> None:
    s = dst(seeds=[1], probe=_probe_function, durable_promise_storage=store)[0]
    s.add("probe-1", Options(durable=True), only_call, n=1)
    s.add("probe-2", Options(durable=True), only_call, n=2)
    s.add("probe-3", Options(durable=True), only_call, n=3)
    s.add("probe-4", Options(durable=True), only_call, n=4)
    s.add("probe-5", Options(durable=True), only_call, n=5)
    s.run()
    assert len(s.probe_results) > 0
    assert s.probe_results[-1] <= now()


def baz(ctx: Context) -> None: ...  # noqa: ARG001, RUF100
def bar(ctx: Context) -> Generator[Yieldable, Any, str]:
    yield ctx.call(baz)
    return "Done"


def foo(ctx: Context) -> Generator[Yieldable, Any, list[str]]:
    p1: Promise[str] = yield ctx.invoke(bar)
    p2: Promise[str] = yield ctx.invoke(bar)

    resp: list[str] = []
    v1: str = yield p1
    resp.append(v1)

    v2: str = yield p2
    resp.append(v2)
    return resp


@pytest.mark.parametrize("store", _promise_storages())
def test_events_logic(store: IPromiseStore) -> None:
    s = dst(seeds=[1], durable_promise_storage=store)[0]
    s.add("foo-events-1", Options(durable=True), foo)
    s.run()
    assert s.get_events() == [
        PromiseCreated(promise_id="foo-events-1", parent_promise_id=None, tick=0),
        ExecutionInvoked(
            promise_id="foo-events-1",
            parent_promise_id=None,
            tick=0,
            fn_name="foo",
            args=(),
            kwargs={},
        ),
        PromiseCreated(
            promise_id="foo-events-1.1", parent_promise_id="foo-events-1", tick=1
        ),
        ExecutionInvoked(
            promise_id="foo-events-1.1",
            parent_promise_id="foo-events-1",
            tick=1,
            fn_name="bar",
            args=(),
            kwargs={},
        ),
        PromiseCreated(
            promise_id="foo-events-1.1.1", parent_promise_id="foo-events-1.1", tick=2
        ),
        ExecutionInvoked(
            promise_id="foo-events-1.1.1",
            parent_promise_id="foo-events-1.1",
            tick=2,
            fn_name="baz",
            args=(),
            kwargs={},
        ),
        ExecutionAwaited(
            promise_id="foo-events-1.1", parent_promise_id="foo-events-1", tick=2
        ),
        PromiseCreated(
            promise_id="foo-events-1.2", parent_promise_id="foo-events-1", tick=3
        ),
        ExecutionInvoked(
            promise_id="foo-events-1.2",
            parent_promise_id="foo-events-1",
            tick=3,
            fn_name="bar",
            args=(),
            kwargs={},
        ),
        ExecutionAwaited(promise_id="foo-events-1", parent_promise_id=None, tick=4),
        PromiseCreated(
            promise_id="foo-events-1.2.1", parent_promise_id="foo-events-1.2", tick=5
        ),
        ExecutionInvoked(
            promise_id="foo-events-1.2.1",
            parent_promise_id="foo-events-1.2",
            tick=5,
            fn_name="baz",
            args=(),
            kwargs={},
        ),
        ExecutionAwaited(
            promise_id="foo-events-1.2", parent_promise_id="foo-events-1", tick=5
        ),
        ExecutionTerminated(
            promise_id="foo-events-1.1.1", parent_promise_id="foo-events-1.1", tick=6
        ),
        PromiseCompleted(
            promise_id="foo-events-1.1.1",
            parent_promise_id="foo-events-1.1",
            tick=6,
            value=Ok(None),
        ),
        ExecutionTerminated(
            promise_id="foo-events-1.2.1", parent_promise_id="foo-events-1.2", tick=7
        ),
        PromiseCompleted(
            promise_id="foo-events-1.2.1",
            parent_promise_id="foo-events-1.2",
            tick=7,
            value=Ok(None),
        ),
        ExecutionResumed(
            promise_id="foo-events-1.1", parent_promise_id="foo-events-1", tick=8
        ),
        ExecutionTerminated(
            promise_id="foo-events-1.1", parent_promise_id="foo-events-1", tick=8
        ),
        PromiseCompleted(
            promise_id="foo-events-1.1",
            parent_promise_id="foo-events-1",
            tick=8,
            value=Ok("Done"),
        ),
        ExecutionResumed(promise_id="foo-events-1", parent_promise_id=None, tick=9),
        ExecutionAwaited(promise_id="foo-events-1", parent_promise_id=None, tick=9),
        ExecutionResumed(
            promise_id="foo-events-1.2", parent_promise_id="foo-events-1", tick=10
        ),
        ExecutionTerminated(
            promise_id="foo-events-1.2", parent_promise_id="foo-events-1", tick=10
        ),
        PromiseCompleted(
            promise_id="foo-events-1.2",
            parent_promise_id="foo-events-1",
            tick=10,
            value=Ok("Done"),
        ),
        ExecutionResumed(promise_id="foo-events-1", parent_promise_id=None, tick=11),
        ExecutionTerminated(promise_id="foo-events-1", parent_promise_id=None, tick=11),
        PromiseCompleted(
            promise_id="foo-events-1",
            parent_promise_id=None,
            tick=11,
            value=Ok(["Done", "Done"]),
        ),
    ]
