from __future__ import annotations

import datetime
import os
import random
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import resonate
from resonate.contants import ENV_VARIABLE_PIN_SEED
from resonate.context import Command
from resonate.dst.scheduler import DSTScheduler
from resonate.events import (
    AwaitedForPromise,
    ExecutionStarted,
    PromiseCreated,
    PromiseResolved,
)
from resonate.testing import dst
from typing_extensions import TypeVar

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.dependency_injection import Dependencies
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


def test_raise_inmediately() -> None:
    s = DSTScheduler(
        seed=1,
        mocks=None,
        log_file=None,
        max_failures=0,
        failure_chance=0,
        mode="concurrent",
        probe=None,
    )
    s.add(raise_inmediately)
    p = s.run()[0]
    assert p.failure()
    with pytest.raises(RuntimeError):
        p.result()


def test_failing_call() -> None:
    s = DSTScheduler(
        seed=1,
        mocks=None,
        log_file=None,
        max_failures=0,
        failure_chance=0,
        mode="concurrent",
        probe=None,
    )
    s.add(coro_that_fails_call)
    promises = s.run()
    assert (p.failure() for p in promises)
    assert (not p.success() for p in promises)

    s = DSTScheduler(
        seed=1,
        mocks=None,
        log_file=None,
        max_failures=0,
        failure_chance=0,
        mode="concurrent",
        probe=None,
    )
    s.add(coro_that_fails_invoke)
    promises = s.run()
    assert (p.failure() for p in promises)
    assert (not p.success() for p in promises)


def test_batching_using_call() -> None:
    s = DSTScheduler(
        seed=1,
        mocks=None,
        log_file=None,
        max_failures=0,
        failure_chance=0,
        mode="concurrent",
        probe=None,
    )
    s.register_command(cmd=GreetCommand, handler=batch_greeting, max_batch=2)

    s.add(greet_with_batching, name="Ging")
    s.add(greet_with_batching, name="Razor")
    s.add(greet_with_batching_but_with_call, name="Eta")
    s.add(greet_with_batching, name="Elena")
    s.add(greet_with_batching, name="Dwun")
    greetings_promises = s.run()
    assert all(p.success() for p in greetings_promises)
    assert [p.result() for p in greetings_promises] == [
        "Hello Ging",
        "Hello Razor",
        "Hello Eta",
        "Hello Elena",
        "Hello Dwun",
    ]


def test_batching() -> None:
    s = DSTScheduler(
        seed=1,
        mocks=None,
        log_file=None,
        max_failures=0,
        failure_chance=0,
        mode="concurrent",
        probe=None,
    )

    s.register_command(cmd=GreetCommand, handler=batch_greeting, max_batch=2)
    assert s.get_handler(GreetCommand) == batch_greeting
    s.add(greet_with_batching, name="Ging")
    s.add(greet_with_batching, name="Razor")
    s.add(greet_with_batching, name="Eta")
    s.add(greet_with_batching, name="Elena")
    s.add(greet_with_batching, name="Dwun")
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
def test_pin_seed() -> None:
    s = dst(seeds=[1])[0]
    assert s.seed == 1

    os.environ[ENV_VARIABLE_PIN_SEED] = "32"
    s = dst(seeds=[1])[0]

    assert s.seed == int(os.environ.pop(ENV_VARIABLE_PIN_SEED))


@pytest.mark.dst()
def test_mock_function() -> None:
    s = DSTScheduler(
        seed=1,
        mocks=None,
        mode="concurrent",
        failure_chance=0,
        max_failures=0,
        log_file=None,
        probe=None,
    )
    s = DSTScheduler(
        seed=1,
        mocks=None,
        mode="concurrent",
        failure_chance=0,
        max_failures=0,
        log_file=None,
        probe=None,
    )
    s.add(only_call, n=3)
    s.add(only_invocation, n=3)
    promises = s.run()
    assert all(p.result() == 3 for p in promises)  # noqa: PLR2004
    s = DSTScheduler(
        seed=1,
        mocks={number: mocked_number},
        mode="concurrent",
        failure_chance=0,
        max_failures=0,
        log_file=None,
        probe=None,
    )
    s = DSTScheduler(
        seed=1,
        mocks={number: mocked_number},
        mode="concurrent",
        failure_chance=0,
        max_failures=0,
        log_file=None,
        probe=None,
    )
    promises = s.run()
    assert all(p.result() == 23 for p in promises)  # noqa: PLR2004


@pytest.mark.dst()
def test_dst_scheduler() -> None:
    for _ in range(100):
        seed = random.randint(0, 1000000)  # noqa: S311
        s = DSTScheduler(
            seed=seed,
            mocks=None,
            max_failures=0,
            failure_chance=0,
            mode="concurrent",
            log_file=None,
            probe=None,
        )
        s = DSTScheduler(
            seed=seed,
            mocks=None,
            max_failures=0,
            failure_chance=0,
            mode="concurrent",
            log_file=None,
            probe=None,
        )
        s.add(only_call, n=1)
        s.add(only_call, n=2)
        s.add(only_call, n=3)
        s.add(only_call, n=4)
        s.add(only_call, n=5)

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
def test_dst_determinitic() -> None:
    seed = random.randint(1, 100)  # noqa: S311
    s = DSTScheduler(
        seed=seed,
        mocks=None,
        log_file=None,
        max_failures=0,
        failure_chance=0,
        mode="concurrent",
        probe=None,
    )
    s = DSTScheduler(
        seed=seed,
        mocks=None,
        log_file=None,
        max_failures=0,
        failure_chance=0,
        mode="concurrent",
        probe=None,
    )
    s.add(only_call, n=1)
    s.add(only_call, n=2)
    s.add(only_call, n=3)
    s.add(only_call, n=4)
    s.add(only_call, n=5)
    promises = s.run()
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004
    expected_events = s.get_events()

    same_seed_s = DSTScheduler(
        seed=seed,
        mocks=None,
        log_file=None,
        max_failures=0,
        failure_chance=0,
        mode="concurrent",
        probe=None,
    )
    same_seed_s = DSTScheduler(
        seed=seed,
        mocks=None,
        log_file=None,
        max_failures=0,
        failure_chance=0,
        mode="concurrent",
        probe=None,
    )
    same_seed_s.add(only_call, n=1)
    same_seed_s.add(only_call, n=2)
    same_seed_s.add(only_call, n=3)
    same_seed_s.add(only_call, n=4)
    same_seed_s.add(only_call, n=5)
    promises = same_seed_s.run()
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004
    assert expected_events == same_seed_s.get_events()

    different_seed_s = DSTScheduler(
        seed=seed + 10,
        mocks=None,
        log_file=None,
        max_failures=0,
        failure_chance=0,
        mode="concurrent",
        probe=None,
    )
    different_seed_s = DSTScheduler(
        seed=seed + 10,
        mocks=None,
        log_file=None,
        max_failures=0,
        failure_chance=0,
        mode="concurrent",
        probe=None,
    )
    different_seed_s.add(only_call, n=1)
    different_seed_s.add(only_call, n=2)
    different_seed_s.add(only_call, n=3)
    different_seed_s.add(only_call, n=4)
    different_seed_s.add(only_call, n=5)
    promises = different_seed_s.run()
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004
    assert expected_events != different_seed_s.get_events()


@pytest.mark.dst()
def test_failing_asserting() -> None:
    s = DSTScheduler(
        seed=1,
        mocks=None,
        log_file=None,
        max_failures=0,
        failure_chance=0,
        mode="concurrent",
        probe=None,
    )
    s = DSTScheduler(
        seed=1,
        mocks=None,
        log_file=None,
        max_failures=0,
        failure_chance=0,
        mode="concurrent",
        probe=None,
    )
    s.add(failing_asserting)
    p = s.run()
    with pytest.raises(AssertionError):
        p[0].result()


@pytest.mark.dst()
@pytest.mark.parametrize("scheduler", resonate.testing.dst([range(10)]))
def test_dst_framework(scheduler: DSTScheduler) -> None:
    scheduler.add(only_call, n=1)
    scheduler.add(only_call, n=2)
    scheduler.add(only_call, n=3)
    scheduler.add(only_call, n=4)
    scheduler.add(only_call, n=5)
    promises = scheduler.run()
    assert sum(p.result() for p in promises) == 15  # noqa: PLR2004


@pytest.mark.dst()
def test_failure() -> None:
    scheduler = DSTScheduler(
        seed=1,
        max_failures=3,
        failure_chance=100,
        mocks=None,
        mode="concurrent",
        log_file=None,
        probe=None,
    )
    scheduler = DSTScheduler(
        seed=1,
        max_failures=3,
        failure_chance=100,
        mocks=None,
        mode="concurrent",
        log_file=None,
        probe=None,
    )
    scheduler.add(only_call, n=1)
    p = scheduler.run()
    assert p[0].done()
    assert p[0].result() == 1
    assert scheduler.tick == 6  # noqa: PLR2004
    assert scheduler.current_failures == 3  # noqa: PLR2004

    scheduler = DSTScheduler(
        seed=1,
        max_failures=2,
        failure_chance=0,
        mocks=None,
        mode="concurrent",
        log_file=None,
        probe=None,
    )
    scheduler = DSTScheduler(
        seed=1,
        max_failures=2,
        failure_chance=0,
        mocks=None,
        mode="concurrent",
        log_file=None,
        probe=None,
    )
    scheduler.add(only_call, n=1)
    p = scheduler.run()
    assert p[0].done()
    assert p[0].result() == 1
    assert scheduler.tick == 3  # noqa: PLR2004
    assert scheduler.current_failures == 0


@pytest.mark.dst()
def test_sequential() -> None:
    seq_scheduler = DSTScheduler(
        seed=1,
        mode="sequential",
        failure_chance=0,
        max_failures=0,
        mocks=None,
        log_file=None,
        probe=None,
    )
    seq_scheduler = DSTScheduler(
        seed=1,
        mode="sequential",
        failure_chance=0,
        max_failures=0,
        mocks=None,
        log_file=None,
        probe=None,
    )
    seq_scheduler.add(only_call, n=1)
    seq_scheduler.add(only_call, n=2)
    seq_scheduler.add(only_call, n=3)
    seq_scheduler.add(only_call, n=4)
    seq_scheduler.add(only_call, n=5)
    promises = seq_scheduler.run()
    assert [p.result() for p in promises] == [1, 2, 3, 4, 5]
    assert seq_scheduler.get_events() == [
        PromiseCreated(
            promise_id="1", tick=0, fn_name="only_call", args=(), kwargs={"n": 5}
        ),
        PromiseCreated(
            promise_id="2", tick=0, fn_name="only_call", args=(), kwargs={"n": 4}
        ),
        PromiseCreated(
            promise_id="3", tick=0, fn_name="only_call", args=(), kwargs={"n": 3}
        ),
        PromiseCreated(
            promise_id="4", tick=0, fn_name="only_call", args=(), kwargs={"n": 2}
        ),
        PromiseCreated(
            promise_id="5", tick=0, fn_name="only_call", args=(), kwargs={"n": 1}
        ),
        ExecutionStarted(
            promise_id="5", tick=1, fn_name="only_call", args=(), kwargs={"n": 1}
        ),
        AwaitedForPromise(promise_id="5.1", tick=1),
        PromiseResolved(promise_id="5", tick=3),
        ExecutionStarted(
            promise_id="4", tick=4, fn_name="only_call", args=(), kwargs={"n": 2}
        ),
        AwaitedForPromise(promise_id="4.1", tick=4),
        PromiseResolved(promise_id="4", tick=6),
        ExecutionStarted(
            promise_id="3", tick=7, fn_name="only_call", args=(), kwargs={"n": 3}
        ),
        AwaitedForPromise(promise_id="3.1", tick=7),
        PromiseResolved(promise_id="3", tick=9),
        ExecutionStarted(
            promise_id="2", tick=10, fn_name="only_call", args=(), kwargs={"n": 4}
        ),
        AwaitedForPromise(promise_id="2.1", tick=10),
        PromiseResolved(promise_id="2", tick=12),
        ExecutionStarted(
            promise_id="1", tick=13, fn_name="only_call", args=(), kwargs={"n": 5}
        ),
        AwaitedForPromise(promise_id="1.1", tick=13),
        PromiseResolved(promise_id="1", tick=15),
    ]

    con_scheduler = DSTScheduler(
        seed=1,
        mode="concurrent",
        mocks=None,
        failure_chance=0,
        max_failures=2,
        log_file=None,
        probe=None,
    )
    con_scheduler = DSTScheduler(
        seed=1,
        mode="concurrent",
        mocks=None,
        failure_chance=0,
        max_failures=2,
        log_file=None,
        probe=None,
    )
    con_scheduler.add(only_call, n=1)
    con_scheduler.add(only_call, n=2)
    con_scheduler.add(only_call, n=3)
    con_scheduler.add(only_call, n=4)
    con_scheduler.add(only_call, n=5)
    promises = con_scheduler.run()
    assert [p.result() for p in promises] == [1, 2, 3, 4, 5]
    assert con_scheduler.get_events() == [
        PromiseCreated(
            promise_id="1", tick=0, fn_name="only_call", args=(), kwargs={"n": 5}
        ),
        PromiseCreated(
            promise_id="2", tick=0, fn_name="only_call", args=(), kwargs={"n": 4}
        ),
        PromiseCreated(
            promise_id="3", tick=0, fn_name="only_call", args=(), kwargs={"n": 3}
        ),
        PromiseCreated(
            promise_id="4", tick=0, fn_name="only_call", args=(), kwargs={"n": 2}
        ),
        PromiseCreated(
            promise_id="5", tick=0, fn_name="only_call", args=(), kwargs={"n": 1}
        ),
        ExecutionStarted(
            promise_id="1", tick=1, fn_name="only_call", args=(), kwargs={"n": 5}
        ),
        AwaitedForPromise(promise_id="1.1", tick=1),
        ExecutionStarted(
            promise_id="5", tick=2, fn_name="only_call", args=(), kwargs={"n": 1}
        ),
        AwaitedForPromise(promise_id="5.1", tick=2),
        ExecutionStarted(
            promise_id="2", tick=3, fn_name="only_call", args=(), kwargs={"n": 4}
        ),
        AwaitedForPromise(promise_id="2.1", tick=3),
        ExecutionStarted(
            promise_id="3", tick=7, fn_name="only_call", args=(), kwargs={"n": 3}
        ),
        AwaitedForPromise(promise_id="3.1", tick=7),
        PromiseResolved(promise_id="1", tick=9),
        PromiseResolved(promise_id="5", tick=10),
        PromiseResolved(promise_id="2", tick=11),
        PromiseResolved(promise_id="3", tick=12),
        ExecutionStarted(
            promise_id="4", tick=13, fn_name="only_call", args=(), kwargs={"n": 2}
        ),
        AwaitedForPromise(promise_id="4.1", tick=13),
        PromiseResolved(promise_id="4", tick=15),
    ]


def test_dump_events() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        log_file_path = Path(temp_dir) / "cool_log_%s.txt"
        s = DSTScheduler(
            seed=1,
            log_file=log_file_path.as_posix(),
            mocks=None,
            max_failures=1,
            failure_chance=0,
            mode="concurrent",
            probe=None,
        )
        formatted_file_path = Path(log_file_path.as_posix() % (s.seed))
        assert not formatted_file_path.exists()
        s.add(only_call, n=1)
        s.add(only_call, n=2)
        s.add(only_call, n=3)
        s.add(only_call, n=4)
        s.add(only_call, n=5)
        s.run()

        assert formatted_file_path.exists()
        assert formatted_file_path.read_text() == "".join(
            f"{e}\n" for e in s.get_events()
        )


def _probe_function(deps: Dependencies, tick: int) -> datetime.datetime:  # noqa: ARG001
    return datetime.datetime.now(tz=datetime.timezone.utc)


def test_probe() -> None:
    s = DSTScheduler(
        seed=1,
        log_file=None,
        mocks=None,
        max_failures=1,
        failure_chance=0,
        mode="concurrent",
        probe=_probe_function,
    )
    s.add(only_call, n=1)
    s.add(only_call, n=2)
    s.add(only_call, n=3)
    s.add(only_call, n=4)
    s.add(only_call, n=5)
    s.run()
    assert len(s._probe_results) > 0  # noqa: SLF001
