from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

from resonate.conventions import Base
from resonate.dependencies import Dependencies
from resonate.loggers import ContextLogger
from resonate.models.commands import Delayed, Function, Invoke, Retry, Return
from resonate.models.result import Ko, Ok
from resonate.options import Options
from resonate.registry import Registry
from resonate.resonate import Context
from resonate.retry_policies import Constant, Exponential, Linear, Never
from resonate.scheduler import Done, More, Scheduler

if TYPE_CHECKING:
    from resonate.models.retry_policy import RetryPolicy


def foo(ctx: Context):  # noqa: ANN201
    return "foo"


def bar_ok(ctx: Context):  # noqa: ANN201
    return "bar"
    yield ctx.lfi(foo)


def bar_ko(ctx: Context):  # noqa: ANN201
    raise ValueError
    yield ctx.lfi(foo)


@pytest.fixture
def scheduler() -> Scheduler:
    return Scheduler(lambda id, cid, info: Context(id, cid, info, Registry(), Dependencies(), ContextLogger("f", "f")))


@pytest.mark.parametrize(
    "retry_policy",
    [
        Never(),
        Constant(),
        Linear(),
        Exponential(),
    ],
)
def test_function_happy_path(scheduler: Scheduler, retry_policy: RetryPolicy) -> None:
    next = scheduler.step(Invoke("foo", Base("foo", sys.maxsize), sys.maxsize, foo, opts=Options(durable=False, retry_policy=retry_policy)))
    assert isinstance(next, More)
    assert len(next.reqs) == 1
    req = next.reqs[0]
    assert isinstance(req, Function)
    next = scheduler.step(Return("foo", "foo", Ok("foo")))
    assert isinstance(next, Done)
    assert scheduler.computations["foo"].result() == Ok("foo")


@pytest.mark.parametrize(
    ("retry_policy", "retries"),
    [
        (Never(), 0),
        (Constant(max_retries=2), 2),
        (Linear(max_retries=3), 3),
        (Exponential(max_retries=2), 2),
    ],
)
def test_function_sad_path(scheduler: Scheduler, retry_policy: RetryPolicy, retries: int) -> None:
    e = ValueError("something went wrong")

    next = scheduler.step(Invoke("foo", Base("foo", sys.maxsize), sys.maxsize, foo, opts=Options(durable=False, retry_policy=retry_policy)))
    assert isinstance(next, More)
    assert len(next.reqs) == 1
    req = next.reqs[0]
    assert isinstance(req, Function)

    for _ in range(retries):
        next = scheduler.step(Return("foo", "foo", Ko(e)))
        assert isinstance(next, More)
        assert len(next.reqs) == 1
        req = next.reqs[0]
        assert isinstance(req, Delayed)

    next = scheduler.step(Return("foo", "foo", Ko(e)))
    assert isinstance(next, Done)
    assert scheduler.computations["foo"].result() == Ko(e)


@pytest.mark.parametrize(
    "retry_policy",
    [
        Never(),
        Constant(),
        Linear(),
        Exponential(),
    ],
)
def test_generator_happy_path(scheduler: Scheduler, retry_policy: RetryPolicy) -> None:
    next = scheduler.step(Invoke("bar", Base("bar", sys.maxsize), sys.maxsize, bar_ok, opts=Options(durable=False, retry_policy=retry_policy)))
    assert isinstance(next, Done)
    assert scheduler.computations["bar"].result() == Ok("bar")


@pytest.mark.parametrize(
    ("retry_policy", "retries"),
    [
        (Never(), 0),
        (Constant(max_retries=2), 2),
        (Linear(max_retries=3), 3),
        (Exponential(max_retries=1), 1),
    ],
)
def test_generator_sad_path(scheduler: Scheduler, retry_policy: RetryPolicy, retries: int) -> None:
    next = scheduler.step(Invoke("bar", Base("bar", sys.maxsize), sys.maxsize, bar_ko, opts=Options(durable=False, retry_policy=retry_policy)))

    for _ in range(retries):
        assert isinstance(next, More)
        assert len(next.reqs) == 1
        req = next.reqs[0]
        assert isinstance(req, Delayed)
        assert isinstance(req.item, Retry)
        next = scheduler.step(req.item)

    assert isinstance(next, Done)
    assert isinstance(scheduler.computations["bar"].result(), Ko)


@pytest.mark.parametrize(
    ("retry_policy", "non_retryable_exceptions"),
    [
        (Never(), (ValueError,)),
        (Constant(max_retries=2), (ValueError,)),
        (Linear(max_retries=3), (ValueError,)),
        (Exponential(max_retries=1), (ValueError,)),
    ],
)
def test_non_retriable_errors(scheduler: Scheduler, retry_policy: RetryPolicy, non_retryable_exceptions: tuple[type[Exception], ...]) -> None:
    opts = Options(durable=False, retry_policy=retry_policy, non_retryable_exceptions=non_retryable_exceptions)
    next = scheduler.step(Invoke("bar", Base("bar", sys.maxsize), sys.maxsize, bar_ko, opts=opts))
    assert isinstance(next, Done)
    assert isinstance(scheduler.computations["bar"].result(), Ko)


@pytest.mark.parametrize(
    "retry_policy",
    [
        Never(),
        Constant(max_retries=2),
        Linear(max_retries=3),
        Exponential(max_retries=1),
    ],
)
def test_timeout_over_delay(scheduler: Scheduler, retry_policy: RetryPolicy) -> None:
    opts = Options(durable=False, retry_policy=retry_policy)
    next = scheduler.step(Invoke("bar", Base("bar", 0), 0, bar_ko, opts=opts))
    assert isinstance(next, Done)
    assert isinstance(scheduler.computations["bar"].result(), Ko)
