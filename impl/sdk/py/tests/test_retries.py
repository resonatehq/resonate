from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

from resonate.conventions import Base
from resonate.dependencies import Dependencies
from resonate.models.commands import Delayed, Function, Invoke, Network, RejectPromiseReq, ResolvePromiseReq, Retry, Return
from resonate.models.result import Ko, Ok
from resonate.options import Options
from resonate.registry import Registry
from resonate.resonate import Context
from resonate.retry_policies import Constant, Exponential, Linear, Never
from resonate.scheduler import Scheduler

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
    return Scheduler(lambda id, info: Context(id, info, Registry(), Dependencies()))


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
    next = scheduler.step(Invoke("foo", Base("foo", sys.maxsize), foo, opts=Options(retry_policy=retry_policy)))
    assert len(next.reqs) == 1
    req = next.reqs[0]
    assert isinstance(req, Function)
    next = scheduler.step(Return("foo", "foo", Ok("foo")))
    assert len(next.reqs) == 1
    req = next.reqs[0]
    assert isinstance(req, Network)
    assert isinstance(req.req, ResolvePromiseReq)


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
    next = scheduler.step(Invoke("foo", Base("foo", sys.maxsize), foo, opts=Options(retry_policy=retry_policy)))
    assert len(next.reqs) == 1
    req = next.reqs[0]
    assert isinstance(req, Function)

    for _ in range(retries):
        next = scheduler.step(Return("foo", "foo", Ko(ValueError("something went wrong"))))
        assert len(next.reqs) == 1
        req = next.reqs[0]
        assert isinstance(req, Delayed)

    next = scheduler.step(Return("foo", "foo", Ko(ValueError("something went wrong"))))
    assert len(next.reqs) == 1
    req = next.reqs[0]
    assert isinstance(req, Network)
    assert isinstance(req.req, RejectPromiseReq)


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
    next = scheduler.step(Invoke("foo", Base("foo", sys.maxsize), bar_ok, opts=Options(retry_policy=retry_policy)))
    assert len(next.reqs) == 1
    req = next.reqs[0]
    assert isinstance(req, Network)
    assert isinstance(req.req, ResolvePromiseReq)


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
    next = scheduler.step(Invoke("foo", Base("foo", sys.maxsize), bar_ko, opts=Options(retry_policy=retry_policy)))
    assert len(next.reqs) == 1
    req = next.reqs[0]
    for _ in range(retries):
        assert isinstance(req, Delayed)
        assert isinstance(req.item, Retry)
        next = scheduler.step(req.item)
        assert len(next.reqs) == 1
        req = next.reqs[0]

    assert isinstance(req, Network)
    assert isinstance(req.req, RejectPromiseReq)


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
    next = scheduler.step(Invoke("foo", Base("foo", sys.maxsize), bar_ko, opts=Options(retry_policy=retry_policy, non_retryable_exceptions=non_retryable_exceptions)))
    assert len(next.reqs) == 1
    req = next.reqs[0]
    assert isinstance(req, Network)
    assert isinstance(req.req, RejectPromiseReq)
