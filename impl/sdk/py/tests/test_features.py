from __future__ import annotations

import json
import os
import time
from random import randint
from typing import TYPE_CHECKING, Any

import pytest

from resonate.commands import manual_completion, remote_function
from resonate.promise import Promise
from resonate.retry_policy import never
from resonate.scheduler import Scheduler
from resonate.storage.resonate_server import RemoteServer

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.promise import Promise
    from resonate.typing import Yieldable


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_human_in_the_loop() -> None:
    def human_in_the_loop(ctx: Context) -> Generator[Yieldable, Any, str]:
        name: str = yield ctx.rfc(
            manual_completion("test-human-in-loop-question-to-answer-1")
        )
        age: int = yield ctx.rfc(
            manual_completion(promise_id="test-human-in-loop-question-to-answer-2")
        )
        return f"Hi {name} with age {age}"

    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    s = Scheduler(store)
    s.register(human_in_the_loop, retry_policy=never())
    p: Promise[str] = s.run("test-feature-human-in-the-loop", human_in_the_loop)
    s.wait_until_blocked()
    store.resolve(
        promise_id="test-human-in-loop-question-to-answer-1",
        ikey=None,
        strict=False,
        headers=None,
        data=json.dumps("Peter"),
    )
    s.clear_blocked_flag()
    s.wait_until_blocked()
    store.resolve(
        promise_id="test-human-in-loop-question-to-answer-2",
        ikey=None,
        strict=False,
        headers=None,
        data=json.dumps(50),
    )
    s.clear_blocked_flag()
    s.wait_until_blocked()
    assert p.done()
    assert p.result() == "Hi Peter with age 50"


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_factorial_same_node() -> None:
    node_group = "test-factorial-same-node"

    def factorial(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1
        return n * (
            yield ctx.rfc(factorial, n - 1).with_options(
                f"factorial-same-node-{n-1}", target=node_group
            )
        )

    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    s = Scheduler(store, logic_group=node_group)
    s.register(factorial)
    n = 5
    p: Promise[int] = s.run(f"factorial-same-node-{n}", factorial, n)
    s.wait_until_blocked()
    assert not p.done()
    assert p.result() == 120  # noqa: PLR2004


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_factorial_multi_node() -> None:
    def factorial_node_1(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1
        return n * (
            yield ctx.rfc(factorial_node_1, n - 1).with_options(
                f"factorial-multi-node-{n-1}", target="test-factorial-multi-node-2"
            )
        )

    def factorial_node_2(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1
        return n * (
            yield ctx.rfc(factorial_node_2, n - 1).with_options(
                f"factorial-multi-node-{n-1}", target="test-factorial-multi-node-1"
            )
        )

    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    s1 = Scheduler(store, logic_group="test-factorial-multi-node-1")
    s1.register(factorial_node_1, name="factorial")
    s2 = Scheduler(store, logic_group="test-factorial-multi-node-2")
    s2.register(factorial_node_2, name="factorial")
    n = 5
    p: Promise[int] = s1.run(f"factorial-multi-node-{n}", factorial_node_1, n)
    assert p.result() == 120  # noqa: PLR2004


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_trigger_on_other_node() -> None:
    def workflow(ctx: Context) -> Generator[Yieldable, Any, str]:
        v1: int = yield ctx.rfc(
            remote_function(
                func_name="foo",
                args=[1],
                target="test-trigger-on-other-node-other",
            ),
        )
        v2: int = yield ctx.rfc(
            remote_function(
                func_name="bar",
                args=["Killua"],
                target="test-trigger-on-other-node-other",
            )
        )
        return f"{v2} is {v1}"

    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    s1 = Scheduler(store, logic_group="test-trigger-on-other-node")
    s1.register(workflow, retry_policy=never())

    def _foo(ctx: Context, n: int) -> int:  # noqa: ARG001
        return n

    def _bar(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    s2 = Scheduler(store, logic_group="test-trigger-on-other-node-other")
    s2.register(_foo, retry_policy=never(), name="foo")
    s2.register(_bar, retry_policy=never(), name="bar")
    p: Promise[str] = s1.run("test-trigger-on-other-node", workflow)
    s1.wait_until_blocked()
    assert not p.done()
    assert p.result() == "Killua is 1"


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_factorial_mechanics() -> None:
    node_group = "test-factorial-mechanics"

    def factorial(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1
        promise_id = f"factorial-mechanics-{n-1}"
        v: int
        if n % randint(1, 10) == 0:  # noqa: S311
            v = yield ctx.rfc(factorial, n - 1).with_options(
                promise_id=promise_id, target=node_group
            )
        else:
            v = yield ctx.lfc(factorial, n - 1).with_options(
                promise_id=promise_id, retry_policy=never()
            )

        return n + v

    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    schedulers: list[Scheduler] = []
    for _ in range(randint(1, 5)):  # noqa: S311
        s = Scheduler(store, logic_group=node_group)
        s.register(factorial, retry_policy=never())
        schedulers.append(s)
    n = randint(10, 30)  # noqa: S311

    p: Promise[int] = schedulers[0].run(f"factorial-mechanics-{n}", factorial, n)
    assert p.result()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_serverless_mechanics() -> None:
    node_group = "test-serverless-mechanics"
    lambda_node_group = "test-serverless-mechanics-lambda"

    def workflow(ctx: Context) -> Generator[Yieldable, Any, int]:
        v: int = yield ctx.rfc(
            remote_function(
                promise_id=None,
                func_name="factorial",
                args=[5],
                target="test-serverless-mechanics-lambda",
            )
        )

        return v

    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    main_node = Scheduler(store, logic_group=node_group)
    main_node.register(workflow, retry_policy=never())
    p: Promise[int] = main_node.run("test-serverless-mechanics", workflow)

    def factorial(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1
        time.sleep(0.5)  # This is not recommended. Just for testing.
        return n * (yield ctx.rfc(factorial, n - 1))

    node_delations: int = 0
    while not p.done():
        serverless_node = Scheduler(store, logic_group=lambda_node_group)
        serverless_node.register(factorial)
        serverless_node.wait_until_blocked(timeout=0.1)
        del serverless_node
        node_delations += 1

    assert p.done()
    assert p.result() == 120  # noqa: PLR2004
    assert node_delations > 1


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_fibonnaci_mechanics_awaiting() -> None:
    node_group = "test-fibonnaci-mechanics-awaiting"

    def fibonnaci(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n

        promise_id_n1 = f"fibonnaci-mechanics-awaiting-{n-1}"
        n1: int
        if n % randint(1, 10) == 0:  # noqa: S311
            n1 = yield ctx.rfc(fibonnaci, n - 1).with_options(
                promise_id=promise_id_n1, target=node_group
            )
        else:
            n1 = yield ctx.lfc(fibonnaci, n - 1).with_options(
                promise_id=promise_id_n1, retry_policy=never()
            )

        promise_id_n2 = f"fibonnaci-mechanics-awaiting-{n-2}"
        n2: int
        if n % randint(1, 10) == 0:  # noqa: S311
            n2 = yield ctx.rfc(fibonnaci, n - 2).with_options(
                promise_id=promise_id_n2, target=node_group
            )
        else:
            n2 = yield ctx.lfc(fibonnaci, n - 2).with_options(
                promise_id=promise_id_n2, retry_policy=never()
            )
        return n1 + n2

    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    schedulers: list[Scheduler] = []
    for _ in range(randint(1, 5)):  # noqa: S311
        s = Scheduler(store, logic_group=node_group)
        s.register(fibonnaci, retry_policy=never())
        schedulers.append(s)
    n = randint(10, 30)  # noqa: S311

    p: Promise[int] = schedulers[0].run(
        f"fibonnaci-mechanics-awaiting-{n}", fibonnaci, n
    )
    assert p.result()


@pytest.mark.skip
@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_fibonnaci_mechanics_no_awaiting() -> None:
    node_group = "test-fibonnaci-mechanics-no-awaiting"

    def fibonnaci(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n

        promise_id_n1 = f"fibonnaci-mechanics-no-awaiting-{n-1}"
        pn1: Promise[int]
        if n % randint(1, 10) == 0:  # noqa: S311
            pn1 = yield ctx.rfi(fibonnaci, n - 1).with_options(
                promise_id=promise_id_n1, target=node_group
            )
        else:
            pn1 = yield ctx.lfi(fibonnaci, n - 1).with_options(
                promise_id=promise_id_n1, retry_policy=never()
            )

        promise_id_n2 = f"fibonnaci-mechanics-no-awaiting-{n-2}"
        pn2: Promise[int]
        if n % randint(1, 10) == 0:  # noqa: S311
            pn2 = yield ctx.rfi(fibonnaci, n - 2).with_options(
                promise_id=promise_id_n1, target=node_group
            )
        else:
            pn2 = yield ctx.lfi(fibonnaci, n - 2).with_options(
                promise_id=promise_id_n2, retry_policy=never()
            )

        n1 = yield pn1
        n2 = yield pn2
        return n1 + n2

    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    schedulers: list[Scheduler] = []
    for _ in range(randint(1, 5)):  # noqa: S311
        s = Scheduler(store, logic_group=node_group)
        s.register(fibonnaci, retry_policy=never())
        schedulers.append(s)
    n = 8

    p: Promise[int] = schedulers[0].run(
        f"fibonnaci-mechanics-no-awaiting-{n}", fibonnaci, n
    )

    assert p.result() == 13  # noqa: PLR2004


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_trigger_on_other_process() -> None:
    node_group = "test-trigger-on-other-process"

    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])

    def factorial(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1
        return n * (yield ctx.rfc(factorial, n - 1))

    this_scheduler = Scheduler(store, logic_group=node_group)
    other_scheduler = Scheduler(store, logic_group=f"{node_group}-other")
    other_scheduler.register(factorial)
    p: Promise[int] = this_scheduler.trigger(
        f"{node_group}-factorial", "factorial", [5], target=f"{node_group}-other"
    )
    assert p.result() == 120  # noqa: PLR2004
