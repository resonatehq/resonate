from __future__ import annotations

import json
import os
import random
import time
from functools import cache, lru_cache
from typing import TYPE_CHECKING, Any

import pytest

from resonate import Handle, Resonate
from resonate.promise import Promise
from resonate.retry_policy import constant, exponential, linear, never
from resonate.stores import LocalStore, RemoteStore
from resonate.targets import poll
from resonate.task_sources import Poller

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.typing import Yieldable


@lru_cache
def _fib(n: int) -> int:
    if n <= 1:
        return n
    return _fib(n - 1) + _fib(n - 2)


@lru_cache
def _factorial(n: int) -> int:
    if n == 0:
        return 1
    return n * _factorial(n - 1)


@cache
def _promise_storages() -> list[LocalStore | RemoteStore]:
    stores: list[LocalStore | RemoteStore] = [LocalStore()]
    if os.getenv("RESONATE_STORE_URL") is not None:
        stores.append(RemoteStore(url=os.environ["RESONATE_STORE_URL"]))
    return stores


@pytest.mark.parametrize("store", _promise_storages())
def test_calling_only_sync_function(store: LocalStore | RemoteStore) -> None:
    def foo_sync(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(store=store)
    resonate.register(foo_sync)
    p: Handle[str] = resonate.run("foo-sync", foo_sync, "hi")
    assert p.result() == "hi"
    resonate.stop()


@pytest.mark.parametrize("store", _promise_storages())
def test_calling_only_async_function(store: LocalStore | RemoteStore) -> None:
    async def foo_async(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(store=store)
    resonate.register(foo_async)
    p: Handle[str] = resonate.run("foo-async", foo_async, "hi")
    assert p.result() == "hi"
    resonate.stop()


@pytest.mark.parametrize("store", _promise_storages())
def test_golden_device_lfi(store: LocalStore | RemoteStore) -> None:
    def foo_golden_device_lfi(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.lfi(bar_golden_device_lfi, n).options(
            durable=random.choice([True, False])  # noqa: S311
        )
        assert isinstance(p, Promise)
        v: str = yield p
        assert isinstance(v, str)
        return v

    def bar_golden_device_lfi(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(store=store)
    resonate.register(foo_golden_device_lfi)
    p: Handle[str] = resonate.run("test-golden-device-lfi", foo_golden_device_lfi, "hi")
    assert isinstance(p, Handle)
    assert p.result() == "hi"
    resonate.stop()


@pytest.mark.parametrize("store", _promise_storages())
def test_factorial_lfi(store: LocalStore | RemoteStore) -> None:
    def exec_id(n: int) -> str:
        return f"test-factorial-lfi-{n}"

    def factorial_lfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1
        p = yield ctx.lfi(factorial_lfi, n - 1).options(
            id=exec_id(n - 1),
            durable=random.choice([True, False]),  # noqa: S311
        )
        return n * (yield p)

    resonate = Resonate(store=store)
    resonate.register(factorial_lfi)
    n = 30
    p: Handle[int] = resonate.run(exec_id(n), factorial_lfi, n)
    assert p.result() == _factorial(n)
    resonate.stop()


@pytest.mark.parametrize("store", _promise_storages())
def test_fibonacci_preorder_lfi(store: LocalStore | RemoteStore) -> None:
    def exec_id(n: int) -> str:
        return f"test-fib-preorder-lfi-{n}"

    def fib_lfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n
        p1 = yield ctx.lfi(fib_lfi, n - 1).options(
            id=exec_id(n - 1),
            durable=random.choice([True, False]),  # noqa: S311
        )
        p2 = yield ctx.lfi(fib_lfi, n - 2).options(
            id=exec_id(n - 2),
            durable=random.choice([True, False]),  # noqa: S311
        )
        n1 = yield p1
        n2 = yield p2
        return n1 + n2

    resonate = Resonate(store=store)
    resonate.register(fib_lfi)
    n = 30
    p: Handle[int] = resonate.run(exec_id(n), fib_lfi, n)
    assert p.result() == _fib(n)
    resonate.stop()


@pytest.mark.parametrize("store", _promise_storages())
def test_fibonacci_postorder_lfi(store: LocalStore | RemoteStore) -> None:
    def exec_id(n: int) -> str:
        return f"test-fib-postorder-lfi-{n}"

    def fib_lfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n
        p1 = yield ctx.lfi(fib_lfi, n - 1).options(
            id=exec_id(n - 1),
            durable=random.choice([True, False]),  # noqa: S311
        )
        p2 = yield ctx.lfi(fib_lfi, n - 2).options(
            id=exec_id(n - 2),
            durable=random.choice([True, False]),  # noqa: S311
        )
        n2 = yield p2
        n1 = yield p1
        return n1 + n2

    resonate = Resonate(store=store)
    resonate.register(fib_lfi)
    n = 30
    p: Handle[int] = resonate.run(exec_id(n), fib_lfi, n)
    assert p.result() == _fib(n)
    resonate.stop()


@pytest.mark.parametrize("store", _promise_storages())
def test_golden_device_lfc(store: RemoteStore | LocalStore) -> None:
    def foo_golden_device_lfc(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        v: str = yield ctx.lfc(bar_golden_device_lfi, n).options(
            durable=random.choice([True, False])  # noqa: S311
        )
        assert isinstance(v, str)
        return v

    def bar_golden_device_lfi(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(store=store)
    resonate.register(foo_golden_device_lfc)
    p: Handle[str] = resonate.run("test-golden-device-lfc", foo_golden_device_lfc, "hi")
    assert isinstance(p, Handle)
    assert p.result() == "hi"
    resonate.stop()


@pytest.mark.parametrize("store", _promise_storages())
def test_factorial_lfc(store: LocalStore | RemoteStore) -> None:
    def exec_id(n: int) -> str:
        return f"test-factorial-lfc-{n}"

    def factorial_lfc(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1

        return n * (
            yield ctx.lfc(factorial_lfc, n - 1).options(
                id=exec_id(n - 1),
                durable=random.choice([True, False]),  # noqa: S311
            )
        )

    resonate = Resonate(store=store)
    resonate.register(factorial_lfc)
    n = 30
    p: Handle[int] = resonate.run(exec_id(n), factorial_lfc, n)
    assert p.result() == _factorial(n)
    resonate.stop()


@pytest.mark.parametrize("store", _promise_storages())
def test_fibonacci_lfc(store: LocalStore | RemoteStore) -> None:
    def exec_id(n: int) -> str:
        return f"test-fib-lfc-{n}"

    def fib_lfc(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n
        n1 = yield ctx.lfc(fib_lfc, n - 1).options(
            id=exec_id(n - 1),
            durable=random.choice([True, False]),  # noqa: S311
        )
        n2 = yield ctx.lfc(fib_lfc, n - 2).options(
            id=exec_id(n - 2),
            durable=random.choice([True, False]),  # noqa: S311
        )
        return n1 + n2

    resonate = Resonate(store=store)
    resonate.register(fib_lfc)
    n = 30
    p: Handle[int] = resonate.run(exec_id(n), fib_lfc, n)
    assert p.result() == _fib(n)
    resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_golden_device_rfi() -> None:
    group = "test-golden-device-rfi"

    def foo_golden_device_rfi(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.rfi("bar_golden_device_rfi", n).options(
            id="bar", send_to=poll(group)
        )
        assert isinstance(p, Promise)
        v: str = yield p
        assert isinstance(v, str)
        return v

    def bar_golden_device_rfi(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    resonate.register(foo_golden_device_rfi)
    resonate.register(bar_golden_device_rfi)
    p: Handle[str] = resonate.run(f"{group}-foo", foo_golden_device_rfi, "hi")
    assert isinstance(p, Handle)
    assert p.result() == "hi"
    resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_factorial_rfi() -> None:
    group = "test-factorial-rfi"

    def exec_id(n: int) -> str:
        return f"test-factorial-rfi-{n}"

    def factorial_rfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1
        p = yield ctx.rfi("factorial_rfi", n - 1).options(
            id=exec_id(n - 1), send_to=poll(group)
        )
        return n * (yield p)

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    resonate.register(factorial_rfi)
    n = 10
    p: Handle[int] = resonate.run(exec_id(n), factorial_rfi, n)
    assert p.result() == _factorial(n)
    resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_fibonacci_preorder_rfi() -> None:
    group = "test-fibonacci-preorder-rfi"

    def exec_id(n: int) -> str:
        return f"test-fib-preorder-rfi-{n}"

    def fib_rfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n
        p1 = yield ctx.rfi(fib_rfi, n - 1).options(
            id=exec_id(n - 1), send_to=poll(group)
        )
        n1 = yield p1
        p2 = yield ctx.rfi(fib_rfi, n - 2).options(
            id=exec_id(n - 2), send_to=poll(group)
        )
        n2 = yield p2
        return n1 + n2

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    resonate.register(fib_rfi)
    n = 10
    p: Handle[int] = resonate.run(exec_id(n), fib_rfi, n)
    assert p.result() == _fib(n)
    resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_fibonacci_postorder_rfi() -> None:
    group = "test-fibonacci-postorder-rfi"

    def exec_id(n: int) -> str:
        return f"test-fib-postorder-rfi-{n}"

    def fib_rfi(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n
        p1 = yield ctx.rfi(fib_rfi, n - 1).options(
            id=exec_id(n - 1), send_to=poll(group)
        )
        p2 = yield ctx.rfi(fib_rfi, n - 2).options(
            id=exec_id(n - 2), send_to=poll(group)
        )
        n1 = yield p1
        n2 = yield p2
        return n1 + n2

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    resonate.register(fib_rfi)
    n = 10

    p: Handle[int] = resonate.run(exec_id(n), fib_rfi, n)
    assert p.result() == _fib(n)
    resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_golden_device_rfi_and_lfc() -> None:
    group = "test-golden-device-rfi-and-lfc"

    def foo(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        v: str = yield ctx.lfc(bar, n).options(
            id="bar",
            durable=False,
        )
        return v

    def bar(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.rfi(baz, n).options(id="baz", send_to=poll(group))
        v: str = yield p
        return v

    def baz(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    resonate.register(foo)
    resonate.register(baz)
    p: Handle[str] = resonate.run(f"{group}-foo", foo, "hi")
    assert p.result() == "hi"
    resonate.stop()


@pytest.mark.skip
@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_fibonacci_full_random() -> None:
    rand = random.Random(444)  # noqa: S311

    def exec_id(n: int) -> str:
        return f"fib({n})"

    def fib(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n

        if rand.choice((True, False)):
            p1 = yield ctx.rfi(fib, n - 1).options(
                id=exec_id(n - 1),
            )
        else:
            p1 = yield ctx.lfi(fib, n - 1).options(
                id=exec_id(n - 1),
                durable=rand.choice((True, False)),
            )

        if rand.choice((True, False)):
            p2 = yield ctx.rfi(fib, n - 2).options(
                id=exec_id(n - 2),
            )
        else:
            p2 = yield ctx.lfi(fib, n - 2).options(
                id=exec_id(n - 2),
                durable=rand.choice((True, False)),
            )

        promises = [p1, p2]
        total: int = 0
        while promises:
            idx = rand.randint(0, len(promises) - 1)
            p = promises.pop(idx)
            v = yield p
            total += v

        return total

    resonate = Resonate(store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]))
    resonate.register(fib)
    n = 3
    p: Handle[int] = resonate.run(exec_id(n), fib, n)
    assert p.result() == _fib(n)
    resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_golden_device_rfi_and_lfc_with_decorator() -> None:
    group = "test-golden-device-rfi-and-lfc-with-decorator"

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )

    @resonate.register()
    def foo(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        v: str = yield ctx.lfc(bar, n).options(
            id="bar",
            durable=False,
        )
        return v

    def bar(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.rfi(baz, n).options(id="baz", send_to=poll(group))
        v: str = yield p
        return v

    @resonate.register()
    def baz(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    p: Handle[str] = foo.run(f"{group}-foo", n="hi")
    assert p.result() == "hi"
    resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_golden_device_rfc() -> None:
    group = "test-golden-device-rfc"

    def foo_golden_device_rfc(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        v: str = yield ctx.rfc(bar_golden_device_rfc, n).options(
            id="bar", send_to=poll(group)
        )
        assert isinstance(v, str)
        return v

    def bar_golden_device_rfc(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    rf = resonate.register(foo_golden_device_rfc)
    resonate.register(bar_golden_device_rfc)
    p: Handle[str] = rf.run(f"{group}-foo", "hi")
    assert isinstance(p, Handle)
    assert p.result() == "hi"
    resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_factorial_rfc() -> None:
    group = "test-factorial-rfc"

    def exec_id(n: int) -> str:
        return f"test-factorial-rfc-{n}"

    def factorial_rfc(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1
        return n * (
            yield ctx.rfc(factorial_rfc, n - 1).options(
                id=exec_id(n - 1), send_to=poll(group)
            )
        )

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    resonate.register(factorial_rfc)
    n = 10
    p: Handle[int] = resonate.run(exec_id(n), factorial_rfc, n)
    assert p.result() == _factorial(n)
    resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_fibonacci_preorder_rfc() -> None:
    group = "test-fibonacci-preorder-rfc"

    def exec_id(n: int) -> str:
        return f"test-fib-preorder-rfc-{n}"

    def fib_rfc(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n <= 1:
            return n
        n1 = yield ctx.rfc(fib_rfc, n - 1).options(
            id=exec_id(n - 1), send_to=poll(group)
        )
        n2 = yield ctx.rfc(fib_rfc, n - 2).options(
            id=exec_id(n - 2), send_to=poll(group)
        )
        return n1 + n2

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    resonate.register(fib_rfc)
    n = 10
    p: Handle[int] = resonate.run(exec_id(n), fib_rfc, n)
    assert p.result() == _fib(n)
    resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_golden_device_rfc_and_lfc() -> None:
    group = "test-golden-device-rfc-and-lfc"

    def foo(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        v: str = yield ctx.lfc(bar, n).options(
            id="bar",
            durable=False,
        )
        return v

    def bar(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        v: str = yield ctx.rfc(baz, n).options(id="baz", send_to=poll(group))
        return v

    def baz(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    resonate.register(foo)
    resonate.register(baz)
    p: Handle[str] = resonate.run(f"{group}-foo", foo, "hi")
    assert p.result() == "hi"
    resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_golden_device_rfc_and_lfc_with_decorator() -> None:
    group = "test-golden-device-rfc-and-lfc-with-decorator"

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )

    @resonate.register()
    def foo(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        v: str = yield ctx.lfc(bar, n).options(
            id="bar",
            durable=False,
        )
        return v

    def bar(ctx: Context, n: str) -> Generator[Yieldable, Any, str]:
        v: str = yield ctx.rfc(baz, n).options(id="baz", send_to=poll(group))
        return v

    @resonate.register()
    def baz(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    p: Handle[str] = foo.run(f"{group}-foo", n="hi")
    assert p.result() == "hi"
    resonate.stop()


@pytest.mark.parametrize("store", _promise_storages())
def test_retry_policies_local_coroutine(store: LocalStore | RemoteStore) -> None:
    def foo_retry_policy(ctx: Context, n: str) -> Generator[Yieldable, str, str]:
        yield ctx.lfc(bar_retry_policy, n)
        msg = "oops something went wrong"
        raise RuntimeError(msg)

    def bar_retry_policy(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    for policy in (
        never(),
        constant(delay=0, max_retries=3),
        linear(delay=0, max_retries=3),
        exponential(base_delay=0, factor=1, max_retries=1, max_delay=5),
    ):
        resonate = Resonate(store=store)
        resonate.register(foo_retry_policy, retry_policy=policy)
        p: Handle[str] = resonate.run(
            f"foo-retry-policy-local-coroutine-{policy!s}", foo_retry_policy, "hi"
        )
        with pytest.raises(RuntimeError):
            p.result()
        resonate.stop()


@pytest.mark.parametrize("store", _promise_storages())
def test_retry_policies_local_func(store: LocalStore | RemoteStore) -> None:
    def foo_retry_policy(ctx: Context, n: str) -> str:  # noqa: ARG001
        msg = "oops something went wrong"
        raise RuntimeError(msg)

    for policy in (
        never(),
        constant(delay=0, max_retries=3),
        linear(delay=0, max_retries=3),
        exponential(base_delay=0, factor=1, max_retries=1, max_delay=5),
    ):
        resonate = Resonate(store=store)
        resonate.register(foo_retry_policy, retry_policy=policy)
        p: Handle[str] = resonate.run(
            f"foo-retry-policy-local-func-{policy!s}", foo_retry_policy, "hi"
        )
        with pytest.raises(RuntimeError):
            p.result()
        resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_human_in_the_loop() -> None:
    group = "test-human-in-the-loop"

    def human_in_the_loop(ctx: Context) -> Generator[Yieldable, Any, str]:
        p_name: Promise[str] = yield ctx.promise(
            "test-human-in-loop-question-to-answer-1"
        )
        name: str = yield p_name

        p_age: Promise[int] = yield ctx.promise(
            id="test-human-in-loop-question-to-answer-2"
        )
        age: int = yield p_age
        return f"Hi {name} with age {age}"

    store = RemoteStore(url=os.environ["RESONATE_STORE_URL"])
    s = Resonate(store=store, task_source=Poller("http://localhost:8002", group=group))
    s.register(human_in_the_loop)
    p: Handle[str] = s.run("test-feature-human-in-the-loop", human_in_the_loop)
    time.sleep(3)
    store.promises.resolve(
        id="test-human-in-loop-question-to-answer-1",
        ikey=None,
        strict=False,
        headers=None,
        data=json.dumps("Peter"),
    )
    time.sleep(3)
    store.promises.resolve(
        id="test-human-in-loop-question-to-answer-2",
        ikey=None,
        strict=False,
        headers=None,
        data=json.dumps(50),
    )
    time.sleep(3)
    assert p.result() == "Hi Peter with age 50"
    s.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_sleep() -> None:
    group = "test-sleep"

    store = RemoteStore(url="http://localhost:8001")
    resonate = Resonate(
        store=store, task_source=Poller("http://localhost:8002", group=group)
    )

    @resonate.register()
    def foo_sleep(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        yield ctx.sleep(n)
        return n

    n = 1
    p: Handle[int] = foo_sleep.run(f"{group}-{n}", n)
    assert p.result() == n
    resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_golden_device_detached() -> None:
    group = "test-golden-device-detached"

    def foo_golden_device_detached(
        ctx: Context, n: str
    ) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.detached("bar", bar_golden_device_detached, n)
        yield ctx.detached("baz.1", baz_golden_device_detached, p.id)
        yield ctx.detached("baz.2", baz_golden_device_detached, p.id)
        yield ctx.detached("baz.3", baz_golden_device_detached, p.id)
        v: str = yield p
        return v

    def bar_golden_device_detached(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    def baz_golden_device_detached(
        ctx: Context,  # noqa: ARG001
        promise_id: str,
    ) -> Generator[Yieldable, Any, str]:
        v: str = yield Promise[str](promise_id)
        return v

    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )
    resonate.register(foo_golden_device_detached)
    resonate.register(bar_golden_device_detached)
    resonate.register(baz_golden_device_detached)
    p: Handle[str] = resonate.run(f"{group}-foo", foo_golden_device_detached, "hi")
    assert isinstance(p, Handle)
    assert p.result() == "hi"
    resonate.stop()


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_golden_device_detached_with_registered() -> None:
    group = "test-golden-device-detached"
    resonate = Resonate(
        store=RemoteStore(url=os.environ["RESONATE_STORE_URL"]),
        task_source=Poller("http://localhost:8002", group=group),
    )

    @resonate.register()
    def foo_golden_device_detached_with_registered(
        ctx: Context, n: str
    ) -> Generator[Yieldable, Any, str]:
        p: Promise[str] = yield ctx.detached(
            "bar", bar_golden_device_detached_with_registered, n
        )
        yield ctx.detached("baz.1", baz_golden_device_detached_with_registered, p.id)
        yield ctx.detached("baz.2", baz_golden_device_detached_with_registered, p.id)
        yield ctx.detached("baz.3", baz_golden_device_detached_with_registered, p.id)
        v: str = yield p
        return v

    @resonate.register()
    def bar_golden_device_detached_with_registered(ctx: Context, n: str) -> str:  # noqa: ARG001
        return n

    @resonate.register()
    def baz_golden_device_detached_with_registered(
        ctx: Context,  # noqa: ARG001
        promise_id: str,
    ) -> Generator[Yieldable, Any, str]:
        v: str = yield Promise[str](promise_id)
        return v

    p: Handle[str] = resonate.run(
        f"{group}-foo", foo_golden_device_detached_with_registered, "hi"
    )

    assert isinstance(p, Handle)
    assert p.result() == "hi"
    resonate.stop()
