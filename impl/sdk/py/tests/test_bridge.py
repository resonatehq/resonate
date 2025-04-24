from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import pytest

from resonate.resonate import Resonate
from resonate.retry_policies.constant import Constant

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.coroutine import Yieldable
    from resonate.models.message_source import MessageSource
    from resonate.models.store import Store
    from resonate.resonate import Context


def foo_lfi(ctx: Context) -> Generator:
    p = yield ctx.lfi(bar_lfi)
    v = yield p
    return v


def bar_lfi(ctx: Context) -> Generator:
    p = yield ctx.lfi(baz)
    v = yield p
    return v


def foo_rfi(ctx: Context) -> Generator:
    p = yield ctx.rfi(bar_rfi)
    v = yield p
    return v


def bar_rfi(ctx: Context) -> Generator:
    p = yield ctx.rfi(baz)
    v = yield p
    return v


def baz(ctx: Context) -> str:
    return "baz"


def fib_lfi(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    p1 = yield ctx.lfi(fib_lfi, n - 1).options(id=f"fli{n - 1}")
    p2 = yield ctx.lfi(fib_lfi, n - 2).options(id=f"fli{n - 2}")

    v1 = yield p1
    v2 = yield p2

    return v1 + v2


def fib_lfc(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    v1 = yield ctx.lfc(fib_lfc, n - 1).options(id=f"flc{n - 1}")
    v2 = yield ctx.lfc(fib_lfc, n - 2).options(id=f"flc{n - 2}")

    return v1 + v2


def fib_rfi(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    p1 = yield ctx.rfi(fib_rfi, n - 1).options(id=f"fri{n - 1}")
    p2 = yield ctx.rfi(fib_rfi, n - 2).options(id=f"fri{n - 2}")

    v1 = yield p1
    v2 = yield p2

    return v1 + v2


def fib_rfc(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    v1 = yield ctx.rfc(fib_rfc, n - 1).options(id=f"frc{n - 1}")
    v2 = yield ctx.rfc(fib_rfc, n - 2).options(id=f"frc{n - 2}")

    return v1 + v2


def sleep(ctx: Context) -> Generator[Yieldable, Any, int]:
    yield ctx.sleep(0)
    return 1


def add_one(ctx: Context, n: int) -> int:
    return n + 1


def get_dependency(ctx: Context) -> int:
    dep = ctx.get_dependency("foo")
    assert dep is not None
    return dep + 1


def rfi_add_one_by_name(ctx: Context, n: int) -> Generator[Any, Any, int]:
    v = yield ctx.rfc("add_one", n)
    return v


def hitl(ctx: Context, id: str | None) -> Generator[Yieldable, Any, int]:
    if id:
        p = yield ctx.promise().options(id=id)
    else:
        p = yield ctx.promise()
    v = yield p
    return v


def random_generation(ctx: Context) -> Generator[Yieldable, Any, float]:
    return (yield ctx.random.randint(0, 10))


@pytest.fixture(scope="module")
def resonate_instance(store: Store, message_source: MessageSource) -> Generator[Resonate, None, None]:
    resonate = Resonate(store=store, message_source=message_source)
    resonate.register(foo_lfi)
    resonate.register(bar_lfi)
    resonate.register(foo_rfi)
    resonate.register(bar_rfi)
    resonate.register(baz)
    resonate.register(fib_lfi)
    resonate.register(fib_lfc)
    resonate.register(fib_rfi)
    resonate.register(fib_rfc)
    resonate.register(sleep)
    resonate.register(add_one)
    resonate.register(rfi_add_one_by_name)
    resonate.register(get_dependency)
    resonate.register(hitl)
    resonate.register(random_generation)
    resonate.start()
    yield resonate
    resonate.stop()

    # this timeout is set to cover the timeout time of the test poller, you can
    # see where this is set in conftest.py
    time.sleep(3)


def test_random_generation(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"random-gen-{timestamp}", random_generation)
    v = handle.result()
    assert v == resonate_instance.run(f"random-gen-{timestamp}", random_generation).result()


@pytest.mark.parametrize("id", ["foo", None])
def test_hitl(resonate_instance: Resonate, id: str | None) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"hitl-{timestamp}", hitl, id)
    time.sleep(1)
    resonate_instance.promises.resolve(id=id or f"hitl-{timestamp}.1", data=1)
    assert handle.result() == 1


def test_get_dependency(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    resonate_instance.set_dependency("foo", 1)
    handle = resonate_instance.run(f"get-dependency-{timestamp}", get_dependency)
    assert handle.result() == 2


def test_basic_lfi(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"foo-lfi-{timestamp}", foo_lfi)
    assert handle.result() == "baz"


def test_basic_rfi(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"foo-rfi-{timestamp}", foo_rfi)
    assert handle.result() == "baz"


def test_rfi_by_name(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.rpc(f"add_one_by_name_rfi-{timestamp}", "rfi_add_one_by_name", 42)
    assert handle.result() == 43


def test_fib_lfi(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"fib_lfi-{timestamp}", fib_lfi, 10)
    fib_10 = 55
    assert handle.result() == fib_10


def test_fib_rfi(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"fib_rfi-{timestamp}", fib_rfi, 10)
    fib_10 = 55
    assert handle.result() == fib_10


def test_fib_lfc(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"fib_lfc-{timestamp}", fib_lfc, 10)
    fib_10 = 55
    assert handle.result() == fib_10


def test_fib_rfc(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"fib_rfc-{timestamp}", fib_rfc, 10)
    fib_10 = 55
    assert handle.result() == fib_10


def test_sleep(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"sleep-{timestamp}", sleep)
    assert handle.result() == 1


def test_basic_retries() -> None:
    # Use a different instance that only do local store
    resonate = Resonate()

    def retriable(ctx: Context) -> int:
        if ctx.info.attempt == 4:
            return ctx.info.attempt
        raise RuntimeError

    f = resonate.register(retriable)
    resonate.start()

    start_time = time.time()
    handle = f.options(retry_policy=Constant(delay=1, max_retries=3)).run(f"retriable-{int(start_time)}")
    result = handle.result()
    end_time = time.time()

    assert result == 4
    delta = end_time - start_time
    assert delta >= 3.0
    assert delta < 4.0  # This is kind of arbitrary, if it is failing feel free to increase the number

    resonate.stop()


def test_listen(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.rpc(f"add_one_{timestamp}", "add_one", 42)
    assert handle.result() == 43


def test_implicit_resonate_start() -> None:
    resonate = Resonate()

    def f(ctx: Context, n: int) -> Generator[Any, Any, int]:
        if n == 0:
            return 1

        v = yield ctx.rfc(f, n - 1)
        return v + n

    r = resonate.register(f)

    timestamp = int(time.time())
    handle = r.run(f"r-implicit-start-{timestamp}", 1)
    result = handle.result()
    assert result == 2
