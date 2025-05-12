from __future__ import annotations

import sys
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any, Literal
from unittest.mock import patch

import pytest

from resonate.errors.errors import ResonateShutdownError, ResonateStoreError
from resonate.resonate import Resonate
from resonate.retry_policies import Constant, Never
from resonate.stores.local import LocalStore

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


def sleep(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
    yield ctx.sleep(n)
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


def info1(ctx: Context, idempotency_key: str, tags: dict[str, str], timeout: float, version: int) -> None:
    assert ctx.info.attempt == 1
    assert ctx.info.idempotency_key == idempotency_key
    assert ctx.info.tags == tags
    assert ctx.info.version == version
    # TODO(dfarr): how to assert the timeout?


def info2(ctx: Context, *args: Any, **kwargs: Any) -> Generator[Yieldable, Any, None]:
    info1(ctx, *args, **kwargs)
    yield ctx.lfc(info1, f"{ctx.id}.1", {"resonate:scope": "local"}, 0, 1)
    yield ctx.rfc(info1, f"{ctx.id}.2", {"resonate:scope": "global", "resonate:invoke": "poll://default"}, 0, 1)
    yield (yield ctx.lfi(info1, f"{ctx.id}.3", {"resonate:scope": "local"}, 0, 1))
    yield (yield ctx.rfi(info1, f"{ctx.id}.4", {"resonate:scope": "global", "resonate:invoke": "poll://default"}, 0, 1))
    yield (yield ctx.detached(info1, f"{ctx.id}.5", {"resonate:scope": "global", "resonate:invoke": "poll://default"}, 0, 1))


def parent_bound(ctx: Context, child_timeout_rel: int, mode: Literal["rfc", "lfc"]) -> Generator[Yieldable, Any, None]:
    match mode:
        case "lfc":
            yield ctx.lfc(child_bounded, ctx.info.timeout).options(timeout=child_timeout_rel)
        case "rfc":
            yield ctx.rfc(child_bounded, ctx.info.timeout).options(timeout=child_timeout_rel)


def child_bounded(ctx: Context, parent_timeout_abs: float) -> None:
    assert not (ctx.info.timeout > parent_timeout_abs)  # child timeout never exceeds parent timeout


def unbound_detached(
    ctx: Context,
    parent_timeout_rel: int,
    child_timeout_rel: int,
) -> Generator[Yieldable, Any, None]:
    p = yield ctx.detached(child_unbounded, parent_timeout_rel, child_timeout_rel, ctx.info.timeout).options(timeout=child_timeout_rel)
    yield p


def child_unbounded(ctx: Context, parent_timeout_rel: int, child_timeout_rel: int, parent_timeout_abs: float) -> None:
    if parent_timeout_rel < child_timeout_rel:
        assert ctx.info.timeout > parent_timeout_abs
    elif parent_timeout_rel > child_timeout_rel:
        assert ctx.info.timeout < parent_timeout_abs
    else:
        assert pytest.approx(ctx.info.timeout) == parent_timeout_abs


def wkflw(ctx: Context, durable: bool) -> Generator[Yieldable, Any, None]:
    yield ctx.lfc(failure_fn).options(timeout=1, retry_policy=Constant(delay=10, max_retries=1_000_000), durable=durable)


def failure_fn(ctx: Context) -> None:
    raise RuntimeError


def failure_wkflw(ctx: Context) -> Generator[Yieldable, Any, None]:
    yield ctx.lfc(add_one, 1)
    raise RuntimeError


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
    resonate.register(info1, name="info", version=1)
    resonate.register(info2, name="info", version=2)
    resonate.register(parent_bound)
    resonate.register(child_bounded)
    resonate.register(unbound_detached)
    resonate.register(child_unbounded)
    resonate.register(wkflw)
    resonate.register(failure_wkflw)
    resonate.start()
    yield resonate
    resonate.stop()

    # this timeout is set to cover the timeout time of the test poller, you can
    # see where this is set in conftest.py
    time.sleep(3)


@pytest.mark.parametrize("durable", [True, False])
def test_fail_inmediatelly_fn(resonate_instance: Resonate, durable: bool) -> None:
    with pytest.raises(RuntimeError):
        resonate_instance.run(f"fail-inmediatelly-fn-{uuid.uuid4()}", wkflw, durable).result()


def test_fail_inmediatelly_coro(resonate_instance: Resonate) -> None:
    with pytest.raises(RuntimeError):
        resonate_instance.options(timeout=1, retry_policy=Constant(delay=10, max_retries=1_000_000)).run(f"fail-inmediatelly-coro-{uuid.uuid4()}", failure_wkflw).result()


@pytest.mark.parametrize("mode", ["rfc", "lfc"])
@pytest.mark.parametrize(("parent_timeout", "child_timeout"), [(1100, 10), (10, 1100), (10, 10), (10, 11), (11, 10)])
def test_timeout_bound_by_parent(resonate_instance: Resonate, mode: Literal["rfc", "lfc"], parent_timeout: int, child_timeout: int) -> None:
    resonate_instance.options(timeout=parent_timeout).run(f"parent-bound-timeout-{uuid.uuid4()}", parent_bound, child_timeout, mode).result()


@pytest.mark.parametrize(
    ("parent_timeout", "child_timeout"),
    [
        (1100, 10),
        (10, 1100),
        (10, 10),
        (10, 11),
        (11, 10),
    ],
)
def test_timeout_unbound_by_parent_detached(resonate_instance: Resonate, parent_timeout: int, child_timeout: int) -> None:
    resonate_instance.options(timeout=parent_timeout).run(f"parent-bound-timeout-{uuid.uuid4()}", unbound_detached, parent_timeout, child_timeout).result()


def test_random_generation(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"random-gen-{timestamp}", random_generation)
    v = handle.result()
    assert v == resonate_instance.run(f"random-gen-{timestamp}", random_generation).result()


@pytest.mark.parametrize("id", ["foo", None])
def test_hitl(resonate_instance: Resonate, id: str | None) -> None:
    uid = uuid.uuid4()
    handle = resonate_instance.run(f"hitl-{uid}", hitl, id)
    time.sleep(1)
    resonate_instance.promises.resolve(id=id or f"hitl-{uid}.1", data=1)
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
    handle = resonate_instance.run(f"sleep-{timestamp}", sleep, 0)
    assert handle.result() == 1


def test_handle_timeout(resonate_instance: Resonate) -> None:
    timestamp = int(time.time())
    handle = resonate_instance.run(f"handle-timeout-{timestamp}", sleep, 1)
    with pytest.raises(TimeoutError):
        handle.result(timeout=0.1)
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


@pytest.mark.parametrize("idempotency_key", ["foo", None])
@pytest.mark.parametrize("tags", [{"foo": "bar"}, None])
@pytest.mark.parametrize("target", ["foo", None])
@pytest.mark.parametrize("timeout", [100, 200, None])
@pytest.mark.parametrize("version", [1, 2])
def test_info(
    idempotency_key: str | None,
    resonate_instance: Resonate,
    tags: dict[str, str] | None,
    target: str | None,
    timeout: float | None,
    version: int,
) -> None:
    id = f"info-{uuid.uuid4()}"

    resonate = resonate_instance.options(
        idempotency_key=idempotency_key,
        retry_policy=Never(),
        tags=tags,
        target=target,
        timeout=timeout,
        version=version,
    )

    handle = resonate.run(
        id,
        "info",
        idempotency_key or id,
        {**(tags or {}), "resonate:scope": "global", "resonate:invoke": target or "poll://default"},
        timeout or 31536000,
        version,
    )

    handle.result()


def test_resonate_get(resonate_instance: Resonate) -> None:
    def resolve_promise_slow(id: str) -> None:
        time.sleep(1)
        resonate_instance.promises.resolve(id=id, data=42)

    timestamp = int(time.time())
    id = f"get.{timestamp}"
    resonate_instance.promises.create(id=id, timeout=sys.maxsize)
    thread = threading.Thread(target=resolve_promise_slow, args=(id,))  # Do this in a different thread to simulate concurrency

    handle = resonate_instance.get(id)
    thread.start()
    res = handle.result()
    assert res == 42
    thread.join()


def test_resonate_platform_errors() -> None:
    # If you look at this test and you think: "This is horrible"
    # You are right, this test is cursed. But it needed to be done.
    local_store = LocalStore()
    resonate = Resonate(
        store=local_store,
        message_source=local_store.message_source("default", "default"),
    )

    original_transition = local_store.promises.transition
    raise_flag = [False]  # Use mutable container for flag

    def side_effect(*args: Any, **kwargs: Any) -> Any:
        if raise_flag[0]:
            msg = "Got an error from server"
            raise ResonateStoreError(msg, "UNKNOWN")

        return original_transition(*args[1:], **kwargs)

    def g(_: Context) -> int:
        return 42

    def f(ctx: Context, flag: bool) -> Generator[Any, Any, None]:
        raise_flag[0] = flag  # Update mutable flag
        val = yield ctx.rfc(g)
        return val

    with patch.object(
        local_store.promises,
        "transition",
        side_effect=side_effect,
    ):
        resonate.register(f)
        resonate.register(g)

        # First test normal behavior
        handle = resonate.run("f-no-err", f, flag=False)
        assert handle.result() == 42

        # Now trigger errors
        handle = resonate.run("f-err", f, flag=True)
        with pytest.raises(ResonateShutdownError):
            handle.result()
