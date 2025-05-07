from __future__ import annotations

import random
import sys
from collections.abc import Callable, Generator
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, assert_type
from unittest.mock import Mock

import pytest

from resonate import Context, Resonate
from resonate.conventions import Remote
from resonate.coroutine import LFC, LFI, RFC, RFI
from resonate.dependencies import Dependencies
from resonate.errors import ResonateValidationError
from resonate.models.commands import Command, Invoke, Listen
from resonate.models.handle import Handle
from resonate.options import Options
from resonate.registry import Registry
from resonate.resonate import Function
from resonate.retry_policies import Constant, Exponential, Linear, Never
from resonate.scheduler import Info

if TYPE_CHECKING:
    from resonate.models.retry_policy import RetryPolicy


def foo(ctx: Context, a: int, b: int) -> int: ...
def bar(ctx: Context, a: int, b: int) -> int: ...
def baz(ctx: Context, a: int, b: int) -> Generator[Any, Any, int]:
    yield ctx.lfc(bar, a, b)
    raise NotImplementedError


# Fixtures


@pytest.fixture
def resonate() -> Resonate:
    resonate = Resonate()
    resonate._started = True  # noqa: SLF001
    return resonate


@pytest.fixture
def registry() -> Registry:
    registry = Registry()
    registry.add(foo, "foo", version=1)
    registry.add(bar, "bar", version=2)
    registry.add(baz, "baz", version=3)
    return registry


# Helper functions


def cmd(resonate: Resonate) -> Command:
    item = resonate._bridge._cq.get_nowait()  # noqa: SLF001
    assert isinstance(item, tuple)

    cmd, _ = item
    return cmd


# Tests


@pytest.mark.parametrize("func", [foo, bar, baz, lambda x: x])
@pytest.mark.parametrize("name", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("version", [1, 2, 3])
def test_register(func: Callable, name: str | None, version: int) -> None:
    # skip lambda functions without name, validation tests will cover this
    if func.__name__ == "<lambda>" and name is None:
        return

    registry = Registry()
    resonate = Resonate(registry=registry)
    f = resonate.register(func, name=name, version=version)

    for v in (0, version):
        assert registry.get(name or func.__name__, v) == registry.get(func, v) == registry.get(f, v) == (name or func.__name__, func, version)


@pytest.mark.parametrize("name", ["bar", "baz", None])
@pytest.mark.parametrize("version", [1, 2, 3])
def test_register_decorator(name: str | None, version: int) -> None:
    registry = Registry()
    resonate = Resonate(registry=registry)

    @resonate.register
    def foo(ctx: Context) -> int: ...

    for v in (0, 1):
        assert registry.get("foo", v) == registry.get(foo, v) == ("foo", foo.func, 1)

    @resonate.register(name=name, version=version)
    def bar(ctx: Context) -> int: ...

    for v in (0, version):
        assert registry.get(name or "bar", v) == registry.get(bar, v) == (name or "bar", bar.func, version)


@pytest.mark.parametrize(
    ("func", "kwargs"),
    [
        (lambda x: x, {"name": "foo"}),
        (lambda x: x, {"name": "foo", "version": 1}),
        (lambda x: x, {"name": "foo", "version": 0}),
        (lambda x: x, {"name": "bar", "version": 2}),
        (lambda x: x, {"name": "bar", "version": 0}),
        (lambda x: x, {"name": "baz", "version": 3}),
        (lambda x: x, {"name": "baz", "version": 0}),
        (lambda x: x, {}),
        (lambda x: x, {"version": 1}),
        (foo, {"name": "bar"}),
        (bar, {"name": "baz"}),
        (baz, {"name": "foo"}),
    ],
)
def test_register_validations(registry: Registry, func: Callable, kwargs: dict) -> None:
    resonate = Resonate(registry=registry)
    with pytest.raises(ResonateValidationError):
        resonate.register(func, **kwargs)

    with pytest.raises(ResonateValidationError):
        resonate.register(**kwargs)(func)


@pytest.mark.parametrize("idempotency_key", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("retry_policy", [Constant(), Linear(), Exponential()])
@pytest.mark.parametrize("target", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("tags", [{"a": "1"}, {"2": "foo"}, {"a": "foo"}, None])
@pytest.mark.parametrize("timeout", [3, 2, 1, None])
@pytest.mark.parametrize("version", [1, 2, 3, None])
@pytest.mark.parametrize(
    ("func", "name", "args", "kwargs"),
    [
        (foo, "foo", (1, 2), {}),
        (bar, "bar", (1, 2), {}),
        (baz, "baz", (1, 2), {}),
        (foo, "foo", (), {"1": 1, "2": 2}),
        (bar, "bar", (), {"1": 1, "2": 2}),
        (baz, "baz", (), {"1": 1, "2": 2}),
    ],
)
def test_run(
    resonate: Resonate,
    idempotency_key: str | None,
    retry_policy: RetryPolicy | None,
    target: str | None,
    tags: dict[str, str] | None,
    timeout: int | None,
    version: int | None,
    func: Callable,
    name: str,
    args: tuple,
    kwargs: dict,
) -> None:
    f = resonate.register(func, name=name, version=version or 1)

    opts = {
        "idempotency_key": idempotency_key,
        "retry_policy": retry_policy,
        "target": target,
        "tags": tags,
        "timeout": timeout,
        "version": version,
    }

    default_opts = Options(version=version or 1)
    default_conv = Remote("f", name, args, kwargs, default_opts)

    updated_opts = Options(version=version or 1).merge(**opts)
    updated_conv = Remote("f", name, args, kwargs, updated_opts)

    assert updated_opts.idempotency_key == (idempotency_key or default_opts.idempotency_key)
    assert updated_opts.target == (target or default_opts.target)
    assert updated_opts.version == (version or default_opts.version)
    assert updated_opts.timeout == (timeout or default_opts.timeout)
    assert updated_opts.tags == (tags or default_opts.tags)

    def invoke(id: str) -> Invoke:
        conv = Remote(id, name, args, kwargs, default_opts)
        promise = resonate.promises.get(id=id)
        return Invoke(id, conv, promise.abs_timeout, func, args, kwargs, default_opts, promise)

    def invoke_with_opts(id: str) -> Invoke:
        conv = Remote(id, name, args, kwargs, updated_opts)
        promise = resonate.promises.get(id=id)
        return Invoke(id, conv, promise.abs_timeout, func, args, kwargs, updated_opts, promise)

    resonate.run("f1", func, *args, **kwargs)
    assert cmd(resonate) == invoke("f1")

    promise = resonate.promises.get(id="f1")
    assert promise.id == "f1"
    assert promise.ikey_for_create == "f1"
    assert promise.param.headers == {}  # TODO(dfarr): this should be None
    assert promise.param.data == {"func": name, "args": list(args), "kwargs": kwargs, "version": version or 1}
    assert promise.tags == {"resonate:invoke": default_opts.target, "resonate:scope": "global"}

    resonate.run("f2", name, *args, **kwargs)
    assert cmd(resonate) == invoke("f2")

    promise = resonate.promises.get(id="f2")
    assert promise.id == "f2"
    assert promise.ikey_for_create == "f2"
    assert promise.param.headers == {}
    assert promise.param.data == {"func": name, "args": list(args), "kwargs": kwargs, "version": version or 1}
    assert promise.tags == {**default_conv.tags, "resonate:scope": "global"} == {"resonate:invoke": default_opts.target, "resonate:scope": "global"}

    resonate.options(**opts).run("f3", func, *args, **kwargs)
    assert cmd(resonate) == invoke_with_opts("f3")

    promise = resonate.promises.get(id="f3")
    assert promise.id == "f3"
    assert promise.ikey_for_create == idempotency_key if idempotency_key else "f3"
    assert promise.param.headers == {}
    assert promise.param.data == {"func": name, "args": list(args), "kwargs": kwargs, "version": version or 1}
    assert promise.tags == {**updated_opts.tags, "resonate:invoke": updated_opts.target, "resonate:scope": "global"}

    resonate.options(**opts).run("f4", name, *args, **kwargs)
    assert cmd(resonate) == invoke_with_opts("f4")

    promise = resonate.promises.get(id="f4")
    assert promise.id == "f4"
    assert promise.ikey_for_create == idempotency_key if idempotency_key else "f4"
    assert promise.param.headers == {}
    assert promise.param.data == {"func": name, "args": list(args), "kwargs": kwargs, "version": version or 1}
    assert promise.tags == {**updated_conv.tags, "resonate:scope": "global"} == {**updated_opts.tags, "resonate:invoke": updated_opts.target, "resonate:scope": "global"}

    f.run("f5", *args, **kwargs)
    assert cmd(resonate) == invoke("f5")

    promise = resonate.promises.get(id="f5")
    assert promise.id == "f5"
    assert promise.ikey_for_create == "f5"
    assert promise.param.headers == {}
    assert promise.param.data == {"func": name, "args": list(args), "kwargs": kwargs, "version": version or 1}
    assert promise.tags == {**default_conv.tags, "resonate:scope": "global"} == {"resonate:invoke": default_opts.target, "resonate:scope": "global"}

    f.options(**opts).run("f6", *args, **kwargs)
    assert cmd(resonate) == invoke_with_opts("f6")

    promise = resonate.promises.get(id="f6")
    assert promise.id == "f6"
    assert promise.ikey_for_create == idempotency_key if idempotency_key else "f6"
    assert promise.param.headers == {}
    assert promise.param.data == {"func": name, "args": list(args), "kwargs": kwargs, "version": version or 1}
    assert promise.tags == {**updated_conv.tags, "resonate:scope": "global"} == {**updated_opts.tags, "resonate:invoke": updated_opts.target, "resonate:scope": "global"}


@pytest.mark.parametrize("idempotency_key", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("target", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("version", [1, 2, 3, None])
@pytest.mark.parametrize("timeout", [3, 2, 1, None])
@pytest.mark.parametrize("tags", [{"a": "1"}, {"2": "foo"}, {"a": "foo"}, None])
@pytest.mark.parametrize("retry_policy", [Constant(), Linear(), Exponential()])
@pytest.mark.parametrize(
    ("func", "name", "args", "kwargs"),
    [
        (foo, "foo", (1, 2), {}),
        (bar, "bar", (1, 2), {}),
        (baz, "baz", (1, 2), {}),
        (foo, "foo", (), {"1": 1, "2": 2}),
        (bar, "bar", (), {"1": 1, "2": 2}),
        (baz, "baz", (), {"1": 1, "2": 2}),
    ],
)
def test_rpc(
    resonate: Resonate,
    idempotency_key: str | None,
    retry_policy: RetryPolicy | None,
    target: str | None,
    tags: dict[str, str] | None,
    timeout: int | None,
    version: int | None,
    func: Callable,
    name: str,
    args: tuple,
    kwargs: dict,
) -> None:
    f = resonate.register(func, name=name, version=version or 1)

    opts = {
        "idempotency_key": idempotency_key,
        "retry_policy": retry_policy,
        "target": target,
        "tags": tags,
        "timeout": timeout,
        "version": version,
    }

    default_opts = Options(version=version or 1)
    default_conv = Remote("f", name, args, kwargs, default_opts)

    updated_opts = Options(version=version or 1).merge(**opts)
    updated_conv = Remote("f", name, args, kwargs, updated_opts)

    assert updated_opts.idempotency_key == (idempotency_key or default_opts.idempotency_key)
    assert updated_opts.target == (target or default_opts.target)
    assert updated_opts.version == (version or default_opts.version)
    assert updated_opts.timeout == (timeout or default_opts.timeout)
    assert updated_opts.tags == (tags or default_opts.tags)

    resonate.rpc("f1", func, *args, **kwargs)
    assert cmd(resonate) == Listen(id="f1")

    promise = resonate.promises.get(id="f1")
    assert promise.id == "f1"
    assert promise.ikey_for_create == "f1"
    assert promise.param.headers == {}  # TODO(dfarr): this should be None
    assert promise.param.data == {"func": name, "args": list(args), "kwargs": kwargs, "version": version or 1}
    assert promise.tags == {"resonate:invoke": default_opts.target, "resonate:scope": "global"}

    resonate.rpc("f2", name, *args, **kwargs)
    assert cmd(resonate) == Listen(id="f2")

    promise = resonate.promises.get(id="f2")
    assert promise.id == "f2"
    assert promise.ikey_for_create == "f2"
    assert promise.param.headers == {}
    assert promise.param.data == {"func": name, "args": list(args), "kwargs": kwargs, "version": version or 1}
    assert promise.tags == {**default_conv.tags, "resonate:scope": "global"} == {"resonate:invoke": default_opts.target, "resonate:scope": "global"}

    resonate.options(**opts).rpc("f3", func, *args, **kwargs)
    assert cmd(resonate) == Listen(id="f3")

    promise = resonate.promises.get(id="f3")
    assert promise.id == "f3"
    assert promise.ikey_for_create == idempotency_key if idempotency_key else "f3"
    assert promise.param.headers == {}
    assert promise.param.data == {"func": name, "args": list(args), "kwargs": kwargs, "version": version or 1}
    assert promise.tags == {**updated_opts.tags, "resonate:invoke": updated_opts.target, "resonate:scope": "global"}

    resonate.options(**opts).rpc("f4", name, *args, **kwargs)
    assert cmd(resonate) == Listen(id="f4")

    promise = resonate.promises.get(id="f4")
    assert promise.id == "f4"
    assert promise.ikey_for_create == idempotency_key if idempotency_key else "f4"
    assert promise.param.headers == {}
    assert promise.param.data == {"func": name, "args": list(args), "kwargs": kwargs, "version": version or 1}
    assert promise.tags == {**updated_conv.tags, "resonate:scope": "global"} == {**updated_opts.tags, "resonate:invoke": updated_opts.target, "resonate:scope": "global"}

    f.rpc("f5", *args, **kwargs)
    assert cmd(resonate) == Listen(id="f5")

    promise = resonate.promises.get(id="f5")
    assert promise.id == "f5"
    assert promise.ikey_for_create == "f5"
    assert promise.param.headers == {}
    assert promise.param.data == {"func": name, "args": list(args), "kwargs": kwargs, "version": version or 1}
    assert promise.tags == {**default_conv.tags, "resonate:scope": "global"} == {"resonate:invoke": default_opts.target, "resonate:scope": "global"}

    f.options(**opts).rpc("f6", *args, **kwargs)
    assert cmd(resonate) == Listen(id="f6")

    promise = resonate.promises.get(id="f6")
    assert promise.id == "f6"
    assert promise.ikey_for_create == idempotency_key if idempotency_key else "f6"
    assert promise.param.headers == {}
    assert promise.param.data == {"func": name, "args": list(args), "kwargs": kwargs, "version": version or 1}
    assert promise.tags == {**updated_conv.tags, "resonate:scope": "global"} == {**updated_opts.tags, "resonate:invoke": updated_opts.target, "resonate:scope": "global"}


@pytest.mark.parametrize(
    ("func", "kwargs"),
    [
        (foo, {"version": 2}),
        (bar, {"version": 3}),
        (baz, {"version": 1}),
        ("foo", {"version": 2}),
        ("bar", {"version": 3}),
        ("baz", {"version": 1}),
    ],
)
def test_run_validations(registry: Registry, func: Callable | str, kwargs: dict) -> None:
    resonate = Resonate(registry=registry)

    with pytest.raises(ResonateValidationError):
        resonate.options(**kwargs).run("f", func)


@pytest.mark.parametrize(
    ("func", "kwargs"),
    [
        (foo, {"version": 2}),
        (bar, {"version": 3}),
        (baz, {"version": 1}),
    ],
)
def test_rpc_validations(registry: Registry, func: Callable | str, kwargs: dict) -> None:
    resonate = Resonate(registry=registry)

    with pytest.raises(ResonateValidationError):
        resonate.options(**kwargs).rpc("f", func)


@pytest.mark.parametrize("id", ["foo", "bar", "baz"])
def test_get(resonate: Resonate, id: str) -> None:
    resonate.promises.create(id=id, timeout=sys.maxsize)
    resonate.get(id)
    assert cmd(resonate) == Listen(id=id)


def test_resonate_type_annotations() -> None:
    # The following are "tests", if there is an issue it will be found by pyright, at runtime
    # assert_type is effectively a noop.

    resonate = Resonate()

    # mock bridge so run and rpc become noops
    resonate._started = True  # noqa: SLF001
    resonate._bridge.run = Mock()  # noqa: SLF001
    resonate._bridge.rpc = Mock()  # noqa: SLF001

    @resonate.register
    def foo(ctx: Context, a: int, b: int, /) -> int: ...

    assert_type(foo, Function[[int, int], int])
    assert_type(foo.run, Callable[[str, int, int], Handle[int]])
    assert_type(foo.rpc, Callable[[str, int, int], Handle[int]])
    assert_type(foo.__call__, Callable[[Context, int, int], int])
    assert_type(resonate.run("f", foo, 1, 2), Handle[int])
    assert_type(resonate.rpc("f", foo, 1, 2), Handle[int])
    assert_type(resonate.run("f", "foo", 1, 2), Handle[Any])
    assert_type(resonate.rpc("f", "foo", 1, 2), Handle[Any])

    @resonate.register
    def bar(ctx: Context, a: int, b: int, /) -> Generator[Any, Any, int]: ...

    assert_type(bar, Function[[int, int], Generator[Any, Any, int]])
    assert_type(bar.run("f", 1, 2), Handle[int])
    assert_type(bar.rpc("f", 1, 2), Handle[int])
    assert_type(bar.__call__, Callable[[Context, int, int], Generator[Any, Any, int]])
    assert_type(resonate.run("f", bar, 1, 2), Handle[int])
    assert_type(resonate.rpc("f", bar, 1, 2), Handle[int])
    assert_type(resonate.run("f", "bar", 1, 2), Handle[Any])
    assert_type(resonate.rpc("f", "bar", 1, 2), Handle[Any])

    @resonate.register()
    def baz(ctx: Context, a: int, b: int, /) -> int: ...

    assert_type(baz, Function[[int, int], int])
    assert_type(baz.run, Callable[[str, int, int], Handle[int]])
    assert_type(baz.rpc, Callable[[str, int, int], Handle[int]])
    assert_type(baz.__call__, Callable[[Context, int, int], int])
    assert_type(resonate.run("f", baz, 1, 2), Handle[int])
    assert_type(resonate.rpc("f", baz, 1, 2), Handle[int])
    assert_type(resonate.run("f", "baz", 1, 2), Handle[Any])
    assert_type(resonate.rpc("f", "baz", 1, 2), Handle[Any])

    @resonate.register()
    def qux(ctx: Context, a: int, b: int, /) -> Generator[Any, Any, int]: ...

    assert_type(qux, Function[[int, int], Generator[Any, Any, int]])
    assert_type(qux.run("f", 1, 2), Handle[int])
    assert_type(qux.rpc("f", 1, 2), Handle[int])
    assert_type(qux.__call__, Callable[[Context, int, int], Generator[Any, Any, int]])
    assert_type(resonate.run("f", qux, 1, 2), Handle[int])
    assert_type(resonate.rpc("f", qux, 1, 2), Handle[int])
    assert_type(resonate.run("f", "qux", 1, 2), Handle[Any])
    assert_type(resonate.rpc("f", "qux", 1, 2), Handle[Any])

    def zog(ctx: Context, a: int, b: int, /) -> int: ...

    f = resonate.register(zog)
    assert_type(f, Function[[int, int], int])
    assert_type(f.run, Callable[[str, int, int], Handle[int]])
    assert_type(f.rpc, Callable[[str, int, int], Handle[int]])
    assert_type(f.__call__, Callable[[Context, int, int], int])
    assert_type(resonate.run("f", f, 1, 2), Handle[int])
    assert_type(resonate.rpc("f", f, 1, 2), Handle[int])
    assert_type(resonate.run("f", zog, 1, 2), Handle[int])
    assert_type(resonate.rpc("f", zog, 1, 2), Handle[int])
    assert_type(resonate.run("f", "zog", 1, 2), Handle[Any])
    assert_type(resonate.rpc("f", "zog", 1, 2), Handle[Any])

    def waz(ctx: Context, a: int, b: int, /) -> Generator[Any, Any, int]: ...

    g = resonate.register(waz)
    assert_type(g, Function[[int, int], Generator[Any, Any, int]])
    assert_type(g.run("g", 1, 2), Handle[int])
    assert_type(g.rpc("g", 1, 2), Handle[int])
    assert_type(g.__call__, Callable[[Context, int, int], Generator[Any, Any, int]])
    assert_type(resonate.run("g", g, 1, 2), Handle[int])
    assert_type(resonate.rpc("g", g, 1, 2), Handle[int])
    assert_type(resonate.run("g", waz, 1, 2), Handle[int])
    assert_type(resonate.rpc("g", waz, 1, 2), Handle[int])
    assert_type(resonate.run("g", "waz", 1, 2), Handle[Any])
    assert_type(resonate.rpc("g", "waz", 1, 2), Handle[Any])

    # The following assertions check the equivalence of the following permissible types:
    # Function[[int, int], int]
    # Function[[int, int], Generator[Any, Any, int]]
    # Callable[[Context, int, int], int]
    # Callable[[Context, int, int], Generator[Any, Any, int]]
    for h in (foo, bar, baz, qux, zog, waz, f, g):
        assert_type(resonate.run("h", h, 1, 2), Handle[int])
        assert_type(resonate.rpc("h", h, 1, 2), Handle[int])

    # The following assertions check the covariance of Function generic R parameter
    i: Function[[int, int], Generator[Any, Any, int]] | Function[[int, int], int] = random.choice([foo, bar, baz, qux, f, g])
    j: Function[[int, int], Generator[Any, Any, int] | int] = random.choice([foo, bar, baz, qux, f, g])
    assert_type(resonate.run("i", i, 1, 2), Handle[int])
    assert_type(resonate.rpc("i", i, 1, 2), Handle[int])
    assert_type(resonate.run("i", j, 1, 2), Handle[int])
    assert_type(resonate.rpc("i", j, 1, 2), Handle[int])


def test_context_type_annotations() -> None:
    # The following are "tests", if there is an issue it will be found by pyright, at runtime
    # assert_type is effectively a noop.

    def foo(ctx: Context, a: int, b: int) -> int: ...
    def bar(ctx: Context, a: int, b: int) -> Generator[Any, Any, int]: ...
    def baz(ctx: Context, a: int, b: int) -> int: ...
    def qux(ctx: Context, a: int, b: int) -> Generator[Any, Any, int]: ...

    registry = Registry()
    registry.add(foo, "foo")
    registry.add(bar, "bar")

    resonate = Resonate(registry=registry)
    baz = resonate.register(baz)
    qux = resonate.register(qux)

    ctx = Context("f", Mock(spec=Info), registry, Dependencies())

    assert_type(ctx.lfi(foo, 1, 2), LFI[int])
    assert_type(ctx.lfi(bar, 1, 2), LFI[int])
    assert_type(ctx.lfc(foo, 1, 2), LFC[int])
    assert_type(ctx.lfc(bar, 1, 2), LFC[int])
    assert_type(ctx.rfi(foo, 1, 2), RFI[int])
    assert_type(ctx.rfi(bar, 1, 2), RFI[int])
    assert_type(ctx.rfi("foo", 1, 2), RFI[Any])
    assert_type(ctx.rfi("bar", 1, 2), RFI[Any])
    assert_type(ctx.rfc(foo, 1, 2), RFC[int])
    assert_type(ctx.rfc(bar, 1, 2), RFC[int])
    assert_type(ctx.rfc("foo", 1, 2), RFC[Any])
    assert_type(ctx.rfc("bar", 1, 2), RFC[Any])
    assert_type(ctx.detached(foo, 1, 2), RFI[int])
    assert_type(ctx.detached(bar, 1, 2), RFI[int])
    assert_type(ctx.detached("foo", 1, 2), RFI[Any])
    assert_type(ctx.detached("bar", 1, 2), RFI[Any])

    for f in (foo, bar, baz, qux):
        assert_type(ctx.lfi(f, 1, 2), LFI[int])
        assert_type(ctx.lfc(f, 1, 2), LFC[int])
        assert_type(ctx.rfi(f, 1, 2), RFI[int])
        assert_type(ctx.rfc(f, 1, 2), RFC[int])
        assert_type(ctx.detached(f, 1, 2), RFI[int])


@pytest.mark.parametrize("funcs", [(foo, bar), (bar, baz), (baz, foo)])
@pytest.mark.parametrize("retry_policy", [Constant(), Exponential(), Linear(), Never(), None])
@pytest.mark.parametrize("target", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("tags", [{"a": "1"}, {"b": "2"}, {"c": "3"}, None])
@pytest.mark.parametrize("timeout", [1, 2, 3, 101, 102, 103, None])
@pytest.mark.parametrize("version", [1, 2, 3])
def test_options(funcs: tuple[Callable, Callable], retry_policy: RetryPolicy | None, target: str | None, tags: dict[str, str] | None, timeout: int | None, version: int) -> None:
    f1, f2 = funcs

    registry = Registry()
    registry.add(f1, "func", version)
    registry.add(f2, "func", version + 1)

    ctx = Context("f", Mock(spec=Info), registry, Dependencies())
    counter = 0

    for f, v in ((f1, version), (f2, version + 1)):
        for lf in (ctx.lfi, ctx.lfc):
            counter += 1

            cmd = lf(f, 1, 2)
            assert cmd.id == cmd.conv.id == f"f.{counter}"
            assert cmd.func == f
            assert cmd.args == (1, 2)
            assert cmd.kwargs == {}
            assert cmd.opts.version == v
            assert cmd.opts.tags == {}
            assert callable(cmd.opts.retry_policy)
            assert isinstance(cmd.opts.retry_policy(f1), Never if isgeneratorfunction(f1) else Exponential)
            assert cmd.conv.idempotency_key == cmd.id
            assert cmd.conv.headers is None
            assert cmd.conv.data is None
            assert cmd.conv.timeout == 31536000
            assert cmd.conv.tags == {"resonate:scope": "local"}
            assert cmd.opts.version == v

            # update the command
            cmd = cmd.options(tags=tags, timeout=timeout, version=version + 1, retry_policy=retry_policy)

            # version is a noop for lfx
            assert cmd.opts.version == v

            if timeout:
                assert cmd.conv.timeout == timeout
            if retry_policy:
                assert isinstance(cmd.opts.retry_policy, retry_policy.__class__)
            if tags:
                assert cmd.conv.tags
                assert all(k in cmd.conv.tags and cmd.conv.tags[k] == v for k, v in tags.items())

        for rf in (ctx.rfi, ctx.rfc, ctx.detached):
            counter += 1

            cmd = rf(f, 1, 2)
            assert cmd.id == cmd.conv.id == f"f.{counter}"
            assert cmd.conv.idempotency_key == cmd.id
            assert cmd.conv.headers is None
            assert cmd.conv.data == {"func": "func", "args": (1, 2), "kwargs": {}, "version": v}
            assert cmd.conv.timeout == 31536000
            assert cmd.conv.tags == {"resonate:scope": "global", "resonate:invoke": "poll://default"}

            cmd = cmd.options(tags=tags, timeout=timeout, version=version, target=target)

            # version is applicable for rfx
            assert cmd.conv.data == {"func": "func", "args": (1, 2), "kwargs": {}, "version": version}

            if timeout:
                assert cmd.conv.timeout == timeout
            if target:
                assert cmd.conv.tags
                assert cmd.conv.tags["resonate:invoke"] == target
            if tags:
                assert cmd.conv.tags
                assert all(k in cmd.conv.tags and cmd.conv.tags[k] == v for k, v in tags.items())
