from __future__ import annotations

import json
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
from resonate.encoders import JsonEncoder, JsonPickleEncoder, NoopEncoder
from resonate.loggers import ContextLogger
from resonate.models.commands import Command, Invoke, Listen
from resonate.models.handle import Handle
from resonate.options import Options
from resonate.registry import Registry
from resonate.resonate import Function
from resonate.retry_policies import Constant, Exponential, Linear, Never
from resonate.scheduler import Info

if TYPE_CHECKING:
    from resonate.models.encoder import Encoder
    from resonate.models.retry_policy import RetryPolicy


def foo(ctx: Context, a: int, b: int) -> int: ...
def bar(ctx: Context, a: int, b: int) -> int: ...
def baz(ctx: Context, a: int, b: int) -> Generator[Any, Any, int]:
    yield ctx.lfc(bar, a, b)
    raise NotImplementedError


def qux(ctx: Context, a: int, b: int) -> int: ...


class Qux:
    def __call__(self, ctx: Context) -> None: ...
    def foo(self, ctx: Context) -> None: ...
    def bar(self, ctx: Context) -> None: ...
    def baz(self, ctx: Context) -> None: ...


# Fixtures


@pytest.fixture
def resonate() -> Resonate:
    resonate = Resonate()

    # set private started to true so calls to run and rpc are noops
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

    # assert function instance
    assert isinstance(f, Function)
    assert f.func == func
    assert f.name == name or func.__name__
    assert f.__module__ == func.__module__
    assert f.__name__ == func.__name__
    assert f.__qualname__ == func.__qualname__
    assert f.__doc__ == func.__doc__
    assert f.__annotations__ == func.__annotations__
    assert f.__type_params__ == func.__type_params__


@pytest.mark.parametrize("func", [foo, bar, baz, lambda x: x])
@pytest.mark.parametrize("name", ["foo", "bar", "baz"])
@pytest.mark.parametrize("version", [1, 2, 3])
def test_register_function(func: Callable, name: str, version: int) -> None:
    registry = Registry()
    resonate = Resonate(registry=registry)

    f = resonate.register(Function(resonate, "", func, Options()), name=name, version=version)
    assert f.func == func
    assert f.name == name
    assert f.__module__ == func.__module__
    assert f.__name__ == func.__name__
    assert f.__qualname__ == func.__qualname__
    assert f.__doc__ == func.__doc__
    assert f.__annotations__ == func.__annotations__
    assert f.__type_params__ == func.__type_params__

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
    ("func", "kwargs", "match"),
    [
        (lambda x: x, {"name": "foo"}, "function foo already registered"),
        (lambda x: x, {"name": "foo", "version": 1}, "function foo already registered"),
        (lambda x: x, {"name": "foo", "version": 0}, "provided version must be greater than zero"),
        (lambda x: x, {"name": "bar", "version": 2}, "function bar already registered"),
        (lambda x: x, {"name": "bar", "version": 0}, "provided version must be greater than zero"),
        (lambda x: x, {"name": "baz", "version": 3}, "function baz already registered"),
        (lambda x: x, {"name": "baz", "version": 0}, "provided version must be greater than zero"),
        (lambda x: x, {}, "name required when registering a lambda function"),
        (lambda x: x, {"version": 1}, "name required when registering a lambda function"),
        (foo, {"name": "bar"}, "function bar already registered"),
        (bar, {"name": "baz"}, "function baz already registered"),
        (baz, {"name": "foo"}, "function foo already registered"),
        (Qux(), {}, "provided callable must be a function"),
        (Qux().foo, {}, "provided callable must be a function"),
        (Qux().bar, {}, "provided callable must be a function"),
        (Qux().baz, {}, "provided callable must be a function"),
    ],
)
def test_register_validations(registry: Registry, func: Callable, kwargs: dict, match: str) -> None:
    resonate = Resonate(registry=registry)
    with pytest.raises(ValueError, match=match):
        resonate.register(func, **kwargs)

    with pytest.raises(ValueError, match=match):
        resonate.register(**kwargs)(func)


@pytest.mark.parametrize("encoder", [JsonEncoder(), JsonPickleEncoder(), NoopEncoder(), None])
@pytest.mark.parametrize("idempotency_key", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("retry_policy", [Constant(), Linear(), Exponential()])
@pytest.mark.parametrize("target", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("tags", [{"a": "foo"}, {"b": "bar"}, {"c": "baz"}, None])
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
    encoder: Encoder[Any, str | None] | None,
    idempotency_key: str | None,
    retry_policy: RetryPolicy | None,
    target: str | None,
    tags: dict[str, str] | None,
    timeout: float | None,
    version: int | None,
    func: Callable,
    name: str,
    args: tuple,
    kwargs: dict,
) -> None:
    f = resonate.register(
        func,
        name=name,
        version=version or 1,
    )
    opts = {
        "encoder": encoder,
        "idempotency_key": idempotency_key,
        "retry_policy": retry_policy,
        "target": target,
        "tags": tags,
        "timeout": timeout,
        "version": version,
    }
    data = {
        "func": name,
        "args": args,
        "kwargs": kwargs,
        "version": version or 1,
    }

    default_opts = Options(version=version or 1)
    default_conv = Remote("f", "f", "f", name, args, kwargs, default_opts)

    updated_opts = Options(encoder=encoder, version=version or 1).merge(**opts)
    updated_conv = Remote("f", "f", "f", name, args, kwargs, updated_opts)

    assert updated_opts.encoder == (encoder or default_opts.encoder)
    assert updated_opts.idempotency_key == (idempotency_key or default_opts.idempotency_key)
    assert updated_opts.target == (target or default_opts.target)
    assert updated_opts.version == (version or default_opts.version)
    assert updated_opts.timeout == (timeout or default_opts.timeout)
    assert updated_opts.tags == (tags or default_opts.tags)

    def invoke(id: str) -> Invoke:
        conv = Remote(id, id, id, name, args, kwargs, default_opts)
        promise = resonate.promises.get(id=id)
        return Invoke(id, conv, promise.abs_timeout, func, args, kwargs, default_opts, promise)

    def invoke_with_opts(id: str) -> Invoke:
        conv = Remote(id, id, id, name, args, kwargs, updated_opts)
        promise = resonate.promises.get(id=id)
        return Invoke(id, conv, promise.abs_timeout, func, args, kwargs, updated_opts, promise)

    for id, fn in [("f1", func), ("f2", name), ("f3", f)]:
        resonate.run(id, fn, *args, **kwargs)
        assert cmd(resonate) == invoke(id)

        promise = resonate.promises.get(id=id)
        assert promise.id == id
        assert promise.ikey_for_create == id
        assert "resonate:format-py" in (promise.param.headers or {})
        assert promise.param.data == json.dumps(data)
        assert (
            promise.tags
            == {**default_conv.tags, "resonate:parent": id, "resonate:root": id, "resonate:scope": "global"}
            == {"resonate:invoke": default_opts.target, "resonate:parent": id, "resonate:root": id, "resonate:scope": "global"}
        )

    for id, fn in [("f4", func), ("f5", name), ("f6", f)]:
        resonate.options(**opts).run(id, fn, *args, **kwargs)
        assert cmd(resonate) == invoke_with_opts(id)

        promise = resonate.promises.get(id=id)
        assert promise.id == id
        assert promise.ikey_for_create == idempotency_key if idempotency_key else id
        if encoder:
            assert promise.param.headers is None
            assert promise.param.data == encoder.encode(data)
        else:
            assert "resonate:format-py" in (promise.param.headers or {})
            assert promise.param.data == json.dumps(data)
        assert (
            promise.tags
            == {**updated_conv.tags, "resonate:parent": id, "resonate:root": id, "resonate:scope": "global"}
            == {**updated_opts.tags, "resonate:parent": id, "resonate:root": id, "resonate:invoke": updated_opts.target, "resonate:scope": "global"}
        )

    f.run("f7", *args, **kwargs)
    assert cmd(resonate) == invoke("f7")

    promise = resonate.promises.get(id="f7")
    assert promise.id == "f7"
    assert promise.ikey_for_create == "f7"
    assert "resonate:format-py" in (promise.param.headers or {})
    assert promise.param.data == json.dumps(data)
    assert (
        promise.tags
        == {**default_conv.tags, "resonate:parent": "f7", "resonate:root": "f7", "resonate:scope": "global"}
        == {"resonate:invoke": default_opts.target, "resonate:parent": "f7", "resonate:root": "f7", "resonate:scope": "global"}
    )

    f.options(**opts).run("f8", *args, **kwargs)
    assert cmd(resonate) == invoke_with_opts("f8")

    promise = resonate.promises.get(id="f8")
    assert promise.id == "f8"
    assert promise.ikey_for_create == idempotency_key if idempotency_key else "f8"
    if encoder:
        assert promise.param.headers is None
        assert promise.param.data == encoder.encode(data)
    else:
        assert "resonate:format-py" in (promise.param.headers or {})
        assert promise.param.data == json.dumps(data)
    assert (
        promise.tags
        == {**updated_conv.tags, "resonate:parent": "f8", "resonate:root": "f8", "resonate:scope": "global"}
        == {**updated_opts.tags, "resonate:parent": "f8", "resonate:root": "f8", "resonate:invoke": updated_opts.target, "resonate:scope": "global"}
    )


@pytest.mark.parametrize("encoder", [JsonEncoder(), JsonPickleEncoder(), NoopEncoder(), None])
@pytest.mark.parametrize("idempotency_key", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("retry_policy", [Constant(), Linear(), Exponential()])
@pytest.mark.parametrize("target", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("tags", [{"a": "foo"}, {"b": "bar"}, {"c": "baz"}, None])
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
def test_rpc(
    resonate: Resonate,
    encoder: Encoder[Any, str | None] | None,
    idempotency_key: str | None,
    retry_policy: RetryPolicy | None,
    target: str | None,
    tags: dict[str, str] | None,
    timeout: float | None,
    version: int | None,
    func: Callable,
    name: str,
    args: tuple,
    kwargs: dict,
) -> None:
    f = resonate.register(
        func,
        name=name,
        version=version or 1,
    )
    opts = {
        "encoder": encoder,
        "idempotency_key": idempotency_key,
        "retry_policy": retry_policy,
        "target": target,
        "tags": tags,
        "timeout": timeout,
        "version": version,
    }
    data = {
        "func": name,
        "args": args,
        "kwargs": kwargs,
        "version": version or 1,
    }

    default_opts = Options(version=version or 1)
    default_conv = Remote("f", "f", "f", name, args, kwargs, default_opts)

    updated_opts = Options(version=version or 1).merge(**opts)
    updated_conv = Remote("f", "f", "f", name, args, kwargs, updated_opts)

    assert updated_opts.encoder == (encoder or default_opts.encoder)
    assert updated_opts.idempotency_key == (idempotency_key or default_opts.idempotency_key)
    assert updated_opts.target == (target or default_opts.target)
    assert updated_opts.version == (version or default_opts.version)
    assert updated_opts.timeout == (timeout or default_opts.timeout)
    assert updated_opts.tags == (tags or default_opts.tags)

    for id, fn in [("f1", func), ("f2", name), ("f3", f)]:
        resonate.rpc(id, fn, *args, **kwargs)
        assert cmd(resonate) == Listen(id=id)

        promise = resonate.promises.get(id=id)
        assert promise.id == id
        assert promise.ikey_for_create == id
        assert "resonate:format-py" in (promise.param.headers or {})
        assert promise.param.data == json.dumps(data)
        assert (
            promise.tags
            == {**default_conv.tags, "resonate:parent": id, "resonate:root": id, "resonate:scope": "global"}
            == {"resonate:invoke": default_opts.target, "resonate:parent": id, "resonate:root": id, "resonate:scope": "global"}
        )

    for id, fn in [("f4", func), ("f5", name), ("f6", f)]:
        resonate.options(**opts).rpc(id, fn, *args, **kwargs)
        assert cmd(resonate) == Listen(id=id)

        promise = resonate.promises.get(id=id)
        assert promise.id == id
        assert promise.ikey_for_create == idempotency_key if idempotency_key else id
        if encoder:
            assert promise.param.headers is None
            assert promise.param.data == encoder.encode(data)
        else:
            assert "resonate:format-py" in (promise.param.headers or {})
            assert promise.param.data == json.dumps(data)
        assert (
            promise.tags
            == {**updated_conv.tags, "resonate:parent": id, "resonate:root": id, "resonate:scope": "global"}
            == {**updated_opts.tags, "resonate:parent": id, "resonate:root": id, "resonate:invoke": updated_opts.target, "resonate:scope": "global"}
        )

    f.rpc("f7", *args, **kwargs)
    assert cmd(resonate) == Listen(id="f7")

    promise = resonate.promises.get(id="f7")
    assert promise.id == "f7"
    assert promise.ikey_for_create == "f7"
    assert "resonate:format-py" in (promise.param.headers or {})
    assert promise.param.data == json.dumps(data)
    assert (
        promise.tags
        == {**default_conv.tags, "resonate:parent": "f7", "resonate:root": "f7", "resonate:scope": "global"}
        == {"resonate:invoke": default_opts.target, "resonate:parent": "f7", "resonate:root": "f7", "resonate:scope": "global"}
    )

    f.options(**opts).rpc("f8", *args, **kwargs)
    assert cmd(resonate) == Listen(id="f8")

    promise = resonate.promises.get(id="f8")
    assert promise.id == "f8"
    assert promise.ikey_for_create == idempotency_key if idempotency_key else "f8"
    if encoder:
        assert promise.param.headers is None
        assert promise.param.data == encoder.encode(data)
    else:
        assert "resonate:format-py" in (promise.param.headers or {})
        assert promise.param.data == json.dumps(data)
    assert (
        promise.tags
        == {**updated_conv.tags, "resonate:parent": "f8", "resonate:root": "f8", "resonate:scope": "global"}
        == {**updated_opts.tags, "resonate:parent": "f8", "resonate:root": "f8", "resonate:invoke": updated_opts.target, "resonate:scope": "global"}
    )


@pytest.mark.parametrize(
    ("func", "kwargs", "match"),
    [
        (foo, {"version": 2}, "function foo version 2 not found in registry"),
        (bar, {"version": 3}, "function bar version 3 not found in registry"),
        (baz, {"version": 1}, "function baz version 1 not found in registry"),
        (qux, {}, "function qux not found in registry"),
        (lambda: None, {}, "function <lambda> not found in registry"),
        ("foo", {"version": 2}, "function foo version 2 not found in registry"),
        ("bar", {"version": 3}, "function bar version 3 not found in registry"),
        ("baz", {"version": 1}, "function baz version 1 not found in registry"),
        ("qux", {}, "function qux not found in registry"),
        (Qux(), {}, "function unknown not found in registry"),
        (Qux().foo, {}, "function foo not found in registry"),
        (Qux().bar, {}, "function bar not found in registry"),
        (Qux().baz, {}, "function baz not found in registry"),
    ],
)
def test_run_and_rpc_validations(registry: Registry, func: Callable | str, kwargs: dict, match: str) -> None:
    resonate = Resonate(registry=registry)

    with pytest.raises(ValueError, match=match):
        resonate.options(**kwargs).run("f", func)

    if not isinstance(func, str):
        with pytest.raises(ValueError, match=match):
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

    ctx = Context("f", "f", Mock(spec=Info), registry, Dependencies(), ContextLogger("f", "f"))

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


@pytest.mark.parametrize(
    ("func", "match"),
    [
        (Qux(), "provided callable must be a function"),
        (Qux().foo, "provided callable must be a function"),
        (Qux().bar, "provided callable must be a function"),
        (Qux().baz, "provided callable must be a function"),
    ],
)
def test_context_lfx_validations(registry: Registry, func: Callable, match: str) -> None:
    ctx = Context("f", "f", Mock(spec=Info), registry, Dependencies(), ContextLogger("f", "f"))

    with pytest.raises(ValueError, match=match):
        ctx.lfi(func)
    with pytest.raises(ValueError, match=match):
        ctx.lfc(func)


@pytest.mark.parametrize(
    ("func", "match"),
    [
        (qux, "function qux not found in registry"),
        (lambda: None, "function <lambda> not found in registry"),
        (Qux(), "function unknown not found in registry"),
        (Qux().foo, "function foo not found in registry"),
        (Qux().bar, "function bar not found in registry"),
        (Qux().baz, "function baz not found in registry"),
    ],
)
def test_context_rfx_validations(registry: Registry, func: Callable, match: str) -> None:
    ctx = Context("f", "f", Mock(spec=Info), registry, Dependencies(), ContextLogger("f", "f"))

    with pytest.raises(ValueError, match=match):
        ctx.rfi(func)
    with pytest.raises(ValueError, match=match):
        ctx.rfc(func)


@pytest.mark.parametrize("funcs", [(foo, bar), (bar, baz), (baz, foo)])
@pytest.mark.parametrize("encoder", [JsonEncoder(), JsonPickleEncoder(), NoopEncoder(), None])
@pytest.mark.parametrize("non_retryable_exceptions", [(NameError,), (ValueError,), (NameError, ValueError), None])
@pytest.mark.parametrize("retry_policy", [Constant(), Exponential(), Linear(), Never(), None])
@pytest.mark.parametrize("target", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("tags", [{"a": "1"}, {"b": "2"}, {"c": "3"}, None])
@pytest.mark.parametrize("timeout", [1, 2, 3, None])
@pytest.mark.parametrize("version", [1, 2, 3])
def test_options(
    funcs: tuple[Callable, Callable],
    encoder: Encoder[Any, str | None] | None,
    non_retryable_exceptions: tuple[type[Exception], ...] | None,
    retry_policy: RetryPolicy | None,
    target: str | None,
    tags: dict[str, str] | None,
    timeout: float | None,
    version: int,
) -> None:
    f1, f2 = funcs

    registry = Registry()
    registry.add(f1, "func", version)
    registry.add(f2, "func", version + 1)

    ctx = Context("f", "f", Mock(spec=Info), registry, Dependencies(), ContextLogger("f", "f"))
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
            assert cmd.conv.data is None
            assert cmd.conv.timeout == 31536000
            assert cmd.conv.tags == {"resonate:parent": "f", "resonate:root": "f", "resonate:scope": "local"}
            assert cmd.opts.version == v

            # update the command
            cmd = cmd.options(
                encoder=encoder,
                non_retryable_exceptions=non_retryable_exceptions,
                retry_policy=retry_policy,
                tags=tags,
                timeout=timeout,
                version=version + 1,
            )

            # version is a noop for lfx
            assert cmd.opts.version == v

            if encoder:
                assert cmd.opts.encoder is encoder
            if non_retryable_exceptions:
                assert cmd.opts.non_retryable_exceptions == non_retryable_exceptions
            if retry_policy:
                assert isinstance(cmd.opts.retry_policy, retry_policy.__class__)
            if tags:
                assert cmd.conv.tags
                assert all(k in cmd.conv.tags and cmd.conv.tags[k] == v for k, v in tags.items())
            if timeout:
                assert cmd.conv.timeout == timeout

        for rf in (ctx.rfi, ctx.rfc, ctx.detached):
            counter += 1

            cmd = rf(f, 1, 2)
            assert cmd.id == cmd.conv.id == f"f.{counter}"
            assert cmd.conv.idempotency_key == cmd.id
            assert cmd.conv.data == {"func": "func", "args": (1, 2), "kwargs": {}, "version": v}
            assert cmd.conv.timeout == 31536000
            assert cmd.conv.tags == {"resonate:parent": "f", "resonate:root": "f", "resonate:scope": "global", "resonate:invoke": "default"}

            cmd = cmd.options(
                encoder=encoder,
                tags=tags,
                target=target,
                timeout=timeout,
                version=version,
            )

            # version is applicable for rfx
            assert cmd.conv.data == {"func": "func", "args": (1, 2), "kwargs": {}, "version": version}

            if encoder:
                assert cmd.opts.encoder is encoder
            if target:
                assert cmd.conv.tags
                assert cmd.conv.tags["resonate:invoke"] == target
            if tags:
                assert cmd.conv.tags
                assert all(k in cmd.conv.tags and cmd.conv.tags[k] == v for k, v in tags.items())
            if timeout:
                assert cmd.conv.timeout == timeout


@pytest.mark.parametrize("value", [-1, -2, -3])
def test_options_validations(registry: Registry, value: int) -> None:
    ctx = Context("f", "f", Mock(spec=Info), registry, Dependencies(), ContextLogger("f", "f"))

    with pytest.raises(ValueError, match="timeout must be greater than or equal to zero"):
        ctx.lfi(foo, 1, 2).options(timeout=value)  # no version for lfi

    with pytest.raises(ValueError, match="timeout must be greater than or equal to zero"):
        ctx.lfc(foo, 1, 2).options(timeout=value)  # no version for lfc

    for timeout, version in [(value, None), (None, value)]:
        with pytest.raises(ValueError, match=r"(timeout|version) must be greater than or equal to zero"):
            ctx.rfi(foo, 1, 2).options(timeout=timeout, version=version)

        with pytest.raises(ValueError, match=r"(timeout|version) must be greater than or equal to zero"):
            ctx.rfc(foo, 1, 2).options(timeout=timeout, version=version)
