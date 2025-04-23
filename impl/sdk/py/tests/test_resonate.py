from __future__ import annotations

from collections.abc import Callable
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, assert_type
from unittest.mock import MagicMock, patch

import pytest

from resonate import Context, Resonate
from resonate.dependencies import Dependencies
from resonate.errors import ResonateValidationError
from resonate.models.commands import Invoke, Listen
from resonate.models.handle import Handle
from resonate.options import Options
from resonate.registry import Registry
from resonate.resonate import Function
from resonate.retry_policies import Constant, Exponential, Linear, Never
from resonate.scheduler import Info

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.models.retry_policy import RetryPolicy


def foo(ctx: Context, a: int, b: int) -> int: ...
def bar(ctx: Context, a: int, b: int) -> int: ...
def baz(ctx: Context, a: int, b: int) -> Generator[Any, Any, int]:
    yield ctx.lfc(bar, a, b)
    raise NotImplementedError


# Fixtures


@pytest.fixture
def bridge() -> Generator[MagicMock, None, None]:
    def side_effect(*args: Any, **kwargs: Any) -> None:
        if futures := kwargs.get("futures"):
            futures[0].set_result(None)  # Unblock future

    with patch("resonate.resonate.Bridge") as mock_bridge_cls:
        mock_bridge = mock_bridge_cls.return_value
        mock_bridge.invoke.side_effect = side_effect
        mock_bridge.listen.side_effect = side_effect
        yield mock_bridge


@pytest.fixture
def registry() -> Registry:
    registry = Registry()
    registry.add(foo, "foo", version=1)
    registry.add(foo, "foo", version=2)
    registry.add(bar, "bar", version=1)
    return registry


# Helper functions


def cmd_invoke(mock_bridge: MagicMock) -> None:
    mock_bridge.invoke.assert_called_once()
    args, _ = mock_bridge.invoke.call_args
    mock_bridge.invoke.reset_mock()
    return args[0]


def cmd_listen(mock_bridge: MagicMock) -> None:
    mock_bridge.listen.assert_called_once()
    args, _ = mock_bridge.listen.call_args
    mock_bridge.listen.reset_mock()
    return args[0]


# Tests


@pytest.mark.parametrize("func", [foo, bar, baz, lambda x: x])
@pytest.mark.parametrize("name", ["foo", "bar", "baz", None])
def test_register(func: Callable, name: str | None) -> None:
    # skip lambda functions without name, validation tests will cover this
    if func.__name__ == "<lambda>" and name is None:
        return

    registry = Registry()
    resonate = Resonate(registry=registry)

    resonate.register(func, name=name)
    assert registry.get(name or func.__name__) == (func, 1)
    assert registry.get(func) == (name or func.__name__, 1)

    resonate.register(func, name=name, version=2)
    assert registry.get(name or func.__name__) == (func, 2)
    assert registry.get(func) == (name or func.__name__, 2)


@pytest.mark.parametrize("send_to", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("version", [1, 2, 3, None])
@pytest.mark.parametrize("timeout", [3, 2, 1, None])
@pytest.mark.parametrize("tags", [{"a": "1"}, {"2": "foo"}, {"a": "foo"}, None])
@pytest.mark.parametrize("retry_policy", [Never(), Constant(delay=1, max_retries=1), Linear(delay=1, max_retries=1), Exponential(delay=0.4, factor=2, max_delay=3, max_retries=1)])
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
    bridge: MagicMock,
    send_to: str | None,
    version: int | None,
    timeout: int | None,
    tags: dict[str, str] | None,
    retry_policy: RetryPolicy,
    func: Callable,
    name: str,
    args: tuple,
    kwargs: dict,
) -> None:
    registry = Registry()
    resonate = Resonate(registry=registry)

    f1 = resonate.register(func, name=name, version=version or 1)

    opts = Options(version=1)
    if send_to is not None:
        opts = opts.merge(send_to=send_to)
    if version is not None:
        opts = opts.merge(version=version)
    if timeout is not None:
        opts = opts.merge(timeout=timeout)
    if tags is not None:
        opts = opts.merge(tags=tags)

    opts.merge(retry_policy=retry_policy)

    invoke = Invoke(id="f", name=name, func=func, args=args, kwargs=kwargs, opts=Options(version=version or 1))
    invoke_with_opts = Invoke(id="f", name=name, func=func, args=args, kwargs=kwargs, opts=opts)

    resonate.run("f", func, *args, **kwargs)
    assert cmd_invoke(bridge) == invoke

    resonate.run("f", name, *args, **kwargs)
    assert cmd_invoke(bridge) == invoke

    resonate.options(**opts.to_dict()).run("f", func, *args, **kwargs)
    assert cmd_invoke(bridge) == invoke_with_opts

    resonate.options(**opts.to_dict()).run("f", name, *args, **kwargs)
    assert cmd_invoke(bridge) == invoke_with_opts

    f1.run("f", *args, **kwargs)
    assert cmd_invoke(bridge) == invoke

    f1.options(**opts.to_dict()).run("f", *args, **kwargs)
    assert cmd_invoke(bridge) == invoke_with_opts

    version = (version or 1) + 1
    f2 = resonate.register(func, name=name, send_to=send_to, version=version, timeout=timeout, tags=tags)
    opts = opts.merge(version=version)
    invoke_with_opts.opts = opts

    f2.run("f", *args, **kwargs)
    assert cmd_invoke(bridge) == invoke_with_opts

    f2.options(**opts.to_dict()).run("f", *args, **kwargs)
    assert cmd_invoke(bridge) == invoke_with_opts


@pytest.mark.parametrize("send_to", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("version", [1, 2, 3, None])
@pytest.mark.parametrize("timeout", [3, 2, 1, None])
@pytest.mark.parametrize("tags", [{"a": "1"}, {"2": "foo"}, {"a": "foo"}, None])
@pytest.mark.parametrize("retry_policy", [Never(), Constant(delay=1, max_retries=1), Linear(delay=1, max_retries=1), Exponential(delay=0.4, factor=2, max_delay=3, max_retries=1)])
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
    bridge: MagicMock,
    send_to: str | None,
    version: int | None,
    timeout: int | None,
    tags: dict[str, str] | None,
    retry_policy: RetryPolicy,
    func: Callable,
    name: str,
    args: tuple,
    kwargs: dict,
) -> None:
    registry = Registry()
    resonate = Resonate(registry=registry)

    f1 = resonate.register(func, name=name, version=version or 1)

    opts = Options(version=1)
    if send_to is not None:
        opts = opts.merge(send_to=send_to)
    if version is not None:
        opts = opts.merge(version=version)
    if timeout is not None:
        opts = opts.merge(timeout=timeout)
    if tags is not None:
        opts = opts.merge(tags=tags)

    opts.merge(retry_policy=retry_policy)

    invoke = Invoke(id="f", name=name, func=None, args=args, kwargs=kwargs, opts=Options(version=version or 1))
    invoke_with_opts = Invoke(id="f", name=name, func=None, args=args, kwargs=kwargs, opts=opts)

    resonate.rpc("f", func, *args, **kwargs)
    assert cmd_invoke(bridge) == invoke

    resonate.rpc("f", name, *args, **kwargs)
    assert cmd_invoke(bridge) == invoke

    resonate.options(**opts.to_dict()).rpc("f", func, *args, **kwargs)
    assert cmd_invoke(bridge) == invoke_with_opts

    resonate.options(**opts.to_dict()).rpc("f", name, *args, **kwargs)
    assert cmd_invoke(bridge) == invoke_with_opts

    f1.rpc("f", *args, **kwargs)
    assert cmd_invoke(bridge) == invoke

    f1.options(**opts.to_dict()).rpc("f", *args, **kwargs)
    assert cmd_invoke(bridge) == invoke_with_opts

    version = (version or 1) + 1
    f2 = resonate.register(func, name=name, send_to=send_to, version=version, timeout=timeout, tags=tags)
    invoke_with_opts.opts = invoke_with_opts.opts.merge(version=version)

    f2.rpc("f", *args, **kwargs)
    assert cmd_invoke(bridge) == invoke_with_opts

    f2.options(**opts.to_dict()).rpc("f", *args, **kwargs)
    assert cmd_invoke(bridge) == invoke_with_opts


@pytest.mark.parametrize("id", ["foo", "bar", "baz"])
def test_get(bridge: MagicMock, id: str) -> None:
    resonate = Resonate()
    resonate.get(id)
    assert cmd_listen(bridge) == Listen(id=id)


def test_type_annotations() -> None:
    # The following are "tests", if there is an issue it will be found by pright, at runtime
    # assert_type is effectively a noop.

    resonate = Resonate()

    # foo
    def foo(ctx: Context, a: int, b: int, /) -> int: ...

    f = resonate.register(foo)
    assert_type(f, Function[[int, int], int])
    assert_type(f.run, Callable[[str, int, int], Handle[int]])
    assert_type(f.rpc, Callable[[str, int, int], Handle[int]])

    # bar
    def bar(ctx: Context, a: str, b: str, /) -> str: ...

    f = resonate.register(bar)
    assert_type(f, Function[[str, str], str])
    assert_type(f.run, Callable[[str, str, str], Handle[str]])
    assert_type(f.rpc, Callable[[str, str, str], Handle[str]])

    # baz
    def baz(ctx: Context, a: int, b: str, /) -> int | str: ...

    f = resonate.register(baz)
    assert_type(f, Function[[int, str], int | str])
    assert_type(f.run, Callable[[str, int, str], Handle[int | str]])
    assert_type(f.rpc, Callable[[str, int, str], Handle[int | str]])


@pytest.mark.parametrize(
    ("func", "kwargs"),
    [
        (lambda x: x, {"name": "foo", "timeout": 1, "version": -1}),
        (lambda x: x, {"name": "foo", "timeout": -1, "version": 1}),
        (lambda x: x, {"timeout": 1, "version": 1}),
        (foo, {"version": 1}),
        (foo, {"version": 2}),
        (bar, {"version": 1}),
        (foo, {"name": "bar"}),
        (bar, {"name": "foo"}),
    ],
)
def test_register_validations(registry: Registry, func: Callable, kwargs: dict) -> None:
    resonate = Resonate(registry=registry)
    with pytest.raises(ResonateValidationError):
        resonate.register(func, **kwargs)

    with pytest.raises(ResonateValidationError):
        resonate.register(**kwargs)(func)


@pytest.mark.parametrize(
    ("func", "kwargs"),
    [
        (foo, {"version": 3}),
        (bar, {"version": 2}),
        (baz, {}),
        ("foo", {"version": 3}),
        ("bar", {"version": 2}),
        ("baz", {}),
    ],
)
def test_run_and_rpc_validations(registry: Registry, func: Callable | str, kwargs: dict) -> None:
    resonate = Resonate(registry=registry)

    with pytest.raises(ResonateValidationError):
        resonate.options(**kwargs).run("f", func)

    if callable(func):
        with pytest.raises(ResonateValidationError):
            resonate.options(**kwargs).rpc("f", func)


@pytest.mark.parametrize(
    "timeout",
    [1, 2, 3],
)
@pytest.mark.parametrize("func", [foo, bar, baz])
@pytest.mark.parametrize("version", [1, 2, 3, 20])
@pytest.mark.parametrize("send_to", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("retry_policy", [Never(), Constant(1, 1), None])
def test_propagation(timeout: int, func: Callable, version: int, send_to: str | None, retry_policy: RetryPolicy | None) -> None:
    registry = Registry()
    registry.add(func, "func", version)

    default_opts = Options()
    opts = Options(timeout=timeout).merge(send_to=send_to, version=version, retry_policy=retry_policy)
    ctx = Context("foo", Info(), opts, registry, Dependencies())

    for f in (ctx.lfi, ctx.lfc):
        cmd = f(func, 1, 2)
        assert cmd.opts.version == version
        assert cmd.opts.tags == default_opts.tags
        assert cmd.opts.send_to == default_opts.send_to
        assert cmd.opts.retry_policy == Never() if isgeneratorfunction(func) else Exponential()

        if f == ctx.detached:
            assert cmd.opts.timeout == default_opts.timeout
        else:
            assert cmd.opts.timeout == timeout
            cmd = cmd.options(timeout=timeout + 1)
            assert cmd.opts.timeout == timeout

        cmd = cmd.options(timeout=timeout - 1)
        assert cmd.opts.timeout == timeout - 1

        assert cmd.options(retry_policy=retry_policy).opts.retry_policy == retry_policy if retry_policy is not None else Never() if isgeneratorfunction(func) else Exponential()

    for f in (ctx.rfi, ctx.rfc, ctx.detached):
        cmd = f(func, 1, 2)
        assert cmd.convention.opts.version == version
        assert cmd.convention.opts.tags == default_opts.tags
        assert cmd.convention.opts.send_to == default_opts.send_to
        assert cmd.convention.opts.retry_policy == Never()

        if f == ctx.detached:
            assert cmd.convention.opts.timeout == default_opts.timeout
        else:
            assert cmd.convention.opts.timeout == timeout
            cmd = cmd.options(timeout=timeout + 1)
            assert cmd.convention.opts.timeout == timeout

        cmd = cmd.options(timeout=timeout - 1)
        assert cmd.convention.opts.timeout == timeout - 1
        assert cmd.options(send_to=send_to).convention.opts.send_to == send_to if send_to is not None else default_opts.send_to
