from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Any

import pytest

from resonate.registry import Registry
from tests.runners import LocalContext, RemoteContext, ResonateLFXRunner, ResonateRFXRunner, ResonateRunner, Runner, SimpleRunner

if TYPE_CHECKING:
    from collections.abc import Callable, Generator


# Functions


def fib(ctx: LocalContext | RemoteContext, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    p1 = yield ctx.rfi(fib, n - 1).options(id=f"fib({n - 1})")
    p2 = yield ctx.rfi(fib, n - 2).options(id=f"fib({n - 2})")

    v1 = yield p1
    v2 = yield p2

    return v1 + v2


def fac(ctx: LocalContext | RemoteContext, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return 1

    return n * (yield ctx.rfc(fac, n - 1).options(id=f"fac({n - 1})"))


def gcd(ctx: LocalContext | RemoteContext, a: int, b: int) -> Generator[Any, Any, int]:
    if b == 0:
        return a

    return (yield ctx.rfc(gcd, b, a % b).options(id=f"gcd({b},{a % b})"))


def rpt(ctx: LocalContext | RemoteContext, s: str, n: int) -> Generator[Any, Any, Any]:
    v = ""
    p = yield ctx.lfi(idv, s)

    for _ in range(n):
        v += yield ctx.lfc(idp, p)

    return v


def idv(c: LocalContext | RemoteContext, x: Any) -> Any:
    return x


def idp(c: LocalContext | RemoteContext, p: Any) -> Any:
    return (yield p)


# Tests


@pytest.fixture(scope="module")
def registry() -> Registry:
    registry = Registry()
    registry.add(fib, "fib")
    registry.add(fac, "fac")
    registry.add(gcd, "gcd")
    registry.add(rpt, "rpt")
    registry.add(idv, "idv")
    registry.add(idp, "idp")

    return registry


@pytest.fixture
def runners(registry: Registry) -> tuple[Runner, ...]:
    return (
        SimpleRunner(),
        ResonateRunner(registry),
        ResonateLFXRunner(registry),
        ResonateRFXRunner(registry),
    )


@pytest.mark.parametrize(
    ("id", "func", "args", "kwargs"),
    [
        *((f"fib({n})", fib, (n,), {}) for n in range(20)),
        *((f"fac({n})", fac, (n,), {}) for n in range(20)),
        *((f"gcd({a},{b})", gcd, (a, b), {}) for a, b in itertools.product(range(10), repeat=2)),
        *((f"rpt({s},{n})", rpt, (s, n), {}) for s, n in itertools.product("abc", range(10))),
    ],
)
def test_equivalencies(runners: tuple[Runner, ...], id: str, func: Callable, args: Any, kwargs: Any) -> None:
    # a promise is not json serializable so we must skip ResonateRFXRunner and rpt
    results = [r.run(id, func, *args, **kwargs) for r in runners if not (isinstance(r, ResonateRFXRunner) and func == rpt)]
    assert all(x == results[0] for x in results[1:])
