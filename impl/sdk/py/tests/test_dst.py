from __future__ import annotations

import logging
import random
from typing import TYPE_CHECKING, Any

from resonate.clocks.step import StepClock
from resonate.conventions import Remote
from resonate.dependencies import Dependencies
from resonate.models.commands import Invoke, Listen
from resonate.options import Options
from resonate.registry import Registry
from resonate.simulator import Server, Simulator, Worker

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate import Context

logger = logging.getLogger(__name__)


def foo(ctx: Context) -> Generator[Any, Any, Any]:
    p1 = yield ctx.lfi(bar)
    p2 = yield ctx.rfi(bar).options(target="sim://any@default")
    yield ctx.lfi(bar)
    yield ctx.lfc(bar)
    yield ctx.rfi(bar).options(target="sim://any@default")
    yield ctx.rfc(bar).options(target="sim://any@default")
    yield ctx.detached(bar).options(target="sim://any@default")

    return (yield p1), (yield p2)


def bar(ctx: Context) -> Generator[Any, Any, Any]:
    p1 = yield ctx.lfi(baz)
    p2 = yield ctx.rfi(baz).options(target="sim://any@default")
    yield ctx.lfi(baz)
    yield ctx.lfc(baz)
    yield ctx.rfi(baz).options(target="sim://any@default")
    yield ctx.rfc(baz).options(target="sim://any@default")
    yield ctx.detached(baz).options(target="sim://any@default")

    return (yield p1), (yield p2)


def baz(ctx: Context) -> str:
    return "baz"


def foo_lfi(ctx: Context) -> Generator:
    p = yield ctx.lfi(bar_lfi)
    v = yield p
    return v


def bar_lfi(ctx: Context) -> Generator:
    p = yield ctx.lfi(baz)
    v = yield p
    return v


def foo_lfc(ctx: Context) -> Generator:
    v = yield ctx.lfc(bar_lfc)
    return v


def bar_lfc(ctx: Context) -> Generator:
    v = yield ctx.lfc(baz)
    return v


def foo_rfi(ctx: Context) -> Generator:
    p = yield ctx.rfi(bar_rfi).options(target="sim://any@default")
    v = yield p
    return v


def bar_rfi(ctx: Context) -> Generator:
    p = yield ctx.rfi(baz).options(target="sim://any@default")
    v = yield p
    return v


def foo_rfc(ctx: Context) -> Generator:
    v = yield ctx.lfc(bar_rfc)
    return v


def bar_rfc(ctx: Context) -> Generator:
    v = yield ctx.rfc(baz).options(target="sim://any@default")
    return v


def foo_detached(ctx: Context) -> Generator:
    yield ctx.detached(bar_detached).options(target="sim://any@default")


def bar_detached(ctx: Context) -> Generator:
    yield ctx.detached(baz).options(target="sim://any@default")


def structured_concurrency(ctx: Context) -> Generator:
    yield ctx.lfi(baz)
    yield ctx.lfi(baz)
    yield ctx.lfi(baz)


def fail_25(ctx: Context) -> None:
    r = ctx.get_dependency("resonate:random")
    assert isinstance(r, random.Random)

    if r.random() < 0.25:
        msg = "Failing 25% of the time"
        raise RuntimeError(msg)


def fail_50(ctx: Context) -> None:
    r = ctx.get_dependency("resonate:random")
    assert isinstance(r, random.Random)

    if r.random() < 0.50:
        msg = "Failing 50% of the time"
        raise RuntimeError(msg)


def fail_75(ctx: Context) -> None:
    r = ctx.get_dependency("resonate:random")
    assert isinstance(r, random.Random)

    if r.random() < 0.75:
        msg = "Failing 75% of the time"
        raise RuntimeError(msg)


def fail_99(ctx: Context) -> None:
    r = ctx.get_dependency("resonate:random")
    assert isinstance(r, random.Random)

    if r.random() < 0.99:
        msg = "Failing 99% of the time"
        raise RuntimeError(msg)


def fib(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    ps = []
    vs = []
    for i in range(1, 3):
        match (yield ctx.random.randint(0, 3).options(id=f"fib:{n - i}")):
            case 0:
                p = yield ctx.lfi(fib, n - i).options(id=f"fib-{n - i}")
                ps.append(p)
            case 1:
                p = yield ctx.rfi(fib, n - i).options(id=f"fib-{n - i}", target="sim://any@default")
                ps.append(p)
            case 2:
                v = yield ctx.lfc(fib, n - i).options(id=f"fib-{n - i}")
                vs.append(v)
            case 3:
                v = yield ctx.rfc(fib, n - i).options(id=f"fib-{n - i}", target="sim://any@default")
                vs.append(v)

    for p in ps:
        v = yield p
        vs.append(v)

    assert len(vs) == 2
    return sum(vs)


def fib_lfi(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    p1 = yield ctx.lfi(fib_lfi, n - 1).options(id=f"fibl-{n - 1}")
    p2 = yield ctx.lfi(fib_lfi, n - 2).options(id=f"fibl-{n - 2}")

    v1 = yield p1
    v2 = yield p2

    return v1 + v2


def fib_lfc(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    v1 = yield ctx.lfc(fib_lfc, n - 1).options(id=f"fibl-{n - 1}")
    v2 = yield ctx.lfc(fib_lfc, n - 2).options(id=f"fibl-{n - 2}")

    return v1 + v2


def fib_rfi(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    p1 = yield ctx.rfi(fib_rfi, n - 1).options(id=f"fibr-{n - 1}", target="sim://any@default")
    p2 = yield ctx.rfi(fib_rfi, n - 2).options(id=f"fibr-{n - 2}", target="sim://any@default")

    v1 = yield p1
    v2 = yield p2

    return v1 + v2


def fib_rfc(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    v1 = yield ctx.rfc(fib_rfc, n - 1).options(id=f"fibr-{n - 1}", target="sim://any@default")
    v2 = yield ctx.rfc(fib_rfc, n - 2).options(id=f"fibr-{n - 2}", target="sim://any@default")

    return v1 + v2


def test_dst(seed: str, steps: int) -> None:
    logger.info("DST(seed=%s, steps=%s)", seed, steps)

    # create seeded random number generator
    r = random.Random(seed)

    # create a step clock
    clock = StepClock()

    # create a registry
    registry = Registry()
    registry.add(foo, "foo")
    registry.add(bar, "bar")
    registry.add(baz, "baz")
    registry.add(foo_lfi, "foo_lfi")
    registry.add(bar_lfi, "bar_lfi")
    registry.add(foo_lfc, "foo_lfc")
    registry.add(bar_lfc, "bar_lfc")
    registry.add(foo_rfi, "foo_rfi")
    registry.add(bar_rfi, "bar_rfi")
    registry.add(foo_rfc, "foo_rfc")
    registry.add(bar_rfc, "bar_rfc")
    registry.add(foo_detached, "foo_detached")
    registry.add(bar_detached, "bar_detached")
    registry.add(structured_concurrency, "structured_concurrency")
    registry.add(fail_25, "fail_25")
    registry.add(fail_50, "fail_50")
    registry.add(fail_75, "fail_75")
    registry.add(fail_99, "fail_99")
    registry.add(fib, "fib")
    registry.add(fib_lfi, "fib_lfi")
    registry.add(fib_lfc, "fib_lfc")
    registry.add(fib_rfi, "fib_rfi")
    registry.add(fib_rfc, "fib_rfc")

    # create dependencies
    dependencies = Dependencies()
    dependencies.add("resonate:random", r)
    dependencies.add("resonate:time", clock)

    # create a simulator
    sim = Simulator(r, clock)

    servers = [Server(r, "server", "server", clock=clock)]
    workers = [Worker(r, f"default/{n}", "default", registry=registry, dependencies=dependencies) for n in range(3)]

    for s in servers:
        sim.add_component(s)

    for w in workers:
        sim.add_component(w)

    for _ in range(steps):
        n = r.randint(0, 99)

        # generate commands
        match r.randint(0, 23):
            case 0:
                sim.send_msg("sim://any@default", Listen(str(n)))
            case 1:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "foo", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, foo, opts=opts))
            case 2:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "bar", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, bar, opts=opts))
            case 3:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "foo", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, baz, opts=opts))
            case 4:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "foo_lfi", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, foo_lfi, opts=opts))
            case 5:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "bar_lfi", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, bar_lfi, opts=opts))
            case 6:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "foo_lfc", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, foo_lfc, opts=opts))
            case 7:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "bar_lfc", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, bar_lfc, opts=opts))
            case 8:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "foo_rfi", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, foo_rfi, opts=opts))
            case 9:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "bar_rfi", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, bar_rfi, opts=opts))
            case 10:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "foo_rfc", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, foo_rfc, opts=opts))
            case 11:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "bar_rfc", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, bar_rfc, opts=opts))
            case 12:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "foo_detached", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, foo_detached, opts=opts))
            case 13:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "bar_detached", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, bar_detached, opts=opts))
            case 14:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "structured_concurrency", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, structured_concurrency, opts=opts))
            case 15:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "fail_25", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, fail_25, opts=opts))
            case 16:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "fail_50", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, fail_50, opts=opts))
            case 17:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "fail_75", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, fail_75, opts=opts))
            case 18:
                opts = Options(target="sim://any@default")
                conv = Remote(str(n), "fail_99", opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, fail_99, opts=opts))
            case 19:
                opts = Options(target="sim://any@default")
                conv = Remote(f"fib-{n}", "fib", (n,), opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, fib, (n,), opts=opts))
            case 20:
                opts = Options(target="sim://any@default")
                conv = Remote(f"fibl-{n}", "fib_lfi", (n,), opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, fib_lfi, (n,), opts=opts))
            case 21:
                opts = Options(target="sim://any@default")
                conv = Remote(f"fibl-{n}", "fib_lfi", (n,), opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, fib_lfc, (n,), opts=opts))
            case 22:
                opts = Options(target="sim://any@default")
                conv = Remote(f"fibr-{n}", "fib_lfi", (n,), opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, fib_rfi, (n,), opts=opts))
            case 23:
                opts = Options(target="sim://any@default")
                conv = Remote(f"fibr-{n}", "fib_lfi", (n,), opts=opts)
                sim.send_msg("sim://any@default", Invoke(conv.id, conv, 0, fib_rfc, (n,), opts=opts))

        # step
        sim.step()

    # log
    for log in sim.logs:
        logger.info(log)
