from __future__ import annotations

import logging
import random
from typing import TYPE_CHECKING, Any

from resonate.models.commands import Invoke, Listen
from resonate.registry import Registry
from sim.simulator import Server, Simulator, Worker

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate import Context

logger = logging.getLogger(__name__)


def foo(ctx: Context) -> Generator[Any, Any, Any]:
    p1 = yield ctx.lfi(bar)
    p2 = yield ctx.rfi(bar).options(send_to="sim://any@default")
    yield ctx.lfi(bar)
    yield ctx.lfc(bar)
    yield ctx.rfi(bar).options(send_to="sim://any@default")
    yield ctx.rfc(bar).options(send_to="sim://any@default")
    yield ctx.detached(bar).options(send_to="sim://any@default")

    return (yield p1), (yield p2)


def bar(ctx: Context) -> Generator[Any, Any, Any]:
    p1 = yield ctx.lfi(baz)
    p2 = yield ctx.rfi(baz).options(send_to="sim://any@default")
    yield ctx.lfi(baz)
    yield ctx.lfc(baz)
    yield ctx.rfi(baz).options(send_to="sim://any@default")
    yield ctx.rfc(baz).options(send_to="sim://any@default")
    yield ctx.detached(baz).options(send_to="sim://any@default")

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
    p = yield ctx.rfi(bar_rfi).options(send_to="sim://any@default")
    v = yield p
    return v


def bar_rfi(ctx: Context) -> Generator:
    p = yield ctx.rfi(baz).options(send_to="sim://any@default")
    v = yield p
    return v


def foo_rfc(ctx: Context) -> Generator:
    v = yield ctx.lfc(bar_rfc)
    return v


def bar_rfc(ctx: Context) -> Generator:
    v = yield ctx.rfc(baz).options(send_to="sim://any@default")
    return v


def foo_detached(ctx: Context) -> Generator:
    yield ctx.detached(bar_detached).options(send_to="sim://any@default")


def bar_detached(ctx: Context) -> Generator:
    yield ctx.detached(baz).options(send_to="sim://any@default")


def structured_concurrency(ctx: Context) -> Generator:
    yield ctx.lfi(baz)
    yield ctx.lfi(baz)
    yield ctx.lfi(baz)


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

    p1 = yield ctx.rfi(fib_rfi, n - 1).options(id=f"fibr-{n - 1}", send_to="sim://any@default")
    p2 = yield ctx.rfi(fib_rfi, n - 2).options(id=f"fibr-{n - 2}", send_to="sim://any@default")

    v1 = yield p1
    v2 = yield p2

    return v1 + v2


def fib_rfc(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    v1 = yield ctx.rfc(fib_rfc, n - 1).options(id=f"fibr-{n - 1}", send_to="sim://any@default")
    v2 = yield ctx.rfc(fib_rfc, n - 2).options(id=f"fibr-{n - 2}", send_to="sim://any@default")

    return v1 + v2


def test_dst(seed: str, steps: int) -> None:
    logger.info("DST(seed=%s, steps=%s)", seed, steps)

    # seed the random number generator
    r = random.Random(seed)

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
    registry.add(fib_lfi, "fib_lfi")
    registry.add(fib_lfc, "fib_lfc")
    registry.add(fib_rfi, "fib_rfi")
    registry.add(fib_rfc, "fib_rfc")

    # create a simulator
    sim = Simulator(r)

    servers = [Server(r, "server", "server")]
    workers = [Worker(r, f"default/{n}", "default", registry=registry, store=servers[0].store, drop_at=r.randint(0, steps) * 1000) for n in range(3)]

    for s in servers:
        sim.add_component(s)

    for w in workers:
        sim.add_component(w)

    for _ in range(steps):
        n = r.randint(0, 99)

        # generate commands
        match r.randint(0, 18):
            case 0:
                sim.send_msg("sim://any@default", Listen(str(n)))
            case 1:
                sim.send_msg("sim://any@default", Invoke(str(n), "foo", foo))
            case 2:
                sim.send_msg("sim://any@default", Invoke(str(n), "bar", bar))
            case 3:
                sim.send_msg("sim://any@default", Invoke(str(n), "baz", baz))
            case 4:
                sim.send_msg("sim://any@default", Invoke(str(n), "foo_lfi", foo_lfi))
            case 5:
                sim.send_msg("sim://any@default", Invoke(str(n), "bar_lfi", bar_lfi))
            case 6:
                sim.send_msg("sim://any@default", Invoke(str(n), "foo_lfc", foo_lfc))
            case 7:
                sim.send_msg("sim://any@default", Invoke(str(n), "bar_lfc", bar_lfc))
            case 8:
                sim.send_msg("sim://any@default", Invoke(str(n), "foo_rfi", foo_rfi))
            case 9:
                sim.send_msg("sim://any@default", Invoke(str(n), "bar_rfi", bar_rfi))
            case 10:
                sim.send_msg("sim://any@default", Invoke(str(n), "foo_rfc", foo_rfc))
            case 11:
                sim.send_msg("sim://any@default", Invoke(str(n), "bar_rfc", bar_rfc))
            case 12:
                sim.send_msg("sim://any@default", Invoke(str(n), "foo_detached", foo_detached))
            case 13:
                sim.send_msg("sim://any@default", Invoke(str(n), "bar_detached", bar_detached))
            case 14:
                sim.send_msg("sim://any@default", Invoke(str(n), "structured_concurrency", structured_concurrency))
            case 15:
                sim.send_msg("sim://any@default", Invoke(f"fibl-{n}", "fib_lfi", fib_lfi, (n,)))
            case 16:
                sim.send_msg("sim://any@default", Invoke(f"fibl-{n}", "fib_lfc", fib_lfc, (n,)))
            case 17:
                sim.send_msg("sim://any@default", Invoke(f"fibr-{n}", "fib_rfi", fib_rfi, (n,)))
            case 18:
                sim.send_msg("sim://any@default", Invoke(f"fibr-{n}", "fib_rfc", fib_rfc, (n,)))

        # step
        sim.step()

    # log
    for log in sim.logs:
        logger.info(log)
