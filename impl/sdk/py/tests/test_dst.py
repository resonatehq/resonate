from __future__ import annotations

import json
import logging
import random
import re
from typing import TYPE_CHECKING, Any

from tabulate import tabulate

from resonate.clocks import StepClock
from resonate.conventions import Remote
from resonate.dependencies import Dependencies
from resonate.models.commands import Invoke, Listen
from resonate.models.result import Ko, Ok, Result
from resonate.options import Options
from resonate.registry import Registry
from resonate.scheduler import Coro, Init, Lfnc, Rfnc
from resonate.simulator import Server, Simulator, Worker

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate import Context
    from resonate.coroutine import Promise
    from resonate.models.context import Info


logger = logging.getLogger(__name__)


def foo(ctx: Context) -> Generator[Any, Any, Any]:
    p1 = yield ctx.lfi(bar)
    p2 = yield ctx.rfi(bar)
    yield ctx.lfi(bar)
    yield ctx.rfi(bar)
    yield ctx.lfc(bar)
    yield ctx.rfc(bar)
    yield ctx.detached(bar)

    return (yield p1), (yield p2)


def bar(ctx: Context) -> Generator[Any, Any, Any]:
    p1 = yield ctx.lfi(baz)
    p2 = yield ctx.rfi(baz)
    yield ctx.lfi(baz)
    yield ctx.rfi(baz)
    yield ctx.lfc(baz)
    yield ctx.rfc(baz)
    yield ctx.detached(baz)

    return (yield p1), (yield p2)


def baz(ctx: Context) -> str:
    return "baz"


def foo_lfi(ctx: Context) -> Generator[Any, Any, Any]:
    p = yield ctx.lfi(bar_lfi)
    v = yield p
    return v


def bar_lfi(ctx: Context) -> Generator[Any, Any, Any]:
    p = yield ctx.lfi(baz)
    v = yield p
    return v


def foo_lfc(ctx: Context) -> Generator[Any, Any, Any]:
    v = yield ctx.lfc(bar_lfc)
    return v


def bar_lfc(ctx: Context) -> Generator[Any, Any, Any]:
    v = yield ctx.lfc(baz)
    return v


def foo_rfi(ctx: Context) -> Generator[Any, Any, Any]:
    p = yield ctx.rfi(bar_rfi)
    v = yield p
    return v


def bar_rfi(ctx: Context) -> Generator[Any, Any, Any]:
    p = yield ctx.rfi(baz)
    v = yield p
    return v


def foo_rfc(ctx: Context) -> Generator[Any, Any, Any]:
    v = yield ctx.lfc(bar_rfc)
    return v


def bar_rfc(ctx: Context) -> Generator[Any, Any, Any]:
    v = yield ctx.rfc(baz)
    return v


def foo_detached(ctx: Context) -> Generator[Any, Any, Any]:
    p = yield ctx.detached(bar_detached)
    return p.id


def bar_detached(ctx: Context) -> Generator[Any, Any, Any]:
    p = yield ctx.detached(baz)
    return p.id


def structured_concurrency_lfi(ctx: Context) -> Generator[Any, Any, Any]:
    p1 = yield ctx.lfi(baz)
    p2 = yield ctx.lfi(baz)
    p3 = yield ctx.lfi(baz)
    return p1.id, p2.id, p3.id


def structured_concurrency_rfi(ctx: Context) -> Generator[Any, Any, Any]:
    p1 = yield ctx.rfi(baz)
    p2 = yield ctx.rfi(baz)
    p3 = yield ctx.rfi(baz)
    return p1.id, p2.id, p3.id


def same_p_lfi(ctx: Context, n: int) -> Generator[Any, Any, None]:
    for _ in range(n):
        yield ctx.lfi(_same_p_lfi, f"{ctx.id}:common")


def _same_p_lfi(ctx: Context, id: str) -> Generator[Any, Any, None]:
    yield ctx.lfi(baz).options(id=id)
    yield ctx.lfi(baz)


def same_p_rfi(ctx: Context, n: int) -> Generator[Any, Any, None]:
    for _ in range(n):
        yield ctx.lfi(_same_p_rfi, f"{ctx.id}:common")


def _same_p_rfi(ctx: Context, id: str) -> Generator[Any, Any, None]:
    yield ctx.rfi(baz).options(id=id)
    yield ctx.lfi(baz)


def same_v_lfi(ctx: Context, n: int) -> Generator[Any, Any, None]:
    p = yield ctx.lfi(baz)
    for _ in range(n):
        yield ctx.lfi(_same_v, p)


def same_v_rfi(ctx: Context, n: int) -> Generator[Any, Any, None]:
    p = yield ctx.rfi(baz)
    for _ in range(n):
        yield ctx.lfi(_same_v, p)


def _same_v(ctx: Context, p: Promise) -> Generator[Any, Any, None]:
    yield p
    yield ctx.lfi(baz)


def fail_25(ctx: Context) -> str:
    r = ctx.get_dependency("resonate:random")
    assert isinstance(r, random.Random)

    if r.random() < 0.25:
        msg = f"ko — {ctx.info.attempt} attempt(s)"
        raise RuntimeError(msg)

    return f"ok — {ctx.info.attempt} attempt(s)"


def fail_50(ctx: Context) -> str:
    r = ctx.get_dependency("resonate:random")
    assert isinstance(r, random.Random)

    if r.random() < 0.50:
        msg = f"ko — {ctx.info.attempt} attempt(s)"
        raise RuntimeError(msg)

    return f"ok — {ctx.info.attempt} attempt(s)"


def fail_75(ctx: Context) -> str:
    r = ctx.get_dependency("resonate:random")
    assert isinstance(r, random.Random)

    if r.random() < 0.75:
        msg = f"ko — {ctx.info.attempt} attempt(s)"
        raise RuntimeError(msg)

    return f"ok — {ctx.info.attempt} attempt(s)"


def fail_99(ctx: Context) -> str:
    r = ctx.get_dependency("resonate:random")
    assert isinstance(r, random.Random)

    if r.random() < 0.99:
        msg = f"ko — {ctx.info.attempt} attempt(s)"
        raise RuntimeError(msg)

    return f"ok — {ctx.info.attempt} attempt(s)"


def fib(ctx: Context, n: int) -> Generator[Any, Any, int]:
    if n <= 1:
        return n

    ps = []
    vs = []
    for i in range(1, 3):
        match (yield ctx.random.choice(["lfi", "rfi", "lfc", "rfc"]).options(id=f"fib:{n - i}")):
            case "lfi":
                p = yield ctx.lfi(fib, n - i).options(id=f"fib-{n - i}")
                ps.append(p)
            case "rfi":
                p = yield ctx.rfi(fib, n - i).options(id=f"fib-{n - i}")
                ps.append(p)
            case "lfc":
                v = yield ctx.lfc(fib, n - i).options(id=f"fib-{n - i}")
                vs.append(v)
            case "rfc":
                v = yield ctx.rfc(fib, n - i).options(id=f"fib-{n - i}")
                vs.append(v)

    for p in ps:
        v = yield p
        vs.append(v)

    assert len(vs) == 2
    return sum(vs)


def test_dst(seed: str, steps: int, log_level: int) -> None:
    logger.setLevel(log_level or logging.INFO)  # if log level is not set use INFO for dst
    logger.info("DST(seed=%s, steps=%s, log_level=%s)", seed, steps, log_level)

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
    registry.add(structured_concurrency_lfi, "structured_concurrency_lfi")
    registry.add(structured_concurrency_rfi, "structured_concurrency_rfi")
    registry.add(same_p_lfi, "same_p_lfi")
    registry.add(same_p_rfi, "same_p_rfi")
    registry.add(same_v_lfi, "same_v_lfi")
    registry.add(same_v_rfi, "same_v_rfi")
    registry.add(fail_25, "fail_25")
    registry.add(fail_50, "fail_50")
    registry.add(fail_75, "fail_75")
    registry.add(fail_99, "fail_99")
    registry.add(fib, "fib")

    # create dependencies
    dependencies = Dependencies()
    dependencies.add("resonate:random", r)
    dependencies.add("resonate:time", clock)

    # create a simulator
    sim = Simulator(r, clock)
    servers: list[Server] = [
        Server(
            r,
            "sim://uni@server",
            "sim://any@server",
            clock=clock,
        ),
    ]
    workers: list[Worker] = [
        Worker(
            r,
            f"sim://uni@default/{n}",
            f"sim://any@default/{n}",
            clock=clock,
            registry=registry,
            dependencies=dependencies,
            log_level=log_level,
        )
        for n in range(10)
    ]

    # add components to simlator
    for c in servers + workers:
        sim.add_component(c)

    # step the simlator
    for _ in range(steps):
        # step
        sim.step()

        # only generate command 10% of the time
        if r.random() > 0.1:
            continue

        # id set
        n = r.randint(0, 99)
        id = str(n)

        # opts
        opts = Options(
            timeout=r.randint(0, steps),
        )

        # generate commands
        match r.randint(0, 24):
            case 0:
                sim.send_msg("sim://any@default", Listen(id))
            case 1:
                conv = Remote(id, id, id, "foo", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, foo))
            case 2:
                conv = Remote(id, id, id, "bar", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, bar))
            case 3:
                conv = Remote(id, id, id, "baz", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, baz))
            case 4:
                conv = Remote(id, id, id, "foo_lfi", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, foo_lfi))
            case 5:
                conv = Remote(id, id, id, "bar_lfi", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, bar_lfi))
            case 6:
                conv = Remote(id, id, id, "foo_lfc", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, foo_lfc))
            case 7:
                conv = Remote(id, id, id, "bar_lfc", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, bar_lfc))
            case 8:
                conv = Remote(id, id, id, "foo_rfi", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, foo_rfi))
            case 9:
                conv = Remote(id, id, id, "bar_rfi", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, bar_rfi))
            case 10:
                conv = Remote(id, id, id, "foo_rfc", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, foo_rfc))
            case 11:
                conv = Remote(id, id, id, "bar_rfc", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, bar_rfc))
            case 12:
                conv = Remote(id, id, id, "foo_detached", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, foo_detached))
            case 13:
                conv = Remote(id, id, id, "bar_detached", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, bar_detached))
            case 14:
                conv = Remote(id, id, id, "structured_concurrency_lfi", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, structured_concurrency_lfi))
            case 15:
                conv = Remote(id, id, id, "same_p_lfi", (n,), opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, same_p_lfi, (n,)))
            case 16:
                conv = Remote(id, id, id, "same_p_rfi", (n,), opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, same_p_rfi, (n,)))
            case 17:
                conv = Remote(id, id, id, "same_v_lfi", (n,), opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, same_v_lfi, (n,)))
            case 18:
                conv = Remote(id, id, id, "same_v_rfi", (n,), opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, same_v_rfi, (n,)))
            case 19:
                conv = Remote(id, id, id, "structured_concurrency_rfi", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, structured_concurrency_rfi))
            case 20:
                conv = Remote(id, id, id, "fail_25", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, fail_25))
            case 21:
                conv = Remote(id, id, id, "fail_50", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, fail_50))
            case 22:
                conv = Remote(id, id, id, "fail_75", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, fail_75))
            case 23:
                conv = Remote(id, id, id, "fail_99", opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, fail_99))
            case 24:
                conv = Remote(id, id, id, "fib", (n,), opts=opts)
                sim.send_msg("sim://any@default", Invoke(id, conv, 0, fib, (n,)))

    # log
    for log in sim.logs:
        logger.info(log)

    print_worker_computations(workers)


def print_worker_computations(workers: list[Worker]) -> None:
    head = ["id", "worker", "func", "result", "attempts", "timeout"]
    data = sorted(
        [
            [c.id, w.uni, func(c.graph.root.value.func), traffic_light(c.result()), info(c.graph.root.value.func).attempt, f"{info(c.graph.root.value.func).timeout:,.0f}"]
            for w in workers
            for c in w.scheduler.computations.values()
        ],
        key=lambda row: natsort(row[0]),  # sort by computation id
    )
    logger.debug("\n%s", tabulate(data, head, tablefmt="outline", colalign=("left", "left", "left", "left", "right", "right")))


def func(f: Init | Lfnc | Rfnc | Coro | None) -> str:
    match f:
        case Init(next):
            return func(next)
        case Lfnc(func=fn) | Coro(func=fn):
            return fn.__name__
        case Rfnc(conv=conv):
            return json.loads(conv.data)["func"]
        case None:
            return ""


def info(f: Init | Lfnc | Rfnc | Coro | None) -> Info:
    class InfoPlaceholder:
        attempt = 0
        idempotency_key = None
        tags = None
        timeout = 0
        version = 0

    match f:
        case Lfnc(ctx=ctx) | Coro(ctx=ctx):
            return ctx.info
        case _:
            return InfoPlaceholder()


def traffic_light(r: Result | None) -> str:
    match r:
        case Ok(v):
            return f"🟢 {v}"
        case None:
            return "🟡"
        case Ko(e):
            return f"🔴 {e}"


def natsort(s: str | int) -> list[str | int]:
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", str(s))]
