from __future__ import annotations

import asyncio

import pytest

from resonate.chain import Chain
from resonate.error import PlatformError, ResonateError


@pytest.mark.asyncio
async def test_links_run_in_acquisition_order_under_concurrency() -> None:
    # Acquire links in order, but run them as concurrent tasks whose work
    # yields the event loop before recording. Order must follow acquisition,
    # not scheduling.
    chain = Chain()
    order: list[int] = []

    async def work(n: int) -> int:
        await asyncio.sleep(0)  # let a non-chained impl race ahead
        order.append(n)
        return n

    # Acquire synchronously in order.
    links = [chain.link() for _ in range(5)]
    # Run later links first to prove the chain — not task start order — wins.
    results = await asyncio.gather(
        *(link.run(lambda n=i: work(n)) for i, link in reversed(list(enumerate(links))))
    )

    assert order == [0, 1, 2, 3, 4]
    assert sorted(results) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_done_resolves_on_success() -> None:
    chain = Chain()
    link = chain.link()
    await link.run(lambda: _value("ok"))
    assert link.done.done()
    assert link.done.result() is None


@pytest.mark.asyncio
async def test_failure_poisons_every_later_link() -> None:
    # A failing link must propagate its exception the full length of the chain:
    # every successor inherits the same exception object and never runs work.
    chain = Chain()
    boom = RuntimeError("boom")
    ran: list[int] = []

    l1 = chain.link()
    l2 = chain.link()
    l3 = chain.link()

    async def fail() -> None:
        raise boom

    async def track(n: int) -> None:
        ran.append(n)

    t1 = asyncio.create_task(l1.run(fail))
    t2 = asyncio.create_task(l2.run(lambda: track(2)))
    t3 = asyncio.create_task(l3.run(lambda: track(3)))

    for t in (t1, t2, t3):
        with pytest.raises(RuntimeError) as exc:
            await t
        assert exc.value is boom  # same object travels down the chain

    assert ran == []  # no successor ran its work
    assert l1.done.exception() is boom
    assert l2.done.exception() is boom
    assert l3.done.exception() is boom


@pytest.mark.asyncio
async def test_platform_error_settles_done() -> None:
    # PlatformError is a BaseException (not Exception), so the except clause
    # lists it explicitly — a platform failure must still settle ``done`` or
    # successors and id-awaiters would deadlock.
    chain = Chain()
    link = chain.link()
    boom = PlatformError([ResonateError("boom")])

    async def fail() -> None:
        raise boom

    with pytest.raises(PlatformError):
        await link.run(fail)
    assert link.done.exception() is boom


@pytest.mark.asyncio
async def test_cancellation_is_not_swallowed() -> None:
    # CancelledError (BaseException, not Exception/PlatformError) must propagate
    # so task cancellation works.
    chain = Chain()
    link = chain.link()
    started = asyncio.Event()

    async def work() -> None:
        started.set()
        await asyncio.sleep(3600)

    task = asyncio.create_task(link.run(work))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def _value(v: str) -> str:
    return v
