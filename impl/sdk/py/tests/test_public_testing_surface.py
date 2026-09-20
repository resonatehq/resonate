"""``resonate.testing`` from an application author's point of view.

The SDK is something other people build on, so its own testability is only
half the job: a user writing durable functions needs a way to test *those*,
without a server and without waiting. This module is that contract, exercised
the way the README documents it -- if these break, the documented workflow
breaks.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from resonate.connections import LocalConnection
from resonate.context import Context
from resonate.error import ApplicationError
from resonate.observability import PromiseCreateRequested
from resonate.resonate import Resonate
from resonate.retry import Constant
from resonate.testing import (
    FakeClock,
    ManualSleeper,
    RecordingObserver,
    RecordingSleeper,
    cache_of,
    instant_sleeper,
    local_resonate,
    pending_promise,
    rejected_promise,
    resolved_promise,
    root_context,
)

if TYPE_CHECKING:
    from resonate.context import Context
    from resonate.resonate import Resonate
    from resonate.types import PromiseCreateReq, PromiseRecord

# ═══════════════════════════════════════════════════════════════
#  End-to-end, no server
# ═══════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_a_workflow_runs_end_to_end_against_the_in_process_server(
    resonate: Resonate,
) -> None:
    """The README's headline example, on the shared fixture."""

    @resonate.register
    async def charge(ctx: Context, trip: str) -> int:
        return len(trip) * 100

    @resonate.register
    async def book(ctx: Context, trip: str) -> str:
        amount = await ctx.run(charge, trip)
        return f"booked {trip} for {amount}"

    handle = resonate.run("booking-1", book, "lisbon")
    assert await handle.result() == "booked lisbon for 600"


@pytest.mark.asyncio
async def test_the_same_id_twice_yields_the_same_result(
    resonate: Resonate,
) -> None:
    """Durable identity works in tests exactly as in production."""
    runs = {"n": 0}

    @resonate.register
    async def once(ctx: Context) -> int:
        runs["n"] += 1
        return runs["n"]

    first = await resonate.run("idem-1", once).result()
    second = await resonate.run("idem-1", once).result()

    assert first == second == 1
    assert runs["n"] == 1


@pytest.mark.asyncio
async def test_a_rejection_crosses_the_boundary_and_is_re_raised(
    resonate: Resonate,
) -> None:
    @resonate.register
    async def fails(ctx: Context) -> None:
        msg = "no seats left"
        raise ApplicationError(msg)

    with pytest.raises(ApplicationError, match="no seats left"):
        await resonate.run("fail-1", fails).result()


@pytest.mark.asyncio
async def test_retries_are_off_by_default_so_failures_surface_at_once(
    resonate: Resonate,
) -> None:
    """The SDK default would retry ~30 times with exponential backoff.

    That is right in production and intolerable in a test suite, so
    ``local_resonate`` pins ``Never()``.
    """
    calls = {"n": 0}

    @resonate.register
    async def flaky(ctx: Context) -> int:
        calls["n"] += 1
        msg = "transient"
        raise RuntimeError(msg)

    with pytest.raises(Exception, match="transient"):
        await resonate.run("flaky-1", flaky).result()
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_a_caller_can_opt_a_retry_policy_back_in() -> None:
    client = local_resonate(retry_policy=Constant(max_retries=2, delay=0))
    calls = {"n": 0}

    @client.register
    async def flaky(ctx: Context) -> int:
        calls["n"] += 1
        if calls["n"] < 3:
            msg = "transient"
            raise RuntimeError(msg)
        return 42

    try:
        assert await client.run("retry-1", flaky).result() == 42
        assert calls["n"] == 3
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_one_clock_governs_both_the_client_and_the_server() -> None:
    """The bug this threading prevents: deadlines dated by a clock nobody enforces.

    A ``FakeClock`` on the client alone would stamp every promise with a 1970
    deadline while the in-process server, still on the wall clock, timed it out
    the moment it arrived. Threading the clock into both halves keeps a frozen
    clock usable for real runs.
    """
    clock = FakeClock(start=1_700_000_000_000)
    client = local_resonate(clock=clock)

    @client.register
    async def greet(ctx: Context, name: str) -> str:
        return f"hello {name}"

    try:
        assert await client.run("clocked-1", greet, "world").result() == "hello world"
    finally:
        await client.stop()


# ═══════════════════════════════════════════════════════════════
#  Isolation guarantees
# ═══════════════════════════════════════════════════════════════


def test_the_shell_environment_cannot_redirect_a_local_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``env={}`` is the guard, and this is the scenario it guards against."""
    monkeypatch.setenv("RESONATE_URL", "http://a-real-server:8001")

    client = local_resonate(autostart=False)

    assert isinstance(client._network, LocalConnection)


@pytest.mark.asyncio
async def test_two_clients_do_not_share_server_state() -> None:
    """Each ``local_resonate`` owns its own server, so tests cannot collide."""
    a, b = local_resonate(), local_resonate()

    @a.register(name="f")
    async def f_a(ctx: Context) -> str:
        return "from a"

    @b.register(name="f")
    async def f_b(ctx: Context) -> str:
        return "from b"

    try:
        assert await a.run("same-id", "f").result() == "from a"
        assert await b.run("same-id", "f").result() == "from b"
    finally:
        await a.stop()
        await b.stop()


# ═══════════════════════════════════════════════════════════════
#  The helpers themselves
# ═══════════════════════════════════════════════════════════════


def test_fake_clock_advances_in_both_units() -> None:
    clock = FakeClock(start=1_000)
    assert clock() == 1_000
    assert clock.advance(500) == 1_500
    assert clock.advance(seconds=2) == 3_500
    assert clock() == 3_500


@pytest.mark.asyncio
async def test_recording_sleeper_records_and_totals() -> None:
    sleeper = RecordingSleeper()
    await sleeper(1.5)
    await sleeper(2.5)
    assert sleeper.delays == [1.5, 2.5]
    assert sleeper.total == 4.0


@pytest.mark.asyncio
async def test_manual_sleeper_parks_until_ticked() -> None:
    sleeper = ManualSleeper()
    done = {"n": 0}

    async def loop() -> None:
        for _ in range(3):
            await sleeper(10.0)
            done["n"] += 1

    task = asyncio.create_task(loop())
    await asyncio.sleep(0)

    assert done["n"] == 0
    await sleeper.tick()
    assert done["n"] == 1
    await sleeper.tick(2)
    assert done["n"] == 3

    await task
    assert sleeper.delays == [10.0, 10.0, 10.0]


@pytest.mark.asyncio
async def test_manual_sleeper_fails_rather_than_hanging_on_a_loop_that_never_sleeps() -> (
    None
):
    """A helper must fail directly -- a hung test tells you nothing."""
    sleeper = ManualSleeper()
    sleeper.max_spin = 5
    with pytest.raises(AssertionError, match="nothing is sleeping"):
        await sleeper.tick()


@pytest.mark.asyncio
async def test_instant_sleeper_yields_without_waiting() -> None:
    await asyncio.wait_for(instant_sleeper(3600), timeout=1.0)


def test_promise_builders_produce_the_states_they_name() -> None:
    assert pending_promise("p").state == "pending"
    assert resolved_promise("p", 1).state == "resolved"
    assert rejected_promise("p").state == "rejected"
    assert rejected_promise("p", state="rejected_canceled").state == "rejected_canceled"


@pytest.mark.asyncio
async def test_root_context_exercises_the_real_durability_boundary() -> None:
    """Not a mock: the value round-trips through encode/fence/decode."""
    ctx = root_context()

    async def double(c: Context, x: int) -> int:
        return x * 2

    assert await ctx.run(double, 21) == 42
    assert cache_of(ctx)["root:1"].state == "resolved"
    assert cache_of(ctx)["root:1"].value.data == 42


@pytest.mark.asyncio
async def test_root_context_accepts_injected_time() -> None:
    clock = FakeClock(start=5_000)
    sleeper = RecordingSleeper()
    ctx = root_context(clock=clock, sleeper=sleeper, timeout_at=1 << 62)

    assert ctx._child_timeout(timedelta(seconds=10)) == 15_000


def test_cache_of_fails_directly_on_a_context_without_real_effects() -> None:
    """A helper that returned ``None`` here would produce a confusing failure later."""

    class NotEffects:
        """A valid :class:`~resonate.effects.Effects` that is not the real one."""

        async def create_promise(self, req: PromiseCreateReq) -> PromiseRecord:
            return pending_promise(req.id)

        async def settle_promise(self, id: str, result: object) -> PromiseRecord:
            return resolved_promise(id, result)

    ctx = root_context()
    ctx._state.effects = NotEffects()

    with pytest.raises(TypeError, match="cache_of expects"):
        cache_of(ctx)


@pytest.mark.asyncio
async def test_the_observer_fixture_sees_real_traffic(
    resonate: Resonate, observer: RecordingObserver
) -> None:
    """The fixture's observer is wired all the way through the client."""

    @resonate.register
    async def leaf(ctx: Context) -> int:
        return 1

    @resonate.register
    async def parent(ctx: Context) -> int:
        return await ctx.run(leaf)

    await resonate.run("observed-1", parent).result()

    assert observer.of(PromiseCreateRequested), "durable ops should be reported"
    assert observer.dropped() == [], "nothing should have been dropped"
