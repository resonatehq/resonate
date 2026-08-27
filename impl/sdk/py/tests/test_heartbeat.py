"""Heartbeat behaviour, driven one beat at a time.

These tests used to say ``await asyncio.sleep(0.12)`` and hope two 50ms beats
had landed -- slow, and flaky on a loaded CI box. The interval is now an
injected :class:`~resonate.testing.ManualSleeper`, so a test releases exactly
the beats it wants and asserts on exactly those.
"""

from __future__ import annotations

import asyncio
from typing import Any

import msgspec
import pytest
from resonate_testing import RecordingNetwork

from resonate.heartbeat import AsyncHeartbeat, NoopHeartbeat
from resonate.send import Sender
from resonate.testing import ManualSleeper
from resonate.transport import Transport

# -- Test harness -------------------------------------------------------------


class Harness:
    """A heartbeat wired to a recording network and a hand-driven sleeper."""

    def __init__(self, interval_ms: int = 50) -> None:
        self.net = RecordingNetwork()
        self.sleeper = ManualSleeper()
        self.heartbeat = AsyncHeartbeat(
            "test-pid", interval_ms, Sender(Transport(self.net), None), self.sleeper
        )

    async def beat(self, times: int = 1) -> None:
        """Let the loop complete ``times`` further iterations."""
        await self.sleeper.tick(times)

    def heartbeats(self) -> list[dict[str, Any]]:
        """Every ``task.heartbeat`` request sent, as decoded ``data`` payloads."""
        out: list[dict[str, Any]] = []
        for raw in self.net.sent:
            req = msgspec.json.decode(raw)
            if req.get("kind") == "task.heartbeat":
                out.append(req["data"])
        return out

    def last_task_ids(self) -> list[str]:
        """Ids carried by the most recent heartbeat."""
        tasks: list[dict[str, Any]] = self.heartbeats()[-1]["tasks"]
        return sorted(t["id"] for t in tasks)


# ── Heartbeat sends ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_first_beat_is_immediate() -> None:
    """The loop beats on start, before consulting the sleeper."""
    h = Harness()
    h.heartbeat.start("task-1", 1)

    # No tick released yet: the first beat must already have gone out.
    await _settle()

    assert len(h.heartbeats()) == 1
    h.heartbeat.shutdown()


@pytest.mark.asyncio
async def test_heartbeat_sends_request_with_tracked_tasks() -> None:
    h = Harness()
    h.heartbeat.start("task-1", 1)
    h.heartbeat.start("task-2", 5)

    await h.beat()

    assert h.heartbeats(), "should have sent at least one heartbeat"
    assert h.heartbeats()[-1]["pid"] == "test-pid"
    assert h.last_task_ids() == ["task-1", "task-2"]
    h.heartbeat.shutdown()


@pytest.mark.asyncio
async def test_heartbeat_reflects_task_removal() -> None:
    h = Harness()
    h.heartbeat.start("task-1", 1)
    h.heartbeat.start("task-2", 2)
    await h.beat()
    assert h.last_task_ids() == ["task-1", "task-2"]

    h.heartbeat.stop("task-1")
    await h.beat()

    assert h.last_task_ids() == ["task-2"]
    h.heartbeat.shutdown()


@pytest.mark.asyncio
async def test_beats_use_the_configured_interval() -> None:
    """The loop sleeps ``interval_ms / 1000`` between beats -- assert the list.

    The delay sequence is the policy's real behaviour; previously it was
    unobservable and only the *effect* of sleeping could be waited on.
    """
    h = Harness(interval_ms=250)
    h.heartbeat.start("task-1", 1)

    await h.beat(3)

    assert h.sleeper.delays[:3] == [0.25, 0.25, 0.25]
    h.heartbeat.shutdown()


@pytest.mark.asyncio
async def test_beat_with_no_tasks_sends_nothing() -> None:
    """A tracked-then-untracked heartbeat keeps looping but stops sending."""
    h = Harness()
    h.heartbeat.start("task-1", 1)
    await h.beat()
    before = len(h.heartbeats())

    # ``stop`` of the last task cancels the loop entirely.
    h.heartbeat.stop("task-1")
    await _settle()

    assert len(h.heartbeats()) == before
    h.heartbeat.shutdown()


@pytest.mark.asyncio
async def test_send_failure_does_not_kill_the_loop() -> None:
    """A failed beat is logged and the next one still goes out."""
    h = Harness()
    h.heartbeat.start("task-1", 1)
    await h.beat()

    # Fail exactly the next send, then recover.
    h.net.fail_next(RuntimeError("network down"))
    await h.beat()
    await h.beat()

    assert len(h.heartbeats()) >= 2
    h.heartbeat.shutdown()


# ── NoopHeartbeat ──────────────────────────────────────────────


def test_noop_heartbeat_start_stop_shutdown_are_harmless() -> None:
    hb = NoopHeartbeat()
    hb.start("task-1", 1)
    hb.start("task-2", 2)
    hb.stop("task-1")
    hb.stop("nonexistent")
    hb.shutdown()


async def _settle() -> None:
    """Yield enough turns for pending event-loop work to run."""
    for _ in range(5):
        await asyncio.sleep(0)
