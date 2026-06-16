from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import msgspec
import pytest

from resonate.heartbeat import AsyncHeartbeat, NoopHeartbeat
from resonate.send import Sender
from resonate.transport import Transport

if TYPE_CHECKING:
    from collections.abc import Callable


# -- Test harness -------------------------------------------------------------


class _RecordingNetwork:
    """A ``Network`` stub that records every sent body and echoes a 200 reply.

    It records each raw envelope and replies with the matching ``kind`` /
    ``corrId`` so :class:`Transport` validation passes.
    """

    def __init__(self, sent: list[str]) -> None:
        self.sent = sent

    def pid(self) -> str:
        return "test-pid"

    def group(self) -> str:
        return "default"

    def unicast(self) -> str:
        return "local://uni@default/test-pid"

    def anycast(self) -> str:
        return "local://any@default/test-pid"

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    def recv(self, callback: Callable[[str], None]) -> None: ...

    def target_resolver(self, target: str) -> str:
        return f"local://any@{target}"

    async def send(self, req: str) -> str:
        self.sent.append(req)
        parsed = msgspec.json.decode(req)
        kind = parsed["kind"]
        corr_id = parsed["head"]["corrId"]
        return msgspec.json.encode(
            {"kind": kind, "head": {"corrId": corr_id, "status": 200}, "data": {}}
        ).decode("utf-8")


class Harness:
    """Records sent requests and builds a :class:`Sender` over them."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    def build_sender(self) -> Sender:
        return Sender(Transport(_RecordingNetwork(self.sent)), None)

    def sent_requests_json(self) -> list[dict[str, Any]]:
        """Return sent requests as flattened JSON (``data`` fields plus ``kind``)."""
        out: list[dict[str, Any]] = []
        for raw in self.sent:
            req = msgspec.json.decode(raw)
            if isinstance(req, dict) and "head" in req and "data" in req:
                data = req.get("data")
                flat: dict[str, Any] = dict(data) if isinstance(data, dict) else {}
                if "kind" in req:
                    flat["kind"] = req["kind"]
                out.append(flat)
            else:
                out.append(req)
        return out


def _test_heartbeat(sender: Sender) -> AsyncHeartbeat:
    return AsyncHeartbeat("test-pid", 50, sender)


# ── Heartbeat sends ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_heartbeat_sends_request_with_tracked_tasks() -> None:
    harness = Harness()
    hb = _test_heartbeat(harness.build_sender())

    hb.start("task-1", 1)
    hb.start("task-2", 5)

    # Wait for at least one heartbeat tick.
    await asyncio.sleep(0.12)

    hb.shutdown()

    requests = harness.sent_requests_json()
    heartbeats = [r for r in requests if r.get("kind") == "task.heartbeat"]

    assert heartbeats, "should have sent at least one heartbeat"

    # Check the last heartbeat contains both tasks.
    last_hb = heartbeats[-1]
    assert last_hb["pid"] == "test-pid"

    tasks = last_hb["tasks"]
    assert len(tasks) == 2

    ids = [t["id"] for t in tasks]
    assert "task-1" in ids
    assert "task-2" in ids


@pytest.mark.asyncio
async def test_heartbeat_reflects_task_removal() -> None:
    harness = Harness()
    hb = _test_heartbeat(harness.build_sender())

    hb.start("task-1", 1)
    hb.start("task-2", 2)

    # Wait for a heartbeat with both tasks.
    await asyncio.sleep(0.08)

    # Remove task-1.
    hb.stop("task-1")

    # Wait for another heartbeat with only task-2.
    await asyncio.sleep(0.08)

    hb.shutdown()

    requests = harness.sent_requests_json()
    heartbeats = [r for r in requests if r.get("kind") == "task.heartbeat"]

    # The last heartbeat should only contain task-2.
    last_hb = heartbeats[-1]
    tasks = last_hb["tasks"]
    assert len(tasks) == 1
    assert tasks[0]["id"] == "task-2"


# ── NoopHeartbeat ──────────────────────────────────────────────


def test_noop_heartbeat_start_stop_shutdown_are_harmless() -> None:
    hb = NoopHeartbeat()
    hb.start("task-1", 1)
    hb.start("task-2", 2)
    hb.stop("task-1")
    hb.stop("nonexistent")
    hb.shutdown()
