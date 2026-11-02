"""Behaviour tests for :mod:`resonate.effects`.

A minimal stub network is inlined here, implementing only the
``promise.create`` / ``promise.settle`` handlers the effects tests exercise.
The stub speaks the
protocol-envelope wire format (echoing ``kind`` and ``corrId``) so the real
:class:`Sender` / :class:`Transport` validation passes unchanged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import msgspec
import pytest

from resonate.codec import Codec, NoopEncryptor, dec_hook
from resonate.effects import ResonateEffects
from resonate.error import PlatformError, SpeculationMismatchError
from resonate.send import Sender
from resonate.transport import Transport
from resonate.types import PromiseCreateReq, PromiseRecord, Value

if TYPE_CHECKING:
    from collections.abc import Callable

I64_MAX = 2**63 - 1


# =============================================================================
# Test harness
# =============================================================================


def _test_codec() -> Codec:
    return Codec(NoopEncryptor())


def _value_from_wire(raw: Any) -> Value:
    """Build a Value from a parsed-JSON ``{headers, data}`` object (or null)."""
    return Value() if raw is None else msgspec.convert(raw, Value, dec_hook=dec_hook)


def _promise_to_json(p: PromiseRecord) -> dict[str, Any]:
    """Convert a PromiseRecord to the server response JSON."""
    return {
        "id": p.id,
        "state": p.state,
        "param": {"headers": p.param.headers, "data": p.param.data},
        "value": {"headers": p.value.headers, "data": p.value.data},
        "tags": p.tags,
        "timeoutAt": p.timeout_at,
        "createdAt": p.created_at,
        "settledAt": p.settled_at,
    }


class StubNetwork:
    """In-memory promise store mimicking the server, tracking send count.

    Implements the :class:`~resonate.connections.Network` protocol; only
    ``promise.create`` and ``promise.settle`` are handled (the operations the
    effects tests use), everything else returns a 400.
    """

    def __init__(self) -> None:
        self.promises: dict[str, PromiseRecord] = {}
        self.send_count = 0

    def unicast(self) -> str:
        return "test-unicast"

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    def recv(self, callback: Callable[[str], None]) -> None: ...

    def resolve_target(self, target: str) -> str:
        return target

    async def send(self, req: str, headers: dict[str, str] | None = None) -> str:
        self.send_count += 1
        req_json = msgspec.json.decode(req)

        # Unwrap envelope: the Sender always sends { kind, head, data }.
        kind = req_json.get("kind", "")
        corr_id = req_json.get("head", {}).get("corrId")
        data = req_json.get("data", {})

        if kind == "promise.create":
            status, resp_data = 200, self._handle_promise_create(data)
        elif kind == "promise.settle":
            status, resp_data = 200, self._handle_promise_settle(data)
        elif kind == "task.fence":
            # Stub: ignore the lease, unwrap the action, and dispatch to the
            # matching promise handler, echoing the fence response shape.
            action = data.get("action", {})
            sub = action.get("data", {})
            if action.get("kind") == "promise.settle":
                inner = self._handle_promise_settle(sub)
            else:
                inner = self._handle_promise_create(sub)
            status = 200
            resp_data = {"action": {"data": inner}, "preload": []}
        else:
            status, resp_data = 400, f"unknown request kind: {kind}"

        resp = {
            "kind": kind,
            "head": {"corrId": corr_id, "status": status, "version": "2025-01-15"},
            "data": resp_data,
        }
        return msgspec.json.encode(resp).decode("utf-8")

    def _handle_promise_create(self, data: Any) -> dict[str, Any]:
        promise_id = data.get("id", "")
        existing = self.promises.get(promise_id)
        if existing is not None:
            return {"promise": _promise_to_json(existing)}

        record = PromiseRecord(
            id=promise_id,
            state="pending",
            timeout_at=data.get("timeoutAt", I64_MAX),
            param=_value_from_wire(data.get("param")),
            value=Value(),
            tags=data.get("tags") or {},
            created_at=0,
            settled_at=None,
        )
        self.promises[promise_id] = record
        return {"promise": _promise_to_json(record)}

    def _handle_promise_settle(self, data: Any) -> dict[str, Any]:
        promise_id = data.get("id", "")
        state_str = data.get("state") or "resolved"
        # resolved -> resolved; rejected / rejected_canceled -> rejected.
        promise_state = "resolved" if state_str == "resolved" else "rejected"
        value = _value_from_wire(data.get("value"))

        existing = self.promises.get(promise_id)
        if existing is not None:
            if existing.state != "pending":
                return {"promise": _promise_to_json(existing)}
            updated = msgspec.structs.replace(
                existing, state=promise_state, value=value, settled_at=1
            )
            self.promises[promise_id] = updated
            return {"promise": _promise_to_json(updated)}

        record = PromiseRecord(
            id=promise_id,
            state=promise_state,
            timeout_at=I64_MAX,
            param=Value(),
            value=value,
            tags={},
            created_at=0,
            settled_at=1,
        )
        self.promises[promise_id] = record
        return {"promise": _promise_to_json(record)}


class Harness:
    """Wraps a :class:`StubNetwork` and builds the client stack over it."""

    def __init__(self) -> None:
        self.network = StubNetwork()

    def get_send_count(self) -> int:
        return self.network.send_count

    def add_promise(self, record: PromiseRecord) -> None:
        self.network.promises[record.id] = record

    def build_effects(self, preload: list[PromiseRecord]) -> ResonateEffects:
        sender = Sender(Transport(self.network), None)
        return ResonateEffects(sender, _test_codec(), "root-task", 1, preload)


def pending_promise(id: str) -> PromiseRecord:
    """Create a pending PromiseRecord with an encoded param."""
    codec = _test_codec()
    return PromiseRecord(
        id=id,
        state="pending",
        timeout_at=I64_MAX,
        param=codec.encode({"func": "test", "args": []}),
        value=Value(),
        tags={},
        created_at=0,
        settled_at=None,
    )


def pending_promise_with_param(id: str, param: Any) -> PromiseRecord:
    """Create a pending PromiseRecord with a specific (encoded) param."""
    codec = _test_codec()
    return PromiseRecord(
        id=id,
        state="pending",
        timeout_at=I64_MAX,
        param=codec.encode(param),
        value=Value(),
        tags={},
        created_at=0,
        settled_at=None,
    )


def resolved_promise(id: str, value: Any) -> PromiseRecord:
    """Create a resolved PromiseRecord with an encoded value."""
    codec = _test_codec()
    return PromiseRecord(
        id=id,
        state="resolved",
        timeout_at=I64_MAX,
        param=Value(),
        value=codec.encode(value),
        tags={},
        created_at=0,
        settled_at=1,
    )


# =============================================================================
# create_promise
# =============================================================================


@pytest.mark.asyncio
async def test_create_returns_cached_promise_from_preload_without_hitting_network() -> (
    None
):
    harness = Harness()
    effects = harness.build_effects([pending_promise("p1")])

    req = PromiseCreateReq(id="p1", timeout_at=I64_MAX, param=Value(), tags={})
    record = await effects.create_promise(req)
    assert record.state == "pending"
    assert harness.get_send_count() == 0


@pytest.mark.asyncio
async def test_create_hits_network_when_promise_not_in_preload() -> None:
    harness = Harness()
    effects = harness.build_effects([])

    req = PromiseCreateReq(id="p2", timeout_at=I64_MAX, param=Value(), tags={})
    record = await effects.create_promise(req)
    assert record.state == "pending"
    # Logged, not sent. The sync point is what talks to the server.
    assert harness.get_send_count() == 0
    await effects.flush()
    assert harness.get_send_count() == 1


@pytest.mark.asyncio
async def test_create_adds_to_cache_second_call_uses_cache() -> None:
    harness = Harness()
    effects = harness.build_effects([])

    req = PromiseCreateReq(id="p3", timeout_at=I64_MAX, param=Value(), tags={})
    await effects.create_promise(req)
    assert harness.get_send_count() == 0

    record = await effects.create_promise(req)
    assert record.state == "pending"
    assert harness.get_send_count() == 0
    # The second call was a cache hit, so the batch carries one create.
    await effects.flush()
    assert harness.get_send_count() == 1


# =============================================================================
# settle_promise
# =============================================================================


@pytest.mark.asyncio
async def test_settle_returns_cached_when_already_settled_in_preload() -> None:
    harness = Harness()
    effects = harness.build_effects([resolved_promise("s1", 42)])

    record = await effects.settle_promise("s1", 99)
    assert record.state == "resolved"
    assert harness.get_send_count() == 0


@pytest.mark.asyncio
async def test_settle_hits_network_when_preloaded_promise_is_pending() -> None:
    harness = Harness()
    # Seed the stub so settle can find it.
    harness.add_promise(pending_promise("s2"))
    effects = harness.build_effects([pending_promise("s2")])

    record = await effects.settle_promise("s2", "ok")
    assert record.state == "resolved"
    assert harness.get_send_count() == 0
    await effects.flush()
    assert harness.get_send_count() == 1


@pytest.mark.asyncio
async def test_settle_updates_cache_second_settle_is_cached() -> None:
    harness = Harness()
    effects = harness.build_effects([])

    # Create the promise first.
    req = PromiseCreateReq(id="s3", timeout_at=I64_MAX, param=Value(), tags={})
    await effects.create_promise(req)
    assert harness.get_send_count() == 0

    # Settle it. Still local until the flush.
    await effects.settle_promise("s3", "done")
    assert harness.get_send_count() == 0

    # Second settle should use cache.
    record = await effects.settle_promise("s3", "done")
    assert record.state == "resolved"
    assert harness.get_send_count() == 0
    await effects.flush()
    assert harness.get_send_count() == 2


@pytest.mark.asyncio
async def test_settle_hits_network_when_promise_not_in_cache() -> None:
    harness = Harness()
    # Seed stub directly (not in preload/cache).
    harness.add_promise(pending_promise("s4"))
    effects = harness.build_effects([])

    record = await effects.settle_promise("s4", "ok")
    assert record.state == "resolved"
    assert harness.get_send_count() == 0
    await effects.flush()
    assert harness.get_send_count() == 1


# =============================================================================
# cached promise values
# =============================================================================


@pytest.mark.asyncio
async def test_preloaded_pending_promise_has_decoded_param() -> None:
    harness = Harness()
    param = {"func": "f", "args": []}
    effects = harness.build_effects([pending_promise_with_param("v1", param)])

    req = PromiseCreateReq(id="v1", timeout_at=I64_MAX, param=Value(), tags={})
    record = await effects.create_promise(req)
    assert record.param.data == param


@pytest.mark.asyncio
async def test_preloaded_resolved_promise_has_decoded_value() -> None:
    harness = Harness()
    val = {"answer": 42}
    effects = harness.build_effects([resolved_promise("v2", val)])

    record = await effects.settle_promise("v2", 0)
    assert record.value.data == val


@pytest.mark.asyncio
async def test_promise_created_via_network_has_correct_decoded_values() -> None:
    harness = Harness()
    effects = harness.build_effects([])

    param_data = {"func": "myFunc", "args": [1, "two"]}
    req = PromiseCreateReq(
        id="v3",
        timeout_at=I64_MAX,
        param=Value(headers=None, data=param_data),
        tags={},
    )
    await effects.create_promise(req)

    # Second call from cache.
    record = await effects.create_promise(req)
    assert record.param.data == param_data


@pytest.mark.asyncio
async def test_promise_settled_via_network_has_correct_decoded_values() -> None:
    harness = Harness()
    effects = harness.build_effects([])

    req = PromiseCreateReq(id="v4", timeout_at=I64_MAX, param=Value(), tags={})
    await effects.create_promise(req)

    val = {"result": "success", "count": 7}
    await effects.settle_promise("v4", val)

    # Second settle from cache.
    record = await effects.settle_promise("v4", val)
    assert record.value.data == val


@pytest.mark.asyncio
async def test_multiple_preloaded_promises_each_have_correct_values() -> None:
    harness = Harness()
    effects = harness.build_effects(
        [
            pending_promise_with_param("m1", {"func": "f", "args": []}),
            resolved_promise("m2", "hello"),
            resolved_promise("m3", [1, 2, 3]),
        ]
    )

    r1 = await effects.create_promise(
        PromiseCreateReq(id="m1", timeout_at=I64_MAX, param=Value(), tags={})
    )
    assert r1.state == "pending"
    assert r1.param.data == {"func": "f", "args": []}

    r2 = await effects.settle_promise("m2", 0)
    assert r2.state == "resolved"
    assert r2.value.data == "hello"

    r3 = await effects.settle_promise("m3", 0)
    assert r3.state == "resolved"
    assert r3.value.data == [1, 2, 3]

    assert harness.get_send_count() == 0


def _create(id: str, scope: str) -> PromiseCreateReq:
    return PromiseCreateReq(
        id=id,
        timeout_at=I64_MAX,
        param=Value(),
        tags={"resonate:scope": scope},
    )


# =============================================================================
# flush: prune, then one ordered batch
# =============================================================================


@pytest.mark.asyncio
async def test_flush_drops_a_pending_local_create() -> None:
    """A local promise that never settled is not a checkpoint. Replay re-runs it."""
    harness = Harness()
    effects = harness.build_effects([])

    await effects.create_promise(_create("local.1", "local"))
    await effects.flush()

    assert harness.get_send_count() == 0
    assert "local.1" not in harness.network.promises


@pytest.mark.asyncio
async def test_flush_keeps_a_pending_local_create_the_suspend_awaits() -> None:
    harness = Harness()
    effects = harness.build_effects([])

    await effects.create_promise(_create("local.1", "local"))
    await effects.flush(["local.1"])

    assert harness.get_send_count() == 1
    assert harness.network.promises["local.1"].state == "pending"


@pytest.mark.asyncio
async def test_flush_sends_a_local_create_and_its_settle() -> None:
    """A finished local body is the checkpoint. Both ops go, create first."""
    harness = Harness()
    effects = harness.build_effects([])

    await effects.create_promise(_create("local.1", "local"))
    record = await effects.settle_promise("local.1", 7)
    assert record.value.data == 7
    assert harness.get_send_count() == 0

    await effects.flush()

    assert harness.get_send_count() == 2
    stored = harness.network.promises["local.1"]
    assert stored.state == "resolved"


@pytest.mark.asyncio
async def test_flush_keeps_a_global_create_that_nobody_awaited() -> None:
    """Detached and other global creates must exist even with an empty todo set."""
    harness = Harness()
    effects = harness.build_effects([])

    await effects.create_promise(_create("remote.1", "global"))
    await effects.flush()

    assert harness.get_send_count() == 1
    assert harness.network.promises["remote.1"].state == "pending"


@pytest.mark.asyncio
async def test_flush_drops_only_the_pending_local_beside_a_remote_todo() -> None:
    harness = Harness()
    effects = harness.build_effects([])

    await effects.create_promise(_create("local.1", "local"))
    await effects.create_promise(_create("remote.1", "global"))
    await effects.flush(["remote.1"])

    assert harness.get_send_count() == 1
    assert "local.1" not in harness.network.promises
    assert "remote.1" in harness.network.promises


@pytest.mark.asyncio
async def test_flush_aborts_the_batch_when_the_server_disagrees() -> None:
    """The body ran as if create returned pending. The server already settled it.

    The conflicting settle is not sent, and neither is anything logged after it.
    """
    harness = Harness()
    harness.add_promise(resolved_promise("c1", 1))
    effects = harness.build_effects([])

    created = await effects.create_promise(_create("c1", "local"))
    assert created.state == "pending"
    await effects.settle_promise("c1", 2)
    await effects.create_promise(_create("remote.1", "global"))

    with pytest.raises(PlatformError) as exc:
        await effects.flush(["remote.1"])

    assert isinstance(exc.value.cause, SpeculationMismatchError)
    assert exc.value.cause.id == "c1"
    # The create was sent; the settle and the later remote create were not.
    assert harness.get_send_count() == 1
    assert "remote.1" not in harness.network.promises
    stored = harness.network.promises["c1"]
    assert stored.state == "resolved"
