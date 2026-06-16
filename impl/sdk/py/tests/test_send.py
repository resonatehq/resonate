from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import msgspec

from resonate.network import LocalNetwork
from resonate.send import (
    Envelope,
    Head,
    Sender,
    SubEnvelope,
)
from resonate.transport import Transport
from resonate.types import (
    PromiseCreateReq,
    PromiseRegisterCallbackData,
    PromiseSettleReq,
    Value,
)

if TYPE_CHECKING:
    from collections.abc import Callable

I64_MAX = 2**63 - 1


def _sender(net: LocalNetwork) -> Sender:
    return Sender(Transport(net), None)


# -- Serialization: verify envelope wire format -------------------------------


def test_envelope_serializes_correct_wire_format() -> None:
    data = PromiseCreateReq(id="p1", timeout_at=999, param=Value(), tags={})
    envelope = Envelope(
        kind="promise.create",
        head=Head(corr_id="test-corr", version="2025-01-15", auth=None),
        data=data,
    )
    json = msgspec.json.decode(msgspec.json.encode(envelope))
    assert json["kind"] == "promise.create"
    assert json["head"]["corrId"] == "test-corr"
    assert json["head"]["version"] == "2025-01-15"
    assert json["data"]["id"] == "p1"
    assert json["data"]["timeoutAt"] == 999
    # auth should be absent when None
    assert "auth" not in json["head"]


def test_sub_envelope_serializes_nested_action() -> None:
    action = PromiseSettleReq(id="p1", state="resolved", value=Value())
    sub = SubEnvelope(
        kind="promise.settle",
        head=Head(corr_id="sub-corr", version="2025-01-15", auth="token123"),
        data=action,
    )
    json = msgspec.json.decode(msgspec.json.encode(sub))
    assert json["kind"] == "promise.settle"
    assert json["head"]["corrId"] == "sub-corr"
    assert json["head"]["auth"] == "token123"
    assert json["data"]["id"] == "p1"
    assert json["data"]["state"] == "resolved"


# -- Round-trip: Sender methods through LocalNetwork --------------------------


async def _raw_send(net: LocalNetwork, req: Any) -> Any:
    """Send a raw envelope and return its decoded ``data`` portion."""
    resp_str = await net.send(msgspec.json.encode(req).decode("utf-8"))
    resp = msgspec.json.decode(resp_str)
    return resp.get("data", resp) if isinstance(resp, dict) else resp


def _create_task_req(corr_id: str, promise_id: str) -> dict[str, Any]:
    return {
        "kind": "task.create",
        "head": {"corrId": corr_id, "version": "2025-01-15"},
        "data": {
            "pid": "pid1",
            "ttl": 60000,
            "action": {
                "kind": "promise.create",
                "head": {"corrId": f"{corr_id}a", "version": "2025-01-15"},
                "data": {
                    "id": promise_id,
                    "timeoutAt": I64_MAX,
                    "param": {"data": "test"},
                    "tags": {},
                },
            },
        },
    }


def test_promise_create_roundtrip() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="test")
        sender = _sender(net)

        req = PromiseCreateReq(id="rt-p1", timeout_at=I64_MAX, param=Value(), tags={})
        record = await sender.promise_create(req)
        assert record.id == "rt-p1"
        assert record.state == "pending"

    asyncio.run(run())


def test_task_acquire_roundtrip() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")

        # First create a task via raw network (proper envelope format).
        rdata = await _raw_send(net, _create_task_req("c1", "rt-p2"))
        task_id = rdata["task"]["id"]

        # Release the task so we can re-acquire.
        release_req = {
            "kind": "task.release",
            "head": {"corrId": "c2", "version": "2025-01-15"},
            "data": {"id": task_id, "version": 0},
        }
        await _raw_send(net, release_req)

        # Now acquire via Sender.
        sender = _sender(net)
        result = await sender.task_acquire(task_id, 1, "pid1", 60000)
        assert result.promise.id == "rt-p2"

    asyncio.run(run())


def test_task_fulfill_roundtrip() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")

        rdata = await _raw_send(net, _create_task_req("c1", "rt-p3"))
        task_id = rdata["task"]["id"]

        sender = _sender(net)
        promise = await sender.task_fulfill(
            task_id,
            0,
            PromiseSettleReq(id="rt-p3", state="resolved", value=Value(data="result")),
        )
        assert promise.id == "rt-p3"

    asyncio.run(run())


def test_task_suspend_roundtrip() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")

        rdata = await _raw_send(net, _create_task_req("c1", "rt-p4"))
        task_id = rdata["task"]["id"]

        sender = _sender(net)
        result = await sender.task_suspend(
            task_id,
            0,
            [PromiseRegisterCallbackData(awaited="dep-a", awaiter=task_id)],
        )
        assert result == "suspended"

    asyncio.run(run())


def test_task_release_roundtrip() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")

        rdata = await _raw_send(net, _create_task_req("c1", "rt-p5"))
        task_id = rdata["task"]["id"]

        sender = _sender(net)
        await sender.task_release(task_id, 0)

    asyncio.run(run())


# -- Auth token in envelope head ----------------------------------------------


class CapturingNetwork:
    """A ``Network`` stub that records every sent body and echoes a canned reply.

    It replies with the right ``kind`` and ``corrId`` (echoed from the
    request) so the :class:`Transport` validation passes, letting tests
    inspect the raw envelope the :class:`Sender` produced.
    """

    def __init__(self) -> None:
        self.sent: list[str] = []

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
        promise = {
            "id": "x",
            "state": "pending",
            "timeoutAt": I64_MAX,
            "param": {},
            "value": {},
            "tags": {},
            "createdAt": 0,
        }
        if kind == "task.create":
            data: Any = {
                "task": {"id": "x", "state": "acquired", "version": 0},
                "promise": promise,
                "preload": [],
            }
        else:
            data = {"promise": promise}
        return msgspec.json.encode(
            {"kind": kind, "head": {"corrId": corr_id, "status": 200}, "data": data}
        ).decode("utf-8")


def test_envelope_head_contains_auth_when_token_provided() -> None:
    net = CapturingNetwork()
    sender = Sender(Transport(net), "my-secret-token")

    req = PromiseCreateReq(id="auth-p1", timeout_at=I64_MAX, param=Value(), tags={})
    asyncio.run(sender.promise_create(req))

    assert len(net.sent) == 1
    head = msgspec.json.decode(net.sent[0])["head"]
    assert head["auth"] == "my-secret-token"
    assert "corrId" in head
    assert "version" in head


def test_envelope_head_omits_auth_when_no_token() -> None:
    net = CapturingNetwork()
    sender = Sender(Transport(net), None)

    req = PromiseCreateReq(id="no-auth-p1", timeout_at=I64_MAX, param=Value(), tags={})
    asyncio.run(sender.promise_create(req))

    assert len(net.sent) == 1
    head = msgspec.json.decode(net.sent[0])["head"]
    assert "auth" not in head


def test_sub_envelope_head_contains_auth_when_token_provided() -> None:
    net = CapturingNetwork()
    sender = Sender(Transport(net), "sub-token")

    action = PromiseCreateReq(id="sub-p1", timeout_at=I64_MAX, param=Value(), tags={})
    asyncio.run(sender.task_create("test-pid", 60000, action))

    assert net.sent
    sub_head = msgspec.json.decode(net.sent[0])["data"]["action"]["head"]
    assert sub_head["auth"] == "sub-token"
