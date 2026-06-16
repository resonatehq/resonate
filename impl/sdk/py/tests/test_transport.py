from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import msgspec
import pytest

from resonate.error import DecodingError, ServerError
from resonate.transport import (
    ExecuteMsg,
    Message,
    Transport,
    UnblockMsg,
)
from resonate.types import PromiseRecord, Value

if TYPE_CHECKING:
    from collections.abc import Callable


class StubNetwork:
    """A minimal in-process ``Network`` standing in for ``LocalNetwork``.

    The stub plays the server: ``send`` returns a canned response and ``recv``
    captures the registered callback so tests can feed it raw messages.
    """

    def __init__(self, response: str = "") -> None:
        self.response = response
        self.sent: list[str] = []
        self.callbacks: list[Callable[[str], None]] = []

    def pid(self) -> str:
        return "test"

    def group(self) -> str:
        return "default"

    def unicast(self) -> str:
        return "local://uni@default/test"

    def anycast(self) -> str:
        return "local://any@default/test"

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def send(self, req: str) -> str:
        self.sent.append(req)
        return self.response

    def recv(self, callback: Callable[[str], None]) -> None:
        self.callbacks.append(callback)

    def target_resolver(self, target: str) -> str:
        return f"local://any@{target}"


def envelope(kind: str, corr_id: str, data: object) -> str:
    return msgspec.json.encode(
        {"kind": kind, "head": {"corrId": corr_id}, "data": data}
    ).decode("utf-8")


# -- send: envelope validation ------------------------------------------------


def test_send_and_validate_envelope_format() -> None:
    response = envelope("promise.create", "env123", {"promise": {"id": "p2"}})
    transport = Transport(StubNetwork(response))

    body = msgspec.json.encode(
        {
            "kind": "promise.create",
            "head": {"corrId": "env123", "version": "2025-01-15"},
            "data": {"id": "p2", "timeoutAt": 2**63 - 1, "param": {}, "tags": {}},
        }
    ).decode("utf-8")

    resp = asyncio.run(transport.send("promise.create", "env123", body))
    assert resp["kind"] == "promise.create"
    assert resp["head"]["corrId"] == "env123"
    assert resp["data"]["promise"]["id"] == "p2"


def test_send_passes_body_to_network() -> None:
    net = StubNetwork(envelope("k", "c", {}))
    transport = Transport(net)
    asyncio.run(transport.send("k", "c", "the-body"))
    assert net.sent == ["the-body"]


def test_send_kind_mismatch() -> None:
    transport = Transport(StubNetwork(envelope("other.kind", "c", {})))
    with pytest.raises(ServerError) as exc:
        asyncio.run(transport.send("expected.kind", "c", "{}"))
    assert exc.value.code == 500
    assert "expected 'expected.kind', got 'other.kind'" in exc.value.message


def test_send_corr_id_mismatch() -> None:
    transport = Transport(StubNetwork(envelope("k", "wrong", {})))
    with pytest.raises(ServerError) as exc:
        asyncio.run(transport.send("k", "right", "{}"))
    assert exc.value.code == 500
    assert "expected 'right', got 'wrong'" in exc.value.message


def test_send_invalid_json_response() -> None:
    transport = Transport(StubNetwork("not json"))
    with pytest.raises(DecodingError):
        asyncio.run(transport.send("k", "c", "{}"))


def test_send_missing_fields_treated_as_empty() -> None:
    # A response with no kind/corrId fails validation against a non-empty kind.
    transport = Transport(StubNetwork("{}"))
    with pytest.raises(ServerError):
        asyncio.run(transport.send("k", "c", "{}"))


# -- recv ---------------------------------------------------------------------


def feed(transport: Transport, net: StubNetwork, raw: str) -> list[Message]:
    received: list[Message] = []
    transport.recv(received.append)
    net.callbacks[0](raw)
    return received


def test_recv_parses_execute_message() -> None:
    net = StubNetwork()
    raw = '{"kind":"execute","data":{"task":{"id":"t1","version":3}}}'
    received = feed(Transport(net), net, raw)
    assert len(received) == 1
    msg = received[0]
    assert isinstance(msg, ExecuteMsg)
    assert msg.task_id == "t1"
    assert msg.version == 3


def test_recv_execute_message_default_version() -> None:
    net = StubNetwork()
    raw = '{"kind":"execute","data":{"task":{"id":"t1"}}}'
    received = feed(Transport(net), net, raw)
    assert isinstance(received[0], ExecuteMsg)
    assert received[0].version == 0


def test_recv_parses_unblock_message() -> None:
    net = StubNetwork()
    raw = (
        '{"kind":"unblock","data":{"promise":'
        '{"id":"p1","state":"resolved","value":{"data":"dmFs"},"timeoutAt":123}}}'
    )
    received = feed(Transport(net), net, raw)
    assert len(received) == 1
    msg = received[0]
    assert isinstance(msg, UnblockMsg)
    assert msg.promise == PromiseRecord(
        id="p1",
        state="resolved",
        value=Value(data="dmFs"),
        timeout_at=123,
    )


def test_recv_discards_invalid_json() -> None:
    net = StubNetwork()
    received = feed(Transport(net), net, "not json")
    assert received == []


def test_recv_discards_unknown_kind() -> None:
    net = StubNetwork()
    received = feed(Transport(net), net, '{"kind":"mystery","data":{}}')
    assert received == []


# -- network accessor ---------------------------------------------------------


def test_network_accessor() -> None:
    net = StubNetwork()
    assert Transport(net).network() is net
