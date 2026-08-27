"""NATS connector internals -- the client seam, subject mapping, and headers.

``NatsConnection`` takes a structural ``NatsClient`` rather than a concrete
``nats.aio.client.Client``, so every branch below is reachable with in-memory
fakes and no broker.
"""

from __future__ import annotations

import base64
from typing import Any, cast

import pytest
from resonate_base import ORIGIN_HEADER
from resonate_base.error import ConnectorError
from resonate_nats import NatsConnection, _publish_subject

# ═══════════════════════════════════════════════════════════════
#  NATS -- the client seam
# ═══════════════════════════════════════════════════════════════
#
# ``NatsConnection`` depends on ``NatsClient``/``NatsSubscription`` -- two
# protocols describing exactly the surface it touches, not the concrete
# ``nats-py`` classes. These fakes implement that surface in memory, so every
# one of ``start``/``stop``/``send``/``_on_msg`` is reachable without a
# broker. No inheritance is needed on either side: structural typing is the
# whole point of the seam.


class _FakeMsg:
    def __init__(self, data: bytes, subject: str = "") -> None:
        self.data = data
        self.subject = subject


class _FakeSubscription:
    def __init__(
        self,
        reply: _FakeMsg | None = None,
        *,
        fail_unsubscribe: bool = False,
    ) -> None:
        self.unsubscribed = False
        self._reply = reply
        self._fail_unsubscribe = fail_unsubscribe

    async def unsubscribe(self, limit: int = 0) -> None:
        if self._fail_unsubscribe:
            msg = "boom"
            raise RuntimeError(msg)
        self.unsubscribed = True

    async def next_msg(self, timeout: float | None = 1.0) -> _FakeMsg:  # noqa: ASYNC109
        if self._reply is None:
            msg = "no reply configured"
            raise TimeoutError(msg)
        return self._reply


class _FakeNatsClient:
    """A minimal, in-memory stand-in for ``nats.aio.client.Client``.

    Satisfies :class:`~resonate_nats.NatsClient` structurally --
    no import of ``nats-py`` required to build or type it.
    """

    def __init__(self) -> None:
        self.subscribed: list[dict[str, Any]] = []
        self.published: list[dict[str, Any]] = []
        self.reply: _FakeMsg | None = _FakeMsg(b'{"ok":true}')
        self.publish_error: Exception | None = None
        self._next_inbox = 0

    def new_inbox(self) -> str:
        self._next_inbox += 1
        return f"_INBOX.{self._next_inbox}"

    async def subscribe(
        self,
        subject: str,
        queue: str = "",
        cb: Any = None,
        max_msgs: int = 0,
    ) -> _FakeSubscription:
        self.subscribed.append(
            {"subject": subject, "queue": queue, "cb": cb, "max_msgs": max_msgs}
        )
        return _FakeSubscription(self.reply)

    async def publish(
        self,
        subject: str,
        payload: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append(
            {"subject": subject, "payload": payload, "headers": headers}
        )


# ── subject mapping ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "origin",
    ["root", "my.app.workflow", "default", "with space", "unicode-\u00e9"],
)
def test_publish_subject_encodes_any_origin_as_one_subject_token(origin: str) -> None:
    """A subject token cannot contain a dot; an origin can. Hence base64url."""
    subject = _publish_subject("resonate.requests", origin)
    prefix, _, token = subject.rpartition(".")
    assert prefix == "resonate.requests"
    assert "." not in token
    assert "=" not in token
    padded = token + "=" * (-len(token) % 4)
    assert base64.urlsafe_b64decode(padded).decode("utf-8") == origin


def test_addresses_derive_from_the_recv_prefix() -> None:
    client = _FakeNatsClient()
    conn = NatsConnection(client, pid="worker-1", group="workers")
    assert conn.unicast() == "nats://resonate.recv.workers.worker-1"
    assert conn.resolve_target("other") == "nats://resonate.recv.other"
    # Resolving this connection's own group is its anycast address -- the
    # subject its group queue-subscribes to.
    assert conn.resolve_target("workers") == "nats://resonate.recv.workers"
    assert conn.resolve_target("workers") == f"nats://{conn._any_subject}"


@pytest.mark.asyncio
async def test_start_without_a_registered_receiver_opens_no_subscriptions() -> None:
    """A network-only connection must not consume the group's queue."""
    client = _FakeNatsClient()
    conn = NatsConnection(client)
    await conn.start()
    assert client.subscribed == []


@pytest.mark.asyncio
async def test_start_with_a_receiver_subscribes_unicast_and_queued_anycast() -> None:
    client = _FakeNatsClient()
    conn = NatsConnection(client, pid="worker-1", group="workers")
    conn.recv(lambda _: None)
    await conn.start()

    assert [s["subject"] for s in client.subscribed] == [
        "resonate.recv.workers.worker-1",
        "resonate.recv.workers",
    ]
    assert client.subscribed[1]["queue"] == "workers"
    assert all(s["cb"] == conn._on_msg for s in client.subscribed)


@pytest.mark.asyncio
async def test_stop_unsubscribes_every_subscription_and_clears_receivers() -> None:
    client = _FakeNatsClient()
    conn = NatsConnection(client)
    conn.recv(lambda _: None)
    await conn.start()
    subs = cast("list[_FakeSubscription]", list(conn._subs))

    await conn.stop()

    assert all(sub.unsubscribed for sub in subs)
    assert conn._subs == []
    assert conn._subscribers == []


@pytest.mark.asyncio
async def test_stop_swallows_an_unsubscribe_failure() -> None:
    """Shutdown must not get stuck on a broker that refuses to unsubscribe."""
    client = _FakeNatsClient()
    conn = NatsConnection(client)
    conn.recv(lambda _: None)
    await conn.start()
    conn._subs = [_FakeSubscription(fail_unsubscribe=True)]

    await conn.stop()  # must not raise

    assert conn._subs == []


@pytest.mark.asyncio
async def test_send_publishes_the_origin_routed_subject_and_reply_header() -> None:
    client = _FakeNatsClient()
    client.reply = _FakeMsg(b'{"kind":"reply"}')
    conn = NatsConnection(client)
    await conn.start()

    req = '{"kind":"task.acquire","head":{"resonate:origin":"root"},"data":{"id":"root:1"}}'
    resp = await conn.send(req, {ORIGIN_HEADER: "root"})

    assert resp == '{"kind":"reply"}'
    assert len(client.published) == 1
    published = client.published[0]
    assert published["subject"] == _publish_subject("resonate.requests", "root")
    assert published["headers"] is not None
    inbox = published["headers"]["Resonate-Reply-To"]
    assert inbox == client.subscribed[0]["subject"]
    # Published verbatim: the connector moves the body, it does not read it.
    assert published["payload"] == req.encode("utf-8")


@pytest.mark.asyncio
async def test_send_before_start_raises_nats_error() -> None:
    """``send`` on a never-started connection must not touch the client."""
    client = _FakeNatsClient()
    conn = NatsConnection(client)

    with pytest.raises(ConnectorError):
        await conn.send(
            '{"kind":"task.acquire","data":{"id":"root"}}', {ORIGIN_HEADER: "root"}
        )
    assert client.published == []


@pytest.mark.asyncio
async def test_send_after_stop_raises_nats_error_without_touching_the_client() -> None:
    client = _FakeNatsClient()
    conn = NatsConnection(client)
    await conn.start()
    await conn.stop()

    with pytest.raises(ConnectorError):
        await conn.send(
            '{"kind":"task.acquire","data":{"id":"root"}}', {ORIGIN_HEADER: "root"}
        )
    assert client.published == []


@pytest.mark.asyncio
async def test_send_wraps_a_publish_failure_in_nats_error() -> None:
    """Every ``nats-py`` failure crosses the boundary as the SDK's own vocabulary."""
    client = _FakeNatsClient()
    client.publish_error = OSError("connection refused")
    conn = NatsConnection(client)
    await conn.start()

    with pytest.raises(ConnectorError) as excinfo:
        await conn.send(
            '{"kind":"task.acquire","data":{"id":"root"}}', {ORIGIN_HEADER: "root"}
        )
    assert isinstance(excinfo.value.error, OSError)


@pytest.mark.asyncio
async def test_send_wraps_a_reply_timeout_in_nats_error() -> None:
    client = _FakeNatsClient()
    client.reply = None  # _FakeSubscription.next_msg raises TimeoutError
    conn = NatsConnection(client)
    await conn.start()

    with pytest.raises(ConnectorError) as excinfo:
        await conn.send(
            '{"kind":"task.acquire","data":{"id":"root"}}', {ORIGIN_HEADER: "root"}
        )
    assert isinstance(excinfo.value.error, TimeoutError)


@pytest.mark.asyncio
async def test_on_msg_dispatches_the_decoded_payload_to_every_subscriber() -> None:
    client = _FakeNatsClient()
    conn = NatsConnection(client)
    first: list[str] = []
    second: list[str] = []
    conn.recv(first.append)
    conn.recv(second.append)

    await conn._on_msg(_FakeMsg(b'{"kind":"execute"}', subject="s"))

    assert first == ['{"kind":"execute"}']
    assert second == ['{"kind":"execute"}']


@pytest.mark.asyncio
async def test_on_msg_drops_a_non_utf8_payload_without_raising() -> None:
    """A long-lived subscription must not be torn down by one bad frame."""
    client = _FakeNatsClient()
    conn = NatsConnection(client)
    received: list[str] = []
    conn.recv(received.append)

    await conn._on_msg(_FakeMsg(b"\xff\xfe invalid", subject="s"))

    assert received == []


@pytest.mark.asyncio
async def test_send_routes_by_the_origin_it_is_given_not_by_the_payload() -> None:
    """The argument is the routing key; the body is opaque.

    A connector that re-derived the origin from the payload would be a second
    copy of the SDK's envelope layout and id format, free to drift from it.
    """
    client = _FakeNatsClient()
    conn = NatsConnection(client)
    await conn.start()

    await conn.send('{"kind":"promise.search","data":{}}', {ORIGIN_HEADER: "default"})

    assert client.published[0]["subject"] == _publish_subject(
        "resonate.requests", "default"
    )
