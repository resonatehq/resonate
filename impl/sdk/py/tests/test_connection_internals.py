"""The logic that used to be welded to a socket.

``sse.py`` sat at 47% coverage and ``nats.py`` at 30% -- not because those
paths were unimportant, but because reaching them meant standing up a real
server. Three pieces were trapped inside the IO:

* SSE **framing** -- buffer, split on the blank line, emit each ``data:``
  payload -- now :class:`~resonate.connections.sse.SseFramer`, a function you
  call with a ``bytes`` literal.
* **Backoff** -- the ``1s -> 60s`` doubling ladder, previously open-coded twice
  (identically, and separately maintained) -- now
  :class:`~resonate.retry.ExponentialBackoff`, shared by both connections and
  injectable so a test never waits.
* The **NATS client seam** -- ``NatsConnection`` used to take ``nats.aio.client.Client``
  by name, so its own ``start``/``stop``/``send``/message-dispatch code could
  only be reached through a real broker. :class:`~resonate.connections.nats.NatsClient`
  and :class:`~resonate.connections.nats.NatsSubscription` now describe just the
  surface it touches, so a fake satisfies them structurally and every one of
  those paths is reachable in memory.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, cast

import aiohttp
import pytest

from resonate.connections.http import HttpConnection
from resonate.connections.nats import NatsConnection, _publish_subject, _routing_origin
from resonate.connections.sse import SSEConnection, SseFramer, _strip_data_prefix
from resonate.error import NatsError
from resonate.retry import Backoff, ExponentialBackoff, NoBackoff
from resonate.testing import RecordingSleeper

# ═══════════════════════════════════════════════════════════════
#  SSE framing -- pure
# ═══════════════════════════════════════════════════════════════


def test_one_complete_event_yields_its_payload() -> None:
    assert SseFramer().feed(b"data: hello\n\n") == ["hello"]


def test_payload_is_returned_only_once_the_frame_terminates() -> None:
    """A frame with no blank line yet is buffered, not guessed at."""
    framer = SseFramer()
    assert framer.feed(b"data: hel") == []
    assert framer.feed(b"lo\n") == []
    assert framer.feed(b"\n") == ["hello"]


def test_a_frame_split_across_chunks_reassembles() -> None:
    """The bug this class exists to make testable: a chunk boundary mid-JSON."""
    framer = SseFramer()
    payload = '{"kind":"execute","data":{"task":{"id":"t1"}}}'
    first, second = payload[:20], payload[20:]

    assert framer.feed(f"data: {first}".encode()) == []
    assert framer.feed(f"{second}\n\n".encode()) == [payload]


def test_multiple_events_in_one_chunk_are_all_emitted_in_order() -> None:
    out = SseFramer().feed(b"data: one\n\ndata: two\n\ndata: three\n\n")
    assert out == ["one", "two", "three"]


def test_multiple_data_lines_in_one_event_each_dispatch() -> None:
    assert SseFramer().feed(b"data: a\ndata: b\n\n") == ["a", "b"]


def test_non_data_lines_are_ignored() -> None:
    """Comments, ids and event names are SSE protocol, not payload."""
    out = SseFramer().feed(b": keep-alive\nid: 7\nevent: x\ndata: payload\n\n")
    assert out == ["payload"]


def test_an_empty_data_line_is_not_a_payload() -> None:
    assert SseFramer().feed(b"data:\n\n") == []
    assert SseFramer().feed(b"data:   \n\n") == []


def test_invalid_utf8_is_discarded_without_raising() -> None:
    """A long-lived stream must not be torn down by one bad frame."""
    framer = SseFramer()
    assert framer.feed(b"\xff\xfe invalid") == []
    assert framer.feed(b"data: still working\n\n") == ["still working"]


def test_the_framer_survives_an_empty_chunk() -> None:
    framer = SseFramer()
    assert framer.feed(b"") == []
    assert framer.feed(b"data: x\n\n") == ["x"]


def test_state_persists_across_calls_but_not_across_instances() -> None:
    a = SseFramer()
    a.feed(b"data: partial")
    # A fresh framer starts clean -- no shared module state.
    assert SseFramer().feed(b"\n\n") == []
    assert a.feed(b"\n\n") == ["partial"]


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("data: x", "x"),
        ("data:x", "x"),
        ("data:   spaced   ", "spaced"),
        ("data:", None),
        ("event: ping", None),
        ("", None),
        (": comment", None),
    ],
)
def test_strip_data_prefix_cases(line: str, expected: str | None) -> None:
    assert _strip_data_prefix(line) == expected


# ═══════════════════════════════════════════════════════════════
#  Backoff -- pure
# ═══════════════════════════════════════════════════════════════


def test_exponential_backoff_doubles_then_caps() -> None:
    backoff = ExponentialBackoff(initial=1.0, factor=2.0, max_delay=8.0)
    assert [backoff.delay(n) for n in range(6)] == [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]


def test_exponential_backoff_defaults_match_the_documented_ladder() -> None:
    """``1s -> 60s``, the value both connections previously hard-coded."""
    backoff = ExponentialBackoff()
    assert backoff.delay(0) == 1.0
    assert backoff.delay(6) == 60.0
    assert backoff.delay(100) == 60.0


def test_a_negative_attempt_yields_the_initial_delay() -> None:
    """Defensive, and cheap to pin: no exponent underflow."""
    assert ExponentialBackoff(initial=3.0).delay(-1) == 3.0


def test_no_backoff_is_always_zero() -> None:
    assert [NoBackoff().delay(n) for n in range(4)] == [0.0, 0.0, 0.0, 0.0]


def test_both_policies_satisfy_the_protocol() -> None:
    policies: list[Backoff] = [ExponentialBackoff(), NoBackoff()]
    assert all(isinstance(p.delay(0), float) for p in policies)


# ═══════════════════════════════════════════════════════════════
#  Injection into the connections
# ═══════════════════════════════════════════════════════════════


def test_connections_default_to_the_shared_ladder() -> None:
    """One policy object, two connections -- they can no longer drift."""
    http = HttpConnection("http://localhost:8001")
    sse = SSEConnection("http://localhost:8001")
    assert isinstance(http._backoff, ExponentialBackoff)
    assert isinstance(sse._backoff, ExponentialBackoff)
    assert http._backoff == sse._backoff


@pytest.mark.asyncio
async def test_http_send_backoff_delays_are_exactly_the_policy_schedule() -> None:
    """The resend ladder, asserted as a list instead of waited out.

    Previously this took 1 + 2 + 4 seconds of real time to observe, so nobody
    observed it; the test only checked that a retry eventually succeeded.
    """
    sleeper = RecordingSleeper()
    net = HttpConnection(
        "http://localhost:8001",
        backoff=ExponentialBackoff(initial=1.0, factor=2.0, max_delay=60.0),
        sleeper=sleeper,
    )
    await net.start()

    failures = {"n": 0}

    class _Ctx:
        async def __aenter__(self) -> object:
            failures["n"] += 1
            if failures["n"] <= 3:
                raise aiohttp.ClientConnectionError
            return self

        async def __aexit__(self, *_: object) -> None: ...

        async def text(self) -> str:
            return "{}"

    class _Session:
        def post(self, *_args: object, **_kwargs: object) -> _Ctx:
            return _Ctx()

        async def close(self) -> None: ...

    net._session = cast("Any", _Session())
    try:
        assert await net.send("{}") == "{}"
        assert sleeper.delays == [1.0, 2.0, 4.0]
    finally:
        net._session = None
        await net.stop()


@pytest.mark.asyncio
async def test_sleep_or_stop_returns_early_when_the_connection_stops() -> None:
    """Shutdown must not wait out a pending backoff."""
    sleeper = RecordingSleeper()
    net = HttpConnection("http://localhost:8001", sleeper=sleeper)
    await net.start()
    await net.stop()

    # ``stop`` set the event, so the wait resolves without consulting the
    # sleeper's full duration.
    await asyncio.wait_for(net._sleep_or_stop(30.0), timeout=1.0)


@pytest.mark.asyncio
async def test_sse_reconnect_uses_the_injected_backoff_and_sleeper() -> None:
    """A failing SSE endpoint backs off on the shared ladder -- assert the list.

    No real socket: the session is a stand-in that always refuses, so the
    reconnect loop spins entirely in memory and the delays are exact rather
    than dependent on how long a DNS failure happens to take.
    """
    sleeper = RecordingSleeper()
    src = SSEConnection(
        "http://localhost:8001",
        backoff=ExponentialBackoff(initial=0.5, factor=2.0, max_delay=4.0),
        sleeper=sleeper,
    )

    class _Refusing:
        def get(self, *_args: object, **_kwargs: object) -> object:
            raise aiohttp.ClientConnectionError

        async def close(self) -> None: ...

    src._session = cast("Any", _Refusing())
    await src.start()
    for _ in range(200):
        await asyncio.sleep(0)
        if len(sleeper.delays) >= 4:
            break
    src._session = None
    await src.stop()

    assert sleeper.delays[:4] == [0.5, 1.0, 2.0, 4.0]


@pytest.mark.asyncio
async def test_sse_read_stream_dispatches_framed_payloads_to_subscribers() -> None:
    """The IO half is now a loop with no logic -- pin that it still dispatches."""
    src = SSEConnection("http://localhost:8001")
    received: list[str] = []
    src.recv(received.append)

    class _Content:
        async def iter_any(self):  # noqa: ANN202
            yield b"data: one\n\ndata: tw"
            yield b"o\n\n"

    class _Resp:
        content = _Content()

    await src._read_stream(cast("Any", _Resp()))
    assert received == ["one", "two"]


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

    Satisfies :class:`~resonate.connections.nats.NatsClient` structurally --
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


# ── ``_routing_origin`` -- pure, one case per request kind ─────────────────


@pytest.mark.parametrize(
    ("req", "origin"),
    [
        ({"kind": "task.acquire", "data": {"id": "root:1"}}, "root"),
        ({"kind": "promise.get", "data": {"id": "root:1.2"}}, "root"),
        (
            {"kind": "promise.register_listener", "data": {"awaited": "root:1"}},
            "root",
        ),
        (
            {"kind": "task.create", "data": {"action": {"data": {"id": "root:1"}}}},
            "root",
        ),
        (
            {"kind": "task.heartbeat", "data": {"tasks": [{"id": "root:1"}]}},
            "root",
        ),
        ({"kind": "something.else", "data": {}}, "default"),
        ({"kind": "", "data": {}}, "default"),
    ],
    ids=lambda v: str(v)[:40],
)
def test_routing_origin_covers_every_request_kind(
    req: dict[str, Any], origin: str
) -> None:
    assert _routing_origin(req) == origin


def test_pid_group_and_addresses_derive_from_the_recv_prefix() -> None:
    client = _FakeNatsClient()
    conn = NatsConnection(client, pid="worker-1", group="workers")
    assert conn.pid() == "worker-1"
    assert conn.group() == "workers"
    assert conn.unicast() == "nats://resonate.recv.workers.worker-1"
    assert conn.anycast() == "nats://resonate.recv.workers"
    assert conn.target_resolver("other") == "nats://resonate.recv.other"


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

    resp = await conn.send('{"kind":"task.acquire","data":{"id":"root:1"}}')

    assert resp == '{"kind":"reply"}'
    assert len(client.published) == 1
    published = client.published[0]
    assert published["subject"] == _publish_subject("resonate.requests", "root")
    assert published["headers"] is not None
    inbox = published["headers"]["Resonate-Reply-To"]
    assert inbox == client.subscribed[0]["subject"]
    envelope = json.loads(published["payload"])
    assert envelope["head"]["resonate:origin"] == "root"


@pytest.mark.asyncio
async def test_send_before_start_raises_nats_error() -> None:
    """``send`` on a never-started connection must not touch the client."""
    client = _FakeNatsClient()
    conn = NatsConnection(client)

    with pytest.raises(NatsError):
        await conn.send('{"kind":"task.acquire","data":{"id":"root"}}')
    assert client.published == []


@pytest.mark.asyncio
async def test_send_after_stop_raises_nats_error_without_touching_the_client() -> None:
    client = _FakeNatsClient()
    conn = NatsConnection(client)
    await conn.start()
    await conn.stop()

    with pytest.raises(NatsError):
        await conn.send('{"kind":"task.acquire","data":{"id":"root"}}')
    assert client.published == []


@pytest.mark.asyncio
async def test_send_wraps_a_publish_failure_in_nats_error() -> None:
    """Every ``nats-py`` failure crosses the boundary as the SDK's own vocabulary."""
    client = _FakeNatsClient()
    client.publish_error = OSError("connection refused")
    conn = NatsConnection(client)
    await conn.start()

    with pytest.raises(NatsError) as excinfo:
        await conn.send('{"kind":"task.acquire","data":{"id":"root"}}')
    assert isinstance(excinfo.value.error, OSError)


@pytest.mark.asyncio
async def test_send_wraps_a_reply_timeout_in_nats_error() -> None:
    client = _FakeNatsClient()
    client.reply = None  # _FakeSubscription.next_msg raises TimeoutError
    conn = NatsConnection(client)
    await conn.start()

    with pytest.raises(NatsError) as excinfo:
        await conn.send('{"kind":"task.acquire","data":{"id":"root"}}')
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
