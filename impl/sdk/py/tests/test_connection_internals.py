"""The logic that used to be welded to a socket.

``sse.py`` sat at 47% coverage and ``nats.py`` at 30% -- not because those
paths were unimportant, but because reaching them meant standing up a real
server. Two pieces were trapped inside the IO:

* SSE **framing** -- buffer, split on the blank line, emit each ``data:``
  payload -- now :class:`~resonate.connections.sse.SseFramer`, a function you
  call with a ``bytes`` literal.
* **Backoff** -- the ``1s -> 60s`` doubling ladder, previously open-coded twice
  (identically, and separately maintained) -- now
  :class:`~resonate.retry.ExponentialBackoff`, shared by both connections and
  injectable so a test never waits.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

import aiohttp
import pytest

from resonate.connections.http import HttpConnection
from resonate.connections.sse import SSEConnection, SseFramer, _strip_data_prefix
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
