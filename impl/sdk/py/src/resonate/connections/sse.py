"""The ``poll://`` source: Server-Sent Events against a Resonate server.

This module owns the ``poll`` scheme end to end -- the listener *and* the
addresses it advertises. Address shape is a connector's own business: the
server parses an address with Go's ``url.Parse``, dispatches on the scheme, and
hands everything to the right of it back to whoever minted it. So there is no
SDK-wide address format to conform to, and none of this is shared with, say,
:class:`resonate_nats.NatsConnection`, which advertises a bare NATS subject
because that already is an address in its own namespace.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from typing import TYPE_CHECKING

import aiohttp

from resonate.retry import ExponentialBackoff
from resonate.timing import sleep

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from resonate.retry import Backoff
    from resonate.timing import Sleeper

logger = logging.getLogger(__name__)

#: URL scheme of the delivery addresses this source advertises. The server
#: dispatches on it to pick SSE long-polling as the delivery mechanism.
SCHEME = "poll"

#: Userinfo marking an address that reaches exactly one process.
UNICAST = "uni"

#: Userinfo marking an address that reaches any one member of a group.
ANYCAST = "any"


def unicast(group: str, pid: str) -> str:
    """Mint the address that reaches one process alone.

    Total by design -- like every connector's address minting (see
    :meth:`resonate_nats.NatsConnection.unicast`), it only formats. An address
    is opaque to the SDK and to the server alike: the server parses it, and a
    malformed one it declines to deliver to, which is observable. The SDK does
    not front-run that judgement -- an incorrect group simply mints an incorrect
    address.

    >>> unicast("workers", "7f3a")
    'poll://uni@workers/7f3a'
    """
    return f"{SCHEME}://{UNICAST}@{group}/{pid}"


def resolve_target(target: str) -> str:
    """Mint the address that reaches any one member of ``target``'s group.

    No pid, and that is the asymmetry with :func:`unicast`: a unicast address
    names a concrete process, while a routing target names a *group* whose
    members whoever resolves it does not know.

    Total, for the same reason as :func:`unicast` -- and doubly so here, since a
    target is resolved lazily while a durable op (``rpc``/``sleep``/``detached``)
    builds its request. Raising there would be caught by the workflow's own error
    boundary and permanently reject the durable promise. So an incorrect target
    just mints an incorrect address, recoverable by fixing it and re-dispatching.

    >>> resolve_target("workers")
    'poll://any@workers'
    """
    return f"{SCHEME}://{ANYCAST}@{target}"


# =============================================================================
# SSE framing -- pure, no IO
# =============================================================================


class SseFramer:
    """Incremental SSE frame parser: bytes in, ``data:`` payloads out.

    Extracted from the read loop so the framing rules -- events separated by a
    blank line, every ``data:`` line in an event dispatched, a chunk boundary
    falling mid-frame -- can be tested by calling a function with a ``bytes``
    literal, instead of by standing up an HTTP server and a real socket.

    Stateful across calls (a frame may span chunks) but entirely in-memory:
    :meth:`feed` performs no IO and never raises.
    """

    def __init__(self) -> None:
        self._buffer = ""

    def feed(self, chunk: bytes) -> list[str]:
        """Absorb one chunk and return every complete ``data:`` payload in it.

        A chunk that is not valid UTF-8 is discarded rather than raising: the
        stream is a long-lived connection and one bad frame must not tear it
        down. A partial frame is retained for the next call.
        """
        try:
            self._buffer += chunk.decode("utf-8")
        except UnicodeDecodeError:
            return []

        out: list[str] = []
        while "\n\n" in self._buffer:
            block, self._buffer = self._buffer.split("\n\n", 1)
            out.extend(_data_lines(block))
        return out


def _data_lines(block: str) -> Iterable[str]:
    """Yield the payload of every ``data:`` line in one SSE event block."""
    for line in block.splitlines():
        data = _strip_data_prefix(line)
        if data is not None:
            yield data


class SSEConnection:
    """:class:`~resonate_base.connections.Source` implementation over Server-Sent Events.

    Incoming messages (execute/unblock) are received via SSE on
    ``GET /poll/{group}/{pid}`` of a Resonate server. Its addresses are the
    ones this module mints: ``poll://uni@group/pid`` for this process, and
    ``poll://any@{group}`` for a routing target.

    ``pid`` and ``group`` identify this listener to the server.
    :class:`~resonate.resonate.Resonate` passes its own, so the addresses
    advertised here and the pid the SDK acquires tasks under always agree;
    constructed standalone they default to a fresh uuid and ``"default"``.

    This is the push-message half only; requests to the server are sent
    through a separate :class:`~resonate_base.connections.Network` (typically
    :class:`~resonate.connections.HttpConnection` against the same server).

    The SSE listener runs as a background asyncio task; callbacks registered
    via :meth:`recv` fire on the event loop as SSE events arrive.
    """

    def __init__(
        self,
        url: str,
        pid: str | None = None,
        group: str | None = None,
        auth: str | None = None,
        backoff: Backoff | None = None,
        sleeper: Sleeper = sleep,
    ) -> None:
        #: Reconnect ladder, shared with :class:`HttpConnection` so the two
        #: cannot drift, and injectable so a test pins every delay to zero.
        self._backoff: Backoff = (
            backoff if backoff is not None else ExponentialBackoff()
        )
        self._sleeper = sleeper
        self._pid = pid if pid is not None else uuid.uuid4().hex
        self._group = group if group is not None else "default"
        self._unicast = unicast(self._group, self._pid)
        # Strip trailing slash(es) from url.
        self._url = url.rstrip("/")
        self._auth = auth

        self._subscribers: list[Callable[[str], None]] = []
        self._session: aiohttp.ClientSession | None = None
        self._sse_handle: asyncio.Task[None] | None = None
        self._running: bool = False
        # True only after :meth:`stop` is called, so :meth:`_ensure_session`
        # can tell "not yet started" from "explicitly stopped" and refuse to
        # open a fresh session after shutdown.
        self._stopped: bool = False

    def unicast(self) -> str:
        return self._unicast

    def resolve_target(self, target: str) -> str:
        """Resolve a target name to a ``poll://`` anycast address."""
        return resolve_target(target)

    async def start(self) -> None:
        """Start the SSE listener for incoming messages from the server."""
        self._running = True
        self._sse_handle = asyncio.create_task(self._sse_loop())

    async def stop(self) -> None:
        self._running = False
        self._stopped = True
        handle = self._sse_handle
        self._sse_handle = None
        if handle is not None:
            handle.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await handle
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._subscribers.clear()

    def recv(self, callback: Callable[[str], None]) -> None:
        """Register a callback for incoming SSE messages."""
        self._subscribers.append(callback)

    # -- internals ------------------------------------------------------------

    def _ensure_session(self) -> aiohttp.ClientSession:
        """Lazily create the listener's :class:`aiohttp.ClientSession`.

        Creation is deferred until an event loop is running, because a
        ``ClientSession`` must be created inside one. Once :meth:`stop` has
        run, this refuses to open a fresh session so a reconnect racing with
        shutdown cannot leak a session that nobody will close.
        """
        if self._session is None:
            if self._stopped:
                msg = "source has been stopped"
                raise RuntimeError(msg)
            # The listener holds a single long-lived GET; the default
            # connector is plenty.
            self._session = aiohttp.ClientSession()
        return self._session

    def _auth_headers(self, headers: dict[str, str]) -> dict[str, str]:
        """Add the bearer ``Authorization`` header when auth is configured."""
        if self._auth is not None:
            headers["Authorization"] = f"Bearer {self._auth}"
        return headers

    async def _sse_loop(self) -> None:
        """Connect to the SSE endpoint, reconnecting with exponential backoff."""
        url = f"{self._url}/poll/{self._group}/{self._pid}"
        headers = self._auth_headers({"Accept": "text/event-stream"})
        attempt = 0
        with contextlib.suppress(asyncio.CancelledError):
            while self._running:
                try:
                    session = self._ensure_session()
                    async with session.get(url, headers=headers) as resp:
                        if not (200 <= resp.status < 300):
                            delay = self._backoff.delay(attempt)
                            logger.warning(
                                "SSE endpoint returned %s, retrying (backoff=%ss)",
                                resp.status,
                                delay,
                            )
                            await self._sleeper(delay)
                            attempt += 1
                            continue

                        # Connection succeeded, reset backoff.
                        attempt = 0
                        logger.info("SSE connection established: %s", url)
                        await self._read_stream(resp)
                except asyncio.CancelledError:
                    raise
                except aiohttp.ClientError as exc:
                    delay = self._backoff.delay(attempt)
                    logger.warning(
                        "SSE connection failed, retrying (backoff=%ss): %s",
                        delay,
                        exc,
                    )
                    await self._sleeper(delay)
                    attempt += 1
                    continue

                if not self._running:
                    break
                delay = self._backoff.delay(attempt)
                logger.info("SSE connection closed, reconnecting (backoff=%ss)", delay)
                await self._sleeper(delay)
                attempt += 1

    async def _read_stream(self, resp: aiohttp.ClientResponse) -> None:
        """Pump the response body through :class:`SseFramer` and dispatch.

        Pure IO: every framing decision lives in :class:`SseFramer`, so this
        method is a loop with no logic to get wrong.
        """
        framer = SseFramer()
        async for chunk in resp.content.iter_any():
            for data in framer.feed(chunk):
                logger.debug("sse_connection sse_recv: %s", data)
                for cb in list(self._subscribers):
                    cb(data)


def _strip_data_prefix(line: str) -> str | None:
    """Return the trimmed payload of an SSE ``data:`` line, else ``None``."""
    if line.startswith("data:"):
        data = line[len("data:") :].strip()
        return data or None
    return None
