from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from typing import TYPE_CHECKING

import aiohttp

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

_INITIAL_BACKOFF_SECS = 1
_MAX_BACKOFF_SECS = 60


class SSEConnection:
    """:class:`~resonate.connections.Source` implementation over Server-Sent Events.

    Incoming messages (execute/unblock) are received via SSE on
    ``GET /poll/{group}/{pid}`` of a Resonate server. Addresses use the
    ``poll://`` scheme: ``poll://uni@group/id`` and ``poll://any@group/id``.

    This is the push-message half only; requests to the server are sent
    through a separate :class:`~resonate.connections.Network` (typically
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
    ) -> None:
        self._pid = pid if pid is not None else uuid.uuid4().hex
        self._group = group if group is not None else "default"
        self._unicast = f"poll://uni@{self._group}/{self._pid}"
        self._anycast = f"poll://any@{self._group}/{self._pid}"
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

    def pid(self) -> str:
        return self._pid

    def group(self) -> str:
        return self._group

    def unicast(self) -> str:
        return self._unicast

    def anycast(self) -> str:
        return self._anycast

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

    def target_resolver(self, target: str) -> str:
        """Resolve a target name to a ``poll://`` anycast address."""
        return f"poll://any@{target}"

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
        backoff = _INITIAL_BACKOFF_SECS
        with contextlib.suppress(asyncio.CancelledError):
            while self._running:
                try:
                    session = self._ensure_session()
                    async with session.get(url, headers=headers) as resp:
                        if not (200 <= resp.status < 300):
                            logger.warning(
                                "SSE endpoint returned %s, retrying (backoff=%ss)",
                                resp.status,
                                backoff,
                            )
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, _MAX_BACKOFF_SECS)
                            continue

                        # Connection succeeded, reset backoff.
                        backoff = _INITIAL_BACKOFF_SECS
                        logger.info("SSE connection established: %s", url)
                        await self._read_stream(resp)
                except asyncio.CancelledError:
                    raise
                except aiohttp.ClientError as exc:
                    logger.warning(
                        "SSE connection failed, retrying (backoff=%ss): %s",
                        backoff,
                        exc,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, _MAX_BACKOFF_SECS)
                    continue

                if not self._running:
                    break
                logger.info(
                    "SSE connection closed, reconnecting (backoff=%ss)", backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF_SECS)

    async def _read_stream(self, resp: aiohttp.ClientResponse) -> None:
        """Parse the SSE byte stream and dispatch each ``data:`` line.

        SSE events are separated by a blank line; every ``data:`` line in an
        event is dispatched to each subscriber.
        """
        buffer = ""
        async for chunk in resp.content.iter_any():
            try:
                text = chunk.decode("utf-8")
            except UnicodeDecodeError:
                continue
            buffer += text

            while "\n\n" in buffer:
                block, buffer = buffer.split("\n\n", 1)
                for line in block.splitlines():
                    data = _strip_data_prefix(line)
                    if data is None:
                        continue
                    logger.debug("sse_connection sse_recv: %s", data)
                    for cb in list(self._subscribers):
                        cb(data)


def _strip_data_prefix(line: str) -> str | None:
    """Return the trimmed payload of an SSE ``data:`` line, else ``None``."""
    if line.startswith("data:"):
        data = line[len("data:") :].strip()
        return data or None
    return None
