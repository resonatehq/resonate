from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING

import aiohttp

from resonate.error import HttpError
from resonate.retry import ExponentialBackoff
from resonate.timing import sleep

if TYPE_CHECKING:
    from resonate.retry import Backoff
    from resonate.timing import Sleeper

logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

#: Total connection cap for the shared :class:`aiohttp.ClientSession`.
#: aiohttp's default of 100 can saturate under heavy fan-out, delaying the
#: periodic ``task.heartbeat`` request until task leases lapse and the server
#: re-delivers them. Keeping the cap well above the execution-concurrency
#: ceiling (see ``resonate.resonate.DEFAULT_MAX_CONCURRENT_TASKS``) guarantees
#: the heartbeat always finds a free connection.
DEFAULT_CONN_LIMIT = 256


class HttpConnection:
    """:class:`~resonate.connections.Network` implementation over HTTP.

    Requests are sent via ``POST /`` (JSON envelope format). This is the
    request/response half only; push messages from the server arrive through a
    separate :class:`~resonate.connections.Source` (typically
    :class:`~resonate.connections.SSEConnection`).
    """

    def __init__(
        self,
        url: str,
        auth: str | None = None,
        conn_limit: int | None = None,
        backoff: Backoff | None = None,
        sleeper: Sleeper = sleep,
    ) -> None:
        # Strip trailing slash(es) from url.
        self._url = url.rstrip("/")
        self._auth = auth
        self._conn_limit = conn_limit if conn_limit is not None else DEFAULT_CONN_LIMIT
        #: Resend ladder, shared with :class:`~resonate.connections.SSEConnection`
        #: so the two cannot drift, and injectable so a test pins delays to zero.
        self._backoff: Backoff = (
            backoff if backoff is not None else ExponentialBackoff()
        )
        self._sleeper = sleeper

        self._session: aiohttp.ClientSession | None = None
        self._running: bool = False
        # True only after :meth:`stop` is called. Distinct from ``_running``
        # (which starts ``False`` before :meth:`start` fires) so that
        # :meth:`send` and :meth:`_ensure_session` can tell "not yet started"
        # from "explicitly stopped": the former is fine to proceed through,
        # the latter must be refused to avoid leaking sessions after shutdown.
        self._stopped: bool = False
        # Set on :meth:`stop` so a ``send`` parked in its retry backoff wakes
        # immediately instead of blocking shutdown.
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        """Mark the connection running so :meth:`send` retries through outages."""
        self._running = True
        self._stop_event.clear()

    async def stop(self) -> None:
        self._running = False
        self._stopped = True
        # Wake any ``send`` parked in the retry backoff so the bounded join in
        # :meth:`~resonate.resonate.Resonate.stop` does not stall.
        self._stop_event.set()
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def send(self, req: str) -> str:
        """Send a request to the Resonate server via ``POST /``.

        Transport-level connection failures are retried with exponential
        backoff (``1s → 60s``), so the SDK survives the server being down at
        startup, restarting, or briefly unreachable. Only connection failures
        are retried: an HTTP response of any status returns normally, so error
        statuses (404, 409, 500, …) propagate to the caller unchanged.

        Once :meth:`stop` is called, any in-flight or new request raises
        :class:`HttpError` instead of retrying, so shutdown is never blocked
        by the backoff loop.
        """
        logger.debug("http_connection http_req: %s", req)
        headers = self._auth_headers({"Content-Type": "application/json"})
        attempt = 0
        while True:
            if self._stopped:
                msg = "network has been stopped"
                raise HttpError(RuntimeError(msg))
            session = self._ensure_session()
            try:
                async with session.post(
                    f"{self._url}/", data=req, headers=headers
                ) as resp:
                    resp_str = await resp.text()
            except (aiohttp.ClientError, RuntimeError) as exc:
                # :meth:`stop` closing the session mid-flight raises
                # ``RuntimeError("Session is closed")``; surface it as
                # :class:`HttpError` so the caller unwinds cleanly. A
                # ``RuntimeError`` while still running is not retriable --
                # re-raise so a real bug is not hidden by infinite backoff.
                if not self._running:
                    raise HttpError(exc) from exc
                if isinstance(exc, RuntimeError):
                    raise
                delay = self._backoff.delay(attempt)
                logger.warning(
                    "HTTP send failed, retrying (backoff=%ss): %s", delay, exc
                )
                await self._sleep_or_stop(delay)
                if not self._running:
                    raise HttpError(exc) from exc
                attempt += 1
                continue
            logger.debug("http_connection http_res: %s", resp_str)
            return resp_str

    # -- internals ------------------------------------------------------------

    def _ensure_session(self) -> aiohttp.ClientSession:
        """Lazily create the shared :class:`aiohttp.ClientSession`.

        Creation is deferred until an event loop is running, because a
        ``ClientSession`` must be created inside one.

        Once :meth:`stop` has run, this refuses to open a fresh session so a
        retry loop racing with shutdown cannot leak a session that nobody
        will close.
        """
        if self._session is None:
            if self._stopped:
                msg = "network has been stopped"
                raise RuntimeError(msg)
            # Raise the connector cap above aiohttp's default 100 so heartbeat
            # and execution traffic never starve each other (see
            # ``DEFAULT_CONN_LIMIT``). The session owns the connector, so
            # ``session.close()`` in :meth:`stop` tears it down.
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=self._conn_limit)
            )
        return self._session

    def _auth_headers(self, headers: dict[str, str]) -> dict[str, str]:
        """Add the bearer ``Authorization`` header when auth is configured."""
        if self._auth is not None:
            headers["Authorization"] = f"Bearer {self._auth}"
        return headers

    async def _sleep_or_stop(self, secs: float) -> None:
        """Sleep for ``secs``, returning early once :meth:`stop` is called.

        Used by :meth:`send`'s retry loop so a pending retry never delays
        shutdown. Races the injected :data:`~resonate.timing.Sleeper` against
        the stop signal rather than using ``wait_for``'s own timer, so an
        injected sleeper genuinely controls the delay.
        """
        napping = asyncio.ensure_future(self._sleeper(secs))
        stopping = asyncio.ensure_future(self._stop_event.wait())
        try:
            await asyncio.wait({napping, stopping}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for pending in (napping, stopping):
                pending.cancel()
                if pending is napping:
                    with contextlib.suppress(asyncio.CancelledError):
                        await pending
