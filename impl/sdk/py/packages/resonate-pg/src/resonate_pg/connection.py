"""``PostgresConnection`` -- a Resonate connector whose server is a database."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from typing import TYPE_CHECKING

from resonate_base import ConnectorError

from resonate_pg.client import PsycopgSessions
from resonate_pg.wire import (
    execute_message,
    outbox_channel,
    resolve_target,
    unblock_message,
    unicast,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from resonate_pg.client import PgConn, PgSessions

logger = logging.getLogger(__name__)

__all__ = ["PostgresConnection"]

# =============================================================================
# CONSTANTS
# =============================================================================

#: Seconds between fallback drains. ``NOTIFY`` is the fast path; this tick is
#: what makes a *missed* notification cost latency instead of a stuck workflow
#: -- rows enqueued while the LISTEN connection was down are picked up here.
DEFAULT_TICK_SECS = 0.25

#: Rows a single ``dequeue_*`` call may claim. The drain pages until a short
#: read, so this caps one statement, not one wake.
DEFAULT_DEQUEUE_LIMIT = 100

#: Ceiling of the LISTEN reconnect ladder, in seconds.
MAX_BACKOFF_SECS = 60.0

#: ponytail: the wire entrypoints granted to ``resonate_worker`` in
#: ``resonate.sql``. ``process_timeouts`` is deliberately *not* among them --
#: see :meth:`PostgresConnection._fire_timers`.
RPC_SQL = "SELECT resonate.resonate_rpc(%s::text::jsonb)::text"
DEQUEUE_EXECUTE_SQL = "SELECT task_id, version FROM resonate.dequeue_execute(%s, %s)"
DEQUEUE_UNBLOCK_SQL = "SELECT promise::text FROM resonate.dequeue_unblock(%s, %s)"
PROCESS_TIMEOUTS_SQL = "SELECT resonate.process_timeouts()"


class PostgresConnection:
    """Postgres connection to a resonate-pg database.

    Implements **both** protocols: :class:`~resonate_base.connections.Network`
    (the request/response ``send`` path) and
    :class:`~resonate_base.connections.Source` (the push-message ``recv``
    path), so one instance can serve as a Resonate client's only connection.

    There is no server process. Requests run
    ``SELECT resonate.resonate_rpc($1::jsonb)``, the stored procedure that *is*
    the protocol. Push messages are rows in ``resonate.outbox`` addressed to
    this node; a pump drains them, woken by ``NOTIFY`` and backstopped by a
    short tick.

    Addresses are ``poll://uni@{group}/{pid}`` and ``poll://any@{group}``.
    Those exact shapes are required, not chosen: ``promise_register_listener``
    rejects an address that is neither HTTP nor ``poll://…@…``, and
    ``dequeue_execute`` matches an address byte-for-byte against the
    ``resonate:target`` a peer stamped.

    Pass ``conninfo`` and this owns its connections; pass ``sessions`` and the
    lifecycle is yours. Install with ``uv add resonate-pg``; it depends on
    ``resonate-base`` and psycopg, never on ``resonate-sdk``.
    """

    def __init__(
        self,
        conninfo: str | None = None,
        pid: str | None = None,
        group: str | None = None,
        *,
        sessions: PgSessions | None = None,
        tick: float = DEFAULT_TICK_SECS,
        dequeue_limit: int = DEFAULT_DEQUEUE_LIMIT,
        drive_timers: bool = True,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if (conninfo is None) == (sessions is None):
            msg = "PostgresConnection takes exactly one of conninfo or sessions"
            raise ValueError(msg)
        if dequeue_limit < 1:
            msg = f"dequeue_limit must be at least 1, got {dequeue_limit}"
            raise ValueError(msg)

        self._sessions: PgSessions = (
            sessions if sessions is not None else PsycopgSessions(str(conninfo))
        )
        self._pid = pid if pid is not None else uuid.uuid4().hex
        self._group = group if group is not None else "default"
        self._unicast = unicast(self._group, self._pid)
        self._anycast = resolve_target(self._group)
        self._tick = tick
        self._limit = dequeue_limit
        self._drive_timers = drive_timers
        self._sleeper = sleeper

        self._subscribers: list[Callable[[str], None]] = []
        self._tasks: list[asyncio.Task[None]] = []
        #: Set by a ``NOTIFY``, by a fresh LISTEN session, and by :meth:`stop`;
        #: awaited by the pump so a notification collapses the tick.
        self._wake = asyncio.Event()
        self._running = False
        self._stopped = False

    def unicast(self) -> str:
        return self._unicast

    def resolve_target(self, target: str) -> str:
        """Resolve a target group name to its ``poll://`` anycast address."""
        return resolve_target(target)

    def recv(self, callback: Callable[[str], None]) -> None:
        """Register a callback for incoming execute/unblock messages."""
        self._subscribers.append(callback)

    # -- internals ------------------------------------------------------------

    def _deliver(self, msg: str) -> None:
        logger.debug("pg_connection recv: %s", msg)
        for cb in list(self._subscribers):
            cb(msg)

    async def _drain(self) -> None:
        """Claim every message addressed to this node and hand it to the SDK.

        Messages are collected inside the session and delivered after it
        closes, so nothing is dispatched on the strength of a ``DELETE`` that
        may still roll back. Delivery is at-least-once either way: a crash
        between the commit and the callback is recovered by the task's own
        retry timeout, which is exactly the guarantee the SDK is built on.
        """
        await self._fire_timers()

        messages: list[str] = []
        async with self._sessions.session() as conn:
            messages += await self._dequeue_executes(conn, self._anycast)
            messages += await self._dequeue_executes(conn, self._unicast)
            messages += await self._dequeue_unblocks(conn, self._unicast)

        for msg in messages:
            self._deliver(msg)

    async def _dequeue_executes(self, conn: PgConn, address: str) -> list[str]:
        """Claim every ``execute`` row at ``address``, paging on a full read."""
        out: list[str] = []
        while True:
            cursor = await conn.execute(DEQUEUE_EXECUTE_SQL, (address, self._limit))
            rows = await cursor.fetchall()
            out.extend(execute_message(str(row[0]), int(row[1])) for row in rows)
            if len(rows) < self._limit:
                return out

    async def _dequeue_unblocks(self, conn: PgConn, address: str) -> list[str]:
        """Claim every ``unblock`` row at ``address``, paging on a full read."""
        out: list[str] = []
        while True:
            cursor = await conn.execute(DEQUEUE_UNBLOCK_SQL, (address, self._limit))
            rows = await cursor.fetchall()
            out.extend(unblock_message(str(row[0])) for row in rows)
            if len(rows) < self._limit:
                return out

    async def _fire_timers(self) -> None:
        """Advance resonate-pg's timers on database time, if we are allowed to.

        Two reasons this is not simply left to pg_cron. A database without the
        extension has no timer driver at all, so durable sleeps would never
        fire; and pg_cron's floor is coarse next to this pump's tick.

        Two reasons it disables itself on the first failure. ``resonate.sql``
        grants ``resonate_worker`` the wire entrypoints but *not*
        ``process_timeouts``, so a least-privilege worker is refused every
        time -- and a warning four times a second is noise, not signal. pg_cron
        is then the sole driver, which is the documented arrangement anyway.

        It runs in its own session so a refusal cannot leave the drain's
        transaction aborted.
        """
        if not self._drive_timers:
            return
        try:
            async with self._sessions.session() as conn:
                await conn.execute(PROCESS_TIMEOUTS_SQL)
        except Exception as exc:
            self._drive_timers = False
            logger.warning(
                "resonate.process_timeouts() unavailable, leaving timers to "
                "pg_cron: %s",
                exc,
            )

    async def start(self) -> None:
        """Start the pump and the LISTEN connection.

        Both are started only when a receiver has been registered via
        :meth:`recv` -- i.e. when this connection is used as a
        :class:`~resonate_base.connections.Source`. A network-only connection
        must not pump: ``dequeue_*`` *deletes* what it reads, so a connector
        with nowhere to deliver would destroy its peers' work rather than
        merely ignore it. Register receivers **before** calling ``start``
        (:class:`~resonate.resonate.Resonate` wires dispatch before starting
        any connection).

        Infallible by design: every connection attempt lives inside a retry
        loop, so a database that is down at boot self-heals.
        """
        self._running = True
        if not self._subscribers or self._tasks:
            return
        self._tasks = [
            asyncio.create_task(self._pump_loop()),
            asyncio.create_task(self._listen_loop()),
        ]
        logger.info(
            "postgres pump started (uni=%s any=%s)", self._unicast, self._anycast
        )

    async def stop(self) -> None:
        """Cancel the loops and release every connection this opened."""
        self._running = False
        self._stopped = True
        self._wake.set()

        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        # Shutdown must not get stuck on a database that refuses to let go.
        with contextlib.suppress(Exception):
            await self._sessions.close()
        self._subscribers.clear()

    async def send(self, req: str, headers: dict[str, str] | None = None) -> str:
        """Run a request through ``resonate.resonate_rpc``, returning the reply.

        The body is bound as a single parameter and never opened: the stored
        procedure reads the envelope, so no second copy of the SDK's wire
        layout lives here to drift out of step with it.

        ``headers`` -- including the routing origin under ``resonate:origin``
        -- are unused. A sharding substrate reads that header to place a
        request; this substrate is one database, and resonate-pg partitions
        internally off the envelope. It stays on the signature because the
        seam, not the connector, defines the call.

        Refused only after :meth:`stop`, matching
        :class:`~resonate.connections.HttpConnection`: a send-only client is a
        legitimate deployment, and :meth:`start` deliberately does nothing for
        one, so "not yet started" must not be an error.
        """
        logger.debug("pg_connection req: %s", req)
        if self._stopped:
            msg = "connection has been stopped"
            raise ConnectorError(RuntimeError(msg))
        try:
            async with self._sessions.session() as conn:
                cursor = await conn.execute(RPC_SQL, (req,))
                row = await cursor.fetchone()
        except Exception as exc:
            raise ConnectorError(exc) from exc

        if row is None or row[0] is None:
            msg = "resonate.resonate_rpc returned no row"
            raise ConnectorError(RuntimeError(msg))

        resp = str(row[0])
        logger.debug("pg_connection res: %s", resp)
        return resp

    async def _pump_loop(self) -> None:
        """Drain, then wait for a ``NOTIFY`` or the tick, forever.

        Errors are logged and retried on the next wake: this loop is the
        runtime, and a loop that dies on a transient database error takes the
        worker's ability to recover with it.
        """
        with contextlib.suppress(asyncio.CancelledError):
            while self._running:
                # Cleared *before* draining, never after waiting. A ``NOTIFY``
                # that lands mid-drain then survives to trigger the next pass,
                # where clearing afterwards would swallow it and leave the row
                # sitting until the tick. The cost is at worst one redundant
                # drain, and a drain over an empty queue is three cheap reads.
                self._wake.clear()
                try:
                    await self._drain()
                except Exception as exc:
                    if self._running:
                        logger.warning("postgres drain failed: %s", exc)
                await self._wait_for_wake()

    async def _wait_for_wake(self) -> None:
        """Sleep until the connection is woken, or the tick expires."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), timeout=self._tick)

    async def _listen_loop(self) -> None:
        """Hold a ``LISTEN`` connection open, reconnecting with a backoff ladder.

        A session that ends cleanly resets the ladder; one that never
        established backs off to :data:`MAX_BACKOFF_SECS`. Either way the pump
        keeps ticking, so a deaf listener costs latency, never correctness.
        """
        delay = 1.0
        with contextlib.suppress(asyncio.CancelledError):
            while self._running:
                try:
                    await self._listen_once()
                except Exception as exc:
                    if self._running:
                        logger.warning("postgres listen failed: %s", exc)
                else:
                    delay = 1.0
                if not self._running:
                    return
                await self._sleeper(delay)
                delay = min(delay * 2, MAX_BACKOFF_SECS)

    async def _listen_once(self) -> None:
        """Open one listening session and forward notifications to the pump."""
        conn = await self._sessions.dedicated()
        try:
            for address in (self._unicast, self._anycast):
                channel = outbox_channel(address)
                # ``outbox_channel`` returns 'resonate_q_' + 32 hex characters
                # -- minted here, never taken from an address -- so this
                # identifier cannot carry anything to escape.
                await conn.execute(f'LISTEN "{channel}"')
            logger.info(
                "postgres listening (uni=%s any=%s)", self._unicast, self._anycast
            )
            # Rows may have landed while the listener was down.
            self._wake.set()
            async for _notification in conn.notifies():
                self._wake.set()
                if not self._running:
                    return
        finally:
            with contextlib.suppress(Exception):
                await conn.close()
