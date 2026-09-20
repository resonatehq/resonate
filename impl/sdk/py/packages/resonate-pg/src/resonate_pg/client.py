"""The psycopg seam -- the only file in this package that imports a driver.

``PostgresConnection`` needs exactly three things from a database: a
short-lived session per query, one long-lived autocommit connection to hold
``LISTEN`` open, and a way to close both. :class:`PgSessions` names those three
and nothing else, so the connector's own IO -- ``send``, the drain, the pump,
the listener -- is reachable with an in-memory fake and no server.

:class:`PsycopgSessions` is the production implementation: a
``psycopg_pool.AsyncConnectionPool`` for queries (concurrent ``send`` calls
must not queue behind one another) and a plain ``psycopg.AsyncConnection`` in
autocommit mode for ``LISTEN``, which cannot share a pooled, transactional
connection.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

import psycopg
from psycopg_pool import AsyncConnectionPool

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence
    from contextlib import AbstractAsyncContextManager

__all__ = [
    "PgConn",
    "PgCursor",
    "PgNotify",
    "PgSessions",
    "PsycopgSessions",
]

#: Connections the query pool may open. Deliberately *below* the SDK's
#: ``DEFAULT_MAX_CONCURRENT_TASKS`` (64): one Postgres backend per in-flight
#: task would cost far more than it buys, since each request is a
#: millisecond-scale local query and the server's own ``max_connections`` is
#: the scarcer resource. Tune with ``PsycopgSessions(dsn, max_size=...)``.
DEFAULT_MAX_POOL_SIZE = 16


class PgNotify(Protocol):
    """The minimal ``psycopg.Notify`` surface this connector reads."""

    channel: str
    payload: str


class PgCursor(Protocol):
    """The minimal ``psycopg.AsyncCursor`` surface this connector reads."""

    async def fetchall(self) -> list[Any]: ...
    async def fetchone(self) -> Any: ...


class PgConn(Protocol):
    """The minimal ``psycopg.AsyncConnection`` surface this connector needs."""

    async def execute(
        self, query: str, params: Sequence[Any] | None = None
    ) -> PgCursor: ...

    def notifies(self) -> AsyncIterator[PgNotify]: ...

    async def close(self) -> None: ...


@runtime_checkable
class PgSessions(Protocol):
    """A source of Postgres sessions.

    A structural seam, not a copy of psycopg's API: any object with these three
    methods satisfies it -- :class:`PsycopgSessions`, or a fake a test builds in
    memory.
    """

    def session(self) -> AbstractAsyncContextManager[PgConn]:
        """Borrow a session for one unit of work, committing when it closes."""
        ...

    async def dedicated(self) -> PgConn:
        """Open an autocommit connection the caller owns and closes."""
        ...

    async def close(self) -> None:
        """Release everything this source opened."""
        ...


class PsycopgSessions:
    """:class:`PgSessions` over psycopg 3.

    ``conninfo`` is a libpq connection string, e.g.
    ``postgresql://user:pass@localhost:5432/db``. Nothing connects until the
    first :meth:`session` or :meth:`dedicated`, so constructing one is free and
    a database that is down at boot self-heals on the connector's own retry
    ladder.
    """

    def __init__(
        self,
        conninfo: str,
        *,
        min_size: int = 1,
        max_size: int = DEFAULT_MAX_POOL_SIZE,
    ) -> None:
        self._conninfo = conninfo
        self._min_size = min_size
        self._max_size = max_size
        self._pool: AsyncConnectionPool | None = None
        # Guards lazy pool creation: two coroutines racing into ``session``
        # must not build two pools, only one of which would ever be closed.
        self._lock = asyncio.Lock()

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[PgConn]:
        pool = await self._ensure_pool()
        async with pool.connection() as conn:
            yield cast("PgConn", conn)

    async def dedicated(self) -> PgConn:
        """Open an autocommit connection, as ``LISTEN`` requires.

        Not pooled: this connection is held open for the lifetime of the
        listener and spends nearly all of it idle, which is the opposite of
        what a pool is for.
        """
        conn = await psycopg.AsyncConnection.connect(self._conninfo, autocommit=True)
        return cast("PgConn", conn)

    async def close(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            await pool.close()

    async def _ensure_pool(self) -> AsyncConnectionPool:
        if self._pool is None:
            async with self._lock:
                if self._pool is None:
                    pool = AsyncConnectionPool(
                        self._conninfo,
                        min_size=self._min_size,
                        max_size=self._max_size,
                        open=False,
                    )
                    await pool.open()
                    self._pool = pool
        return self._pool
