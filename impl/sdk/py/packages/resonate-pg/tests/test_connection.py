"""The Postgres connector's IO paths, against an in-memory database.

``PostgresConnection`` depends on ``PgSessions``/``PgConn`` -- protocols
describing exactly the psycopg surface it touches -- so ``send``, the drain,
the timer call, the pump, and the LISTEN loop are all reachable here without a
server. No inheritance is needed on either side: structural typing is the
whole point of the seam.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import TYPE_CHECKING, Any

import pytest
from resonate_base import Network, Source
from resonate_base.error import ConnectorError
from resonate_pg.connection import (
    DEQUEUE_EXECUTE_SQL,
    DEQUEUE_UNBLOCK_SQL,
    PROCESS_TIMEOUTS_SQL,
    RPC_SQL,
    PostgresConnection,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

# ═══════════════════════════════════════════════════════════════
#  Postgres -- the session seam
# ═══════════════════════════════════════════════════════════════


class _FakeNotify:
    def __init__(self, channel: str = "resonate_q_x", payload: str = "execute") -> None:
        self.channel = channel
        self.payload = payload


class _FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    async def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, db: _FakeDatabase) -> None:
        self._db = db
        self.closed = False

    async def execute(self, query: str, params: Any = None) -> _FakeCursor:
        self._db.queries.append((query, params))
        error = self._db.errors.get(query)
        if error is not None:
            raise error
        if query == RPC_SQL:
            return _FakeCursor(
                [] if self._db.rpc_reply is None else [(self._db.rpc_reply,)]
            )
        if query == DEQUEUE_EXECUTE_SQL:
            address, limit = params
            return _FakeCursor(self._db.take(self._db.executes, address, limit))
        if query == DEQUEUE_UNBLOCK_SQL:
            address, limit = params
            return _FakeCursor(self._db.take(self._db.unblocks, address, limit))
        if query.startswith("LISTEN"):
            self._db.listened.append(query)
        return _FakeCursor([])

    def notifies(self) -> AsyncIterator[_FakeNotify]:
        return self._db.notify_stream()

    async def close(self) -> None:
        self.closed = True


class _FakeDatabase:
    """An in-memory stand-in for a resonate-pg database.

    Satisfies :class:`~resonate_pg.client.PgSessions` structurally -- no
    psycopg, no server, no event loop tricks.
    """

    def __init__(self) -> None:
        self.queries: list[tuple[str, Any]] = []
        self.listened: list[str] = []
        self.executes: dict[str, list[tuple[str, int]]] = {}
        self.unblocks: dict[str, list[tuple[str]]] = {}
        self.errors: dict[str, Exception] = {}
        self.rpc_reply: str | None = '{"kind":"promise.get","head":{},"data":{}}'
        #: Notifications a test hands to the listener, one at a time. A queue
        #: rather than a count: the pump collapses every pending wake into one
        #: drain, so notifications delivered up front would all be spent before
        #: a test could enqueue the work they are supposed to wake it for.
        self.notify_queue: asyncio.Queue[_FakeNotify] = asyncio.Queue()
        #: Open by default. A test clears it to pin *when* the listener
        #: connects, which is the only way to prove the fresh-listener wake.
        self.dedicated_gate = asyncio.Event()
        self.dedicated_gate.set()
        self.dedicated_conns: list[_FakeConn] = []
        self.dedicated_error: Exception | None = None
        self.session_error: Exception | None = None
        self.open_sessions = 0
        self.closed = False

    def take(self, queues: dict[str, list[Any]], address: str, limit: int) -> list[Any]:
        pending = queues.get(address, [])
        taken, queues[address] = pending[:limit], pending[limit:]
        return taken

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[_FakeConn]:
        if self.session_error is not None:
            raise self.session_error
        self.open_sessions += 1
        try:
            yield _FakeConn(self)
        finally:
            self.open_sessions -= 1

    async def dedicated(self) -> _FakeConn:
        await self.dedicated_gate.wait()
        if self.dedicated_error is not None:
            raise self.dedicated_error
        conn = _FakeConn(self)
        self.dedicated_conns.append(conn)
        return conn

    async def close(self) -> None:
        self.closed = True

    async def notify_stream(self) -> AsyncIterator[_FakeNotify]:
        # Parks on an empty queue, like a live LISTEN connection with nothing
        # to say -- but a test can make it speak, exactly once, on demand.
        while True:
            yield await self.notify_queue.get()


def connection(db: _FakeDatabase, **kwargs: Any) -> PostgresConnection:
    """Build a connector over ``db`` with test-friendly defaults."""
    kwargs.setdefault("pid", "worker-1")
    kwargs.setdefault("group", "workers")
    kwargs.setdefault("tick", 0.01)
    return PostgresConnection(sessions=db, **kwargs)


# ── construction and addressing ────────────────────────────────────────────


def test_the_connector_is_both_a_network_and_a_source() -> None:
    conn = connection(_FakeDatabase())
    assert isinstance(conn, Network)
    assert isinstance(conn, Source)


def test_addresses_are_the_poll_shapes_the_server_accepts() -> None:
    conn = connection(_FakeDatabase())
    assert conn.unicast() == "poll://uni@workers/worker-1"
    assert conn.resolve_target("other") == "poll://any@other"


def test_the_drained_anycast_address_equals_this_groups_resolved_target() -> None:
    """``dequeue_execute`` matches byte-for-byte; a mismatch delivers nothing."""
    conn = connection(_FakeDatabase())
    assert conn._anycast == conn.resolve_target("workers")


def test_defaults_are_a_fresh_hex_pid_in_the_default_group() -> None:
    conn = PostgresConnection(sessions=_FakeDatabase())
    assert conn.unicast().startswith("poll://uni@default/")
    pid = conn.unicast().rsplit("/", 1)[1]
    assert len(pid) == 32
    assert all(c in "0123456789abcdef" for c in pid)


def test_exactly_one_of_conninfo_and_sessions_is_required() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        PostgresConnection()
    with pytest.raises(ValueError, match="exactly one"):
        PostgresConnection("postgresql:///db", sessions=_FakeDatabase())


def test_a_dequeue_limit_below_one_is_refused() -> None:
    """The drain pages until a short read; a limit of 0 would never finish."""
    with pytest.raises(ValueError, match="dequeue_limit"):
        PostgresConnection(sessions=_FakeDatabase(), dequeue_limit=0)


def test_recv_registers_every_callback() -> None:
    conn = connection(_FakeDatabase())
    first: list[str] = []
    second: list[str] = []
    conn.recv(first.append)
    conn.recv(second.append)

    conn._deliver('{"kind":"execute"}')

    assert first == ['{"kind":"execute"}']
    assert second == ['{"kind":"execute"}']


# ── send ───────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_send_runs_the_request_through_resonate_rpc() -> None:
    db = _FakeDatabase()
    db.rpc_reply = '{"kind":"promise.get","head":{"corrId":"c1"},"data":{}}'
    conn = connection(db)
    await conn.start()

    req = '{"kind":"promise.get","head":{"corrId":"c1"},"data":{"id":"root:1"}}'
    resp = await conn.send(req, {"resonate:origin": "root"})

    assert resp == db.rpc_reply
    # Sent verbatim as the one bound parameter: the connector moves the body,
    # it does not read it.
    assert db.queries == [(RPC_SQL, (req,))]


@pytest.mark.asyncio
async def test_send_after_stop_raises_without_touching_the_database() -> None:
    db = _FakeDatabase()
    conn = connection(db)
    await conn.start()
    await conn.stop()

    with pytest.raises(ConnectorError):
        await conn.send('{"kind":"promise.get"}')
    assert db.queries == []


@pytest.mark.asyncio
async def test_send_wraps_a_driver_failure_in_connector_error() -> None:
    """Every psycopg failure crosses the boundary as the SDK's own vocabulary."""
    db = _FakeDatabase()
    db.errors[RPC_SQL] = OSError("server closed the connection unexpectedly")
    conn = connection(db)
    await conn.start()

    with pytest.raises(ConnectorError) as excinfo:
        await conn.send('{"kind":"promise.get"}')
    assert isinstance(excinfo.value.error, OSError)


@pytest.mark.asyncio
async def test_send_wraps_a_failure_to_borrow_a_session() -> None:
    db = _FakeDatabase()
    db.session_error = OSError("connection refused")
    conn = connection(db)
    await conn.start()

    with pytest.raises(ConnectorError) as excinfo:
        await conn.send('{"kind":"promise.get"}')
    assert isinstance(excinfo.value.error, OSError)


@pytest.mark.asyncio
async def test_send_refuses_an_empty_result_rather_than_returning_none() -> None:
    """A missing row would otherwise reach the transport as ``'None'``."""
    db = _FakeDatabase()
    db.rpc_reply = None
    conn = connection(db)
    await conn.start()

    with pytest.raises(ConnectorError):
        await conn.send('{"kind":"promise.get"}')


# ── the drain ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_drain_takes_executes_from_both_addresses_and_unblocks_from_unicast() -> (
    None
):
    """Anycast carries the group's work; unblocks answer *this* listener."""
    db = _FakeDatabase()
    db.executes["poll://any@workers"] = [("root:1", 0)]
    db.executes["poll://uni@workers/worker-1"] = [("root:2", 3)]
    db.unblocks["poll://uni@workers/worker-1"] = [
        ('{"id":"root:3","state":"RESOLVED"}',)
    ]
    conn = connection(db, drive_timers=False)
    received: list[str] = []
    conn.recv(received.append)

    await conn._drain()

    assert [json.loads(m) for m in received] == [
        {
            "kind": "execute",
            "head": {},
            "data": {"task": {"id": "root:1", "version": 0}},
        },
        {
            "kind": "execute",
            "head": {},
            "data": {"task": {"id": "root:2", "version": 3}},
        },
        {
            "kind": "unblock",
            "head": {},
            "data": {"promise": {"id": "root:3", "state": "RESOLVED"}},
        },
    ]


@pytest.mark.asyncio
async def test_drain_never_dequeues_unblocks_on_the_shared_anycast_address() -> None:
    """A listener is registered with the unicast address; nothing else is ours."""
    db = _FakeDatabase()
    conn = connection(db, drive_timers=False)

    await conn._drain()

    unblock_addresses = [
        params[0] for query, params in db.queries if query == DEQUEUE_UNBLOCK_SQL
    ]
    assert unblock_addresses == ["poll://uni@workers/worker-1"]


@pytest.mark.asyncio
async def test_drain_pages_until_a_short_read() -> None:
    """A full page means more may be waiting; stopping there would stall work."""
    db = _FakeDatabase()
    db.executes["poll://any@workers"] = [(f"root:{i}", 0) for i in range(5)]
    conn = connection(db, drive_timers=False, dequeue_limit=2)
    received: list[str] = []
    conn.recv(received.append)

    await conn._drain()

    assert len(received) == 5
    anycast_reads = [
        params
        for query, params in db.queries
        if query == DEQUEUE_EXECUTE_SQL and params[0] == "poll://any@workers"
    ]
    # 2 + 2 + 1: three statements, the last one short.
    assert anycast_reads == [("poll://any@workers", 2)] * 3


@pytest.mark.asyncio
async def test_drain_delivers_only_after_the_deleting_transaction_commits() -> None:
    """A callback that ran inside the session could see a rolled-back delete."""
    db = _FakeDatabase()
    db.executes["poll://any@workers"] = [("root:1", 0)]
    conn = connection(db, drive_timers=False)
    open_at_delivery: list[int] = []
    conn.recv(lambda _: open_at_delivery.append(db.open_sessions))

    await conn._drain()

    assert open_at_delivery == [0]


# ── timers ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_each_drain_advances_the_databases_timers() -> None:
    """pg_cron is optional; a running node keeps sleeps and retries moving."""
    db = _FakeDatabase()
    conn = connection(db)

    await conn._drain()

    assert (PROCESS_TIMEOUTS_SQL, None) in db.queries


@pytest.mark.asyncio
async def test_the_timer_call_stands_down_after_the_first_refusal() -> None:
    """``process_timeouts`` is not granted to ``resonate_worker``.

    A worker on a least-privilege role must degrade to pg_cron rather than
    log a permission error four times a second forever.
    """
    db = _FakeDatabase()
    db.errors[PROCESS_TIMEOUTS_SQL] = PermissionError("permission denied")
    conn = connection(db)

    await conn._drain()
    await conn._drain()

    attempts = [q for q, _ in db.queries if q == PROCESS_TIMEOUTS_SQL]
    assert attempts == [PROCESS_TIMEOUTS_SQL]


@pytest.mark.asyncio
async def test_a_refused_timer_call_does_not_stop_the_drain() -> None:
    """The timer runs in its own session precisely so it cannot poison one."""
    db = _FakeDatabase()
    db.errors[PROCESS_TIMEOUTS_SQL] = PermissionError("permission denied")
    db.executes["poll://any@workers"] = [("root:1", 0)]
    conn = connection(db)
    received: list[str] = []
    conn.recv(received.append)

    await conn._drain()

    assert len(received) == 1


@pytest.mark.asyncio
async def test_drive_timers_false_never_calls_process_timeouts() -> None:
    db = _FakeDatabase()
    conn = connection(db, drive_timers=False)

    await conn._drain()

    assert PROCESS_TIMEOUTS_SQL not in [q for q, _ in db.queries]


# ── lifecycle and the pump ─────────────────────────────────────────────────


async def _until(predicate: Callable[[], bool], timeout_secs: float = 2.0) -> None:
    """Await ``predicate`` becoming true, failing the test if it never does.

    Not named ``timeout``: ruff's ``ASYNC109`` reserves that parameter name on
    an async function for one that is actually forwarded to a timeout
    primitive.
    """
    deadline = asyncio.get_running_loop().time() + timeout_secs
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            msg = "condition never became true"
            raise AssertionError(msg)
        await asyncio.sleep(0.005)


def _drains(db: _FakeDatabase) -> int:
    """Count drain passes that have finished reading.

    The unblock read is a drain's last statement, and with an empty unblock
    queue there is exactly one per pass -- so this counts passes, and a test
    that waits on it can enqueue work knowing the pass it just watched cannot
    still pick it up.
    """
    return sum(1 for query, _ in db.queries if query == DEQUEUE_UNBLOCK_SQL)


@pytest.mark.asyncio
async def test_start_without_a_receiver_opens_nothing() -> None:
    """A network-only connection must not claim the group's outbox rows.

    ``dequeue_*`` deletes what it reads. A connector with nowhere to deliver
    would silently destroy its peers' work.
    """
    db = _FakeDatabase()
    conn = connection(db)

    await conn.start()
    await asyncio.sleep(0.05)

    assert db.queries == []
    assert conn._tasks == []
    await conn.stop()


@pytest.mark.asyncio
async def test_start_with_a_receiver_pumps_the_outbox() -> None:
    db = _FakeDatabase()
    db.executes["poll://any@workers"] = [("root:1", 0)]
    conn = connection(db)
    received: list[str] = []
    conn.recv(received.append)

    await conn.start()
    try:
        await _until(lambda: len(received) == 1)
    finally:
        await conn.stop()

    assert json.loads(received[0])["data"]["task"]["id"] == "root:1"


@pytest.mark.asyncio
async def test_the_pump_survives_a_failing_drain_and_tries_again() -> None:
    """The database going away must not kill the loop that recovers from it."""
    db = _FakeDatabase()
    db.session_error = OSError("connection refused")
    conn = connection(db)
    conn.recv(lambda _: None)

    await conn.start()
    try:
        await _until(lambda: len(db.dedicated_conns) >= 1)
        db.session_error = None
        db.executes["poll://any@workers"] = [("root:1", 0)]
        received: list[str] = []
        conn.recv(received.append)
        await _until(lambda: len(received) == 1)
    finally:
        await conn.stop()


@pytest.mark.asyncio
async def test_a_wake_collapses_the_tick() -> None:
    """A wake is the fast path; the tick is only the backstop.

    With a 30s tick, a delivery inside the 2s budget can only have come from
    the wake.
    """
    db = _FakeDatabase()
    conn = connection(db, tick=30.0)
    received: list[str] = []
    conn.recv(received.append)
    await conn.start()
    try:
        await _until(lambda: _drains(db) >= 1)
        db.executes["poll://any@workers"] = [("root:1", 0)]
        conn._wake.set()
        await _until(lambda: len(received) == 1)
    finally:
        await conn.stop()


@pytest.mark.asyncio
async def test_stop_cancels_the_loops_closes_sessions_and_clears_receivers() -> None:
    db = _FakeDatabase()
    conn = connection(db)
    conn.recv(lambda _: None)
    await conn.start()
    await _until(lambda: db.queries != [])

    await conn.stop()

    assert conn._tasks == []
    assert conn._subscribers == []
    assert db.closed
    assert all(c.closed for c in db.dedicated_conns)


@pytest.mark.asyncio
async def test_stop_before_start_is_a_no_op() -> None:
    """Shutdown runs on paths where startup never did; it must not raise."""
    await connection(_FakeDatabase()).stop()


# ── the LISTEN connection ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_the_listener_subscribes_to_both_of_this_nodes_channels() -> None:
    db = _FakeDatabase()
    conn = connection(db)
    conn.recv(lambda _: None)

    await conn.start()
    try:
        await _until(lambda: len(db.listened) == 2)
    finally:
        await conn.stop()

    # md5 of 'poll://uni@workers/worker-1' and of 'poll://any@workers' -- this
    # connection's own two addresses, not the ones test_wire.py pins.
    assert db.listened == [
        'LISTEN "resonate_q_8732afae3b48a0bd4e1a91e8b7e8e73c"',
        'LISTEN "resonate_q_9f09af58d876414736aceefc75c1dbfe"',
    ]


@pytest.mark.asyncio
async def test_a_fresh_listener_wakes_the_pump() -> None:
    """Rows may have landed while the listener was down.

    The listener is held shut until after the pump's first drain has finished
    reading, and the tick is 30s, so the delivery below can only have come
    from the wake a fresh LISTEN session fires.
    """
    db = _FakeDatabase()
    db.dedicated_gate.clear()
    conn = connection(db, tick=30.0)
    received: list[str] = []
    conn.recv(received.append)

    await conn.start()
    try:
        await _until(lambda: _drains(db) >= 1)
        db.executes["poll://any@workers"] = [("root:1", 0)]
        db.dedicated_gate.set()
        await _until(lambda: len(received) == 1)
    finally:
        await conn.stop()


@pytest.mark.asyncio
async def test_a_notification_wakes_the_pump() -> None:
    """NOTIFY is the fast path.

    Two drains settle first -- the pump's own, then the fresh-listener wake --
    so the third can only have been triggered by the notification. With a 30s
    tick nothing else could have delivered inside the budget.
    """
    db = _FakeDatabase()
    conn = connection(db, tick=30.0)
    received: list[str] = []
    conn.recv(received.append)

    await conn.start()
    try:
        await _until(lambda: _drains(db) >= 2)
        db.executes["poll://any@workers"] = [("root:1", 0)]
        await db.notify_queue.put(_FakeNotify())
        await _until(lambda: len(received) == 1)
    finally:
        await conn.stop()


@pytest.mark.asyncio
async def test_the_listener_reconnects_with_a_backoff_ladder() -> None:
    """A database restart must cost latency, not a permanently deaf worker."""
    db = _FakeDatabase()
    db.dedicated_error = OSError("connection refused")
    delays: list[float] = []

    async def record(secs: float) -> None:
        delays.append(secs)
        await asyncio.sleep(0)

    conn = connection(db, sleeper=record)
    conn.recv(lambda _: None)

    await conn.start()
    try:
        await _until(lambda: len(delays) >= 4)
    finally:
        await conn.stop()

    assert delays[:4] == [1.0, 2.0, 4.0, 8.0]


@pytest.mark.asyncio
async def test_the_listener_closes_its_connection_on_every_attempt() -> None:
    """A leaked LISTEN connection is a leaked backend for the pool to fight."""
    db = _FakeDatabase()
    conn = connection(db)
    conn.recv(lambda _: None)
    await conn.start()
    await _until(lambda: len(db.dedicated_conns) == 1)

    await conn.stop()

    assert db.dedicated_conns[0].closed
