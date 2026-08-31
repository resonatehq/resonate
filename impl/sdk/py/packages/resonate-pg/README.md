# resonate-pg

Postgres connector for Resonate. The server *is* the database.

`PostgresConnection` is both a `Network` (request/reply) and a `Source`
(`execute` / `unblock` messages), so it can serve as a Resonate client's only
connection -- passed as `network`, it doubles as the source.

```python
from resonate.resonate import Resonate
from resonate_pg import PostgresConnection

conn = PostgresConnection(
    "postgresql://user:pass@localhost:5432/app",
    group="workers",
)
resonate = Resonate(network=conn, group="workers")
```

Name the same `group` on both. The connection uses it to build the addresses
it drains; Resonate uses it as the routing target a `run`/`rpc` defaults to.
Told two different groups, a worker would dispatch its own work to an address
it is not draining.

## The server

Apply [`resonate.sql`](https://github.com/resonatehq/resonate-pg) to Postgres
16+ once:

```bash
psql -d yourdb -f resonate.sql
```

Every protocol action is then a stored procedure, and this connector runs each
request as `SELECT resonate.resonate_rpc($1::jsonb)`.

## Addressing

Addresses are `poll://` URLs, the same shapes the SDK's SSE source mints:

| | Address | Drained with |
|---|---|---|
| unicast | `poll://uni@{group}/{pid}` | `dequeue_execute` + `dequeue_unblock` |
| anycast | `poll://any@{group}` | `dequeue_execute`, which deletes `FOR UPDATE SKIP LOCKED` -- so exactly one group member wins each row |

These are required, not chosen. `resonate.promise_register_listener` rejects
any address that is not `http(s)://` or `poll://…@…`, and `dequeue_execute`
matches an address byte-for-byte against the `resonate:target` a peer stamped.

## Delivery

A background pump drains this node's outbox rows and hands them to the SDK. It
wakes on `NOTIFY` -- resonate-pg signals `resonate_q_{md5(address)}` for every
row it enqueues -- and, failing that, on a 250 ms tick, so a notification lost
to a dropped connection costs latency and nothing else. Messages are delivered
only after the claiming transaction commits.

Delivery is at-least-once. A crash between the commit and the callback is
recovered by the task's own retry timeout, which is the guarantee the SDK is
built on anyway.

## Timers

resonate-pg's timers -- durable sleeps, task retries, promise timeouts -- are
driven by `resonate.process_timeouts()`, which pg_cron normally runs every 5s.
The pump also calls it on every wake, so a database without pg_cron still
works.

`resonate.sql` does **not** grant `process_timeouts` to the `resonate_worker`
role. A worker on that role is refused, logs once, and leaves timers to
pg_cron. Pass `drive_timers=False` to skip the call entirely.

## Connections

Pass a connection string and the connector owns its connections: a pooled one
per query, plus one autocommit connection holding `LISTEN` open. To own them
yourself -- a shared pool, custom TLS, a proxy -- pass anything satisfying
`PgSessions`:

```python
from resonate_pg import PostgresConnection, PsycopgSessions

sessions = PsycopgSessions("postgresql:///app", max_size=64)
conn = PostgresConnection(sessions=sessions, group="workers")
```

## Errors

Transport failures surface as `ConnectorError`, so application code can handle
*any* transport giving up without importing this package or knowing which
connector is in use:

```python
from resonate_base import ConnectorError

try:
    await resonate.run("greet", ...)
except ConnectorError as exc:
    ...
```

## Operations

Completed workflows stay in the database. Delete old ones on a schedule:

```sql
select cron.schedule('resonate-gc', '0 3 * * *',
  $$select resonate.gc((extract(epoch from now())*1000 - 7*86400000)::bigint)$$);
```

## Install

```bash
pip install resonate-pg
```

Deliberately not an extra of `resonate-sdk`: an extra would put this package in
the SDK's published dependency metadata, pointing the arrow back down the
stack. `resonate-pg` depends only on
[`resonate-base`](https://pypi.org/project/resonate-base/) and psycopg, never
on `resonate-sdk`.