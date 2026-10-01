# resonate-server-mongodb

The Resonate server over MongoDB: one document per promise, callbacks and
resumes as arrays, one multi-document transaction per transition.

```toml
[servers]
active = "server_mongodb"

[servers.server_mongodb]
uri = "mongodb://localhost:27017/?replicaSet=rs0"
database = "resonate"   # optional; else the URI's path, else "resonate"
```

Or `resonate serve --storage-type mongodb --storage-mongodb-uri mongodb://localhost:27017/?replicaSet=rs0`.

MongoDB as a **replica set or a sharded cluster** (tested on 8.0, which CI
runs): every transition is a transaction, and a standalone `mongod` cannot run
one. One member is enough — `mongod --replSet rs0` and `rs.initiate()` once — and the
server refuses to start, in those words, on a standalone. The schema is
indexes only, created on start (`migrate = true`, the default; creating an
index that exists is a no-op).

```sh
docker compose --profile mongodb up    # mongo:8 as a one-member replica set, and the server
```

## The documents

`promises`, one document per promise, `_id` = the promise id. The task's
fields sit in the same document, because a promise runs as a task exactly
when it carries a `resonate:target`:

| Field | Meaning |
|---|---|
| `state`, `param_*`, `value_*`, `tags`, `timeout_at`, `created_at`, `settled_at` | The promise record. |
| `task_state`, `task_version`, `retry_timeout_at`, `lease_timeout_at`, `ttl`, `pid` | The task, or `null` when there is none. |
| `callbacks` | The awaiters blocked on this promise. |
| `resumes` | The settled promises this task has not consumed yet. |
| `listeners` | Addresses to unblock when this promise settles. |
| `origin`, `root`, `target`, `parent_id`, `branch_id`, `is_timer`, `external` | The tags' projections, as Postgres's generated columns. |
| `rev` | The lock: see below. |

`schedules`, one document per schedule, `_id` = the schedule id; its
`next_run_at` is its queue.

Tags and headers are stored as arrays of `{k, v}` pairs rather than as
subdocuments. A key is caller text, and a `.` in a field name is a path to
every query and update operator. As pairs, tag containment is an `$all` of
`$elemMatch`es over a multikey index on `tags.k, tags.v`.

The four deadline queues are partial indexes, as they are in Postgres:
`timeout_at` over pending external promises, `retry_timeout_at` over pending
tasks, `lease_timeout_at` over acquired tasks, and `next_run_at` over
schedules. A sweep reads its queue and nothing else.

## How a transition runs

Every operation is one transaction (snapshot reads, majority writes), and it
begins by *locking* the documents it will decide on:

```js
db.promises.findOneAndUpdate({_id: id}, {$inc: {rev: 1}}, {returnDocument: "after"})
```

A transaction reads a snapshot and takes no read locks, so a read alone
protects nothing. A write does: from the moment this transaction has written a
document, any other transaction that writes it fails with a `WriteConflict`.
The Rust between that read and the writes that follow can therefore reason
about state nothing else is changing, and a settle plus its awaiter fan-out
plus its listener unblocks commit together or not at all.

Unlike Postgres's `FOR UPDATE` or Neo4j's write locks, the loser does not
wait. It is aborted at once with a `TransientTransactionError`, rolls back,
and runs again with jittered backoff — up to twelve times before the caller
sees a 503. Nobody waits, so nobody deadlocks, and lock order does not
matter. A commit whose outcome is unknown is retried as a commit, never as a
replay of the body.

The arrays are what make the fan-out sound. Linking an awaiter writes the
awaited promise's document, so a link and a concurrent settle of the same
promise conflict, and one of them runs again on the other's result. Edges in
a collection of their own would not: inserting an edge and settling the
promise touch different documents, and nothing would notice the race.

## What is shared, and what is copied

`engine.rs`, `server.rs`, `deadlines.rs`, `sweep.rs` and `errors.rs` are
byte-for-byte copies of `resonate-server-scylladb`'s; `metrics.rs` differs in
its two metric names. That is the Neo4j server's arrangement, carried over, so
the copies can be diffed and any divergence read off:

```sh
for f in engine.rs server.rs deadlines.rs sweep.rs metrics.rs errors.rs; do
  diff crates/resonate-server-scylladb/src/$f crates/resonate-server-mongodb/src/$f
done
```

The transitions in `ops_*.rs` are the Neo4j server's, decision for decision;
what differs is the storage underneath them in `db.rs`.

## Testing

The differential (`diff/differential.rs`) and the console tests (`tests/ui.rs`)
run this engine beside SQLite, the oracle and whatever else is named, through
`diff/mongodb_adapter.rs`:

```sh
mongod --replSet rs0 --dbpath /tmp/rs0 &   # then, once: mongosh --eval 'rs.initiate()'
TEST_MONGODB_URI='mongodb://localhost:27017/?replicaSet=rs0' \
  cargo test --release --test differential -- --nocapture
TEST_MONGODB_URI='mongodb://localhost:27017/?replicaSet=rs0' \
  cargo test --test ui
```

Or exactly as CI does, on a fresh database that is dropped afterwards:

```sh
XTASK_MONGODB_URI='mongodb://localhost:27017/?replicaSet=rs0' cargo xtask differential --backend mongodb
XTASK_MONGODB_URI='mongodb://localhost:27017/?replicaSet=rs0' cargo xtask porcupine --backend mongodb
```
