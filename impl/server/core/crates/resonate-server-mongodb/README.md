# resonate-server-mongodb

The Resonate server over MongoDB: one document per promise, callbacks and
resumes as arrays, one multi-document transaction per transition.

```toml
[servers]
active = "server_mongodb"

[servers.server_mongodb]
uri = "mongodb://localhost:27017/?replicaSet=rs0"
database = "resonate"   # optional; else the URI's path, else "resonate"
shard = false           # true: shard `promises` by hashed origin (needs mongos)
sweep_interval = 600000 # ms between backstop sweeps; default 10 minutes
```

Deadlines fire from an in-memory timer, one transaction each, as they come
due. The timer reloads the nearest durable deadlines every `wheel_refresh`
(30 s by default). The sweep is the backstop for whatever overflowed the timer
(`wheel_capacity`, 8,192 by default). It scans every deadline queue, so it runs
rarely: every 10 minutes by default.

Or `resonate serve --storage-type mongodb --storage-mongodb-uri mongodb://localhost:27017/?replicaSet=rs0`.

MongoDB as a **replica set or a sharded cluster** (tested on 8.0): every
transition is a transaction, and a standalone `mongod` cannot run one. One
member is enough — `mongod --replSet rs0` and `rs.initiate()` once — and the
server refuses to start, in those words, on a standalone. For a sharded
cluster, see [Sharding](#sharding). The schema is
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
| `awaiting` | The promises whose `callbacks` hold this task: the reverse of `callbacks`. |
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

## Sharding

Point `uri` at the `mongos` routers and set `shard = true`. On start (with
`migrate`), the server creates a hashed index on `origin` and runs
`shardCollection` on `promises` with `{origin: "hashed"}`. This is idempotent,
and it works on a collection that already holds documents. `schedules` stays
unsharded on the database's primary shard.

**Why `origin`.** Every promise of one call tree shares its origin, so the tree
lives on one shard, the way the ScyllaDB server partitions by origin. Most
transitions stay inside one tree: a settle and its fan-out to awaiting
siblings, a suspend on its children, a fulfil. Those are single-shard
transactions. Only an await *across* trees spans shards, and it still works,
at the cost of a two-phase commit. The key is hashed because root ids that
grow with time would otherwise all land in the last range, on one shard.

**Targeting.** `origin` is a function of `_id`, so every per-document filter
names both (`db::key`, `db::keys`), and `mongos` routes it to the one shard
that owns it. A filter on `_id` alone would go to every shard. The `awaiting`
array exists for the same reason. A fulfilled task withdraws itself from the
`callbacks` of exactly the promises it was blocked on. A scan for
`{callbacks: id}` would write to every shard, and so make every shard a
participant in every fulfil's commit.

**What still asks every shard.** The deadline sweeps, `upcoming`, the searches
and the console's lists are queries over a queue or an index, not over one
tree. They run on every shard and the results are merged. They are background
work or paged reads, and the partial indexes keep each shard's part small. So
does the branch-sibling preload, because nothing makes a branch's members
share an origin.

`_id` is unique per shard rather than across the cluster, which is what
MongoDB guarantees for any shard key that does not start with `_id`. Here that
is enough: an id always maps to the same origin, so to the same shard, so two
creates of one id meet on one shard and one of them loses.

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

Against a sharded cluster, through `mongos`, with the engine sharding
`promises` itself:

```sh
TEST_MONGODB_URI='mongodb://localhost:27030/resonate_test' TEST_MONGODB_SHARD=true \
  cargo test --release --test differential -- --nocapture
```

Or exactly as CI does, on a fresh database that is dropped afterwards:

```sh
XTASK_MONGODB_URI='mongodb://localhost:27017/?replicaSet=rs0' cargo xtask differential --backend mongodb
XTASK_MONGODB_URI='mongodb://localhost:27017/?replicaSet=rs0' cargo xtask porcupine --backend mongodb
```

The crate depends on the driver with `bson-3` rather than its default
`bson-2`. bson 2 turns on `serde_json/preserve_order`, and Cargo features
apply to the whole binary. With it, every JSON map every backend writes would
keep insertion order, and `serde_json::Value` would grow past clippy's
`result_large_err` limit for the whole workspace.
