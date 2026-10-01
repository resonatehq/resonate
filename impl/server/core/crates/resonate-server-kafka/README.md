# resonate-server-kafka

A Resonate server over Kafka that scales across nodes by partition: every node
serves a share of the partitions, any node takes any request, and a node that
joins, leaves or dies moves only its own share.

It is a second shell around the blob backend's kernel — the protocol's state
machine as a pure function — so the semantics are the same by construction, and
the differential suites hold it to the oracle step for step.

## The design in one page

**Partition by origin.** Every promise and task operation is single-origin, so
the origin (everything before the first `:` of an id) picks the partition:
Kafka's own murmur2 partitioner over the origin, modulo a fixed partition
count. Schedules are partitioned by their id the same way. A partition owner
can answer any single operation on its partition without talking to anyone.

**Topics.** Two, compacted, from one prefix:

| topic | partitions | key | value |
|---|---|---|---|
| `<prefix>.promises` | N | promise id | the promise **and its task** — one record |
| `<prefix>.schedules` | N | schedule id | the schedule |

A record is the object's whole current version, never a delta, so compaction
keeps exactly the current state and replaying a record twice is harmless. The
value is the blob backend's document encoding of a document holding that one
promise and its task, deadlines included.

**Ownership: a consumer group.** Every node joins one group, cooperative-sticky,
subscribed to the promise topic. Assigned partitions are paused — the consumer
is there for its membership, not its records. A revoke callback blocks the poll
thread until the node has stopped the partitions, which is what makes an
ordinary handoff clean.

**Safety: a claim.** No Kafka transactions — one costs about ten times a
produce. Taking partition *p* over begins with a claim appended to its logs,
and from then on no earlier owner's record counts — including a paused node
that does not know it lost *p*. Only then is the log read to its end; only
then is the partition served. [Below](#fencing-and-atomicity-without-transactions)
is how.

**Takeover**, in order: claim → check the local copy against the log's start
offset → replay from the local checkpoint to the end → write again what a
fenced writer's records hide → rebuild the timer index → serve.

**A round.** One actor per partition drains its mailbox, loads each origin the
batch names from the local store, folds the batch through the kernel, turns
each decision into records in an order where every prefix is valid, produces
them in one write (nothing at all if nothing changed), applies them locally
with the checkpoint the commit returned, re-arms timers, sends messages
post-commit, and answers. A commit whose outcome is unknown takes the
partition over again: claim, re-read, serve.

**Local copy: RocksDB, WAL off.** One database per node, one column family per
partition. Each batch writes its records and its checkpoint into the
partition's column family in one write, so a crash can lose a tail of batches
but never leave the checkpoint ahead of the data — and Kafka replays the tail.
A revoked partition's column family is kept for a grace period (a partition
that comes straight back replays only its tail) and then dropped. Memory is the
block cache and memtables, shared across column families, and the timer index.

**Local format: postcard, stamped.** The log keeps the blob codec's versioned
lines, because every node and every future version reads them. The local copy
is private and disposable, so it holds the same promise and task as postcard,
which decodes 3–4× faster (`examples/local_codec.rs`). Each column family is
stamped with the local format; a copy stamped otherwise is dropped and rebuilt
from the log at takeover, so the local format can change between any two
releases with no migration. Hot origins skip decoding altogether: each
partition keeps an LRU of decoded documents (`cache_promises`), owned by its
actor, refilled only by committed rounds, and empty after every takeover.

**Timers** are fields of records, committed with the state that arms them. The
index is in memory only, rebuilt from the records on takeover.

**Nodes find each other through the group.** No node keeps a list of the
others. Each node's group consumer carries `resonate/<node>/<peer url>` as its
`client.id`, and every node asks the group coordinator
(`DescribeConsumerGroups`) every couple of seconds — and at once after a failed
forward — which member is assigned which partition. A node that dies drops out
when its session times out. The directory is for routing only: a stale answer
costs a 503 and a retry, never a lost or doubled write, because the fence is
what keeps writes safe.

**Any node answers.** A request for a partition this node does not serve is
forwarded once, over an internal HTTP listener, to the owner the group names;
a forwarded request is never forwarded again. Searches are a
scatter-gather over every owner. A schedule firing into another partition's
origin crosses the same way.

## Fencing and atomicity without transactions

A Kafka transaction would give two things: a broker that refuses a fenced
writer, and commits that land whole. It costs about ten times a plain produce
(`examples/txn_cost.rs`: 4.3 ms against 0.44 ms on one broker), and the commit
is the request — so both come from elsewhere.

**Fencing: claims and epochs** (`src/log/epoch.rs`). Nothing on the broker
refuses a writer any more, so ownership is decided in the log, the same way by
every reader:

- Taking a partition over appends a **claim** to each of its two logs, carrying
  the offset it expects to land at: the end as the claimer read it. A claim is
  valid only if it landed exactly there. Two claimers cannot both win, and the
  loser sees it in the offset it got. A valid claim's offset is the log's new
  **epoch**.
- Every record carries its writer's epoch in a header. A reader admits a record
  only if that is the epoch in force where the record lies, so a fenced
  writer's records, landing after a newer claim, are read past.
- A writer checks that every record landed exactly where it last left the
  log. If anything else landed in between, it stops, and the partition is
  taken over again. An idle owner asks every second
  (`Writer::check`).
- A refused record is still the newest of its key, and compaction keeps the
  newest. So replay reports every key whose newest record it refused, and the
  owner writes the admitted value again before serving. `min.compaction.lag.ms`
  (default 1 h, set on topics this server creates) gives that time.

Claims are keyed by their epoch, so compaction keeps every one. A record
without an epoch is refused. On connect, both topics must be compacted with
a `min.compaction.lag.ms` of at least the producer's delivery timeout plus a
minute, or the server refuses to start. Because a fenced writer's records do
land, nothing but this server should read the topics: a reader without the
epoch filter would see them.

**Atomicity: an order where every prefix is valid** (`record::ordered`,
`kernel::recover`). A commit can stop after any prefix of its records. The specification does not make a settlement and its
consequences one step: settling writes the promise, and delivering each
callback (resume the awaiter, drop the callback) and each listener (unblock,
drop the listener) are later steps of their own. Timeouts are likewise
processed one object at a time. So every kernel decision's records are written
in the specification's order:

1. settled promises, still holding the callbacks and listeners they owe;
2. records that only gained callbacks (a task's registrations come before
   it suspends);
3. everything else (awaiters resumed, tasks created or moved);
4. records that lost callbacks (a finished task's registrations on others);
5. each settled promise's final value, without what it owed;
6. tombstones.

Whatever prefix lands, the state is one the specification allows, except
that deliveries may still be owed. `kernel::recover` makes them before
anything else reads the origin, exactly as the settlement's fan-out would
have. It is idempotent: an awaiter already resumed only records the resume
again. Origins that need it are armed at once on takeover. As on every
backend, sends go out after the commit and at most once: a cut commit loses its
round's messages. Executes are re-sent by the retry timer; unblocks are not.

`tests/differential.rs::prefix_random` checks this: the whole differential
trajectory, with every step's commit cut part-way, entirely or not at all, and
then reported lost. After the partition is taken over and recovered, the node
must hold what the oracle holds, or a retry must bring it there with the
oracle's answer.

## Configuration

`[servers.server_kafka]`:

| key | default | |
|---|---|---|
| `brokers` | unset | `bootstrap.servers`. Unset: one node over an in-process log, nothing durable. |
| `partitions` | 64 | Fixed for the life of the deployment. |
| `topic_prefix` | `resonate` | |
| `group_id` | the topic prefix | |
| `replication_factor` | 3 | For topics this server creates. |
| `create_topics` | true | An existing topic with a different partition count is refused. |
| `node_id` | `$HOSTNAME` | Unique in the cluster. |
| `peer_bind`, `peer_url` | `0.0.0.0:8002`, unset | The internal forwarding listener, and where other nodes reach it. |
| `peer_token` | unset | Shared secret for peer requests. |
| `directory_refresh_ms` | 2000 | How often the group is asked who owns what. |
| `data_dir` | unset | RocksDB directory. Unset: local copies in memory, restored from Kafka on every start. |
| `block_cache_mb`, `write_buffer_mb` | 256, 128 | Shared across partitions. |
| `session_timeout_ms` | 10000 | How long a silent node keeps its partitions. |
| `instance_id` | unset | `group.instance.id`, for static membership. |
| `drop_grace_secs` | 900 | How long a revoked partition's local copy is kept. |
| `max_batch` | 512 | The group commit's ceiling. |
| `cache_promises` | 2000 | Per partition: decoded hot documents kept, counted in promises. 0 turns it off. |
| `search_enabled` | false | Searches read every record of every partition. |
| `min_compaction_lag_ms` | 3600000 | `min.compaction.lag.ms` on topics this server creates. An existing topic with less than 70 s is refused. |
| `librdkafka` | `{}` | Extra client properties (SASL, TLS, tuning). |

The plugin is not in the `resonate` binary's registry: it builds librdkafka and
RocksDB from source. A binary that wants it names it, as the top-level README
shows for any plugin.

## Brokers

Apache Kafka (tested with 3.9.1, KRaft) and Redpanda (tested with 26.2.3): the
live tests and the differential pass on both. Everything used is in the
Kafka protocol both implement: idempotent producers, record headers,
compacted topics with `min.compaction.lag.ms`, `DescribeConfigs`, the classic
group protocol with the cooperative-sticky assignor, and
`DescribeConsumerGroups`. No transactions, so no control batches: offsets are
exact on both brokers.

## Tests

```
cargo test -p resonate-server-kafka                       # unit, cluster, crash, differential
TEST_KAFKA_BROKERS=localhost:9092 \
  cargo test -p resonate-server-kafka -- --test-threads=1 # + fencing, restart, two nodes, differential on a broker
```

- `tests/differential.rs` — the blob backend's differential, against this node,
  in memory and on a broker, and `prefix_random`: the same trajectory with
  every commit cut.
- `tests/cluster.rs` — several nodes on one in-process log: routing, leave,
  a zombie whose records land, are read past and are written over before
  compaction, restore from a compacted log, refused and uncertain commits.
- `tests/crash.rs` — a child process aborts mid-write with the WAL off; the
  surviving checkpoint names only records that survived.
- `tests/live.rs` — real Kafka: fencing by claim, restart from RocksDB, two nodes with a real consumer group and HTTP forwarding.

## Performance

Measured end to end over HTTP with `examples/load.rs` against
`examples/server.rs` (each client: `promise.create` then `promise.settle` on
a fresh origin, 256-byte payloads, one request in flight per client). One
4-vCPU VM ran everything — broker, node(s) and load generator — with a single
broker and replication factor 1, so these are relative numbers, not capacity.

| broker | clients | req/s | client p50 / p99 | server mean | commit mean | requests per round |
|---|---|---|---|---|---|---|
| Kafka | 64 | 6,361 | 10 / 22 ms | 4.8 ms | 1.8 ms | 1.4 |
| Kafka | 256 | 6,236 | 39 / 94 ms | 18.9 ms | 2.0 ms | 1.5 |
| Redpanda | 64 | 6,727 | 9 / 22 ms | 6.5 ms | 3.2 ms | 1.8 |

1 node, 16 partitions. A commit is a produce. At about 6,300 req/s the shared
four cores are the limit: 256 clients went no faster than 64, and client time
is twice server time. Loading, deciding and applying take well under a
millisecond; the HTTP edge adds microseconds.

**Why there are no transactions.** The same runs, on the same VM and build,
with every round committed in a Kafka transaction (the design this backend
started with):

| broker | clients | req/s | client p50 / p99 | server mean | commit mean | requests per round |
|---|---|---|---|---|---|---|
| Kafka | 64 | 2,473 | 19 / 125 ms | 23.9 ms | 11.6 ms | 2.1 |
| Kafka | 256 | 4,649 | 49 / 169 ms | 37.6 ms | 12.5 ms | 4.1 |
| Redpanda | 64 | 2,069 | 24 / 131 ms | 30.1 ms | 16.1 ms | 2.2 |

The transaction was the request, and throughput was transactions per second
times requests per round: the broker completed roughly 1,000–1,500
transactions per second in every run, and more partitions only made each
slower.

## Known limits

- A **hot origin** is one partition's work: one actor, one core.
- **Rebalance pause**: a moving partition is unserved from its revoke until its
  new owner has replayed it. Keeping local copies keeps that short.
- A **zombie** (a frozen node whose session expired) that wakes still believing
  it owns a partition may fence the rightful owner once before its next poll
  tells it otherwise. No write is ever lost or double-acknowledged — fencing
  guarantees that — but the partition can flap for a moment.
- **Debug operations** need one node serving every partition.
- `poll://` workers connect to one node; a message for them produced on another
  node is delivered through that node's router, not forwarded.
