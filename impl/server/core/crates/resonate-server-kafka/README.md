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

**Safety: a fence.** Partition *p* has one transactional id. Taking *p* over
begins with `init_transactions` on it, which refuses every earlier producer —
including a paused node that does not know it lost *p*. Only then is the log
read to its end; only then is the partition served. Every round commits its
records in one transaction, so a fenced writer can never have a write
acknowledged.

**Takeover**, in order: fence → check the local copy against the log's start
offset → replay from the local checkpoint to the end → rebuild the timer index
→ serve.

**A round.** One actor per partition drains its mailbox, loads each origin the
batch names from the local store, folds the batch through the kernel, diffs the
documents into records, commits them in one transaction (nothing at all if
nothing changed), applies them locally with the checkpoint the commit returned,
re-arms timers, sends messages post-commit, and answers. A commit whose outcome
is unknown takes the partition over again: fence, re-read, serve.

**Local copy: RocksDB, WAL off.** One database per node, one column family per
partition. Each batch writes its records and its checkpoint into the
partition's column family in one write, so a crash can lose a tail of batches
but never leave the checkpoint ahead of the data — and Kafka replays the tail.
A revoked partition's column family is kept for a grace period (a partition
that comes straight back replays only its tail) and then dropped. Memory is the
block cache and memtables, shared across column families, and the timer index.

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

## Configuration

`[servers.server_kafka]`:

| key | default | |
|---|---|---|
| `brokers` | unset | `bootstrap.servers`. Unset: one node over an in-process log, nothing durable. |
| `partitions` | 64 | Fixed for the life of the deployment. |
| `topic_prefix` | `resonate` | |
| `group_id`, `txn_prefix` | the topic prefix | |
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
| `search_enabled` | false | Searches read every record of every partition. |
| `librdkafka` | `{}` | Extra client properties (SASL, TLS, tuning). |

The plugin is not in the `resonate` binary's registry: it builds librdkafka and
RocksDB from source. A binary that wants it names it, as the top-level README
shows for any plugin.

## Tests

```
cargo test -p resonate-server-kafka                       # unit, cluster, crash, differential
TEST_KAFKA_BROKERS=localhost:9092 \
  cargo test -p resonate-server-kafka -- --test-threads=1 # + fencing, restart, two nodes, differential on Kafka
```

- `tests/differential.rs` — the blob backend's differential, against this node.
- `tests/cluster.rs` — several nodes on one in-process log: routing, leave,
  a zombie that cannot commit, restore from a compacted log, refused and
  uncertain commits.
- `tests/crash.rs` — a child process aborts mid-write with the WAL off; the
  surviving checkpoint names only records that survived.
- `tests/live.rs` — real Kafka: fencing, restart from RocksDB, two nodes with a
  real consumer group and HTTP forwarding.

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
