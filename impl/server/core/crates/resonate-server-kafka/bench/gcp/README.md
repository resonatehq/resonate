# The Kafka backend on GCP

Scripts that stand up a benchmark fleet in one GCP region, drive
`examples/load.rs` against it, inject failures, and tear it all down — and
the results of the first session (phase 1, 2026-10-02, commit `0f457a48`).

## Fleet

| role | count | machine | notes |
|---|---|---|---|
| broker | 3 | n2-standard-4, 500 GB pd-ssd | Apache Kafka 3.9.1, KRaft (broker+controller), one per zone (us-central1-a, -b, -f), RF 3, `min.insync.replicas=2` |
| node | 3 or 6 | n2-standard-4, local NVMe SSD | `examples/server` (Kafka plugin, HTTP push, gateway, metrics), RocksDB on the SSD, 128 partitions |
| loadgen | 2 | n2-standard-8 | `examples/load`, which is also the push worker |

No load balancer: each load generator round-robins over the node gateways, so
about (N−1)/N of requests are forwarded once, as behind a plain L4 balancer.

```
./up.sh bucket && ./up.sh builder && ./build.sh   # build at HEAD, publish to GCS
./up.sh brokers && ./up.sh nodes 6 && ./up.sh loadgens 2
TOPIC=t1 ./run.sh start 3                          # fresh local copies, topic prefix t1
NODES=3 ./run.sh load NAME 2 --mode workflow --rate 600 --seconds 120 --retry-ms 30000
./fail.sh NAME 35 "kill-node 2" 65 "restart-node 2" -- --mode workflow --rate 600 --seconds 120
./down.sh                                          # every instance; ./down.sh all also drops the bucket
```

`results/` holds every run: each load generator's full output and per-second
timeline, a combined `summary.txt`, and for failure runs the takeover metrics
of every node before and after.

## Workloads

- **lifecycle** — `promise.create` then `promise.settle` on a fresh origin: 2 requests.
- **workflow** — `task.create`, 3 local steps (fenced create + settle), a fenced
  remote step targeted at the push worker, `task.suspend`; the worker acquires
  and fulfills the remote step, then the resumed root: 13 requests per workflow,
  9 from the client and 4 from the worker.

Open loop (`--rate`), each operation measured from when it was due. Every
acknowledged write is journalled and read back after the run.

## Results

**Correctness: 11,355,872 acknowledged promises read back across all runs —
including node kills, broker kills, a zone partition and a node cut off from
the brokers — with none missing and none less settled than acknowledged.**

### Capacity and scaling (lifecycle, 256-byte payloads)

On clean brokers (no idle topics), 30 s per point:

| nodes | ops/s | req/s | p50 | p99 | node CPU | requests/round |
|---|---|---|---|---|---|---|
| 3 | 10,000 | 20,000 | 21 ms | 84 ms | 82% | 1.3 |
| 3 | 12,500 | 25,000 | 1.5 s | 2.9 s | 89% | 44 |
| 6 | 10,000 | 20,000 | 21 ms | 49 ms | 47% | 1.3 |
| 6 | 15,000 | 30,000 | 25 ms | 81 ms | 63% | 1.8 |
| 6 | 20,000 | 40,000 | 55 ms | 566 ms | 91% | 3.0 |
| 6 | 25,000 | 50,000 | 0.9 s | 2.0 s | 90% | 54 |

- **Nodes are the bottleneck, not Kafka.** Node CPU reaches ~90% while
  brokers stay lightly loaded. Doubling the nodes gives ~1.5× the rate at p99 < 100 ms and ~1.8× the saturation
  throughput; total CPU for the same rate is ~14% higher at 6 nodes, about the
  cost of forwarding 5/6 of requests instead of 2/3.
- **Where node CPU goes** (perf, 6 nodes at 30k req/s): ~63% tokio workers
  (HTTP, kernel, codec), ~36% librdkafka broker threads —
  `rd_kafka_toppar_producer_serve` alone is 14% of the process. One producer
  per partition with `linger.ms=0` wakes a broker thread per record, and each
  wakeup walks the producer's partitions.
- **Commit** (one produce, `acks=all` across three zones) is 4–6 ms mean when
  unsaturated: the floor under every request.
- **Workflows** (3 nodes): 1,000 wf/s at p99 163 ms, 1,250 wf/s at p99 267 ms
  (~16k server req/s); 1,500 wf/s saturates.
- **Past capacity, latency is unbounded.** Nothing sheds load: queue wait and
  commit grow (rounds of 50+ requests, commits of 400 ms on a CPU-starved node)
  until clients time out. No 503s are returned for overload.
- An earlier 6-node run on brokers carrying ~9 idle topic pairs (1,150+ idle
  partitions per broker) had commit times 2–3× higher and broker CPU ~55%:
  idle partitions are not free.

### Failures (3 nodes, workflows at 600/s ≈ 50% of capacity)

Clients retry 503s and transport errors for 30 s at the next gateway; an
operation fails only after that. Times are from the loadgen timelines.

| fault | failed ops | worst p99 | degraded (p50 > 1 s) | lost |
|---|---|---|---|---|
| kill -9 a node, restart 30 s later (fresh topic) | 11 | 23 s | ~20 s, then ~10 s again on rejoin | 0 |
| kill -9 a node, after ~10 min of load | 1,828 | 52 s | ~50 s | 0 |
| rolling restart, 3 nodes, 20 s apart | 0 | 18 s | ~15 s per node | 0 |
| kill a broker, restart 45 s later | 2,239 | 31 s | ~20 s, **5 partitions unserved ~4 min** | 0 |
| same, `topic.metadata.refresh.interval.ms=10000` | 122 | 31 s | ~17 s | 0 |
| isolate a zone (node + broker) 45 s, clients round-robin all gateways | 2,789 | 80 s | ~90 s | 0 |
| same, clients avoid the isolated gateway | 119 | 48 s | ~90 s | 0 |
| cut a node off from the brokers 45 s | 399 | 22 s | ~25 s, then ~15 s on heal | 0 |

What they show:

1. **Takeover time grows with the log, not with recent work.** A survivor
   has no local copy of a partition it never owned, so it replays the
   partition's log from the start. Ids are fresh per workflow, so compaction
   removes almost nothing (and cannot start for `min.compaction.lag.ms`, 1 h).
   After ~10 min of load each survivor replayed 1.25 M records and takeovers
   averaged 24 s; on a fresh topic, 50 k records and 3 s. Without retention of
   completed promises, failover time grows without bound. A node returning
   with its RocksDB copy replays only the tail (~1 s per partition).
2. **A stale metadata cache can strand partitions for minutes.** After a
   broker kill, takeovers of 5 partitions failed with `Meta data fetch error:
   UnknownPartition` for ~4 minutes though Kafka showed every partition led
   and in sync. The node reads high watermarks through one long-lived,
   never-polled consumer (`probe` in `log/kafka.rs`) whose metadata only
   refreshes every `topic.metadata.refresh.interval.ms` (librdkafka default
   5 min). Setting it to 10 s through the plugin's `librdkafka` table removed
   the stranding; refreshing metadata on that error, or a lower default, would
   fix it in the code.
3. **A broker loss stalls most of the cluster, not a third of it.** Even with
   the fix, p50 rose to ~13 s right after a broker kill. Kafka needs ~9 s
   (`broker.session.timeout.ms`) to elect new leaders, and the routing
   directory comes from the group coordinator: while it moves, nodes answer
   "no owner is known yet" for every forwarded request.
4. **Planned operations are not free.** A graceful restart moves a node's
   partitions twice (away and back), each move a pause; a rolling restart of
   three nodes gave three ~15 s windows with p99 up to 18 s.
5. **The client side matters.** An unreachable gateway that hangs (rather
   than refuses) costs a 10 s timeout per attempt; a health-checked balancer
   removed 95% of the failures in the zone test. A real SDK would also back off
   rather than retry every 50 ms as this generator does.

### Caveats

- One session, one run per point; capacity points varied by ±30% between
  sessions (3 nodes at 10k ops/s: p99 50 ms in one, 84 ms in another).
- Broker CPU readings varied between sessions; brokers were never above ~55%.
- The load generator's retries are not idempotent: a retried `task.create` or
  `task.suspend` whose first attempt landed gets a 409 and counts as failed.
  Those are not lost writes — the read-back confirms it.
- No Managed Prometheus; the breakdown is from scraping `/metrics` before and
  after each run. The server breakdown is meaningless for runs where a node
  restarted (its counters reset).
- The zone test isolates with iptables (drop, except ssh) rather than stopping
  the zone, so isolated processes keep running as zombies — the harder case.

## Not yet run

Phase 1 left out: the 12-node point, Managed Kafka, payload and skew
variants, timers, the 24 h soak, and the Postgres/GCS comparison.
