# Resonate on an object store, in Zig

A complete Resonate server — the whole protocol, one endpoint, one process —
whose only durable state is objects in a bucket. No database, no log, no
consensus, no locks, no leases, no background compaction. Twenty-two thousand
lines of Zig and nothing outside the standard library.

```
zig build -Doptimize=ReleaseSafe
zig-out/bin/resonate serve --store s3 --endpoint http://127.0.0.1:9000 --bucket resonate
zig-out/bin/resonate serve --store memory --debug --port 8021      # for tests
```

There is no TLS and no authentication here on purpose. Put a proxy in front: it
terminates TLS, authenticates, authorizes, and forwards what is left.

## What it is

One `POST /` taking one envelope:

```json
{"kind":"promise.create","head":{"corrId":"1","version":"2026-04-01"},"data":{...}}
```

Twenty-eight kinds across `promise.*`, `task.*`, `schedule.*` and `debug.*`,
`GET /ready`, `GET /metrics`, and nothing else. The protocol is the Rust tree's,
version for version: the same validation, the same rejection messages, the same
tags that drive the semantics (`resonate:target` makes a promise a task,
`resonate:timer` decides whether a deadline resolves or rejects, `resonate:scope`
and `resonate:external` make a promise awaitable, the lineage tags say what a
promise belongs to).

## How the state fits in a bucket

Three prefixes, and the keys *are* the schema:

| key | holds |
|---|---|
| `wf/<origin>` | one document: every promise and task of one origin |
| `sched/<id>` | one schedule |
| `t/<NN>/<deadline>_<target>@<generation>` | a deadline, as a zero-byte object |

The whole design rests on one property of the protocol: **every operation it
admits is single-origin.** A callback names an awaiter, a fence names a task, a
settle names a promise — and in each case everything the operation has to read
and everything it has to write belongs to one origin. So a document per origin
makes one conditional write enough to commit a whole transition, and two servers
over one bucket need nothing else to agree.

Concurrency control is what S3 already offers: `If-None-Match: *` to create,
`If-Match: <etag>` to replace what was read. A failed precondition means the
decision was made against state that no longer exists, so it is **re-decided**,
never replayed. A read is validated with `If-None-Match: <etag>`, so a cache hit
costs a round trip and no body. What is held in memory is bounded twice, by
document count and by weight (`--cache-entries`, `--cache-bytes`): a count alone
does not bound the memory, because an origin's document grows with every promise
in it.

Deadlines are keys. A zero-padded deadline sorts lexicographically into time
order, so the nearest deadlines are a capped ascending listing; the shard prefix
spreads a monotone key space, which is the one access pattern object stores are
worst at; and the generation names the commit that armed it, so a writer only
ever removes the deadline its own predecessor wrote.

Writes go in an order every crash window survives: **arm the deadline, commit
the document, disarm the old deadline, send the messages, answer.** A crash
between any two of them leaves a recoverable state — an orphan deadline that
fires into a document that has moved on and is collected, or a message that was
not sent and is re-sent by the retry deadline the commit already carries.

One actor per origin, and a request only *enqueues*: the event loop drains, so
everything that arrived in one poll rides one commit and each request still sees
the one before it.

[docs/on-s3.md](docs/on-s3.md) is the argument in full: why one document per
origin is enough, what each of the three failure outcomes of a conditional write
means, what is left at every point a process can stop, and what repairs it.

## The shape of the code

```
src/
  main.zig        the binary: flags, routes, metrics
  server.zig      edge validation, routing, and the composition root
  handle.zig      the state machine: every operation, as a pure function of a document
  doc.zig         the origin document and its canonical encoding
  protocol.zig    states, tags, ids, addresses, envelopes
  applier.zig     the commit loop: load, decide, arm, commit, disarm, retry
  schedules.zig   the schedule service
  timerd.zig      the deadline poller
  scan.zig        the searches and the snapshot
  sender.zig      the outbox
  store.zig       the object store port, the key space, and an in-memory store
  s3.zig          S3 over HTTP: GET, PUT, DELETE, list-type=2, preconditions
  net.zig         HTTP server and client over one ring
  io.zig          io_uring: accept, recv, send, connect, close, timeout
  http.zig        HTTP/1.1 both ways, including chunked
  json.zig        a parser and a canonical writer
  cron.zig        5, 6 and 7 field cron
  env.zig         the environment: clock, timer, message bus
  bus.zig         HTTP push, and nowhere
  sim/            the simulator, the model, the checker, the workload
  differ.zig      this server against another one
  fakes3.zig      a stand-in S3, as a library and as a process
  simulator.zig   run, soak, check
docs/             the design argument, and how it is validated
tools/            the checks that need two processes
```

Three ideas hold it together, all of them TigerBeetle's:

* **Everything I/O is a port.** `store.Store`, `env.Clock`, `env.Timer`,
  `env.MessageBus`. Production is io_uring plus HTTP; the simulator is an
  in-memory store, a clock it owns, and a bus that takes every message and checks
  it against the bucket. The server cannot tell, which is what makes a simulated
  run a real run.
* **No hidden control flow.** No threads, no locks, no async. Every I/O path is
  an explicit callback state machine on one ring, so what can interleave is
  visible in the code.
* **Determinism is a feature.** Documents have one canonical byte form, the
  encoder is ASCII-only and order-fixed, a document that did not change is not
  written, and the simulator replays a seed byte for byte.

## Is it right?

Six checks, described in [docs/validation-plan.md](docs/validation-plan.md):

```
zig build test                                   # 210 unit tests
zig-out/bin/simulator run  --seed 1 --servers 3 --clients 4 --operations 200 \
    --conflict 15 --reorder 30 --unavailable 3 --lost-ack 3
zig-out/bin/simulator soak --runs 200 --crash 2 --unavailable 5 --lost-ack 5
zig-out/bin/differ --a http://127.0.0.1:8021/ --b http://127.0.0.1:8022/
tools/deadline-survives-a-restart.sh
tools/memory-stays-flat.sh
```

The simulator runs several servers over one bucket with faults injected into the
store — no answer, a write that lands and reports failure, two writes it could
not order, completions out of order, servers killed mid-decision — and then asks
a linearizability checker whether the recorded history has a sequential
explanation *in this same state machine*, ending in the state the bucket
actually holds. Everything in the first list below was found that way.

Found by the simulator, in the order they turned up:

1. A cached read served state a concurrent writer had already replaced.
2. Two deletes of one schedule both reported that they had deleted it.
3. A create that lost a race answered "not found" — an answer no sequential
   execution gives.
4. A sweep that fired a schedule and then lost the commit left it un-advanced.
5. A disarm removed a deadline another commit had just armed.
6. A delete that purged a schedule restarted the counter its deadlines are named
   by.
7. A schedule advanced past a run that was never stored.
8. An arm that failed to commit left an object behind under the name the retry
   would use, so collecting the orphan took the live deadline with it — a promise
   that never times out, with nothing anywhere to say so. The simulation now
   checks the invariant that broke, after every operation: every deadline a
   document records has an object of that name.

And two the checker could not have found, which the differential did:

* The outbox keyed every `unblock` to one address the same way, so an address
  listening on two promises was told about whichever settled last. A server that
  loses the same message on both sides of a comparison with itself is still
  linearizable.
* An occurrence reached after its own deadline had passed was offered to a worker
  for work that was already over.

And one that no check could see until there was a check for it: every protocol
request was given an arena of its own that nothing freed, about five kilobytes a
request. `tools/memory-stays-flat.sh` is that check.

## What is not here

* `ui.*` — the console's read model. Refused with a 404, the same as any server
  built without it.
* TLS, authentication, authorization. A proxy's job, and the reason there is no
  flag for them.
* Anything on wall time under `--debug`: the clock belongs to the caller, which
  is what makes a differential and a recorded history comparable at all.
* Retrying the *store*. A bucket that answers 503 SlowDown is a caller told 503,
  which is the honest answer — the request may or may not have been applied, and
  the protocol says so. Backing off inside the store would hide from the caller
  how long its request has been outstanding, and every operation is idempotent,
  so the retry belongs where the deadline is known.

`SIGTERM` stops it gracefully: no new connections, then up to
`--shutdown-timeout` milliseconds for what is in flight. Nothing about that is
needed for safety — every transition is committed before it is answered, so a
server killed outright loses nothing — it is for the caller that was about to be
told something, which during a rolling restart is every request in flight.
