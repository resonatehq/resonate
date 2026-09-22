# How this implementation is validated

Six checks. They answer different questions, and none of them substitutes for
another.

| | asks | answers with |
|---|---|---|
| `zig build test` | does each part do what it says | 210 unit tests |
| `simulator run` / `soak` | is the concurrency sound | a linearizability search over a simulated run |
| `differ` | is this the protocol everybody else implements | another server, request for request |
| `simulator check` | was *that* run sound | a recorded history from a real server |
| `tools/deadline-survives-a-restart.sh` | does a deadline outlive the process | a server stopped before one and started again |
| `tools/memory-stays-flat.sh` | does answering cost anything to keep | the resident set either side of three thousand reads, and the allocator's own report at exit |

## 1. Unit tests — `zig build test`

Per module, and listed rather than discovered so that adding one is a deliberate
act (`src/tests.zig`): JSON, HTTP both ways, cron, the canonical document codec,
the state machine's transitions, the key space, the object store port, the
commit loop, the schedule service, the deadline poller, the S3 client against a
fake S3 over the real loopback stack — and the checker itself, against histories
that are known-good and known-bad.

## 2. Deterministic simulation — `simulator run --seed N`

TigerBeetle's VOPR, in miniature. One process, one seed, no wall clock and no
sockets: byte-identical runs from the same seed, which is what makes a failure
something to reproduce rather than something to remember.

The simulator owns the environment through the same ports production uses:

* **the clock** — time moves only when the simulator moves it, and every request
  carries the instant it is decided at (`resonate:debug_time`);
* **the object store** — an in-memory S3 with real conditional-write semantics,
  and five knobs: `--unavailable` (no answer), `--lost-ack` (the write lands and
  is reported as failed), `--conflict` (two conditional writes the store could
  not order), `--defer` (held back to complete on a later drain — not a fault,
  but the only way two requests are ever really in flight) and `--reorder`
  (completed out of submission order, which *is* a fault);
* **the servers** — several against one bucket, and `--crash` kills one at an
  arbitrary point, taking its cache, its deadline queue, its actors and every
  decision in flight with it;
* **the message bus** — a recording one. Not a bus that serves nothing: that
  would mean the sender never renders a message, and the run would never
  exercise the one path that turns a committed transition into something a
  worker can read. Every message is checked against the bucket for the property
  it has to have — an `execute` names a task that is really there, an `unblock` a
  promise that is really settled — and counted, so a run that sent none says so.
  Not the version an offer carries: an offer is true when it is made, and by the
  time it is read the task may have been acquired by somebody else, which is what
  the version is for.

### What the search checks

The specification **is this server** over an in-memory store, stepped one
operation at a time (`src/sim/model.zig`). So the question asked is exactly:
*is this concurrent history equivalent to some sequential execution of this same
state machine?* That is linearizability, and it is precisely the class of bug the
concurrent paths can have and the sequential ones cannot — a lost update, a
commit applied twice, a read that saw a write that was later undone. It is
silent about whether the state machine is the right one, which is what §3 is for.

The search is Wing & Gong's, with Lowe's pruning and memoization on
(which operations are placed, which of them happened, the state's hash). Three
things about how it treats a real run:

* **An operation that has no answer may not have happened.** A 503 says the
  request may or may not have been applied, and a caller whose server died was
  told even less. Such an operation is placed for its effect alone, with no
  answer to match, and the search may also leave it out entirely. What still
  holds it in place is *when*: a server answers only once the store has answered
  it, so nothing it started lands after the caller gave up.
* **The state at the end is part of the question.** Two orders can explain the
  same answers and leave different state behind, so checking one particular
  order would fail a correct server. The condition is folded into the search
  instead: an order is accepted only if it explains every answer *and* leaves
  the state a cold server reads out of the bucket — no cache, no deadline queue,
  nothing in memory.
* **The cross-origin reads are excluded.** The searches and `debug.snap` are
  surveys of many objects read one at a time and were never atomic. Excluding
  them is sound because they change nothing; including them would refute a
  correct server. `debug.tick` is included, and issued as a barrier — nothing
  else in flight — which is what makes it one step in the recorded history.

Two things are checked against the bucket rather than against the answers, after
every operation and wherever the store is quiet enough for the answer to mean
something:

* **Every deadline a document records has an object of that name.** No projection
  of the documents shows the difference — the document looks right, and the
  promise simply never settles.
* **Nothing runnable is stranded.** A pending task owes a retry deadline and an
  acquired one owes its lease: those are the only things that ever hand work on.
  A task in either state without one is work nobody will be offered again, and
  every answer about it stays correct forever. Suspended and halted are parked on
  purpose and owe nothing.

Both are invisible to a linearizability search, which is what makes them worth
checking separately — and a run that breaks either fails whatever the search
says, naming what was wrong and the operation it was first wrong after.

Every report says how much overlap there actually was (`concurrency 34 at once,
323 overlapping pairs`) and which operation kinds never succeeded, because a
green result over a sequential run, or over a run that only reached 400s, has
said nothing.

```
zig build -Doptimize=ReleaseSafe
zig-out/bin/simulator run  --seed 1 --servers 3 --clients 4 --operations 200 \
    --conflict 15 --reorder 30 --unavailable 3 --lost-ack 3
zig-out/bin/simulator soak --runs 200 --servers 3 --clients 4 --operations 200 \
    --unavailable 5 --lost-ack 5 --crash 2
```

With `--no-check` a run costs milliseconds instead of seconds, because the search
is the whole cost. That buys a *wide* sweep — thousands of seeds, more servers,
more clients, heavier faults — over exactly the invariants above, and over the
statuses: three thousand runs in twenty seconds.

```
zig-out/bin/simulator soak --runs 3000 --servers 4 --clients 6 --operations 400 \
    --conflict 20 --reorder 40 --unavailable 5 --lost-ack 5 --crash 2 --no-check
```

A search that runs out of steps says so and proves nothing either way, which is
what `--max-steps` trades: lower it and more seeds get looked at, less deeply. A
step that mostly gives up is a step that mostly says nothing, so CI runs ten seeds
to the full depth and three thousand with the search off.

The way to make more of them provable, if that is ever wanted, is to check each
origin's operations as a history of its own, with the sweeps as barriers in all of
them — every operation the protocol admits is single-origin, so the partitions are
real. It is not done here because it is also weaker: a partitioned check cannot
see a violation that involves two origins, and the machinery two origins share —
the cache, the deadline queue, the outbox — is exactly where such a violation
would come from.

A failing run prints the command that reproduces it, with every knob. `--dump
<file>` writes the history, and `--verbose` prints the crashes, the coverage,
and — when every answer is explicable but the state is not — the state the order
leaves against the state that is there.

### A cache that evicts on every commit

The document cache is bounded by entry count and by weight, so a busy server
evicts, and eviction is the sort of thing that is either invisible or a
correctness bug: a document dropped and read again is a cost, a document kept
after another server has committed past it is a stale answer. Which of the two it
is, is a claim about answers, so the search is what settles it:

```
zig-out/bin/simulator soak --runs 200 --servers 3 --clients 4 --operations 200 \
    --conflict 15 --reorder 30 --cache-entries 1 --cache-bytes 64
```

One document, sixty-four bytes: nearly every commit evicts, and the validated
read is the only thing standing between that and a wrong answer. Ten seeds of it
in CI, and the same setting over real sockets is a `conctrace` history that the
checker reads like any other.

"Standing between that and a wrong answer" is a claim about the checker as much
as about the server, so it has a negative control — the one step in CI that fails
when it passes:

```
simulator soak --runs 3 --servers 3 --clients 4 --operations 200 \
    --conflict 15 --reorder 30 --trust-cache
```

`--trust-cache` answers from a cached document without asking the store whether
anyone has moved past it. That is sound where one process writes the bucket and
wrong where three do, and the search refutes it on seed 1 — the same seed that is
linearizable with validation on, under the same tiny cache. A check that cannot
be made to fail is not evidence of anything.

### The one path debug mode cannot reach

Everything above runs with the clock in the caller's hands, which is what makes
it reproducible — and it means none of it ever sees a server come up, read the
deadlines out of the bucket, and fire one on wall time with nobody asking.
That is the whole durability promise of a timer, so it is checked on its own:

```
impl/server/s3/tools/deadline-survives-a-restart.sh
```

A stand-in S3, a server with no debug mode, a promise that times out in eight
seconds, the server stopped before the deadline and started again — and then the
deadline fires by itself, the promise resolves at the instant it was due, and the
deadline object is collected. It fails loudly on the first thing that is not true.

### What none of the others can see

A request that changes nothing must cost nothing to have answered. The simulator
drives the state machine in process, the tests answer a handful of requests and
exit, and a differential compares answers rather than resident memory — so a
server that grows by a few kilobytes a request passes every one of them, and dies
inside a day.

```
impl/server/s3/tools/memory-stays-flat.sh
```

Three thousand reads down one connection, with the resident set measured either
side of them, and then the precise version of the same question: the allocator
reports every allocation still outstanding when the process exits, and `SIGTERM`
is what makes it exit rather than be killed. The resident set is too coarse to see
a small leak; the allocator sees any of them.

It found the defect it was written for: every protocol request was given an arena
of its own that nothing freed, about five kilobytes a request. The same script
fails the build from before that fix — at 7495 bytes a request, and again on the
allocator's report even when the allowance is raised past it — and passes the one
after it at 23 bytes and nothing outstanding.

### The one thing that cannot be checked

A sweep (`debug.tick`) fires everything due at one instant. A store that stops
answering halfway through leaves it having fired *some* of it, and no single
sequential operation means that. The server retries inside the sweep so that
this is rare, the simulation sends an unfinished sweep again — exact, because a
sweep runs as a barrier and nothing can observe the state between two attempts —
and a sweep that still did not finish makes the run **not checked** rather than
checked against something it cannot be. The report says so.

## 3. Differential — `differ --a <url> --b <url>`

One seeded trajectory into two servers over HTTP, comparing what they say. Both
get the same envelope carrying the same instant, so time is an input rather than
a race. After every request the response `data` must agree; then `debug.snap`
must agree — promises, tasks, callbacks, listeners, both timeout tables and the
queued messages — which makes the comparison about the whole state and not only
about the answers.

Nothing is injected: a difference is a difference, and failures in two
independent processes would produce differences that mean nothing.

The peer to compare against is the Rust tree's blob server: it holds its
messages for the snapshot, as this one does under debug, so the whole state is
comparable.

```
zig build -Doptimize=ReleaseSafe
zig-out/bin/resonate serve --debug --store memory --port 8021 &
cargo build --release
./target/release/resonate serve --debug --server-port 8022 --level error \
    --set servers.active=server_blob \
    --set servers.server_blob.search_enabled=true \
    --set servers.server_blob.retry_timeout=30000 &
zig-out/bin/differ --a http://127.0.0.1:8021/ --label-a zig \
                   --b http://127.0.0.1:8022/ --label-b blob \
                   --seed 1 --operations 2000
```

The two settings are not cosmetic: the blob plugin answers no searches unless
told to, and its pending-retry default is 60s where the oracle's, the SQL
engines' and this server's is 30s. A difference is printed with the request,
both answers, and — for a state difference — only the sections of the snapshot
that differ.

A SQL engine works as a peer too, with `--storage-type sqlite` and
`--ignore-messages`, because it *delivers* its messages rather than holding
them: the section then says how far delivery got rather than what the server
decided to send, which is not a comparison of anything.

### Over the S3 path, not only the memory one

`--store memory` and `--store s3` are the same server over two implementations
of one port, and the memory one is not the one that ships. `fakes3` serves a
stand-in S3 — the six operations the port has, with real conditional writes, and
nothing else — so the whole server can be run and checked over the code that
talks HTTP to an object store:

```
zig-out/bin/fakes3 --port 9100 &
zig-out/bin/resonate serve --debug --store s3 \
    --endpoint http://127.0.0.1:9100 --bucket b --port 8031 &
zig-out/bin/differ --a http://127.0.0.1:8031/ --b http://127.0.0.1:8022/ --seed 5 --operations 400
```

The same trajectory, the same answers, the same state: the S3 path is not a
second implementation of anything, and this is what says so.

### The edge, before the trajectory

Every run starts with two dozen requests that are not requests: a body that is
not JSON, an envelope with each of its parts missing in turn, a version nobody
speaks, a `data` that is not an object, an operation nobody has, and operations
the protocol has asked for wrongly. The trajectory only ever sends envelopes the
protocol admits, so without this the part of the surface a client is most likely
to reach by accident is the part nothing compares.

Where the rejection is the protocol's, the words are compared too. Where it is a
*parser's* — "expected ident at line 1 column 2", "invalid type: string, expected
i64", a byte offset — only the status is, because that prose is one library's and
no other implementation can reproduce it or client key off it.

### What it found, and the two places it disagrees on purpose

It found two real bugs here, neither of which the simulator could have found: a
server that loses the same message on both sides of a comparison with itself is
still linearizable.

* The outbox keys an `unblock` on the promise and the address, and the effect the
  state machine emitted never carried the promise id — so one address listening
  on two promises was told about whichever settled last.
* A schedule's occurrence reached after its own deadline had passed was created
  pending and offered to a worker, for work that was already over. Three thousand
  requests in, the only thing the two servers disagreed about was one `execute`
  message this one had no business sending.

Two differences remain, and this server is deliberately on the side it is on:

* **`task.create` on a task that is already fulfilled.** The oracle, the SQLite
  engine and the ScyllaDB engine all answer with the branch preload — "preload
  is branch-scoped, not lifecycle-scoped, so a fulfilled task's siblings are as
  real as an acquired one's". `resonate-server-blob` answers with an empty one,
  citing a line range in a `server.rs` that no longer says that. This server
  does what the three of them do, so a run against the blob server reports this
  difference and nothing else.
* **How far a sweep catches a schedule up.** A tick that crosses several cron
  boundaries fires every occurrence it passed here and in the blob server; the
  SQL engines fire one per sweep. So a run against a SQL engine agrees until a
  tick crosses two boundaries of the workload's 30-second schedules, and then
  diverges by the occurrences the other has not created yet.

## 4. Linearizability of a real history — `simulator check <file>`

`impl/server/core/examples/conctrace.rs` records a concurrent history against
any `--url`: several clients, measured overlap, `call`/`return` instants in
nanoseconds, one NDJSON line per operation. The checker reads that file and asks
the same question §2 asks, about a run that really happened over real sockets.

```
cargo build --release --example conctrace --manifest-path impl/server/core/Cargo.toml
impl/server/s3/zig-out/bin/resonate serve --debug --store memory --port 8021 &
impl/server/core/target/release/examples/conctrace --url http://127.0.0.1:8021/ \
    --out trace --clients 8 --ops 600
zig-out/bin/simulator check trace.history
```

The check refuses a vacuous history — one where nothing succeeded, or where
nothing overlapped — because passing one would mean nothing. A refutation prints
the deepest order it reached, the operation it could not place, and what the
specification would have answered instead.

One thing differs from §2, and it is the recorder's fault rather than the
server's: `check` does **not** require the instants the requests carry to be
non-decreasing. A simulated run's do not go backwards by construction — the clock
moves only on a sweep and a sweep is a barrier — but a recorder stamps an instant
and *then* sends, from several clients at once, so a request carrying an earlier
instant is routinely applied after one carrying a later instant. The server does
not order by it. Insisting on it refutes a correct server, which is what it did
here at 1500 operations and twelve clients before the default was changed.
`--time-order` asks for it anyway, for a recorder that can promise it.

## 5. Somebody else's checker — `tools/against-the-go-checker.sh`

Everything above grades this server against a specification written in this
repository. The simulator's search replays the same state machine the server runs,
and the differential compares against the Rust tree. Agreement there is agreement
with ourselves, which is worth a great deal and is not the same as being right.

So a recorded history also goes to Porcupine, driven by the model in `spec/`,
which nobody working on this directory wrote:

```
cargo build --release --example conctrace --manifest-path impl/server/core/Cargo.toml
impl/server/s3/tools/against-the-go-checker.sh 10
```

The script builds `spec/valid/porc/cmd/conccheck`, starts this server over
`fakes3` so no bucket is needed, records ten histories of a thousand operations at
twelve clients, and checks each. Before any of them it takes a real history,
changes one field of one answer, and requires the checker to refuse it — a checker
that cannot be made to fail is not evidence for anything it accepts.

`conccheck` rather than `lincheck`: `lincheck` asks whether the one order the
recorder wrote down satisfies the model, which on a concurrent run refutes almost
anything, because return order is one legal linearization out of many.
`conccheck` reads the real instants and asks whether *any* consistent order works.
`-partition=false` because upstream's `originOf` splits an id on `.`, so it reads
every `:`-id as its own partition; unpartitioned replays against whole state,
which is the stronger question.

### Their generator too, not only their checker

`conctrace` is the core server's. The specification has its own generator,
`spec/valid/porc/cmd/loadgen`, and the script runs that as well, so a run exists
in which nothing in the loop belongs to this directory but the server.

It was passed over once for two reasons that are no longer true: it opened with
`debug.start`, which is a startup flag here and answers 400, and it built ids like
`c0.a0` where an origin here ends at the first `:`. It now builds `c0:a0`, and its
`post` only reports a transport failure, so the 400 costs it nothing — it falls
back to an instant of its own and carries on.

It reaches states `conctrace` never builds: callbacks, heartbeats, awaits across
origins, sub-origins. It also aims straight at the boundary the whole design rests
on, and is refused:

| what it sends | what the protocol says |
|---|---|
| `resonate:origin` of `c0:sub` | origin must not contain `:` |
| id `f0:a0` tagged `resonate:origin: c0` | id must be prefixed by the origin |
| callback awaiting `f0:a0` from `c0:x0` | awaiter and awaited must share an origin |
| heartbeat over two origins' tasks | all tasks must belong to one origin |

Those are a third of any run, by design, and `resonate-server-blob` refuses the
same ones. At one client the two servers' profiles are identical: 101 answered, 185
refused as malformed, 297 not found, 17 conflicted, out of 600. Porcupine proves
both.

How much state it builds is limited by its own ids rather than by the server. The
workflow index is `i / 6` over a counter every client shares, while the origin is
per client, so at eight clients a given id is touched about once and nothing
accumulates. One client gives depth and no concurrency; eight give concurrency
across origins and little depth. The script runs 1, 2 and 8.

And it requires a floor on how much succeeded, because **a history in which nothing
worked is linearizable for free**. Filtering one of these runs down to its 297
misses and nothing else gives a file Porcupine calls linearizable in a millisecond.
A verdict on such a file is not evidence, so `count-successes.py` counts, and the
check fails below forty.

### What this checker cannot be asked

* **Schedules.** It refuses a history that mentions them: `occurrences` and
  `nextCron` are opaque in the specification, so there is no calendar to check an
  answer against.
* **An outcome nobody knows.** Its model never produces a 503 and its harness
  pairs every operation with a definite response, so there is no way to write down
  "this may or may not have taken effect". A history containing one is refuted for
  that reason alone. `simulator --dump-spec` therefore refuses to write a file
  from a run that used `--unavailable`, `--lost-ack` or `--crash`, and says why.
  Those runs are for `simulator check`, whose own search does model indeterminacy.
* **The surveys.** `promise.search`, `task.search` and `debug.snap` are a listing
  followed by a read of every object it named, each at its own instant, and
  nothing in the protocol ever promised that is a snapshot. The Go model holds
  them to being atomic, so they are left out of a history written for it — the
  same set `§2`'s search leaves out, and for the same reason.

### A third disagreement, and this time not with the Rust tree

`simulator --dump-spec` writes a simulated run in the form this checker reads, so
a fault-injected run across three servers can be put to it as well. Fifty seeds of
`--conflict 15 --reorder 30`: forty-four proved, six refuted. Every one of the six
registers a callback on a promise that had **already settled**, and that turns out
to be a disagreement between the specification and both implementations rather
than a defect in either.

Five requests, sequential, no concurrency at all:

```
promise.create hr:root.t0   (resonate:target, resonate:branch — so it is a task)
promise.create hr:root.p0   (resonate:scope global)
promise.settle hr:root.p0   resolved
promise.register_callback   awaited=hr:root.p0  awaiter=hr:root.t0
task.get       hr:root.t0
```

This server answers `state pending, version 0, resumes 1`. So does
`resonate-server-blob`, byte for byte. The Go model answers `resumes 0` and
refutes the file, from either server: its `ProcessCallback` fires only for
callbacks recorded in `Promise.Callbacks`, and registering on a promise that has
already settled records none — the awaiter is told the outcome in the response
instead, so on that reading there is nothing left to resume it for.

Two implementations agreeing against a model is not proof the model is wrong, and
it is not recorded here as one. It is recorded because it is the whole reason to
run somebody else's checker, and because the check that would have found it
earlier does not exist upstream either: `conctrace` emits nine kinds and none of
them is `register_callback`, so no backend's Porcupine run has ever reached this
state. The live check above is unaffected, since the producer never builds that
request.

## 6. Two servers, one bucket — `tools/two-servers-one-bucket.sh`

Everything the design claims rests on one thing: several servers sharing a bucket
need nothing but a conditional write to agree. No log, no lease, no lock, no
coordination of any kind. §2 exercises that claim hard, and it exercises it inside
one process, driving several server state machines over one in-memory store.

Across processes the situation is materially different. Each server has its own
document cache, its own deadline queue, its own event loop and its own clock
handling, and the only thing joining them is the object store. Nothing above this
section ever ran two of them.

```
cargo build --release --example conctrace --manifest-path impl/server/core/Cargo.toml
impl/server/s3/tools/two-servers-one-bucket.sh 1000 12
```

A stand-in S3, two server processes over one bucket, and a round-robin front
(`two-servers-one-bucket.py`) so a recorded history is a history of the *system*
rather than of a server. The script fails if either server answered nothing, since
that would not have been two servers. Both checkers then read the file: this
directory's search and the specification's Porcupine.

Then the same run again with `--sole-writer` on both, which tells each server it
is the only writer and lets it answer from its cache without revalidating. With
two of them that is a lie, and the script requires *both* checkers to refuse the
result. They do. That is the negative control for the whole section: a
cross-process check that could not detect cross-process staleness would be
evidence of nothing.

```
── two servers over one bucket, revalidating their caches
  501 requests to one, 501 to the other
  this repository's search: linearizable
  the specification's Porcupine: LINEARIZABLE
── the same two, each told it is the only writer, which is a lie
  501 requests to one, 501 to the other
  this repository's search: refuted
  the specification's Porcupine: NOT LINEARIZABLE
```

### What is still not checked anywhere

One thing, and it is the largest: **no bucket has ever been a real one.** Every
check here runs against `fakes3`, which implements the six operations the store
port needs with real conditional writes — and which I wrote, from the same reading
of the same documentation as the client. If that reading is wrong about S3, both
sides are wrong together and nothing here would notice.

The Rust tree has the test this needs, in `impl/server/core/crates/resonate-server-blob/tests/live.rs`,
skipped unless `TEST_S3_BUCKET` is set, and its own comment says why it exists: a
store that accepts `If-Match` and ignores it "would pass every other test in the
repository and lose writes in production". It names MinIO, B2 and Spaces as stores
that do exactly that. The equivalent for this server does not exist yet, and until
someone points it at a real bucket the claim "this works on S3" rests on the
documentation rather than on evidence.
