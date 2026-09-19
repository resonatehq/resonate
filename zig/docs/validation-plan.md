# How this implementation is validated

Four checks. They answer different questions, and none of them substitutes for
another.

| | asks | answers with |
|---|---|---|
| `zig build test` | does each part do what it says | 210 unit tests |
| `simulator run` / `soak` | is the concurrency sound | a linearizability search over a simulated run |
| `differ` | is this the protocol everybody else implements | another server, request for request |
| `simulator check` | was *that* run sound | a recorded history from a real server |

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

With `--no-check` a run costs milliseconds instead of seconds, because the search
is the whole cost. That buys a *wide* sweep — thousands of seeds, more servers,
more clients, heavier faults — over exactly the properties above:

```
zig-out/bin/simulator soak --runs 3000 --servers 4 --clients 6 --operations 400 \
    --conflict 20 --reorder 40 --unavailable 5 --lost-ack 5 --crash 2 --no-check
```

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

A failing run prints the command that reproduces it, with every knob. `--dump
<file>` writes the history, and `--verbose` prints the crashes, the coverage,
and — when every answer is explicable but the state is not — the state the order
leaves against the state that is there.

### The one path debug mode cannot reach

Both checks above run with the clock in the caller's hands, which is what makes
them reproducible — and it means neither of them ever sees a server come up, read
the deadlines out of the bucket, and fire one on wall time with nobody asking.
That is the whole durability promise of a timer, so it is checked on its own:

```
zig/tools/deadline-survives-a-restart.sh
```

A stand-in S3, a server with no debug mode, a promise that times out in eight
seconds, the server stopped before the deadline and started again — and then the
deadline fires by itself, the promise resolves at the instant it was due, and the
deadline object is collected. It fails loudly on the first thing that is not true.

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

`examples/conctrace.rs` in the Rust tree records a concurrent history against
any `--url`: several clients, measured overlap, `call`/`return` instants in
nanoseconds, one NDJSON line per operation. The checker reads that file and asks
the same question §2 asks, about a run that really happened over real sockets.

```
cargo build --release --example conctrace
zig-out/bin/resonate serve --debug --store memory --port 8021 &   # or --store s3
./target/release/examples/conctrace --url http://127.0.0.1:8021/ \
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
