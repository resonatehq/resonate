# How this implementation is validated

Four checks. They answer different questions, and none of them substitutes for
another.

| | asks | answers with |
|---|---|---|
| `zig build test` | does each part do what it says | 206 unit tests |
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
* **the message bus** — nowhere, because a message is not part of what a caller
  observes. Under debug the server holds them and `debug.snap` shows them, which
  is where they are compared.

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

```
zig-out/bin/resonate serve --debug --store memory --port 8021 &
RESONATE_DEBUG=true cargo run --release -- serve &      # the reference, on 8022
zig-out/bin/differ --a http://127.0.0.1:8021/ --label-a zig \
                   --b http://127.0.0.1:8022/ --label-b rust \
                   --seed 1 --operations 500
```

A difference is printed with the request, both answers, and — for a state
difference — only the sections of the snapshot that differ.

## 4. Linearizability of a real history — `simulator check <file>`

`examples/conctrace.rs` in the Rust tree records a concurrent history against
any `--url`: several clients, measured overlap, `call`/`return` instants in
nanoseconds, one NDJSON line per operation. The checker reads that file and asks
the same question §2 asks, about a run that really happened over real sockets.

```
cargo build --release --example conctrace
zig-out/bin/resonate serve --debug --store memory --port 8021 &
./target/release/examples/conctrace --url http://127.0.0.1:8021/ \
    --out trace --clients 8 --ops 600
zig-out/bin/simulator check trace.history
```

The check refuses a vacuous history — one where nothing succeeded, or where
nothing overlapped — because passing one would mean nothing. A refutation prints
the deepest order it reached, the operation it could not place, and what the
specification would have answered instead.
