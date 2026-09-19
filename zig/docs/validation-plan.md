# How this implementation is validated

Three independent checks, from cheapest to strongest.

## 1. Unit tests — `zig build test`

Per-module tests: JSON, HTTP, cron, the binary codec, the state machine's
transition tables, the object-store log protocol, and the linearizability
checker itself (checked against histories that are known-good and known-bad).

## 2. Deterministic simulation — `zig build simulate -- --seed N`

TigerBeetle's VOPR, in miniature. One process, one seed, no wall clock and no
sockets. The simulator owns:

* **the clock** — time only moves when the simulator moves it;
* **the object store** — an in-memory S3 with real conditional-write semantics,
  plus injected latency, 500s, 503s and slow writes that land *after* the
  caller gave up;
* **the message bus** — inbound requests and outbound pushes, with drops,
  duplicates, reordering and partitions;
* **the servers** — several of them against one bucket, restarted and crashed
  at arbitrary points.

After every run it asserts:

* every acknowledged request appears exactly once in the log, in an order
  consistent with the real-time order of the requests (this is the
  linearizability check, done directly against the log rather than by search);
* replaying the log from a cold start reproduces the state byte for byte;
* the state after recovery equals the state before the crash, for every
  committed prefix.

## 3. Differential against the reference model

The Rust tree ships an executable model of the protocol
(`crates/resonate-oracle`) and a generator that drives it —
`crates/resonate-server-blob/tests/differential.rs`. That harness is written
against the `ResonateServer` port, whose only method is
`process(&RequestEnvelope) -> Result<ResponseEnvelope, Unavailable>`, so an
adapter that POSTs the envelope to a URL plugs this server in beside the model:

* every request goes to both at the same logical instant
  (`head["resonate:debug_time"]`);
* statuses must match, response `data` must match, and the whole `debug.snap`
  — promises, tasks, callbacks, listeners, both timeout tables and the queued
  messages — must match after every single step.

`tools/differential/` holds that adapter and the harness copy. It needs this
server started with `--debug`, which is what makes `debug.reset`,
`debug.snap` and `debug.tick` answer and what stops anything running on wall
time.

## 4. Linearizability of a concurrent history

`examples/conctrace.rs` in the Rust tree records a real concurrent history
against any `--url`: eight clients, measured overlap, `call`/`return` instants
in nanoseconds, one NDJSON line per operation. The checker in
`src/sim/checker.zig` reads that file and asks whether **any** total order
consistent with those intervals satisfies the model — Wing & Gong's search
with Porcupine's pruning. A refutation is a real violation; a pass is limited
to the history it was given.

```
cargo build --release --example conctrace          # in the Rust tree
zig-out/bin/resonate serve --debug --port 8021 --store memory &
./target/release/examples/conctrace --url http://127.0.0.1:8021/ \
    --out trace --clients 8 --ops 600
zig-out/bin/simulator check trace.history
```
