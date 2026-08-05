# Recorded runs from a real server

Captured with `capture.py` from `resonatehq/resonate` v0.9.8, release
build, **SQLite** backend:

```bash
RESONATE_DEBUG=true RESONATE_STORE__TYPE=sqlite resonate serve
N=200 OUT=trace.ndjson python3 capture.py
lake exe checktrace < trace.ndjson
```

One JSON object per external call — `req` is the `data` the client sent,
`res` is the whole `{kind, head, data}` envelope the server returned. It
is resonate's wire format, so a capture is tee'd traffic, not a shape
invented here.

| file | events | verdict |
|---|---|---|
| `resonate-sqlite-50wf.ndjson` | 550 | ADMISSIBLE, 50 τ recovered, 29 ms |
| `resonate-sqlite-200wf.ndjson` | 2200 | **REFUTED at event 886** (capture error, see below) |
| `resonate-sqlite-200wf-debugstart.ndjson` | 2200 | ADMISSIBLE, 200 τ recovered, 1986 ms |

## What the 550-event run shows

Fifty workflows, every call `200`, and the checker recovers fifty
`τResume` steps the server never reported. resonate discharges the resume
inside `promise.settle`'s transaction, so an observer sees a task go from
`suspended` to `pending` with no request to explain it. The specification
leaves that schedule free, so the checker has to supply the missing steps
itself — which is the whole reason to ask for admissibility rather than
for reproduction.

## What the 2200-event run shows — and the cause, verified in the source

`resonate-sqlite-200wf.ndjson` is REFUTED at event 886:

```
880  promise.create o80.x @9000 -> 200  timeoutAt 900000, pending
886  task.get       o80.x @9040 -> 200  state "fulfilled"      <- refuted here
889  promise.get    o80.x @9070 -> 200  "rejected_timedout", settledAt 900000
```

The promise's deadline is 900000 and every observation carries a
`resonate:debug_time` below 20000, yet the server reports it timed out,
stamped at its own deadline. No schedule explains that.

The cause is a **capture error**, and it is worth stating exactly,
because the first guess ("the banner says loops are paused, so something
is leaking") was wrong about the mechanism:

```rust
// server.rs:54     — starts FALSE
debug_mode: AtomicBool::new(false),
// server.rs:450    — ONLY this sets it
"debug.start" => state.debug_mode.store(true, Ordering::SeqCst),

// processing_timeouts.rs:30
if state.debug_mode.load(SeqCst) { continue; }
let now = util::system_time_ms();   // :34 — WALL CLOCK
```

`RESONATE_DEBUG=true` enables debug OPERATIONS (without it they answer
`403`) and lets `resonate:debug_time` through `resolve_time`. It pauses
nothing. The startup banner — "Debug mode enabled — background loops
paused" — prints unconditionally from `main.rs:140` and is simply
misleading: pausing requires a `debug.start` call, which emits its own
`"Debug mode started — background loops paused"` log line.

The first capture never sent `debug.start`. So the timeout loop ran
throughout at `poll_interval` = 1000 ms, judging deadlines against
`system_time_ms()` ≈ 1.75e12 while promises carried `timeout_at` = 900000
— expiring whatever was pending when it happened to fire. Two of two
hundred workflows were caught, ~0.6 s apart in wall clock, which is the
1 Hz signature.

`resonate-sqlite-200wf-debugstart.ndjson` is the same workload with
`debug.start` sent first:

```
loaded 2200 events
ADMISSIBLE   events=2200 maxFanout=1 witnessTaus=200   1986ms
```

Clean. So resonate is NOT mishandling timeouts under a single consistent
clock; the refutation was mixed clocks, caused by the harness. Both files
are kept — the refuting one because it is the only end-to-end evidence
that the checker refutes real traffic at the right event, and the clean
one because it is the actual conformance result.

**If you build a suite this way, send `debug.start`.** The banner will
tell you loops are paused before they are.

## Two places resonate is stricter than the specification

Found because the first capture attempts were rejected, not by the
checker:

* `task.suspend` requires awaiter and awaited to share `resonate:origin`
  — `400 "Awaiter and awaited must belong to the same origin"`.
* `promise.create` requires the id to equal `resonate:origin` or start
  with `origin ++ "."` — `400 "Promise ID must be prefixed by
  resonate:origin"`.

Neither rule is in `01-protocol/validation.lean`. Both are extra `400`s,
so they never appear in an accepted trace — a suite that only replays
accepted traffic is structurally unable to see them.
