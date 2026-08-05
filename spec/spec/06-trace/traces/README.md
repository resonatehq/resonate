# Recorded runs from a real server

Captured with `capture.py` from `resonatehq/resonate` v0.9.8, release
build, **SQLite** backend:

```bash
RESONATE_DEBUG=true RESONATE_STORE__TYPE=sqlite resonate serve
N=200 OUT=trace.ndjson python3 capture.py
lake exe checktrace trace.ndjson
```

One JSON object per external call — `req` is the `data` the client sent,
`res` is the whole `{kind, head, data}` envelope the server returned. It
is resonate's wire format, so a capture is tee'd traffic, not a shape
invented here.

| file | events | verdict |
|---|---|---|
| `resonate-sqlite-50wf.ndjson` | 550 | ADMISSIBLE, 50 τ recovered, 29 ms |
| `resonate-sqlite-200wf.ndjson` | 2200 | **REFUTED at event 886** |

## What the 550-event run shows

Fifty workflows, every call `200`, and the checker recovers fifty
`τResume` steps the server never reported. resonate discharges the resume
inside `promise.settle`'s transaction, so an observer sees a task go from
`suspended` to `pending` with no request to explain it. The specification
leaves that schedule free, so the checker has to supply the missing steps
itself — which is the whole reason to ask for admissibility rather than
for reproduction.

## What the 2200-event run shows

Refuted at event 886, and the cause is worth recording.

```
880  promise.create o80.x @9000 -> 200  timeoutAt 900000, pending
884  task.suspend   o80.x @9020 -> 200
886  task.get       o80.x @9040 -> 200  state "fulfilled"      <- refuted here
889  promise.get    o80.x @9070 -> 200  "rejected_timedout", settledAt 900000
```

The promise's deadline is 900000 and every observation carries a
`resonate:debug_time` below 20000, yet the server reports it timed out —
stamped at its own deadline, which is what the timeout transition writes.
Under the specification no schedule explains a promise expiring 890
seconds before its deadline, so the trace is refuted.

Exactly two of two hundred workflows are affected, `o80` and `o178`,
about 98 apart, and their log lines are ~0.6 s apart in wall clock. The
server reports `timeout_poll_interval_ms=1000`. So the shape is a
periodic background sweep firing roughly once a second and judging
deadlines against the REAL clock, while the requests are driven by
`resonate:debug_time`. The startup banner says "Debug mode enabled —
background loops paused"; something is evidently not paused, and it
leaves no log line.

**This is a clock-mixing artifact of the harness, not a protocol
violation.** It says a debug-time capture is corrupted at about 1 Hz,
which matters if you intend to build a conformance suite this way — but
it is not evidence that resonate mishandles timeouts under a single
consistent clock. Confirming that needs either a server whose sweep
honours `debug_time`, or a capture driven by wall-clock time throughout.

The trace is kept refuting rather than trimmed to the clean prefix,
because it is the only end-to-end demonstration that the checker refutes
real traffic at the right event.

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
