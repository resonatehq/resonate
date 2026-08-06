# Workflow scenarios

Drives a resonate server through four workflow shapes and records every
request and response, so the traffic can be fed to the checkers in this
repository. Built on the Go SDK; the TypeScript canonical workloads live
in the sibling directory, [`work/ts/`](../ts/README.md).

## Build and run

```bash
cd work/go && go build -o scenarios .

RESONATE_DEBUG=true RESONATE_STORE__TYPE=sqlite resonate serve &

./scenarios simple-run   -runs 20 -parallel 4 -loops 2..5
./scenarios simple-rpc   -runs 20 -parallel 4 -loops 1..3
./scenarios simple-sleep -runs 10 -parallel 2 -sleep 5..50ms
./scenarios fan-out      -runs 10 -parallel 8 -fanout 3..6 -depth 1..2 -contention 0.5
```

Every numeric flag accepts an **interval** — `3`, `2..5`, `5..50ms` —
drawn per invocation. A scenario pinned to one shape produces one shape of
trace, and the original captures were already a single shape
repeated 200 times.

| flag | |
|---|---|
| `-runs` | total workflow invocations |
| `-parallel` | fake clients running concurrently |
| `-contention` | chance `[0,1]` a run reuses another client's origin |
| `-loops` `-fanout` `-sleep` `-depth` | scenario shape, as intervals |
| `-out` | prefix; writes `.ndjson` and `.history` |
| `-debug-time` | stamp `resonate:debug_time` (default on) |

Each run writes two files:

```bash
lake exe checktrace          < trace.ndjson    # the Lean checker
go run ./valid/porc/cmd/lincheck  < trace.ndjson    # sequential
go run ./valid/porc/cmd/conccheck < trace.history   # searches orders
```

## How the recording works

`resonate.Config.Network` accepts any `Network`, and every call the SDK
makes goes through one method:

```go
Send(ctx context.Context, body string) (string, error)
```

So `record.go` decorates `httpnet.HTTPNetwork` and tees the conversation —
no proxy, no packet capture, and no risk of recording something the SDK
did not send. The decorator also stamps `resonate:debug_time` into each
envelope, because the Lean checker needs a monotone clock and a server on
wall clock produces traces whose instants are ~1.7e12 while promises carry
small deadlines. That mismatch is the entire cause of the one REFUTED
capture in the original corpus.

Requests share instants in batches (`-batch`), so concurrent clients can
legitimately overlap: `ValidM` requires non-decreasing, not increasing.

## Status: end to end

All four scenarios, nothing dropped, both checkers accept:

```
simple-run     32 events  DROPPED:0  ADMISSIBLE  /  LINEARIZABLE
simple-rpc     48 events  DROPPED:0  ADMISSIBLE  /  LINEARIZABLE
simple-sleep   52 events  DROPPED:0  ADMISSIBLE  /  LINEARIZABLE
fan-out        40 events  DROPPED:0  ADMISSIBLE  /  LINEARIZABLE
```

Getting there needed four fixes, three of them real bugs:

1. **`task.fence` was in neither checker.** The specification has it
   (`spec/02-abstract/p.lean:229`) and `valid/lean/validator.lean` already knew
   what it touches, but `valid/lean/json.lean` could not decode it and the Go
   model had no handler. The SDK issues one per fenced create/settle — 36
   of 60 events in a fan-out run — so every trace was a partial view.
   Added to both, transcribed guard for guard, validation-first.
2. **`task.create` was in neither model.** The SDK issues one per root
   workflow, so without it nothing else in the trace has a promise to
   refer to and the first event refutes.
3. **`valid/lean/json.lean` decoded `task.create` wrongly.** It read
   `id`/`timeoutAt` off the action ENVELOPE rather than its `data`, and
   threw `property not found: id` on the first real SDK trace. No
   hand-written capture had ever sent a `task.create`.
4. **`task.create` carries no top-level `id`.** The Go decoder left
   `Op.ID` empty, `originOf("")` put every creation in its own partition,
   and the promise was invisible to the partition that read it.

`SCENARIOS_TRACE_KINDS=1` prints every kind the SDK sends, recorded or
not. It is how (1) was found: the runs succeeded and the trace merely
looked thin.

### The one thing still not modelled

`TaskFenceRes.preload`. The wire carries the task's preloaded promises;
`preload` defaults to `[]` in the specification and no handler ever sets
it. Both decoders read it as empty on purpose — comparing it would refute
every fence for a channel the spec does not describe. That is a genuine
gap in the specification, recorded rather than papered over.

The binary works — all four scenarios run against a real server, with
contention, and produce both files. But the SDK's real protocol usage
includes two kinds the checkers cannot read, and they are dropped:

```
recorded 12 protocol events
DROPPED (no checker can decode these): task.create=4 task.fence=24
```

* **`task.fence`** — the big one. The specification has it
  (`spec/02-abstract/p.lean:229`, and `valid/lean/validator.lean:186` already
  knows what it touches) but `valid/lean/json.lean` never learned to decode it,
  and the Go model has no `TaskFence` at all. The SDK issues one for every
  fenced create/settle, so it dominates: 24 of 36 events in a small
  fan-out run.
* **`task.create`** — decodable by the Lean side, absent from the Go model.

Until both are added, a trace from this binary is a partial view and
feeding it to a checker proves little: the checker would be explaining the
events it can see while the ones that actually mutate state are missing.

`SCENARIOS_TRACE_KINDS=1` prints every kind the SDK sends, recorded or
not. That is how the gap was found — the runs succeeded and the trace just
looked thin.
