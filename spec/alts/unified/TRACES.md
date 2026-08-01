# Validating traces from any implementation

The goal: one validator, every implementation. An implementation joins by
writing an **adapter**, not by touching the model.

```
harness NDJSON  --adapters/from_<impl>.py-->  canonical NDJSON  --UTrace.tla-->  TLC
```

`UTrace.tla` is implementation-agnostic. The adapters normalise SHAPE and
ACTION NAMES only; they never invent an action argument, because the
harnesses record none. `UTrace.tla` recovers arguments by existential
quantification, and post-state agreement selects the witness — the technique
resonate-pg's own `Trace.tla` uses.

## Two questions per trace

```bash
./validate-traces.sh impl    # does the model describe the implementation?
./validate-traces.sh spec    # was this recorded execution conformant?
```

The second is the one no per-implementation model could ask: it replays a
real execution against the **specification** and reports whether the
implementation's actual behaviour was conformant on that run.

## Status

| implementation | adapter | traces | under own profile | under spec profile |
|---|---|---|---|---|
| resonate-pg | `adapters/from_pg.py` | 5 (39 transitions) | **all replay** | **all replay** |
| resonate | — | — | — | — |
| resonate-on-convex | — | — | — | — |

**Read the spec-profile result correctly.** That all five replay under the
specification means those five recorded executions were conformant — not that
resonate.sql is. The scenarios are scripted happy paths; the divergences the
model checker found need particular interleavings (a logically dead promise
plus a claim, a halt, or a resume) that these scenarios never produce. Trace
validation would not have found any of resonate-pg's five bugs. The two
techniques are complementary and neither substitutes for the other.

## What a trace constrains

No harness records everything the unified model carries. The adapter declares
the components its implementation observes, and the validator checks exactly
those and no more:

| component | resonate-pg |
|---|---|
| promises (state, timeoutAt, kind) | ✓ |
| tasks (state, version, timerKind) | ✓ |
| task `timerAt` | ✓ **only when the task has a timer** (see below) |
| task `pid` | ✗ — resonate-pg does not track ownership |
| callbacks / listeners / resumes / outbox | ✓ |
| worker claims, delivery stage | ✗ — resonate-pg has neither |
| responses | ✗ — no harness records what an RPC returned |

Stating the set is the point. A validator that silently compares a subset is
indistinguishable from one that compares everything, and worth much less.

### The one deliberate relaxation

`timerAt` is compared only when `timerKind # "none"`. This is a
REPRESENTATION difference, not a behavioural one: resonate-pg has a single
`timeout_at` column that keeps its stale value when a task suspends or halts,
while the unified model names the timer and zeroes the instant. In a state
with no timer the field is undefined on both sides. `timerKind` is compared in
every state, so "this task has no timer" is still checked — only the
meaningless number is skipped.

## Model change this required

One: the `WorkerLayer` switch. `resonate` has a delivery stage between the
outbox and `task.acquire` (the fire-and-forget transport); resonate-pg and
resonate-on-convex do not — acquire is an RPC and the outbox is drained
separately. With `WorkerLayer = FALSE` a worker may acquire against a message
still queued. Every guard in `task.acquire` is unchanged; only the message's
source moves, so `WorkerLayer = TRUE` is exactly the previous behaviour.

## Adding an implementation

1. Write `adapters/from_<impl>.py`: map action names to the unified
   vocabulary, normalise the state shape, and declare `observed`.
2. Add its profile to the `case` in `validate-traces.sh`.
3. Run both questions.

Nothing in `UTrace.tla` changes.
