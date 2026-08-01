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
| resonate-on-convex | `adapters/from_convex.py` | 5 (32 transitions) | **all replay** | **2 of 5 REFUSED** |
| resonate | — | — | — | — |

## The two convex traces the specification refuses

`convex-normal` and `convex-timeout` replay perfectly under convex's own
profile and are then **refused by the specification** — at `task.suspend`,
with no enabled action at all rather than a state mismatch.

The reason is exact. Both traces create the awaited promise with
`CreatePlain` — no `resonate:target`, no timer — and then suspend a task on
it:

```
CreateRooted p1      kind = target
CreatePlain  p2      kind = plain      <- internal
Suspend      p1 on p2                  <- callback <<p2, p1>> registered
```

The specification refuses that with `422`: external-only waiters
(`6ddfab7`). Only external promises carry an armed timeout, so only they can
discharge an obligation.

**This is a divergence the model-checking profiles did not report**, and the
reason is worth recording. `MC_convex.cfg` sets
`CallbackExternalGuard = FALSE` — the profile ENCODES convex's behaviour as
expected, so no property fires. And `ObligationsAreDischargeable` does not
fire either, because convex arms a timeout for *every* promise
(`ArmPolicy = "all"`), so the obligation really is dischargeable there. The
defect is invisible to the state properties and visible only when a real
execution is measured against the specification.

That is the argument for this whole apparatus in one example: replaying real
traces against the spec asks a question no per-implementation model asks,
because a per-implementation model has that implementation's choices baked
into its profile.

Note what it does NOT say: convex is not stranding anyone. Under
`ArmPolicy = "all"` the awaiter is woken. The finding is a protocol-conformance
divergence, not a liveness bug.

**Read the spec-profile result correctly.** That all five resonate-pg traces
replay under the specification means those five recorded executions were
conformant — not that resonate.sql is. The scenarios are scripted happy paths; the divergences the
model checker found need particular interleavings (a logically dead promise
plus a claim, a halt, or a resume) that these scenarios never produce. Trace
validation would not have found any of resonate-pg's five bugs. The two
techniques are complementary and neither substitutes for the other.

## What a trace constrains

No harness records everything the unified model carries. The adapter declares
the components its implementation observes, and the validator checks exactly
those and no more:

| component | resonate-pg | resonate-on-convex |
|---|---|---|
| promises (state, timeoutAt, kind) | ✓ | ✓ |
| tasks (state, version, timerKind) | ✓ | ✓ |
| task `timerAt` | ✓ only when the task has a timer | ✓ same |
| task `ttl` | ✗ | ✓ |
| task `pid` | ✗ | ✗ (recorded, not yet compared) |
| callbacks / resumes / outbox | ✓ | ✓ |
| listeners | ✓ | ✗ — convex has none |
| worker claims, delivery stage | ✗ | ✗ |
| responses | ✗ — no harness records what an RPC returned | ✗ |

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

## Model changes this required

Trace validation found three fidelity gaps in the unified model. All three are
places where the model was *sloppier than the specification*, which is what
this phase is for.

1. **Per-task `ttl`.** The model had a single `Ttl` constant; convex's `crash`
   scenario acquires with a short lease (300ms) and then a long one (60s), and
   could not be replayed. The specification puts `ttl` on `TaskRecord` — the
   model was less faithful than the spec. Now `TTLs` is the set a worker may
   present and the task carries its own; a heartbeat refreshes by it. Model
   configs set `TTLs = {Ttl}`, so the state space is unchanged.
2. **Clearing `ttl` when the lease ends.** Suspend, release, halt, lease
   expiry and settlement all set `ttl := NULL` in the specification; the model
   left it stale. Four convex traces caught this.
3. **`TNoOp` did not constrain `now`.** A genuine unsoundness in the
   validator, not the model: TLC reported the successor state as incompletely
   specified.

And one switch: `WorkerLayer`. `resonate` has a delivery stage between the
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

Two things the convex adapter had to handle that a new one probably will too:

* **No-op events.** Convex's deadline jobs fire whenever the scheduler runs
  them and let the handler decide, so the harness records genuine no-ops. The
  model's rules are guarded and cannot fire when not due. An event whose
  normalised state equals its predecessor's is emitted as `NoOp` and replays
  as a stutter that still checks the state.
* **Workers.** The `Workers` set comes from the trace's own `pids` when the
  harness names them. The convex `crash` scenario needs the second worker:
  the first died holding its claim and nothing in the recorded trace frees
  it.

Nothing in `UTrace.tla` changes.
