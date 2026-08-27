# A linearizability checker for the abstract machine

A Go port of `spec/02-abstract`, wired up as a nondeterministic model for
[porcupine](https://github.com/anishathalye/porcupine), checked against
both read disciplines of the machine.

```
go run ./cmd/lincheck < trace.ndjson
```

```
loaded 550 events   history: sequential   partition: true

-p (projected)     LINEARIZABLE  45ms
     witness: 50 internal rule firings the server never reported
       R4 resume o0.a -> o0.x
       R4 resume o1.a -> o1.x
       …
-m (materialized)  LINEARIZABLE  49ms
     witness: 50 internal rule firings the server never reported
       …
```

Exit `0` both linearize, `1` one or both do not, `2` usage or parse
error, `3` inconclusive (a rule closure hit its fuel bound).

## Why bother, given `valid/lean/` already checks traces

Because it is an **independent implementation**. Different language,
different data structures, and — critically — a search written by someone
else: porcupine's `NondeterministicModel.ToModel()` does the power-set
construction, not code from this repository. Agreement between the two is
evidence that the specification says what we think, rather than that one
program is self-consistent.

The agreement is not vague. On the mixed-clock capture both checkers
refute, and both name **event 886**. `TestBadCaptureRefutedAtSameEvent`
pins that number, so if either implementation drifts the test says so.

## Two models, and why they must agree

`spec/02-abstract` has two read disciplines:

| | reads | writes facts |
|---|---|---|
| **-p** `p.lean` | serve the projection | never — the rules persist facts later |
| **-m** `m.lean` | materialize on touch | on every read |

They are ported as ONE handler set taking a `Discipline`, because the two
Lean files are **the same code** modulo `viewPromise`/`viewTask` versus
`touchPromise`/`touchTask`. That was verified by diffing them with the
names normalised, not assumed from the comments.

That the two disciplines return the same verdict is a **theorem**, not an
observation: `responseLockstepAbstract` in `spec/04-theorems/proof.lean`
is an unconditional mechanized proof — no `sorry`, standard axioms — that
they answer identically on the response channel.

**But be careful how much that buys here.** Because both disciplines are
one code path with a flag, they differ in exactly two `if` statements:

```go
q := p.Project(now)
if d == Materialized && q.State != p.State { s.SetPromise(q) }   // readPromise
u := t.View(p)
if d == Materialized && u != t           { s.SetTask(u) }        // readTask
```

So running both can only catch bugs in those two lines. A bug in the
shared 99% appears identically under both and the agreement test sails
past it. It is a real check on the discipline plumbing, and NOT the
independent confirmation the phrase "both models agree" suggests.

## Where the nondeterminism comes from

The abstract machine has **no deferred queue**. `promiseSettle` writes the
promise and nothing else; a settled promise keeps its callbacks and
listeners until the batch rules drain them:

```go
// PromiseSettle
q.State = st
q.SettledAt = u64p(now)
// The promise ONLY. The task is fulfilled by fact T; the awaiters and
// listeners stay on the promise for the batch rules.
s.SetPromise(q)
```

Nothing an observer can see says whether `R4 resume` has fired yet. So
`Step` returns **every** state consistent with some drain schedule:

```go
cands, ok := closure([]candidate{{ms.state, ms.witness}}, op.Now, Fuel)
for _, c := range cands {
    next := c.state.clone()
    if !matches(op.apply(next, d), want) { continue }   // ← the pruning
    out = append(out, modelState{next, c.witness})
}
```

`matches` against the recorded response is what keeps that set small: on
a real capture the fanout collapses to one almost immediately.

The rules ported are R1–R6 from `rules.lean`. **R7 `scheduleFire` is
absent** — `nextCron` and `occurrences` are `opaque` in the Lean with no
value, so there is nothing to port, and the loader refuses a trace
mentioning schedules rather than checking it against an empty calendar.
The Lean checker declines the same traces for the same reason
(`valid/lean/schedules.lean`).

## Partitioning, and the property it rests on

Without partitioning the check is exponential in the history — 11 events
1 ms, 22 events 88 ms, 33 events 7.4 s, 44 events does not finish in 30 s.
With partitioning, 2 200 events take 145 ms.

But partitioning by object is **not sound in general** in this
machine: `task.suspend x awaiting a` links two objects, and settling `a`
wakes `x`.

What rescues it is that an awaiter and its awaited must share
`resonate:origin`. That used to be a rule of resonate's rather than of
the specification's; it is the specification's now —
`promiseRegisterCallback` and `taskSuspend` answer `400` to a
cross-origin registration, `taskFence` to a cross-origin target, and
`well_formed_promise_callbacks_same_origin` says no reachable state
holds such an edge however it got there. Ids are `origin:suffix`, so
partitioning on the origin keeps every awaits-edge inside one partition.

The door is lexical — comparing two origins reads nothing — so the
refusal costs no lookup, which is what lets a backend answer before the
request reaches its store.

That is still a hypothesis about the INPUT, and a trace is input, so it
remains **checked, not assumed**:

```go
if err := model.CheckPartitionable(ops); err != nil {
    // refuse to partition rather than partition unsoundly
}
```

and `-partition=false` turns it off. `TestPartitioningAgrees` confirms the
verdicts match with and without.

## Concurrent traffic from a real server — `cmd/loadgen`, `cmd/conccheck`

```
RESONATE_DEBUG=true RESONATE_STORE__TYPE=sqlite resonate serve &
go run ./cmd/loadgen -clients 8 -ops 400 -out run
go run ./cmd/conccheck < run.history       # porcupine, real intervals
../.lake/build/bin/checktrace < run.ndjson # Lean, one fixed order
```

Against `resonatehq/resonate` v0.9.8 (commit `c8d7c7b`), release build,
SQLite, `debug.start` sent:

```
400 events (0 transport errors) from 8 clients
  overlapping pairs: 2751   max concurrency: 32
```

**The clock is the hard part.** The Lean checker requires a monotone
clock, and a concurrent run has no single instant per operation. Every
request carries an explicit `resonate:debug_time` from a counter that only
advances, and operations in a batch SHARE an instant — ties are legal,
`ValidM` says non-decreasing. The file is then ordered by return time,
which respects the real-time partial order and is therefore a legal
linearization candidate — but only ONE of them.

That is the whole difference between the two tools:

| | question |
|---|---|
| Lean `checktrace` | does some schedule explain THIS ORDER |
| porcupine `conccheck` | does some schedule explain SOME order consistent with the intervals |

So `porcupine LINEARIZABLE + Lean REFUTED` would mean the server is fine
and the harness guessed the order wrong. `porcupine NOT LINEARIZABLE` is
the only one of the two that is a claim about the server.

### Results

```
Lean,      400 events, return order    ADMISSIBLE  43ms  0 hidden steps
porcupine,  40 events, real intervals  LINEARIZABLE both disciplines    1ms
porcupine,  80 events                  LINEARIZABLE both disciplines  126ms
porcupine, 160 events                  TIMEOUT (see below)
porcupine, 400 events                  did not finish
```

The scaling wall is real and worth naming: partitioning is by origin and
each client owns its origin, but a client's operations still interleave
with the RULE nondeterminism, and porcupine's power-set construction over
a 25-deep overlap does not close. **80 concurrent events is the ceiling.**

Two details that matter more than the numbers:

* At 160 events the SEQUENTIAL question still answers instantly —
  `recorded order alone also explains it`. So the cheap question is
  decidable at a size where the expensive one is not, and a green Lean
  run at 400 events says strictly less than a green porcupine run at 80.
* The timeout used to be advisory. porcupine only tests its deadline
  BETWEEN calls into the model, so a step running a deep rule closure could
  not be interrupted: a 180s budget on 160 events ran for **6m25s**. Fixed
  by wiring `StepContext` (below).

## Three fixes to the porcupine wiring

The first version of this package left three things on the table, and all
three are in porcupine's own documentation.

1. **Cache the canonical key.** `Equal` was `a.state.Key() == b.state.Key()`,
   rebuilding a string over every promise, task and outbox entry on BOTH
   sides of every comparison — and porcupine's `merge` calls it O(n²) times
   per step when it deduplicates a power-set state. `modelState` and
   `candidate` now carry the key, computed once. The Lean side had this
   right all along: `Cand.key` in `valid/lean/validator.lean` is filled by
   `mkCand`.
2. **Set `Hash`.** Documented as "reduces the number of `Equal`
   comparisons" — `merge` then compares integers and only falls back to
   `Equal` on collisions.
3. **Set `StepContext` instead of `Step`.** The context carries the
   deadline. Without it porcupine wraps `Step` in a shim that DROPS the
   ctx, so nothing inside the model can see the clock. `closure` now checks
   it between rounds.

Measured:

| | before | after |
|---|---|---|
| 80 concurrent events | 126 ms | **18 ms** |
| 160 events, 45s budget | ran 6m25s against 180s | **stops at 49s** |

(1) and (2) are the 7x. (3) is what makes any future scaling number
trustworthy: "TIMEOUT after N" now means roughly N, not "gave up somewhere
after N". The residual few seconds of overshoot is the granularity of what
remains uninterruptible — `merge` itself is not ctx-checked inside
porcupine.

### What this run actually found

A bug in **this repository's NDJSON decoder**, not in resonate.
`promise.register_listener` was ported to the model and to the emitter but
never to `decodeReq`, so a recorded listener registration read its id from
`id` (absent — the field is `awaited`) and its address from nowhere. The
model computed `400 bad address` where the server had said
`404 promise not found`, and the first concurrent history refuted at event
4 because of it.

The differential fuzzer **cannot** catch this, by construction: there the
Go side uses generated `Op` values in memory and only the Lean side reads
a file, so a bug in the Go decoder is invisible. It took traffic recorded
from a real server to surface — which is the argument for doing both.

## Sequential histories, and what that costs

A capture is sequential — one call at a time, each with a single instant.
With non-overlapping intervals, linearizability degenerates to *"the model
accepts this sequence"*. That is still the question worth asking of a
nondeterministic model, but it is **not a concurrency test**, and calling
it one would be overselling.

`-concurrent=k` widens the intervals so `k` consecutive calls overlap,
which makes porcupine search for an order rather than verify the recorded
one. Since a real capture is sequential, widening can only make more
histories legal, so it is a smoke test rather than a stronger check. The
honest way to exercise linearizability properly is to capture genuinely
concurrent traffic, which a sequential capture proxy does not produce.

### The cone of influence

The closure used to fire every enabled rule, so a promise carrying k
drainable callbacks branched 2^k ways whether or not any of them could
reach the next event — which made ~80 events in one partition the
practical ceiling (the canonical W3 workload, a 78-event single-origin
tree, did not finish). The Lean checker never had this problem because
`valid/lean/validator.lean` restricts each gap to the τs that can affect
what the next observation reads (`relevantTaus`, `touches`, `affects`).

That reduction is now ported (`touches`/`affects`/`relevantRules` in
`rules.go`, on by default, `-cone=false` for the full closure): at each
observed event only the rules whose affected objects transitively meet
what the request touches are fired. The rest are NOT discarded —
enabledness lives in the state, so a deferred rule stays armed and is
explored at the first later event that names its objects (partial-order
reduction: independent steps commute). Two details worth knowing:

* **R1's affected set includes the promise's callbacks**, not just its
  own id — settling is what arms R4 for each awaiter, and R4 is not yet
  enabled when the cone is computed. This is the same chain that made
  the Lean cone unsound the first time.
* **R3 (notify) and R6 (dispatch) fall out of the cone entirely**: they
  mutate only state no response projects (listener lists, `retryAt`,
  the outbox), and those fields feed no rule but themselves. R6 was the
  one rule that discharges no obligation — the closure's documented
  fanout source — so its exclusion is most of the win. The reduction is
  sound for the RESPONSE channel, which is all this checker compares;
  a snapshot channel would need `touches` widened, exactly as the Lean
  header warns.

Measured: the 78-event W3 tree went from TIMEOUT (>60s per discipline)
to LINEARIZABLE in 3–5s; a 26-event rpc workload from 1.3s to 12ms; the
fuzzer corpus runs ~10x faster. Equivalence with the full closure is
enforced two ways: the fuzzer's `-conecheck` property (reduced and full
must agree on every verdict — 600 traces + 600 mutants across calm and
jumpy clocks, 0 disagreements) and the existing suite passing unchanged
under the default.

## Pending ops — a 500 is "no verdict", not a response

A faithful implementation over a fallible store (the SDK's S3 network is
one) answers **500** exactly when it cannot determine an ambiguous write's
fate: the machine's response for the request exists but nobody observed it,
and guessing in either direction would be a response the machine never
produced. The machine has no 500 transition, so this checker treats a
500-answered op as **pending** and constrains it only existentially — at
its step, EITHER the state is unchanged (the request did not go through) OR
the request applied once, with the response unobserved. Both branches ride
the nondeterministic state set (`PendingOp`, `checker.go`); the surrounding
observations collapse them, and the witness names the resolution:

```
pending: 1 of 66 ops answered 500 — each may or may not have applied; the verdict leaves them free
...
  pending task.acquire wf @1010: applied, response unobserved
```

A pending op is not a wildcard — an observation neither branch explains
still refutes (`TestPendingStillRefutesTheImpossible`). But it IS
unconstrained evidence, so the count is part of the verdict: LINEARIZABLE
with N pending ops means "with these N free", and a server that answered
500 to everything would pass vacuously. The count is printed whenever it is
nonzero, in both `lincheck` and `conccheck`.

The semantics is fuzzed (`cmd/fuzz -pending`, on by default; `-goonly`
skips the Lean half, whose verdicts then count as vacuous DECLINEs) via
two properties that are theorems of the construction, checked per
generated trace:

* **weaken** — pendingization is evidence-weakening: replacing any
  responses of a valid trace with 500s must keep it accepted, because the
  "applied, response unobserved" branch subsumes the original evidence;
* **mask** — pendingizing the corrupted response of a refuted mutant must
  flip it back to accepted, because the corruption lived entirely in the
  response channel the 500 discards.

Measured: 2×1000 traces (calm and jumpy clocks, 50 steps), 4000 pending
checks, 0 violations; valid traces ACCEPT=1000/1000 and mutants
REFUTE=998/1000 unchanged (the 2 accepts are the documented
model-admits-it case — generated traces contain no 500s, so the mutant
path cannot reach the pending code).

The Lean checker is deliberately not taught this: its sequential `Valid`
would need the same disjunction threaded through the interval reduction,
and the porcupine side is the one that checks concurrent histories, where
ambiguous writes actually arise.

## Fuel, and the third verdict

`closure` is bounded. Exhausting the bound means the model stopped
looking, which is not the same as finding nothing — so `Saturation`
travels with the check and a verdict resting on a truncated closure is
reported as INCONCLUSIVE, never as a refutation. Same discipline as the
Lean checker's three-valued `Verdict`, and for the same reason: a
completeness claim is only true when the search actually saturated.

## Layout

| file | |
|---|---|
| `state.go` | objects, `Project`/`View` (facts P and T), the read disciplines, canonical keys |
| `handlers.go` | protocol handlers, one copy parameterised by discipline |
| `rules.go` | R1–R6 and the closure — where the nondeterminism lives |
| `checker.go` | the porcupine model, and `Replay` for witnesses |
| `partition.go` | partitioning and its soundness check |
| `cmd/lincheck` | the CLI |

## Tests

```
go test ./...
```

Clean captures linearize under both disciplines; the bad capture is
refuted at 886 under both; partitioned and unpartitioned agree; a
cross-origin suspend is refused; tampered responses are rejected on three
different channels; the witness recovers exactly 50 hidden resumes;
concurrent histories still pass; schedule traces are refused.

## Differential fuzzing — `cmd/fuzz`

```
(cd .. && lake build checktrace)      # the Lean checker the fuzzer compares against
go run ./cmd/fuzz -n 150 -jumpy=false
```

Generates traces, runs BOTH checkers on each, and requires them to agree.
Nothing is shared but the specification and the file format, so a
disagreement means one of them is wrong — and the trace that proves it is
printed (or written out with `-keep`).

**Why generated traces need no oracle.** A script is a list of external
requests and internal rule firings. Run it, keep only the external steps,
and the result is explainable BY CONSTRUCTION — the script is the
execution. So a checker that refutes it is wrong, with no need to know the
"right" answer independently. Mutants give the other direction: corrupt
one response and both must refuse.

**A generator needs structure.** The first version drew requests uniformly
from the alphabet and produced 50 x 404 and ZERO state-changing rule
firings in five traces: nothing existed for a rule to act on, so the
corpus could not reach the code it was written to test. It now lays down a
backbone of well-formed workflows and perturbs it — wrong versions,
missing ids, unsettable states, self-awaits, bad listener addresses.

## What the fuzzer found

**A misleading diagnostic in the Lean checker, and a real limit behind
it.** Generated traces came back `INCONCLUSIVE ... (fuel 16)`. Raising
fuel to 1024 changed nothing and still returned in 1 ms — because fuel was
never the cause. `stepObservedBy` conflated two independent reasons a step
can be undecided:

* the τ-closure hit its fuel bound;
* a τ armed a deadline INSIDE the gap that the critical-instant set does
  not cover (`noNewInGapDeadline`), so the interval reduction is not
  justified there.

Both reported the first. The two are now separate, and the second says so
— including that raising fuel will not help. Recorded captures never
exposed it because their gaps are far shorter than the retry cadence; a
generator that jumps the clock hits it immediately.

**And a measurement about the fuzzer itself.** With large clock jumps —
which is what makes R1/R5 reachable — the Lean checker DECLINES about
60% of traces. A decline agrees with anything, so those comparisons are
vacuous, and the fuzzer says so rather than counting them:

```
NOTE: the Lean checker DECLINED 18/30 (60%). A decline agrees with
      anything, so those comparisons are vacuous — the real sample size
      is 12, not 30.
```

`-jumpy=false` keeps jumps under the retry cadence: every comparison is
then real, at the cost of reaching the timeout rules less often. Both
settings are worth running, for opposite reasons.

## The run

```
$ go run ./cmd/fuzz -n 150 -steps 50 -jumpy=false

150 traces, 150 mutants, 11m5s
  go   ACCEPT=150
  lean ACCEPT=150
  mutants:  go REFUTE=150 | lean REFUTE=150
  rule firings that changed state: R1=11 R3=82 R4=30 R5=7 R6=22
  response statuses generated:     200=3563 300=63 400=558 404=312 409=1042

  no disagreements
```

300 comparisons, none vacuous, both directions: every valid trace accepted
by both checkers, every mutant refuted by both. Compare the captures — 7
handler kinds, status 200 only, one rule.

Most of the 11 minutes is process spawn: 300 launches of a 118 MB Lean
binary at ~1.9 s each. The checking itself is milliseconds.

**What this still does not establish.** Both checkers were written from
the same specification by the same author, so a common-mode misreading
would agree with itself; only porcupine's SEARCH is genuinely foreign
code. And R1 and R5 fire only 7-11 times each here, because the
regime that reaches them freely is the one the Lean checker declines. That
is the next thing worth fixing: extend the interval reduction to cover
in-gap deadlines, and the jumpy corpus starts counting.

## What is NOT tested — measured, not guessed

The captures exercise a narrow slice, and the numbers are worth having in
front of you before trusting a green run:

| | |
|---|---|
| handler kinds exercised | **7** of the 10 ported |
| never exercised | `promise.register_callback`, `task.release`, `task.heartbeat` |
| response statuses seen | **200 only** (the clean captures are all 200) |
| rules that ever fire | **R4 only** — 500 firings across both traces |
| rules never fired | **R1, R3, R5, R6** |

That is the CAPTURES. The fuzzer closes most of it — see below — which is
why it exists.

So four of the five ported rules are, as far as this suite is concerned,
dead code. Guard-ordering (400 before 404 before 409), listener
notification, lease expiry and dispatch are all unexercised, and the
INCONCLUSIVE path has never been reached because the closures never come
close to the fuel bound.

The captures also being SEQUENTIAL means the linearizability search never
has to search. Taken together: this is a working cross-check of the happy
path, not a validated model. Closing the gap needs generated traces —
ideally differential against `valid/lean/`, which is the same specification
under a completely different implementation — not more runs of the same
three files.
