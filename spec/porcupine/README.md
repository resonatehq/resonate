# A linearizability checker for the abstract machine

A Go port of `spec/02-abstract`, wired up as a nondeterministic model for
[porcupine](https://github.com/anishathalye/porcupine), checked against
both read disciplines of the machine.

```
go run ./cmd/lincheck < ../valid/traces/resonate-sqlite-50wf.ndjson
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

## Why bother, given `valid/` already checks traces

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
(`valid/schedules.lean`).

## Partitioning, and the property it rests on

Without partitioning the check is exponential in the history — 11 events
1 ms, 22 events 88 ms, 33 events 7.4 s, 44 events does not finish in 30 s.
With partitioning, 2 200 events take 145 ms.

But partitioning by object is **not sound in general** in this
machine: `task.suspend x awaiting a` links two objects, and settling `a`
wakes `x`.

What rescues it is a rule of resonate's, not of the specification's — an
awaiter and its awaited must share `resonate:origin`, enforced with
`400 "Awaiter and awaited must belong to the same origin"`. Ids are
`origin.suffix`, so partitioning on the prefix keeps every awaits-edge
inside one partition.

That is a hypothesis about the input, so it is **checked, not assumed**:

```go
if err := model.CheckPartitionable(ops); err != nil {
    // refuse to partition rather than partition unsoundly
}
```

and `-partition=false` turns it off. `TestPartitioningAgrees` confirms the
verdicts match with and without.

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
concurrent traffic, which `valid/traces/capture.py` does not do.

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

## What is NOT tested — measured, not guessed

The captures exercise a narrow slice, and the numbers are worth having in
front of you before trusting a green run:

| | |
|---|---|
| handler kinds exercised | **7** of the 10 ported |
| never exercised | `promise.register_callback`, `task.release`, `task.heartbeat` |
| response statuses seen | **200 only** (the clean captures are all 200) |
| rules that ever fire | **R4 only** — 500 firings across both traces |
| rules never fired | **R1, R2, R3, R5, R6** |

So five of the six ported rules are, as far as this suite is concerned,
dead code. Guard-ordering (400 before 404 before 409), listener
notification, lease expiry and dispatch are all unexercised, and the
INCONCLUSIVE path has never been reached because the closures never come
close to the fuel bound.

The captures also being SEQUENTIAL means the linearizability search never
has to search. Taken together: this is a working cross-check of the happy
path, not a validated model. Closing the gap needs generated traces —
ideally differential against `valid/`, which is the same specification
under a completely different implementation — not more runs of the same
three files.
