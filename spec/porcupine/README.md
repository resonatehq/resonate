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
which is what makes R1/R2/R5 reachable — the Lean checker DECLINES about
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
  rule firings that changed state: R1=11 R2=8 R3=82 R4=30 R5=7 R6=22
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
code. And R1, R2 and R5 fire only 7-11 times each here, because the
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
| rules never fired | **R1, R2, R3, R5, R6** |

That is the CAPTURES. The fuzzer closes most of it — see below — which is
why it exists.

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
