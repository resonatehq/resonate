# Resonate Specification

The Resonate protocol, specified as an executable **abstract machine** in
Lean 4: a state, a set of effects (atomic operations on the state), and a set
of request handlers (transitions composed from effects).

## The machines

One state, one wire surface, four machines — two levels, each in two read
disciplines:

- **`-p`, projected** — a read serves the *projection* of a timed-out object
  and writes nothing; the timeout transition persists the fact later.
- **`-m`, materialized** — a read *materializes* first — fires the
  anticipated timeout at the moment of observation — then serves stored
  state.

The **concrete machine** ([`spec/03-concrete`](spec/03-concrete)) carries
explicit timeout components and a deferred queue. The **abstract machine**
([`spec/02-abstract`](spec/02-abstract)) coalesces them: deadlines live on
the objects, awaiters and listeners stay on the settled promise and are
drained by chosen-element rules, cadence is a rule parameter.

### The relations

All in [`spec/04-theorems`](spec/04-theorems), checked by kernel `decide` on
a targeted battery and exhaustively at small scope (every script up to
length 4 over a 9-request alphabet):

| claim | statement |
|---|---|
| `Indistinguishable` | `-p` and `-m` are weakly bisimilar: every valid trace of one has a valid trace of the other with the same externalized behavior and the same messages at quiescence, each machine scheduling its own τ steps |
| `ConcreteRefinesAbstract`, `AbstractRefinesConcrete` | refinement holds in both directions, via the structural abstraction `alpha` and a syntactic rule translation as the constructive schedule |
| `TheSquare` | all four machines sit in one weak-bisimilarity class — every implementation choice the machines embody is unobservable, and the protocol has exactly one behavior space |
| `LockstepAbstract` | at the abstract level the twins answer pointwise-identically under any shared rule schedule, adversarial firings included |
| `responseLockstepAbstract` | the response half of lockstep is a fully mechanized theorem over all traces ([`proof.lean`](spec/04-theorems/proof.lean)) — no `sorry`, standard axioms only |

The design invariant underneath all of them: **timeout always wins**. Every
decision — handler or rule — consults the view, so no transition creates new
work for a logically dead object.

## The Machine

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/am-b.png">
    <img src="docs/am-w.png" alt="Abstract Machine">
  </picture>
</p>

### State

[`state.lean`](spec/03-concrete/state.lean)

- **objects** — promises, tasks, and schedules
- **deferred** — resume obligations the server records at settlement and invokes on itself later
- **timeouts** — obligations the environment fires later, as internal transitions
- **outbox** — messages awaiting delivery: `execute` dispatches a task to a worker, `unblock` notifies a listener of a settled promise

Wire-level records and request/response types are in [`types.lean`](spec/01-protocol/types.lean).

### Effects

The atomic operations of the machine ([`state.lean`](spec/03-concrete/state.lean)) — lookups, keyed upserts, and deletes per state component:

| Component | Effects |
|---|---|
| promises | `getPromise` / `setPromise` |
| tasks | `getTask` / `setTask` |
| schedules | `getSchedule` / `setSchedule` / `delSchedule` |
| deferred | `defer` / `undefer` |
| timeouts | `setPromiseTimeout` / `setTaskTimeout` / `setScheduleTimeout` / `del…Timeout` |
| outbox | `setMessage` |

Handlers touch state only through effects. Together they are the contract a
concrete implementation must realize. Settlement's write set, restricted to
promises and tasks, is `{p.id}`: settle neither reads nor writes any promise
but its own — resumes are recorded as deferred work, discharged by the drain.

### Handlers

Every handler is a pure function

```lean
Req → (now : Nat) → M Res    -- M = StateM ServerState
```

composed from effects. Deterministic and total; there is no hidden clock —
time enters only through `now`.

Conventions the whole model leans on:

- **Projection** — a pending promise past `timeoutAt` is *observed* as already settled (`resolved` for timers, `rejectedTimedout` otherwise) even before its timeout transition persists that fact.
- **Validation** — anything rejectable by inspecting the request alone is `400`, with highest precedence: before existence, state, or version are consulted.

The full handler catalogue — P-01 … S-04 plus the internal transitions, one
file per handler — is in
[`spec/03-concrete/README.md`](spec/03-concrete/README.md).

## Tools

Two binaries, built by [`.github/workflows/binaries.yml`](.github/workflows/binaries.yml)
for linux, macOS and Windows and published on every release:

| binary | source | what it does |
|---|---|---|
| `lincheck` | [`valid/porc/cmd/lincheck`](valid/porc/cmd/lincheck) | linearizability checker — reads an NDJSON trace on **stdin**, answers under both read disciplines |
| `scenarios` | [`work/go`](work/go) | traffic generator — drives the Go SDK's durable functions against a real server and records the trace |

They form a pipeline with the Lean checker:

```
scenarios fan-out -runs 12 -parallel 4 -contention 0.3 -out run
lincheck < run.ndjson              # Go, every order consistent with the history
lake exe checktrace < run.ndjson   # Lean, one fixed order, against the spec itself
```

## Conformance

[`valid/`](valid) is a **trace checker** built on the specification: it takes
traffic recorded from a real server and asks whether the machine can account
for it.

```
lake exe checktrace < trace.ndjson
```

```
loaded 550 events
ADMISSIBLE   events=550 maxFanout=1 witnessTaus=50   26ms
witness: 50 internal steps the server never reported
  @1030  τ resume o0.a → o0.x
  @1130  τ resume o1.a → o1.x
  …
```

Exit `0` admissible, `1` refuted, `2` parse error, `3` inconclusive.

Input is NDJSON, one event per line — the format is stated in
[`valid/README.md`](valid/README.md). Internal steps are **not** in the file and must not be: the
specification deliberately leaves the τ schedule open, so there is no single
trace to reproduce. The checker asks the existential question instead —

> Does there EXIST a schedule of internal steps under which the
> specification produces exactly these external events?

— and answers it by subset construction: carry the set of states the server
could be in, close each under the internal steps enabled since the last
observation, apply the observed request, and keep only the candidates whose
response matches what was seen. Empty set means no schedule explains the
trace.

The verdict is the specification's own `Valid`, not a restatement:

```lean
theorem accepted_trace_implies_valid_trace : Accepted t fuel cap →   Valid t
theorem rejected_trace_implies_not_valid_trace : Rejected t fuel cap → ¬ Valid t
```

Three verdicts, not two: `.inconclusive` is a first-class answer, because
completeness is only true when the search actually saturated.

**Status: the statements are mechanized, most proofs are not.** The remaining
obligations are stated and open; empirically, 16 203 generated scripts show
0 soundness violations. See
[`valid/lean/correctness.lean`](valid/lean/correctness.lean) for what is
claimed and [`valid/lean/schedules.lean`](valid/lean/schedules.lean) for the
one thing that cannot be: `occurrences` is `opaque`, so traces mentioning
schedules are declined rather than judged.

## Implementing the protocol

[`spec/implementation-questions.md`](spec/implementation-questions.md) is the
catalogue of questions every implementation answers: the conformance
questions with exactly one right answer, each paired with the property that
fires when it is answered otherwise; the latitude the machines genuinely
permit; and the platform questions the specification is silent about but
every substrate forces. It carries an answer sheet, one column per
implementation — and the ones answered badly are recorded under their bug
numbers in its coverage record.

## Build

```
lake build            # everything, including the exhaustive decide sweeps (minutes)
lake build spec       # the specification alone — the fast loop
lake build valid      # the trace checker
lake exe checktrace   # reads a trace on stdin
```
