# Three TLA+ models, one protocol

A comparison of the three TLA+ models the [Specula](https://github.com/specula-org/Specula)
toolkit produced for the three Resonate server implementations — and then of all
three against the abstract specification in this repository.

## The artifacts

| # | Model | Target | Branch / path | Size |
|---|---|---|---|---|
| 1 | `Resonate.tla` | `resonatehq/resonate` @ `c8d7c7b` (Rust + SQL backends) | `specula:.specula-output/spec/` | 409 lines |
| 2 | `Resonate.tla` | `resonatehq/resonate-on-convex` @ `8c1b4d3` (TypeScript on Convex) | `specula:.specula-output/spec/` | 428 lines |
| 3 | `base.tla` | `resonatehq/resonate-pg` @ `54fe651` (`resonate.sql`, PL/pgSQL) | `specula:.specula-output/spec/` | 521 lines |
| — | **the reference** | this repo, Lean 4 — `spec/02-abstract` + `spec/03-concrete` | `main` / `claude/close-the-square` | ~3 000 lines |
| — | *(older alt)* | TLA+ port of the Lean spec | `tla+:alts/tla+/Server.tla` | 1 087 lines |

Below, the three Specula models are **server**, **convex** and **pg**. All three
were produced by the same pipeline (code analysis → spec generation → harness →
trace validation → model checking → reproduction) run independently against three
codebases, with no shared prompt about the reference spec — only **pg** was
explicitly diffed against it (`spec-comparison.md`). That independence is what
makes the agreements below worth something.

---

# Part 1 — The three models against each other

## 1.1 They converged on the same state vector

Modulo naming and packaging, all three chose the same abstraction:

```
promise state    +  deadline  +  "is this promise externally addressable"
task state       +  version   +  a deadline that is either a retry or a lease
callbacks        (awaited → awaiter)
outbox           keyed by task id, upsert-not-append
a global clock
```

The three notable shared decisions, each independently derived from a different
source language:

1. **The outbox is a keyed map, not a queue.** server calls it `UpsertExec`
   (from `ON CONFLICT (id) DO UPDATE`), convex writes one slot per task
   (`outbox[i] = version`), pg calls it `PutExec` (from `_emit_execute`'s
   `ON CONFLICT (key)`). Nobody modelled message *ordering*, and all three were
   right not to: the protocol never depends on it.
2. **Version bumps on acquire only.** A resume or a redispatch re-emits the
   *current* version. All three encode this; none bumps on lease expiry.
3. **Projection is a predicate, not a state.** A pending promise past its
   deadline is *logically* settled before anything persists that. server calls it
   `Expired(p)`, convex `LogSettled(st, now, i)`, pg `Proj(i)`. This is the single
   most important concept in the protocol and all three found it unprompted.

## 1.2 Where they diverge structurally

| | **server** | **convex** | **pg** |
|---|---|---|---|
| Variables | 12 flat variables | 1 record `s` + `clock` | 10 variables incl. 3 history |
| Promise states | 4 (`settled` collapses resolved/rejected/canceled) | 4 | **5 — the full protocol set** |
| Timer promises (`resonate:timer` resolves on expiry) | ✗ | ✗ | **✓ (`isTimer`)** |
| `resonate:external` vs `resonate:target` | conflated into `hasTarget` | conflated into `ptarget` | **distinguished (`Kinds`)** |
| Listeners / `unblock` | ✗ | ✗ (explicitly out of scope) | **✓** |
| `task.halt` / `task.continue` | ✗ | ✗ | **✓** |
| `task.create` re-claim path | ✗ (noted, not modelled) | ✗ | **✓ (`TaskClaim`)** |
| `task.release` | ✓ | ✗ | ✓ |
| `task.heartbeat` | ✓ | ✓ | ✗ |
| Explicit worker processes | **✓ (`claim` per worker + `WorkerCrash`)** | ✗ | ✗ |
| Message delivery stage | **✓ (`outbox` → `delivered` → acquire)** | partial (`Consume`) | ✗ (`Dequeue` only) |
| Fault injection | **counter-bounded drops + crashes** | message loss via `Consume` | none |
| Clock | `AdvanceClock(t)`, arbitrary jump | `Tick` +1 | `Tick` +1 |
| Liveness | ✗ | **✓ `FairSpec` + `TasksConverge`** | ✗ (uses `Quiesced` guards instead) |
| History/ghost variables | none | `everSettled`, `zombieSend` | `obs`, `badDispatch`, `badHalt` |

Three genuinely different modelling *postures* fall out of this:

- **server is the most operational.** It is the only model with a worker-side
  state variable (`claim[w]` — what each worker *believes* it holds), the only one
  that separates enqueue from delivery from acquisition, and the only one with
  counter-bounded fault injection (`MaxDrops`, `MaxCrashes`). That buys it the one
  property the other two cannot state: `AtMostOneValidClaim` — at-most-once
  execution across worker crashes and lease expiry. It is also the only model
  whose clock can jump arbitrarily, which is what lets the same module be driven
  by real millisecond timestamps from a trace.

- **convex is the most temporal.** It is the only model that checks a liveness
  property, and the only one whose scheduler is explicitly *unpunctual*: the
  deadline jobs are enabled from their deadline onwards and are allowed to fire
  arbitrarily late or spuriously (a stale job "dies quietly" inside the effect
  rather than being disabled). `FairSpec` + `TasksConverge` then asks the question
  the architecture actually rests on — *can a workflow be stranded by a lazy
  scheduler?* The report is also the only one that mutation-tests its own liveness
  property and concludes, in print, that it is weaker than it looks (the
  promise-timeout job alone satisfies it; it is not evidence about redelivery).

- **pg is the most observer-facing.** Its invariants are not about state shape but
  about what a *client* can see: `Stickiness` (once `promise.get` reports settled,
  it never reports anything else) is phrased on a history variable `obs` recording
  the first non-pending projection ever observable. `badDispatch` / `badHalt` are
  ghost variables that record the *act* of doing something wrong rather than a bad
  state. It is also the only model with **configuration switches**
  (`ListenerExternalGuard`, `PromiseLivenessGuard`, `TimeoutLivenessGuard`,
  `SequencedDriver`) so the same module runs as either the shipped server or the
  spec-conformant one — which is how it verified its own proposed fixes rather
  than merely arguing for them.

## 1.3 Property vocabulary

Almost no invariant name is shared, but the sets overlap heavily in content:

| Concept | server | convex | pg |
|---|---|---|---|
| settled promise ⇒ fulfilled task | `SettledImpliesFulfilled` | `FulfilledImpliesSettled` (converse) | `TaskPromiseCoherence` |
| suspended task can be woken | `NoBlockedOnExpired` | `SuspendedHasCallback` | `NoStrandedTask` |
| pending task will be re-dispatched | `ExecuteNotLost` | `PendingHasRetry` | — |
| acquired task will be reclaimed | `NoOrphanPendingTask` | `AcquiredHasLease` | — |
| at-most-once execution | **`AtMostOneValidClaim`** | `OutboxNeverAhead` (weaker) | — |
| never dispatch the logically dead | — | **`NoZombieSend`** | **`NoDeadDispatch`** |
| settlement is sticky on the wire | — | `NoResurrection` (state-level) | **`Stickiness`** (observation-level) |
| unattended promise still expires | **`NoStrandedDeadline`** | — (not applicable) | — |
| listener obligations discharged | — | — | **`NoStrandedListener`** |

Two properties exist in exactly one model each and are the ones that found the
headline bug there: `NoBlockedOnExpired` (server), `NoZombieSend` (convex).
`NoDeadDispatch` (pg) is the same idea as `NoZombieSend`, arrived at
independently and instrumented over more sites.

## 1.4 Scale and results

| | server | convex | pg |
|---|---|---|---|
| Scope | 3 ids, 2 workers, horizon 6 | 2 ids, 1 pid, horizon 3 (deep: 3 ids, 2 pids) | 2 ids, 1 address, horizon 3, versions ≤ 2 |
| Exhaustive run | 2 721 210 distinct, depth 37 | 18 858 distinct (3 802 871 deep) | ~4.7 M distinct |
| Largest hunt | 42 953 187 states generated, BFS **incomplete** | 224 626 generated, complete | complete |
| Traces replayed | 4 scenarios / 50 events | 5 scenarios / 32 transitions | 5 scenarios / 39 transitions, all 16 actions |
| Validator mutation-tested | — | **5 mutants, all caught** | **2 injected faults, both caught** |
| Bugs confirmed | 2 (1 model-checked, 1 by code analysis) | 2 (1 model-checked, 1 by analysis) | 5 (3 model-checked, 1 analysis, 1 spec-diff) |

The honesty markers differ in a way worth recording: server states plainly that
its fencing hunt was a **BFS-incomplete** search, so "no violation" there is
bounded evidence, not a proof; convex retracts the strength of its own liveness
result; pg's `spec-comparison.md` opens with two corrections against its own first
pass ("the model could not have found BUG-2's response half — `obs` records
`Proj(i)`, not what each handler actually returned"). All three name what they did
not run.

---

# Part 2 — The three models against the abstract specification

## 2.1 What the reference actually is

The specification in this repository is not one machine but **four**, in one
weak-bisimilarity class (`TheSquare`):

- the **concrete** machine in two read disciplines — `-p` (projected: a read
  serves the projection and writes nothing) and `-m` (materialized: a read fires
  the anticipated timeout at the moment of observation, then reads stored state);
- the **abstract** (coalesced) machine in the same two disciplines — no timeout
  components, no deferred queue, obligations retained on the object, and seven
  guarded rules `R1..R7` fired by the environment at any pace.

The relevant consequence for this comparison: **the specification does not
prescribe a read discipline.** Lazy convergence, eager materialization, and
projection-on-read are all conformant — the spec proves they are indistinguishable.
What it *does* prescribe is that the discipline be applied uniformly, because the
one thing the square forbids is *stored-vs-projected divergence becoming
observable* (`T-09-task.halt.lean`: "Branching on the raw stored task here would
make the stored-vs-projected divergence observable — the one thing the projection
discipline forbids").

Two protocol rules do most of the work below:

- **TIMEOUT ALWAYS WINS** — no transition creates new work for a logically dead
  task; every *decision* consults the promise through the view, even where the
  *read* is raw (`rules.lean` R5/R6; `ResumeTasks` in the TLA+ port requires
  `ps[id].timeoutAt > tnow`).
- **External-only waiters** — only external promises (tagged
  `resonate:external`, targeted, or timers) carry an armed timeout, so only they
  can discharge an obligation. Both registration paths, `P-04` and `P-05`, refuse
  an internal awaited with `422`.

> ⚠️ **`alts/tla+/Server.tla` is not the reference.** It is a July-15 port and
> predates `6ddfab7` (external-only listeners) and `3e8a1d6` (timeout-wins on
> lease/retry). Its `PromiseRegisterListener` has no `external` guard and its
> `TaskCreate` claim branch has no promise-liveness gate — i.e. it exhibits two of
> the divergences catalogued below. Use it for the invariant vocabulary, not for
> the guards. The normative machine is the Lean one.

## 2.2 Coverage against the protocol surface

| Spec handler | server | convex | pg |
|---|---|---|---|
| P-01 `promise.get` | ✓ (as `ClientGet`, the ghost-step carrier) | ✓ (`TimeoutJob`) | implicit in `Proj` |
| P-02 `promise.create` | ✓ (live path only) | ✓ (rooted / plain) | ✓ (incl. born-settled path) |
| P-03 `promise.settle` | ✓ | ✓ | ✓ |
| P-04 `register_callback` | ✗ (only via suspend) | ✗ (only via suspend) | ✓ |
| P-05 `register_listener` | ✗ | ✗ | ✓ |
| T-02 `task.create` | ✗ | ✗ | ✓ |
| T-03 `task.acquire` | ✓ | ✓ | ✓ |
| T-04 `task.fence` | ✗ | ✗ | ✗ |
| T-05 `task.heartbeat` | ✓ | ✓ | ✗ |
| T-06 `task.suspend` | ✓ (n-ary) | ✓ (unary only) | ✓ (n-ary) |
| T-07 `task.fulfill` | ✓ | ✓ | ✓ |
| T-08 `task.release` | ✓ | ✗ | ✓ |
| T-09 `task.halt` | ✗ | ✗ | ✓ |
| T-10 `task.continue` | ✗ | ✗ | ✓ |
| R1/R2 timeout facts | ✓ | ✓ | ✓ |
| R3 `notify` | ✗ | ✗ | ✓ |
| R4 `resume` | ✓ (inside the settle cascade) | ✓ (inside the settle cascade) | ✓ (inside the cascade) |
| R5 `leaseExpiry` | ✓ | ✓ | ✓ |
| R6 `dispatch` | ✓ | ✓ | ✓ |
| R7 `scheduleFire` / S-01..04 | ✗ | ✗ | ✗ |
| Response payloads | ✗ | ✗ | ✗ |

**Union coverage is good; no single model covers the surface.** pg is closest
(15 of 20 rows). Nobody models schedules, `task.fence`, or — critically —
**response payloads**. pg names that last gap itself: a handler that returns an
unprojected record is *invisible* to a model whose only observation channel is the
state. The spec's own stability theorem is exactly a statement about responses, so
this is the largest systematic blind spot the three models share.

## 2.3 Where all three agree with the spec

These are non-trivial and all three got them right:

- Version bumps only on acquire; resume and redispatch re-emit the current
  version (spec: "the execute is a wake-up hint, not a fresh fencing token").
- Lease expiry does **not** bump the version — the fence token survives the lease.
- The outbox is a keyed upsert, so re-emission is idempotent (spec R6's
  justification for allowing any re-arm instant, including one in the past).
- A promise settles at most once; settlement is terminal.
- Settlement fulfils the promise's own task and drops that task's registrations.
- `task.suspend` on an already-settled awaited returns the `300` re-check path
  rather than parking (server, convex, pg all encode this).
- Deadline handlers re-check their own due time — an armed timer means *not
  before*, never *exactly at* (all three model arbitrary-late firing; convex
  models it most aggressively, with spurious firings too).

## 2.4 Divergences, keyed to the spec

### D1 — Internal promises can accrue obligations they can never discharge

**Spec:** external-only waiters, `422` on both registration paths; only external
promises carry an armed timeout (`NonExternalPromiseHasNoTimeout`).

- **server: violated, confirmed, High.** `c8d7c7b` made `promise_timeouts` rows
  conditional on `resonate:target` *and* made the scheduler drive expiry from that
  table — while leaving `promise.register_callback` / `register_listener` open to
  any promise. The model's `NoStrandedDeadline` / `NoBlockedOnExpired` violate in
  8 states. A suspended task waits on a promise that `promise.get` already reports
  `rejected_timedout`. This is precisely the obstruction `6ddfab7` closed in the
  spec, found independently from the Rust side.
- **pg: violated, confirmed (BUG-1).** `promise_register_listener` has no
  `external` guard; `promise_register_callback` does. Same defect, different
  half of the same rule.
- **convex: not applicable.** Convex schedules a per-promise timeout job at
  creation regardless of target, so every promise converges. The obligation is
  always dischargeable. Convex is *accidentally* conformant here — it satisfies
  the rule by arming everything rather than by refusing the registration.

Two of three implementations violate the same spec rule, at the two different
sites the spec explicitly enumerates. That the rule was added to the spec on
2026-07-30 and both violations were found independently afterwards is the strongest
validation of the rule in this whole exercise.

### D2 — Work is created for the logically dead

**Spec:** TIMEOUT ALWAYS WINS. `ResumeTasks` skips an awaiter whose own
`timeoutAt ≤ now`; R5 and R6 consult `viewPromise` before re-pending or emitting.

- **convex: violated, confirmed (BUG-1).** `triggerCallbacks` resumes a suspended
  awaiter and emits its `execute` without ever projecting the awaiter's own
  promise. `NoZombieSend` violates at depth 7 — and the recorded `timeout` trace
  *contains* the interleaving, so it is not an artifact of the abstraction.
- **pg: violated at three sites, two confirmed.** `task.create`'s claim branch and
  `task.continue` consult neither promise state nor timeout (BUG-2); the retry and
  lease timeout handlers read the promise only for its target (BUG-5) — latent,
  masked by `process_timeouts` draining the promise loop first, i.e. **correct by
  sequencing, not by guard**. That masking is exactly the kind of thing the spec's
  independent-τ-rules formulation makes impossible to rely on.
- **server: present in the model, unprobed.** `SettleChain` computes
  `resumed = suspended awaiters \ selfFulfilled` — no check on the awaiter's own
  deadline, structurally identical to convex's `Suspenders`. No invariant in the
  server model names this outcome, so the run reports no violation. This is a gap
  in the *model*, not a clean bill of health for the Rust server.

**This is the finding that only emerges from putting the three side by side.** All
three models encode a resume cascade with no timeout-wins guard on the awaiter;
only convex instrumented for it and only convex reported it. The spec had already
closed the same hole (`3e8a1d6`, "no new work for the dead"). If the server model
grew convex's `zombieSend` probe or pg's `badDispatch` ghost, it would be a
one-line change and the outcome is predictable from the code alone.

### D3 — Read discipline applied non-uniformly

**Spec:** any discipline, uniformly; stored-vs-projected divergence must never
become observable.

- **pg: violated, confirmed (BUG-4).** `task.get` projects a task `fulfilled`
  while `task.halt` never loads the promise at all and returns `200` — for a task
  whose projected state makes halt a `409`. Two endpoints, one task, one instant,
  contradictory answers. Also BUG-2's response half: `task.create` serves
  `_promise_json_raw`, unprojected, so `promise.get` can answer
  `rejected_timedout` beside `task.create` answering `pending`.
- **server: divergence deliberately modelled at one site.** `Heartbeat` extends
  the lease without checking that the promise is still live — the model's comment
  cites the acknowledged `TODO` in the code. Spec T-05 requires
  `promises[r.id].state = "pending" ∧ timeoutAt > now`. Modelled faithfully,
  but no invariant names it, so it is recorded rather than reported.
- **convex: conformant here.** `Heartbeat` checks `~LogSettled`, matching T-05.
  More generally convex is the model closest to the spec's **`-m` (materialized)**
  twin: `Converge` is materialize-on-touch, run at the head of the handlers that
  the implementation actually runs it for — and the model is careful to note where
  it is *not* run (`server.ts:177-178`, heartbeats), which is where the real
  divergence would live.

### D4 — Scope gaps that hide spec rules entirely

Schedules (S-01..04, R7) and `task.fence` (T-04) are modelled by nobody, and
`promise.register_callback` as a standalone request only by pg. The spec's
catch-up semantics for backlogged occurrences ("a backlogged occurrence is born
settled, exactly as if it had been created on time") — a genuinely subtle rule,
and the one the mechanized proof needed `occurrences` declared opaque to
discharge — is untested against every implementation.

## 2.5 Invariant vocabulary: spec vs. models

The spec's TLA+ port carries 28 invariants; they are almost all **structural
well-formedness** (`TaskHasAtMostOneTimeout`, `LeaseTimeoutOnlyForAcquiredTask`,
`NonAcquiredTaskNoPidOrTtl`). The Specula models' are almost all
**observer-facing safety**. The two vocabularies overlap at four points and are
otherwise complementary:

| spec invariant | model counterpart |
|---|---|
| `SuspendedTaskHasCallback` | convex `SuspendedHasCallback`, pg `NoStrandedTask` |
| `SettledPromiseHasFulfilledTask` | server `SettledImpliesFulfilled`, pg `TaskPromiseCoherence` |
| `PendingTaskHasRetryTimeout` | server `NoOrphanPendingTask`, convex `PendingHasRetry` |
| `AcquiredTaskHasLeaseTimeout` | server `NoOrphanPendingTask`, convex `AcquiredHasLease` |

Worth trading in both directions:

- **Into the models:** `NonExternalPromiseHasNoTimeout`. The reference never
  *arms* a timeout for an internal promise, so the D1 obligation cannot exist
  structurally. `resonate.sql` stores a `NOT NULL timeout_at` for every promise and
  merely declines to enforce it — the structural root of BUG-1, invisible to an
  invariant phrased on behaviour.
- **Into the spec:** nothing in the spec's invariant set corresponds to
  `AtMostOneValidClaim`. The spec has no worker-side state, so at-most-once
  execution is a property it cannot currently state — it is implied by the fencing
  discipline but never checked. server's model is the only artifact in this family
  that checks it.

---

# Bottom line

**Between the models.** Three independent runs over three unrelated codebases
converged on the same state abstraction and the same three key modelling
decisions, which is strong evidence the abstraction is the right one. They diverge
in posture, not in content: server models *workers and messages* (and is alone in
checking at-most-once execution), convex models *time* (and is alone in checking
liveness), pg models *observations* (and is alone in running its own proposed
fixes back through the checker). None dominates; the union is close to a complete
model and no member is.

**Against the spec.** The specification wins every disagreement, and two of its
most recent rules — external-only waiters (`6ddfab7`) and timeout-wins-on-redispatch
(`3e8a1d6`), both added 2026-07-30 — were each independently violated by two of the
three implementations and rediscovered from the code side within days. That is the
spec doing its job.

Three concrete follow-ups, in order of value:

1. **Add the timeout-wins probe to the server model** (D2). `zombieSend` /
   `badDispatch` is a few lines; the model already contains the divergence and
   simply does not look at it. Predicted outcome: violation.
2. **Model response payloads somewhere** (§2.2). Every model observes state only,
   and the two response-level defects found so far (pg BUG-2's second half, BUG-4)
   were both found by *reading the spec*, not by checking. Response stability is
   the spec's own headline theorem and nothing checks it against an implementation.
3. **Adopt `NonExternalPromiseHasNoTimeout` into the implementation models**
   (§2.5) — it converts D1 from a behavioural property into a structural one, and
   structural properties fail faster and localize better.
