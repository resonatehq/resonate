# The unified model — acceptance record

`Unified.tla` claims to be strictly stronger than the three implementation
models it was derived from. That claim is only worth something if it is
falsifiable, so it is stated as a test:

> **The specification profile must clear every property. Each implementation
> profile must reproduce that implementation's known defects, as violations of
> *named* properties, without any bespoke instrumentation.**

Every implementation's divergence from the specification is a named boolean
CONSTANT; a profile is an assignment of those constants. Nothing else changes
between runs — one module, one `Next`, six configurations.

## The switches

| constant | `TRUE` (specification) | `FALSE` (as shipped somewhere) |
|---|---|---|
| `ArmPolicy` | `"external"` — only external promises carry an armed timeout | `"target"` (resonate), `"all"` (convex) |
| `CallbackExternalGuard` | P-04 refuses an internal awaited (422) | accepts any awaited |
| `ListenerExternalGuard` | P-05 refuses an internal awaited (422) | accepts any awaited |
| `PromiseLivenessGuard` | T-02 claim / T-09 halt / T-10 continue gate on the projection | ungated |
| `TimeoutLivenessGuard` | R5 lease expiry / R6 dispatch gate on the projection | ungated |
| `ResumeLivenessGuard` | R4 resume skips an awaiter that is itself logically dead | resumes it anyway |
| `HeartbeatGuard` | T-05 gates on the projection | ungated |
| `SequencedDriver` | — | promise-timeout loop drains before the task-timeout loop |
| `FaultsOn` | message loss + worker crashes enabled | — |

## Results

TLC 1.8.0 (`2026.07.31`), 4 workers. Scope for every run: 2 ids, 1 listener
address, 1 worker, horizon 2, versions ≤ 2, `Retry = Ttl = 1`, faults on.

| profile | switches off | property checked | result |
|---|---|---|---|
| `MC_spec` | *none* | `Safety` (all 17) | ⏳ **run not yet complete** — see below |
| `MC_server` | arm=`target`, callback, listener, promise, timeout, resume, heartbeat | `ObligationsAreDischargeable` | **violated**, 153 states, depth 4 |
| `MC_convex` | arm=`all`, callback, listener, resume | `NoDeadDispatch` | **violated**, 17 792 states, depth 7 |
| `MC_pg` | listener, promise, timeout, resume (+`SequencedDriver`) | `NoHaltOnDead` | **violated**, 819 states, depth 5 |
| `MC_pg_listener` | as above | `ObligationsAreDischargeable` | **violated**, 321 states, depth 4 |
| `MC_resume_gap` | **resume only** | `NoDeadDispatch` | **violated**, 15 201 states, depth 7 |

> **Status of the `MC_spec` row.** The five implementation-profile rows are
> complete and reproducible. The specification-profile run — the half of the
> acceptance test that says the guards, all switched on, actually close every
> defect — had **not finished** when this file was written; the partial log is
> in `output/spec.out`. Until it completes, this model has demonstrated that it
> *detects* all five divergences, and has **not** demonstrated that the
> specification profile is clean. Do not read the table as if it had. The
> earlier claim that it was clean came from a run whose state space was
> silently truncated (see "corrections" below), so it carries no weight and is
> not repeated here.

Reproduce:

```bash
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -workers 4 -config MC_<profile>.cfg MC
```

Each violation maps to a finding the three Specula runs reported independently:

| violation | the finding it reproduces |
|---|---|
| `MC_server` → `ObligationsAreDischargeable` | resonate BUG-1 — `c8d7c7b` gated `promise_timeouts` on `resonate:target` while leaving registration open, so a suspended task waits on a promise nothing will ever settle |
| `MC_pg_listener` → `ObligationsAreDischargeable` | resonate-pg BUG-1 — `promise_register_listener` has no `external` guard |
| `MC_convex` → `NoDeadDispatch` | resonate-on-convex BUG-1 — `triggerCallbacks` resurrects and dispatches a timed-out workflow |
| `MC_pg` → `NoHaltOnDead` | resonate-pg BUG-4 — `task.halt` returns 200 on a task `task.get` already reports `fulfilled` |
| `MC_resume_gap` → `NoDeadDispatch` | **the divergence all three models contained and none of the three probed** (see below) |

## The row that justifies the exercise

`MC_resume_gap` is the specification profile with **one** switch flipped:
`ResumeLivenessGuard = FALSE`. Everything else is spec-conformant. It violates
`NoDeadDispatch` at depth 7.

That switch corresponds to a line that is missing from **all three**
implementation models: server's `SettleChain` computes
`resumed == suspended awaiters \ selfFulfilled`, convex's `TriggerSettlement`
uses `Suspenders(st, i)`, and pg's `CascadeTasks` uses `SuspAwaiters(i)` —
none consults the awaiter's *own* deadline, where the specification's
`ResumeTasks` requires `ps[id].timeoutAt > tnow`. Only the convex run
instrumented for the consequence, so only the convex run reported it. In the
unified model it is one named property, checked in every profile at once.

## Two corrections the model forced on itself

Recorded because both were found by running it, and both invalidated an earlier
result of this exercise:

1. **`obs` must not appear in any action's `UNCHANGED` list.** `obs` is a
   history variable written by `Next`. When it was also listed in each action's
   `UNCHANGED`, `Tick` became *disabled* in exactly those states where advancing
   the clock would newly expose an expiry — because the two conjuncts
   contradicted. The first spec-profile run reported 1 004 962 distinct states
   and "no error"; that number was an artifact. After the fix the reachable
   space is larger and the convex/resume-gap violations appear (they were
   unreachable before). A "no violation" result over a silently truncated state
   space is the worst outcome a model can produce, and nothing in TLC's output
   flags it — the reachability probe (`NoDeadSuspender`, kept in `MC.tla`) is
   what exposed it.

2. **`SuspendedTaskHasCallback` needed a liveness qualifier.** Stated as *every
   suspended task has a callback*, it fails on the **specification** profile at
   depth 7: when a promise settles it scrubs its callback rows while
   deliberately not resuming an awaiter that is itself past its deadline. That
   awaiter is dead weight owned by the promise-timeout rule, which will fulfil
   it — so it legitimately sits suspended with no callback. The property is
   correct only as *every suspended task **that is still logically alive** has a
   callback*. This is TIMEOUT ALWAYS WINS showing up as a constraint on what the
   invariant is allowed to say.

## What this model does not do

- **No response payloads.** Like all three source models, the only observation
  channel is state plus history variables. `obs` records `Proj(i)`, not what a
  handler actually returned, so a handler serving an unprojected record is
  invisible. resonate-pg BUG-2's response half and the specification's own
  response-stability theorem are both out of reach. This is the largest gap.
- **No schedules** (S-01..S-04, R7), and no `task.fence` (T-04).
- **Small scope.** 2 ids / horizon 2 is enough for every violation above to
  appear at depth ≤ 7, but the negative result (`MC_spec`) is bounded evidence,
  not a proof. The specification's own theorems are the proof; this is a check.
- **Atomic handlers.** Each action is one atomic step. That reflects the
  advisory-lock / transaction discipline of the implementations rather than
  verifying it.
- **Liveness is checked separately** (`MC_liveness.cfg`, `FairSpec` +
  `TasksConverge`) and is weaker than it looks, for the reason the convex run
  documented: within a bounded horizon every promise eventually times out, so
  the timeout rule alone discharges the "eventually". It is evidence that
  deadlines converge a workflow, not that redelivery works.
