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
| `MC_spec` | *none* | `Safety` (all 19) | ⏳ re-running with the response channel |
| `MC_server` | arm=`target`, callback, listener, promise, timeout, resume, heartbeat | `ObligationsAreDischargeable` | **violated**, 153 states, depth 4 |
| `MC_convex` | arm=`all`, callback, listener, resume | `NoDeadDispatch` | **violated**, 17 792 states, depth 7 |
| `MC_pg` | listener, promise, timeout, resume (+`SequencedDriver`) | `NoHaltOnDead` | **violated**, 819 states, depth 5 |
| `MC_pg_listener` | as above | `ObligationsAreDischargeable` | **violated**, 321 states, depth 4 |
| `MC_resume_gap` | **resume only** | `NoDeadDispatch` | **violated**, 15 201 states, depth 7 |
| `MC_pg_response` | + `ProjectedResponses` | `ResponsesNeverRegress` | **violated**, 3 872 states, depth 6 |
| `MC_pg_projection` | + `ProjectedResponses` | `ResponsesAreProjected` | **violated**, 788 states |
| `MC_liveness` | *none* | `TasksConverge` under `FairSpec` | **holds**, complete state space |

Before the response channel was added, `MC_spec` was exhaustive and clean:
21 874 654 states generated, 2 469 914 distinct, 0 left on queue, depth 25,
faults on, all 17 properties. Adding the response channel adds two properties
and three variables, so that run is being repeated; **until it finishes, the
19-property spec profile is not claimed.** The `MC_liveness` row is on the
17-property module.

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
| `MC_pg_response` → `ResponsesNeverRegress` | resonate-pg BUG-2, **response half** — `task.create` serves `_promise_json_raw`, so `promise.get` answers `rejected_timedout` and a later `task.create` answers `pending`, for the same promise |

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

## The response channel

Everything else in this model observes STATE. The three source models all did,
and resonate-pg's report names the consequence precisely:

> The model could not have found BUG-2's response half. `obs` records `Proj(i)`,
> not what each handler actually returned, so a handler serving an unprojected
> record is invisible to it.

`RespondP(i, ps)` closes that. It records, per promise, what a handler
**answered**:

- `resRegress` — a response answered `pending` for a promise some earlier
  response had already answered settled. This is settled-promise stability *on
  the wire*: the client-visible statement, which does not presuppose the
  projection discipline.
- `resUnprojected` — a response carried a record that is not the projection at
  the answering instant. The discipline itself; strictly stronger.

`promise.get` is the reference endpoint (always projected), and `task.create`'s
claim branch answers `Proj(i)` or the raw stored row according to
`ProjectedResponses`. The counterexample is 6 states:

| # | | |
|---|---|---|
| 2-3 | `promise.create(a, timeoutAt=1, target)`, claim it | |
| 4 | `Tick` → `now = 1`; `promise.get(a)` | answers `rejected_timedout`, `obsRes[a]` set |
| 5 | `task.create(a)` — ungated, serves the raw row | answers **`pending`** → `resRegress = {a}` |

Two endpoints, one promise, one instant, contradictory answers — reproduced
from the model rather than from reading the spec.

**What the channel does not yet cover.** It records promise records only. A
`task.get` action would answer `Proj(i)` — identical to `promise.get` —
modelling nothing extra, so it is deliberately absent; the task-side wire
contradiction (`task.get` says `fulfilled` while `task.halt` returns 200) is
still carried by the `badHalt` ghost rather than by a response. A real task
response channel needs `ProjTask` on its own axis, and status codes are not
modelled at all. So this is one axis of the response surface, not the surface.

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

- **Only one axis of the response surface.** Promise records on `promise.get`
  and `task.create`; no task records, no status codes, no `preload`. See "The
  response channel" above for what that leaves out.
- **No schedules** (S-01..S-04, R7), and no `task.fence` (T-04).
- **Small scope.** 2 ids / horizon 2 is enough for every violation above to
  appear at depth ≤ 7, but the negative result (`MC_spec`) is bounded evidence,
  not a proof. The specification's own theorems are the proof; this is a check.
- **Atomic handlers.** Each action is one atomic step. That reflects the
  advisory-lock / transaction discipline of the implementations rather than
  verifying it.
- **Read the liveness result narrowly.** `TasksConverge` holds under
  `FairSpec` over the complete state space (2 469 914 distinct; temporal check
  over 4 939 828 branches; 16m42s), with zero `UNCHANGED` warnings. The first
  attempt was discarded rather than reported: the fairness sub-actions were
  written `A /\ Next`, which expands `A` against every disjunct of `Step`, so
  one action's `UNCHANGED` contradicted another's assignments and TLC warned on
  each pair. Those conjuncts are semantically `FALSE`, so the formulation was
  wrong rather than unsound — but a liveness result computed through it is not
  worth reporting. `Next` now factors as `Step /\ ObsUpdate`. For the reason the convex run
  documented: within a bounded horizon every promise eventually times out, so
  the timeout rule alone discharges the "eventually". `TasksConverge` is
  evidence that deadlines converge a workflow, not that redelivery works.
