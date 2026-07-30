# Resonate Specification

The Resonate protocol, specified as an executable **abstract machine** in Lean 4: a state, a set of effects (atomic operations on the state) and a set of request handlers (transitions composed from effects).

The machine comes in **twin variants** over one state and one wire surface, differing only in read discipline:

- **`-p`, the projected machine** ([`spec/03-concrete/p`](spec/03-concrete/p)) — a read serves the *projection* of a timed-out object and writes nothing; the timeout transition persists the fact later.
- **`-m`, the materialized machine** ([`spec/03-concrete/m`](spec/03-concrete/m)) — a read *materializes* first, by firing the anticipated timeout transition at the moment of observation, then serves stored state. Handlers are line-aligned with `-p`: reads become touches, `project` disappears from responses, the extra writes hide inside the touch.

Above both concrete variants sits the **abstract machine** ([`spec/02-abstract`](spec/02-abstract)) — the coalesced model: no timeout components (deadlines live on the objects — `timeoutAt`, `expiresAt`, `retryAt`, `nextRunAt`), no deferred queue (awaiters and listeners stay on the settled promise, drained by chosen-element rules), no config (cadence is a rule parameter), and materialize-on-touch in place of projection. **The concrete machine refines the abstract one** (`ConcreteRefinesAbstract`, [`spec/04-theorems`](spec/04-theorems)): every valid concrete trace has a valid abstract trace with the same externalized behavior and the same messages at quiescence, via the purely structural abstraction `alpha` (deferred entries fold back into callbacks, task timers become the task's own deadlines) and a syntactic rule translation as the constructive schedule — executed on the battery and exhaustively at small scope, all kernel `decide`. This is exactly the refinement that was *impossible* against the unfixed machine: the three fixes below were its obstructions.

The abstract machine itself comes in the same two read disciplines — [`p.lean`](spec/02-abstract/p.lean) serves the views, [`m.lean`](spec/02-abstract/m.lean) persists them, over shared `state.lean` and `rules.lean` (rules are material transitions; the read discipline concerns handlers only). The square is sharper at this level: because obligations are retained on the objects and no handler reads raw state, the two disciplines answer **identically under any shared rule schedule** — checked exhaustively with the adversarial rules in the alphabet — and only the message channel (raw choice-rules R5/R6 on fact-lagged tasks) requires each machine its own schedule: `IndistinguishableAbstract` in [`abstract-twins.lean`](spec/04-theorems/abstract-twins.lean), witness and repair included.

Between the concrete variants themselves: `-p` and `-m` are **weakly bisimilar** (`MRefinesP` and `PRefinesM`, together `Indistinguishable`): every valid trace of one has a valid trace of the other with the same externalized behavior — internal (τ) steps silent, each machine scheduling its own — and the same messages at quiescence. The τ schedule genuinely cannot be shared: -m's touches move the obligation records that enable τs, and [`spec/04-theorems/lockstep.lean`](spec/04-theorems/lockstep.lean) machine-checks the distinguishing trace. See [`spec/04-theorems`](spec/04-theorems): traces and `Valid` follow the trace framework (a trace is an infinite state-action sequence; validity is step-connectivity plus the monotone clock), the claims are in [`equivalence.lean`](spec/04-theorems/equivalence.lean), and both constructive directions are executed — the lazy schedule ([`lazy.lean`](spec/04-theorems/lazy.lean)) and the eager schedule ([`eager.lean`](spec/04-theorems/eager.lean)) — on a targeted battery and exhaustively on every script up to length 4 over a 9-request alphabet (7 381 scripts each) — all by kernel `decide`; the spec's string scans are structurally recursive precisely so no proof needs `native_decide`. 

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

Handlers touch state only through effects. Together they are the contract a concrete implementation must realize. Settlement's write set, restricted to promises and tasks, is `{p.id}`: settle neither reads nor writes any promise but its own — resumes are recorded as deferred work, discharged by the drain.

### Handlers

Every handler is a pure function

```lean
Req → (now : Nat) → M Res    -- M = StateM ServerState
```

composed from effects. Deterministic and total; there is no hidden clock — time enters only through `now`.

Conventions the whole model leans on:

- **Projection** — a pending promise past `timeoutAt` is *observed* as already settled (`resolved` for timers, `rejectedTimedout` otherwise) even before its timeout transition persists that fact.
- **Validation** — anything rejectable by inspecting the request alone is `400`, with highest precedence: before existence, state, or version are consulted. Examples: settling to a non-settable state, self-await, duplicate or empty awaited lists, an untargeted `task.create` action, an undeliverable listener address.

## Protocol Handlers

### Promises

| | Handler | Transition |
|---|---|---|
| P-01 | [`promise.get`](spec/03-concrete/p/P-01-promise.get.lean) | Read a promise (with timeout projection). |
| P-02 | [`promise.create`](spec/03-concrete/p/P-02-promise.create.lean) | Create a pending promise; a `resonate:target` tag also spawns a task and an `execute` message, optionally delayed. |
| P-03 | [`promise.settle`](spec/03-concrete/p/P-03-promise.settle.lean) | Settle a pending promise: fulfill its task, notify listeners, resume awaiters. |
| P-04 | [`promise.register_callback`](spec/03-concrete/p/P-04-promise.register_callback.lean) | Subscribe an awaiter promise for resume when the awaited promise settles. |
| P-05 | [`promise.register_listener`](spec/03-concrete/p/P-05-promise.register_listener.lean) | Subscribe an address for an `unblock` message when the promise settles. |
| P-06 | [`promise.search`](spec/03-concrete/p/P-06-promise.search.lean) | Not yet specified (`501`). |

### Tasks

| | Handler | Transition |
|---|---|---|
| T-01 | [`task.get`](spec/03-concrete/p/T-01-task.get.lean) | Read a task (projected `fulfilled` once its promise is no longer pending). |
| T-02 | [`task.create`](spec/03-concrete/p/T-02-task.create.lean) | Create a promise with an immediately-acquired task, or re-acquire an existing pending task. |
| T-03 | [`task.acquire`](spec/03-concrete/p/T-03-task.acquire.lean) | Worker claims a pending task: bump version, arm the lease. |
| T-04 | [`task.fence`](spec/03-concrete/p/T-04-task.fence.lean) | Run a `promise.create`/`promise.settle` guarded by the task's fencing token. |
| T-05 | [`task.heartbeat`](spec/03-concrete/p/T-05-task.heartbeat.lean) | Extend the leases of a worker's acquired tasks. |
| T-06 | [`task.suspend`](spec/03-concrete/p/T-06-task.suspend.lean) | Park an acquired task on awaited promises; `300` if any is already settled. |
| T-07 | [`task.fulfill`](spec/03-concrete/p/T-07-task.fulfill.lean) | Settle the task's promise and fulfill the task in one transition. |
| T-08 | [`task.release`](spec/03-concrete/p/T-08-task.release.lean) | Return an acquired task to pending and re-enqueue its `execute`. |
| T-09 | [`task.halt`](spec/03-concrete/p/T-09-task.halt.lean) | Take a task out of circulation. |
| T-10 | [`task.continue`](spec/03-concrete/p/T-10-task.continue.lean) | Return a halted task to pending and re-enqueue its `execute`. |
| T-11 | [`task.search`](spec/03-concrete/p/T-11-task.search.lean) | Not yet specified (`501`). |

### Schedules

| | Handler | Transition |
|---|---|---|
| S-01 | [`schedule.get`](spec/03-concrete/p/S-01-schedule.get.lean) | Read a schedule. |
| S-02 | [`schedule.create`](spec/03-concrete/p/S-02-schedule.create.lean) | Create a schedule and arm its first fire. |
| S-03 | [`schedule.delete`](spec/03-concrete/p/S-03-schedule.delete.lean) | Delete a schedule and disarm its timeout. |
| S-04 | [`schedule.search`](spec/03-concrete/p/S-04-schedule.search.lean) | Not yet specified (`501`). |

### Internal Transitions

| Handler | Transition |
|---|---|
| [`resume`](spec/03-concrete/p/03-resume.lean) | Drain a deferred resume: wake a suspended awaiter (re-pending + `execute`) or record the trigger on an active one; the deadline guard re-checks at drain time (timeout always wins). |
| [`timeouts`](spec/03-concrete/p/02-timeouts.lean) | Environment-fired transitions: promise timeout, task retry, lease expiry, schedule fire (with catch-up). Each re-checks its own due time — an armed timer means *not before*, enforced by the machine, not trusted to the environment. |

## Build

```
cd spec && lake build
```
