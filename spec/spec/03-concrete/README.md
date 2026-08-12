# The handler catalogue

The concrete machine's transitions. The catalogue numbers are the index;
the handlers themselves live in [`external-steps-p.lean`](external-steps-p.lean)
and its `-m` twin [`external-steps-m.lean`](external-steps-m.lean), in this
order, line-aligned with each other.

The τs are in [`internal-steps-p.lean`](internal-steps-p.lean) and
[`internal-steps-m.lean`](internal-steps-m.lean) — one pair per discipline,
unlike `02-abstract`, whose internal steps are shared. That difference is
the machines': the concrete τs move obligation records, so the two
disciplines see different ones.

## Promises

| | Handler | Transition |
|---|---|---|
| P-01 | `promise.get` | Read a promise (with timeout projection). |
| P-02 | `promise.create` | Create a pending promise; a `resonate:target` tag also spawns a task and an `execute` message, optionally delayed. |
| P-03 | `promise.settle` | Settle a pending promise: fulfill its task, notify listeners, resume awaiters. |
| P-04 | `promise.register_callback` | Subscribe an awaiter promise for resume when the awaited promise settles. |
| P-05 | `promise.register_listener` | Subscribe an address for an `unblock` message when the promise settles (external promises only). |
| P-06 | `promise.search` | Not yet specified (`501`). |

## Tasks

| | Handler | Transition |
|---|---|---|
| T-01 | `task.get` | Read a task (projected `fulfilled` once its promise is no longer pending). |
| T-02 | `task.create` | Create a promise with an immediately-acquired task, or re-acquire an existing pending task. |
| T-03 | `task.acquire` | Worker claims a pending task: bump version, arm the lease. |
| T-04 | `task.fence` | Run a `promise.create`/`promise.settle` guarded by the task's fencing token. |
| T-05 | `task.heartbeat` | Extend the leases of a worker's acquired tasks. |
| T-06 | `task.suspend` | Park an acquired task on awaited promises; `300` if any is already settled. |
| T-07 | `task.fulfill` | Settle the task's promise and fulfill the task in one transition. |
| T-08 | `task.release` | Return an acquired task to pending and re-enqueue its `execute`. |
| T-09 | `task.halt` | Take a task out of circulation. |
| T-10 | `task.continue` | Return a halted task to pending and re-enqueue its `execute`. |
| T-11 | `task.search` | Not yet specified (`501`). |

## Schedules

| | Handler | Transition |
|---|---|---|
| S-01 | `schedule.get` | Read a schedule. |
| S-02 | `schedule.create` | Create a schedule and arm its first fire. |
| S-03 | `schedule.delete` | Delete a schedule and disarm its timeout. |
| S-04 | `schedule.search` | Not yet specified (`501`). |

## Internal Transitions

| Handler | Transition |
|---|---|
| `resume` | Drain a deferred resume: wake a suspended awaiter (re-pending + `execute`) or record the trigger on an active one; the deadline guard re-checks at drain time (timeout always wins). |
| `timeouts` | Environment-fired transitions: promise timeout, task retry, lease expiry, schedule fire (with catch-up). Each re-checks its own due time — an armed timer means *not before*. |
