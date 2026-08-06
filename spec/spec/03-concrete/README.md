# The handler catalogue

The concrete machine's transitions, one file per handler. Links go to the
`-p` (projected) variant; [`m/`](m) holds the `-m` (materialized) twin,
line-aligned handler for handler.

## Promises

| | Handler | Transition |
|---|---|---|
| P-01 | [`promise.get`](p/P-01-promise.get.lean) | Read a promise (with timeout projection). |
| P-02 | [`promise.create`](p/P-02-promise.create.lean) | Create a pending promise; a `resonate:target` tag also spawns a task and an `execute` message, optionally delayed. |
| P-03 | [`promise.settle`](p/P-03-promise.settle.lean) | Settle a pending promise: fulfill its task, notify listeners, resume awaiters. |
| P-04 | [`promise.register_callback`](p/P-04-promise.register_callback.lean) | Subscribe an awaiter promise for resume when the awaited promise settles. |
| P-05 | [`promise.register_listener`](p/P-05-promise.register_listener.lean) | Subscribe an address for an `unblock` message when the promise settles (external promises only). |
| P-06 | [`promise.search`](p/P-06-promise.search.lean) | Not yet specified (`501`). |

## Tasks

| | Handler | Transition |
|---|---|---|
| T-01 | [`task.get`](p/T-01-task.get.lean) | Read a task (projected `fulfilled` once its promise is no longer pending). |
| T-02 | [`task.create`](p/T-02-task.create.lean) | Create a promise with an immediately-acquired task, or re-acquire an existing pending task. |
| T-03 | [`task.acquire`](p/T-03-task.acquire.lean) | Worker claims a pending task: bump version, arm the lease. |
| T-04 | [`task.fence`](p/T-04-task.fence.lean) | Run a `promise.create`/`promise.settle` guarded by the task's fencing token. |
| T-05 | [`task.heartbeat`](p/T-05-task.heartbeat.lean) | Extend the leases of a worker's acquired tasks. |
| T-06 | [`task.suspend`](p/T-06-task.suspend.lean) | Park an acquired task on awaited promises; `300` if any is already settled. |
| T-07 | [`task.fulfill`](p/T-07-task.fulfill.lean) | Settle the task's promise and fulfill the task in one transition. |
| T-08 | [`task.release`](p/T-08-task.release.lean) | Return an acquired task to pending and re-enqueue its `execute`. |
| T-09 | [`task.halt`](p/T-09-task.halt.lean) | Take a task out of circulation. |
| T-10 | [`task.continue`](p/T-10-task.continue.lean) | Return a halted task to pending and re-enqueue its `execute`. |
| T-11 | [`task.search`](p/T-11-task.search.lean) | Not yet specified (`501`). |

## Schedules

| | Handler | Transition |
|---|---|---|
| S-01 | [`schedule.get`](p/S-01-schedule.get.lean) | Read a schedule. |
| S-02 | [`schedule.create`](p/S-02-schedule.create.lean) | Create a schedule and arm its first fire. |
| S-03 | [`schedule.delete`](p/S-03-schedule.delete.lean) | Delete a schedule and disarm its timeout. |
| S-04 | [`schedule.search`](p/S-04-schedule.search.lean) | Not yet specified (`501`). |

## Internal Transitions

| Handler | Transition |
|---|---|
| [`resume`](p/03-resume.lean) | Drain a deferred resume: wake a suspended awaiter (re-pending + `execute`) or record the trigger on an active one; the deadline guard re-checks at drain time (timeout always wins). |
| [`timeouts`](p/02-timeouts.lean) | Environment-fired transitions: promise timeout, task retry, lease expiry, schedule fire (with catch-up). Each re-checks its own due time — an armed timer means *not before*. |
