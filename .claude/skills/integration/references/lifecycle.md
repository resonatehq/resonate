# Lifecycle: acquire, create, monitor, settle

The worked implementation is `RunContext` in `src/transport/transport_airflow.rs`. This is
the reasoning behind it.

```
execute ──► task.acquire ──► [1] create ──► [2] monitor ──► [3] settle ──► task.fulfill
               │  409                          ▲     │
               └─ drop                         └─────┘   poll until terminal
```

## Phase 0 — Acquire

```rust
let acquired = match self.acquire(&pid).await { Some(a) => a, None => return };
let version = acquired.task.version;   // response version, NOT the message version
let promise = acquired.promise;
```

- **Use `task.version` from the response** — the request version plus one. Every later
  `heartbeat`, `release` and `fulfill` must carry it, or they get `409`.
- **`409` is normal.** The message was a duplicate you lost the race for. Log at debug,
  drop, and never touch the downstream system.
- **Any other non-200 is transient.** Drop the task and let the lease expire; do not settle
  the promise on a server-side hiccup.
- **Pick `ttl` from downstream *create* latency, not run duration.** Heartbeats extend it;
  it only has to outlast one heartbeat interval plus jitter.

Start the heartbeat immediately after acquiring, **before** the downstream create — a slow
create is exactly when the lease is most likely to lapse.

```rust
let heartbeat = self.spawn_heartbeat(pid.clone(), version);   // ticks at ttl/3
let outcome = self.create_and_monitor(&promise).await;
heartbeat.abort();
```

## Phase 1 — Create

**Runs on every delivery.** Do not try to detect whether an earlier attempt already created
the run — you cannot, and the design does not require it.

```rust
let input  = decode_param(promise.param.data.as_deref())?;
let run_id = derive_run_id(&promise.id);      // pure function of the promise id
self.create_dag_run(&run_id, &input).await?;  // 409 ⇒ Ok(()), re-attach
```

- **Deterministic key**, derived from `promise.id` — and optionally `promise.created_at`,
  which is also fixed at creation. Never a UUID, a clock read, a hostname, or
  `task.version`.
- **Conflict means success.** Handling the duplicate code *is* the recovery path. An
  integration that logs 409 as an error and gives up is broken.
- **Validate the param before the first side effect.** A schema violation settles
  `rejected` on delivery one; it will never become valid on retry, and leaving it pending
  burns a redelivery every 30 s until the promise times out.

## Error classification

Every downstream error is exactly one of three things, and they are handled differently.
Getting this table right is most of what separates a solid integration from a flaky one.

| Bucket | Examples | Action |
|---|---|---|
| **Permanent** | 400, 404 unknown DAG, 422 invalid conf, unparseable address, malformed param | `task.fulfill` → `rejected` with a structured error. No retry. |
| **Transient** | connection refused, 429, 5xx, timeout | **Do not settle.** Return without fulfilling; the lease expires and the message is redelivered. |
| **Ambiguous** | request sent, response lost; 500 with no body | Treat as transient. The next attempt re-issues the idempotent create and learns the truth. **This bucket is why idempotency is mandatory.** |

In the Airflow worker this is an enum, so the compiler forces a decision at every call
site:

```rust
enum AirflowError {
    Permanent { kind: &'static str, message: String },
    Transient(String),
}
```

`401`/`403` deserve a note: permanent for *this* request, but usually an operational
problem (expired token) rather than an application error. The Airflow worker rejects them
so the failure is visible rather than an invisible retry loop; if a deployment rotates
credentials routinely, treating them as transient with alerting is the better trade.

## Phase 2 — Monitor

### Shape A — in-process loop (default)

```rust
let mut interval = self.poll_interval;
loop {
    let now = system_time_ms();
    if now >= promise.timeout_at { return Ok(Monitored::DeadlineReached); }
    match self.get_dag_run(&run_id).await? {
        RunState::Pending => {}
        RunState::Succeeded(run) => return Ok(Monitored::Succeeded(run)),
        RunState::Failed(run)    => return Ok(Monitored::Failed(run)),
    }
    let sleep_ms = interval.min(promise.timeout_at - now).max(0) as u64;
    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    interval = (interval * 2).min(self.max_poll_interval);   // back off
}
```

Restart-safe **because create is idempotent**: a crash drops the lease, the server
redelivers within the lease timeout, the new attempt re-creates (409 → attach) and resumes.
Cost: one lease, one heartbeat and one task per active run, and a restart re-polls
everything it was watching.

Back off with a cap. A DAG that runs for six hours must not be polled every five seconds
for six hours.

**Map states exhaustively, and never guess:**

| Downstream | Promise |
|---|---|
| Completed successfully | `resolved`, value carries `output` |
| Failed / errored | `rejected`, `error.kind = "downstream_failed"` |
| Queued / running | not terminal — keep monitoring |
| **Unrecognised state** | **not terminal** — keep monitoring and log loudly |

Treating an unknown state as failure turns a downstream upgrade into a fleet of spuriously
rejected promises. Treating it as success is worse. `classify_run` in the Airflow worker
has a unit test pinning this.

### Shape B — durable suspend (scale-out)

The worker holds no state and no lease between polls. Poll once; if the run is still going,
create a self-resolving timer promise and suspend the task on it. When the timer resolves,
the server sends a fresh `execute` and the worker re-enters at Phase 1 — re-creating
idempotently, re-attaching, and polling again.

```rust
// promise.create
{ "id": format!("{}:timer.{}", promise.id, version),   // unique per acquisition
  "timeoutAt": wake_at,                                 // must be > now
  "param": {}, "tags": { "resonate:timer": "true" } }   // timers RESOLVE at timeoutAt

// task.suspend
{ "id": task_id, "version": version,
  "actions": [ { "kind": "promise.register_callback", "head": {},
                 "data": { "awaited": timer_id, "awaiter": task_id } } ] }
```

Details that matter:

- **`resonate:timer: "true"` makes the promise resolve rather than reject** at `timeoutAt`.
  Without it you get a misleading `rejected_timedout` promise (which still wakes the task).
- **`origin(awaited)` must equal `origin(awaiter)`** — origin is everything before the first
  `:`. Naming the timer `<promiseId>:timer.<n>` satisfies this whether or not the promise
  id already contains a `:`.
- **Derive the suffix from `task.version`**, which increments on every acquisition. Not from
  `task.resumes` — `task.acquire` always reports it as `0`. Reusing a resolved timer id
  yields `300` (immediate resume) and a busy loop.
- **Guard `wake_at > now`**, or the timer resolves instantly and suspend returns `300`
  forever.
- **Orphan timers are expected.** A crash between `promise.create` and `task.suspend`
  leaves a timer nobody awaits. It resolves harmlessly; do not build cleanup for it.
- Cost: two extra RPCs and one promise per poll, plus one `execute` per cycle.

Start with A. Move to B when lease churn or memory becomes the bottleneck, or when runs
routinely outlive server deployments.

## Phase 3 — Settle

```rust
self.settle(version, "resolved", json!({ "run": …, "output": … })).await;
```

`task.fulfill` completes task and promise in one transaction — this is what finally stops
redelivery. `action.data.id` must equal the task id. `value.data` is base64-encoded UTF-8
JSON, matching what the CLI and SDKs produce.

**On `409`:** the lease was lost, or the promise already settled (usually a timeout). Never
retry. Read the promise if you need to know which.

## Timeouts and orphans

`promise.timeoutAt` is a hard deadline the server enforces independently of any worker:
the promise is settled `rejected_timedout`, the task is marked `fulfilled`, and **the worker
is not notified**. The downstream run keeps going.

That orphan is a design decision:

- **Leave it running (what the Airflow worker does).** Simplest and usually right — the run
  may still be valuable, and cancelling costs downstream state. Log the run id so an
  operator can find it.
- **Cancel on deadline.** Watch the clock, cancel downstream at `timeoutAt - grace`, then
  attempt `task.fulfill` with `rejected_canceled`. It races the server and often returns
  `409`; the cancel is the part that matters. Gate it behind an option — it is not safe for
  every downstream system.
- **Reap out of band.** `promise.register_listener` plus a separate reaper. Most robust,
  most machinery; worth it only when orphans are expensive.

Set `timeoutAt` from the downstream SLA plus headroom. Too tight and healthy runs get
orphaned; too loose and stuck runs are never noticed.

## Observability

Make the integration debuggable from the promise alone:

- Tag integration promises so `promise.search` can find them.
- Log `task_id`, `version`, the downstream run id and the address together on every
  transition. The run id is derived from the promise id, so each is reachable from the
  other.
- Put a downstream UI link in the value on **both** branches. The first question about a
  failed promise is "where do I look?".
- Count acquires, `409` acquires, creates, **create-conflicts**, polls, resolves, rejects,
  deadline hits. A conflict rate that suddenly drops to zero usually means the idempotency
  key stopped being deterministic.
