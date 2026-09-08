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
let heartbeat = self.spawn_heartbeat(pid.clone(), version);   // lease clock
let outcome = self.create_and_monitor(&promise).await;        // downstream clock
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

Monitoring is **two independent clocks**, and collapsing them into one loop is the most
common way to get this phase wrong.

| | Lease clock | Downstream clock |
|---|---|---|
| Does what | `task.heartbeat` so the server does not redispatch the task | Asks the external system whether the run finished |
| Cadence set by | The lease TTL you passed to `task.acquire` — a third of it | The downstream system's cost, rate limits and latency |
| Typical | every 5–10 s | every 5 s, backing off to minutes |
| Consequence of getting it wrong | Lease lapses; the task is redispatched and a second attempt starts | Wasted API calls, or a promise that settles later than it could |

They answer to different authorities, so they belong in different tasks:

```rust
// Lease clock — its own task, cadence from the lease and nothing else.
let heartbeat = self.spawn_heartbeat(pid.clone(), version);

// Downstream clock — sleeps as long as the downstream system warrants.
let outcome = self.create_and_monitor(&promise).await;

heartbeat.abort();
```

Fold the heartbeat into the poll loop and the two become one cadence, which fails in both
directions: a poll interval that backs off past the lease TTL silently drops the task, and
a poll interval short enough to keep the lease alive hammers the downstream API. Keeping
them separate is precisely what lets `max_poll_interval` exceed `lease_timeout` safely —
and for long-running jobs it should.

**Two facts about the lease clock:**

- **`task.heartbeat` answers `200` whether or not it refreshed anything.** The storage
  update is guarded on `state = 'acquired'` at the right version and pid; when the guard
  fails it updates zero rows and reports nothing back. A worker therefore *cannot* learn
  from a heartbeat that it lost its lease — ignore the response, and say so in a comment so
  the next reader does not mistake it for sloppiness. Lease loss surfaces at
  `task.fulfill`, as a `409`.
- **The cadence must fit inside the lease.** `ttl / 3` leaves room to miss two beats, but a
  floor added to avoid hammering (`(ttl / 3).max(1000)`) will push the first beat past a
  lease shorter than the floor. Clamp against the lease as well:

  ```rust
  fn heartbeat_interval_ms(lease_timeout: i64) -> u64 {
      let third = (lease_timeout / 3).max(1_000);
      third.min((lease_timeout / 2).max(1)).max(1) as u64
  }
  ```

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
Cost: one lease, one heartbeat task and one poll task per active run, and a restart
re-polls everything it was watching.

Note what the loop does *not* do: it never heartbeats. That is the other clock's job, and
the two only meet when the poll loop returns and the heartbeat is aborted.

Back off with a cap. A DAG that runs for six hours must not be polled every five seconds
for six hours.

**Map states exhaustively, and never guess:**

| Downstream | Promise |
|---|---|
| Completed successfully | `resolved`, value carries `output` |
| Failed / errored | `rejected`, `error.kind = "downstream_failed"` |
| Cancelled in the downstream system | `rejected`, `error.kind = "canceled"` |
| Queued / running | not terminal — keep monitoring |
| **Unrecognised state** | **not terminal** — keep monitoring and log loudly |

Note the two cancellations, which are not the same event. A run cancelled **in the
downstream system** settles `rejected` with `kind = "canceled"`. The promise state
`rejected_canceled` is reserved for cancellation initiated **through Resonate** — including
the deadline-driven cancel below. One rule, every integration, so a caller does not have to
know which one it is talking to.

Treating an unknown state as failure turns a downstream upgrade into a fleet of spuriously
rejected promises. Treating it as success is worse. `classify_run` in the Airflow worker
has a unit test pinning this.

### Shape B — durable suspend (scale-out)

The worker holds no state and no lease between polls. Poll once; if the run is still going,
create a self-resolving timer promise and suspend the task on it. When the timer resolves,
the server sends a fresh `execute` and the worker re-enters at Phase 1 — re-creating
idempotently, re-attaching, and polling again.

```rust
let timer_id = format!("{}:timer.{}", promise.id, version);   // unique per acquisition

server.process(Request::PromiseCreate(PromiseCreateData {
    id: timer_id.clone(),
    timeout_at: wake_at,                                       // must be > now
    param: PromiseValue::default(),
    // Timers RESOLVE at timeoutAt rather than rejecting.
    tags: HashMap::from([("resonate:timer".to_string(), "true".to_string())]),
})).await?;

server.process(Request::TaskSuspend(TaskSuspendData {
    id: task_id.clone(),
    version,
    actions: vec![TaskSuspendAction {
        kind: "promise.register_callback".to_string(),
        data: PromiseRegisterCallbackData { awaited: timer_id, awaiter: task_id },
    }],
})).await?;
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
- **There is no lease clock at all.** The worker holds no lease between polls, so the
  heartbeat disappears and the timer promise's `timeoutAt` becomes the only cadence. That
  is the shape's real attraction for long-running jobs: one clock instead of two, and
  nothing to keep alive while the downstream system takes its hours.

Start with A. Move to B when lease churn or memory becomes the bottleneck, or when runs
routinely outlive server deployments.

## Phase 3 — Settle

```rust
self.settle(version, SettleState::Resolved, value_of(run, output)).await;
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

**The default is to leave it running**, and every integration does that unless it says
otherwise. It is usually right — the run may still be valuable, and cancelling costs
downstream state — and it is the behaviour a reader should be able to assume without
checking. Log the run id so an operator can find the orphan.

Two deviations, both of which belong in the README's *Limitations* if you take them:

- **Cancel on deadline.** Watch the clock, cancel downstream at `timeoutAt - grace`, then
  attempt `task.fulfill` with `rejected_canceled` — Resonate initiated this one, so it is
  the `rejected_canceled` case rather than `kind = "canceled"`. It races the server and
  often returns `409`; the cancel is the part that matters. Gate it behind a config option,
  off by default: it is not safe for every downstream system.
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
