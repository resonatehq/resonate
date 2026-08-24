# Registration — plugging a worker into the server

Adding an integration is five edits. Nothing in `src/core/` changes: a new scheme is a
registration, by design.

Use `src/transport/transport_airflow.rs` and its config/registration as the template.

## 1. The worker — `src/transport/transport_<name>.rs`

```rust
/// Everything the worker holds. Shared by every run behind one `Arc`, so a
/// delivery costs a pointer clone rather than a copy of the config.
struct Airflow {
    /// The inbound port: how a run claims its task, heartbeats it, and settles
    /// its promise. This worker is in the server's own process, so it holds the
    /// port directly, but every state change still goes through `process` — the
    /// same path a remote worker's HTTP calls take.
    server: Arc<dyn ResonateServer>,
    client: reqwest::Client,
    deployments: HashMap<String, AirflowDeployment>,
    lease_timeout: i64,
    poll_interval: i64,
    max_poll_interval: i64,
}

pub struct AirflowWorker {
    inner: Arc<Airflow>,
}

impl AirflowWorker {
    pub fn new(
        server: Arc<dyn ResonateServer>,
        config: &AirflowConfig,
        lease_timeout: i64,
    ) -> Self {
        Self {
            inner: Arc::new(Airflow {
                server,
                client: reqwest::Client::new(),
                deployments: config.deployments.clone(),
                lease_timeout,
                poll_interval: config.poll_interval,
                max_poll_interval: config.max_poll_interval,
            }),
        }
    }
}

#[async_trait]
impl ResonateWorker for AirflowWorker {
    async fn send(&self, _address: &str, msg: &Message) -> Result<(), Unavailable> {
        // Only `execute` asks for work. An `unblock` is a notification for a
        // worker that is waiting on a promise; this worker never waits.
        let task = match msg {
            Message::Execute(e) => &e.data.task,
            Message::Unblock(_) => return Ok(()),
        };

        // Hand off, and do nothing else here.
        //
        // The first real step is claiming the task, and that is a server round
        // trip. `process_batch` awaits `send` sequentially over the whole
        // batch, so a round trip here would stall delivery of every other
        // message in it — including messages for other schemes. Validation
        // waits too: until the task is claimed the only way to report anything
        // is `Err(Unavailable)`, which the dispatch loop logs and drops.
        //
        // `send` means accepted for delivery, not executed.
        //
        // The `address` parameter goes unused here. It is what a *proxy* worker
        // needs — HTTP push has a URL to POST to, poll a group to fan out to —
        // and they have no promise in hand. This worker claims the task, so it
        // reads its address off the promise instead: one durable source of
        // truth, and the same one every other input comes from.
        let ctx = RunContext {
            worker: Arc::clone(&self.inner),
            task: task.clone(),
        };
        tokio::spawn(async move { ctx.run().await });
        Ok(())
    }
}

/// One delivery in flight. Two things and nothing else: the worker it belongs
/// to, and the task it was told about. Everything else comes from the promise,
/// once the task is claimed.
struct RunContext {
    worker: Arc<Airflow>,
    task: ExecuteMsgTask,
}

/// The address and deployment, resolved once the task is owned.
struct Target {
    addr: AirflowAddress,
    deployment: AirflowDeployment,
}
```

Note what `RunContext` is *not*. No `task_id`/`task_version` pair — that is
`ExecuteMsgTask`, one thing, and splitting it loses the name. No `address` — the promise
carries it, and the promise is the durable record every other input already comes from.
Two fields, because a run is a worker and a task.

`send` itself does almost nothing. Its whole job is to decide whether this message is
yours and then get off the dispatch thread — verbatim:

```rust
#[async_trait]
impl ResonateWorker for AirflowWorker {
    async fn send(&self, _address: &str, msg: &Message) -> Result<(), Unavailable> {
        // Only `execute` asks for work. An `unblock` is a notification for a
        // worker that is waiting on a promise; this worker never waits.
        let task = match msg {
            Message::Execute(e) => &e.data.task,
            Message::Unblock(_) => return Ok(()),
        };

        // Hand off, and do nothing else here.
        //
        // The first real step is claiming the task, and that is a server round
        // trip. `process_batch` awaits `send` sequentially over the whole
        // batch, so a round trip here would stall delivery of every other
        // message in it — including messages for other schemes. Validation
        // waits too: until the task is claimed the only way to report anything
        // is `Err(Unavailable)`, which the dispatch loop logs and drops.
        //
        // `send` means accepted for delivery, not executed.
        //
        // The `address` parameter goes unused here. It is what a *proxy* worker
        // needs — HTTP push has a URL to POST to, poll a group to fan out to —
        // and they have no promise in hand. This worker claims the task, so it
        // reads its address off the promise instead: one durable source of
        // truth, and the same one every other input comes from.
        let ctx = RunContext {
            worker: Arc::clone(&self.inner),
            task: task.clone(),
        };
        tokio::spawn(async move { ctx.run().await });
        Ok(())
    }
}
```

The work is in the spawned task, split where the error channel changes. `run` is the
protocol frame: claim, work, settle. `execute` is the work, and everything that can go
wrong in it comes back as one `MyError`, so `run` has exactly one place where the promise
is settled and one rule for when it is not.

```rust
/// The protocol frame: claim the task, do the work, settle the promise.
///
/// Everything that can go wrong inside `execute` comes back as one
/// `MyError`, so this body has exactly one place where the promise is
/// settled and one rule for when it is not.
async fn run(self) {
    // ── 1. Claim the task ────────────────────────────────────────────────
    //
    // Nothing may happen on behalf of a task this worker does not own — and
    // nothing can be *reported* until it does. Before the claim the only
    // outcome available is `Err(Unavailable)`, which the dispatch loop logs
    // and drops; after it, every failure can settle the promise. So the
    // claim comes first and validation comes after.
    //
    // Anything that is not "here is the task" — a 409 race, a transient
    // error, an unreachable server — means this attempt does not run.
    // Redelivery brings us back; there is nothing to decide between them.
    let claim = self
        .worker
        .server
        .process(Request::TaskAcquire(TaskAcquireData {
            id: self.task.id.clone(),
            version: self.task.version, // the fencing token from `execute`
            pid: PID.to_string(),
            ttl: self.worker.lease_timeout,
        }))
        .await;
    let Ok(Response::TaskAcquire(acquired)) = claim else {
        tracing::debug!(task_id = %self.task.id, "my: task not acquired");
        return;
    };
    let version = acquired.task.version; // the RESPONSE version (n+1), from here on
    let promise = acquired.promise; // param, timeoutAt, createdAt, tags

    // ── 2. Do the work, and settle with whatever it decided ──────────────
    //
    // Three arms because there are three outcomes. `Monitored` has no `Failed`
    // and `MyError` has no separate "gave up": a run that finished in a failure
    // state is a permanent error like any other, which is what keeps one arm
    // per outcome instead of one per variant.
    let (state, value) = match self.execute(&promise, version).await {
        Ok(Monitored::Succeeded { run, output }) => (SettleState::Resolved, value_of(run, output)),
        Err(MyError::Permanent { kind, message, run }) => {
            tracing::warn!(task_id = %self.task.id, kind, %message, "my: rejecting");
            (SettleState::Rejected, error_of(run, kind, message))
        }
        // Nothing to settle: the server settles a timed-out promise itself, and
        // a transient failure must be left for redelivery to retry.
        other => {
            tracing::warn!(task_id = %self.task.id, ?other, "my: promise left unsettled");
            return;
        }
    };

    let settled = self
        .worker
        .server
        .process(Request::TaskFulfill(TaskFulfillData {
            id: self.task.id.clone(),
            version,
            action: TaskFulfillAction {
                kind: "promise.settle".to_string(),
                data: TaskFulfillActionData {
                    id: self.task.id.clone(), // must equal the task id
                    state,
                    value, // the integration's own value schema, opaque here
                },
            },
        }))
        .await;

    match settled {
        Ok(Response::TaskFulfill(_)) => {
            tracing::info!(task_id = %self.task.id, ?state, "my: promise settled")
        }
        // The lease was lost, or the promise already settled — almost always a
        // timeout. Nothing is retryable either way.
        other => tracing::warn!(task_id = %self.task.id, ?state, ?other, "my: promise not settled"),
    }
}
```

```rust
/// Resolve, create, monitor. One error channel, so `?` does the work.
///
/// The two ways resolution fails are not the same failure: a malformed
/// address is the caller's error and can never become valid, because promise
/// tags are immutable, so it rejects the promise; an unconfigured deployment
/// is the operator's error that a rollout fixes, so it retries.
async fn execute(
    &self,
    promise: &PromiseRecord,
    version: i64,
) -> Result<Monitored, MyError> {
    // The address comes off the promise, not off the message: the promise is
    // the durable record, and it is where every other input already comes
    // from. A promise that has a task always carries this tag — that tag is
    // what caused the task to exist.
    let address = promise
        .tags
        .get(TARGET_TAG)
        .ok_or_else(|| MyError::permanent("invalid_request", format!("no {TARGET_TAG} tag")))?;
    let addr = MyAddress::parse(address)
        .map_err(|e| MyError::permanent("invalid_request", format!("bad address '{address}': {e}")))?;
    let deployment = self
        .worker
        .deployments
        .get(&addr.deployment)
        .cloned()
        .ok_or_else(|| MyError::transient(format!("no deployment '{}' configured", addr.deployment)))?;
    let target = Target { addr, deployment };

    let input = decode_param(promise.param.data.as_deref())?;
    let run_id = derive_run_id(&promise.id);

    // No `?` past this point: the heartbeat below has to be aborted, and an
    // early return would leave it beating for a lease nobody holds.
    let heartbeat = {
        // The lease clock. Its own task, on a cadence derived from the lease
        // TTL and nothing else — which is what lets the downstream clock back
        // off past the lease without the lease lapsing.
        let server = Arc::clone(&self.worker.server);
        let task_id = self.task.id.clone();
        let beat_ms = heartbeat_interval_ms(self.worker.lease_timeout);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(beat_ms));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                // Deliberately ignored. `task.heartbeat` answers 200 whether or
                // not it refreshed anything — a heartbeat for a lease this
                // worker no longer holds is a silent no-op — so the response
                // carries no signal. Losing the lease surfaces at
                // `task.fulfill`.
                let _ = server
                    .process(Request::TaskHeartbeat(TaskHeartbeatData {
                        pid: PID.to_string(),
                        tasks: vec![TaskHeartbeatTask { id: task_id.clone(), version }],
                    }))
                    .await;
            }
        })
    };

    // The downstream clock: create once, then poll on an interval sized for
    // the downstream system rather than for the lease.
    let outcome = self.create_and_monitor(&target, promise, &run_id, &input).await;
    heartbeat.abort();
    outcome
}
```

Three things to notice in the shape:

- **Every state change goes through `server.process`** — a typed `Request` in, a typed
  `Response` out. There is no envelope to build, no `kind` to spell as a string, and no
  response to pick apart: those are wire concerns, and an in-process worker has no wire.
- **The claim has one interesting case.** A 409 race, a transient error and an unreachable
  server are all "this attempt does not run"; there is nothing to decide between them, so
  they share one `let … else`. `Response::TaskAcquire` is the other.
- **Three outcomes: resolve, reject, leave alone.** That `match` is where an integration's
  whole error policy lives, and it stays three arms only because the two enums do not
  cross-cut — see below.

### Keep the outcome enums from cross-cutting

The obvious shapes give you five arms for three outcomes:

```rust
enum Monitored  { Succeeded { .. }, Failed { .. }, DeadlineReached }
enum MyError    { Permanent { .. }, Transient(..) }
```

`Monitored::Failed` and `MyError::Permanent` are both "reject with a reason";
`DeadlineReached` and `Transient` are both "do not settle". Fold the first pair together —
a downstream run that finished in a failure state *is* a permanent failure of the work, and
the run summary rides along on the error:

```rust
enum Monitored { Succeeded { run, output }, DeadlineReached }
enum MyError   { Permanent { kind, message, run }, Transient(String) }
```

Now each arm is one outcome, and the two that do not settle share a catch-all.

The only JSON left is the promise *value* — `value_of` and `error_of` build the
integration's own value schema, which the protocol carries as opaque bytes. See
`references/schemas.md`.

### Why the claim comes first

It is the line that divides "can be reported" from "cannot".

| | Before the claim | After the claim |
|---|---|---|
| Ways to report a failure | `Err(Unavailable)` only | Settle the promise, or drop the task |
| What the dispatch loop does with it | Logs it and drops it | — |
| What the caller sees | Nothing, until `rejected_timedout` | The reason, immediately |
| What redelivery does | Repeats the same failure every `retry_timeout` | Nothing — the promise is settled |

So push everything you can past the claim. A malformed address validated in `send` becomes
a log line every 30 seconds until the promise times out; validated after the claim, it
becomes `rejected` with `kind = "invalid_request"` and the address quoted.

### The two failure classes, once you own the task

Owning the task does not make every failure a rejection. `resolve_target` produces both:

| | Malformed address | Unconfigured deployment |
|---|---|---|
| Whose mistake | The caller's | The operator's |
| Fixable without a new promise | No — promise tags are immutable | Yes — deploy the config |
| Classification | `Permanent` | `Transient` |
| What the worker does | `task.fulfill` → `rejected` | Return without settling |
| What happens next | Caller sees the reason | Lease expires, redelivery retries |

### What must never go in `send`

`process_batch` awaits `route` sequentially over the batch:

```rust
for msg in execute_msgs {
    if let Err(e) = router.route(&msg.address, &payload).await { tracing::warn!(...) }
}
```

Anything slow there stalls every other message in the batch, including other schemes'.

| Never in `send` | Why |
|---|---|
| `task.acquire` | A server round trip per message, serialised |
| The downstream call | Can take hours |
| Retries or backoff | Blocks the batch for the duration |
| Validation that could reject the promise | Its failures are unreportable until the claim |
| Blocking I/O, `std::fs`, `block_on` | Stalls the runtime thread |

What is left is the message-kind match and the spawn. That is the whole of `send`.

Keep the pure parts (address parsing, the idempotency key, param decoding, downstream state
classification) as free functions with unit tests. Those are the parts that carry the
design, and they test without a server or a network.

## 2. Declare the module — `src/transport/mod.rs`

```rust
pub mod transport_my;
```

## 3. Configuration — `src/config.rs`

Add a field to `TransportsConfig`:

```rust
/// My integration configuration
#[serde(default)]
pub my: MyConfig,
```

and the config type. Follow `AirflowConfig`:

- `enabled: bool`, defaulting to `false`. An unregistered scheme is undeliverable, not
  broken, so shipping disabled is safe.
- `lease_timeout: Option<i64>` with `resolve_lease_timeout(&TasksConfig)` falling back to
  `tasks.lease_timeout`. The lease only has to outlast one heartbeat interval plus jitter,
  not the downstream run.
- A `deployments: HashMap<String, MyDeployment>` map when addresses name a deployment.
  **Credentials live here, never in an address**, and reach production through the
  environment: `RESONATE_TRANSPORTS__MY__DEPLOYMENTS__PROD__TOKEN=…`.

Then extend `Config::validate` with the failures that would otherwise be silent:

```rust
// task.acquire validates ttl >= 1, so a non-positive lease makes every
// acquire 400 and the worker silently never runs anything.
if let Some(ttl) = self.transports.my.lease_timeout {
    if ttl < 1 { return Err(...); }
}
// An enabled worker with no deployments can never deliver anything.
if self.transports.my.enabled && self.transports.my.deployments.is_empty() {
    return Err(...);
}
```

Validate anything whose failure mode is "the worker accepts messages and quietly does
nothing". Those are the bugs that cost hours.

## 4. Register the scheme — `src/main.rs`

Inside `run_server`, before the router is built:

```rust
if state.config.transports.my.enabled {
    tracing::info!(deployments = ?state.config.transports.my.deployments.keys().collect::<Vec<_>>(),
                   "My integration enabled");
    workers.insert(
        "my".to_string(),
        Arc::new(transport::transport_my::MyWorker::new(
            Arc::clone(&server),
            &state.config.transports.my,
            state.config.transports.my.resolve_lease_timeout(&state.config.tasks),
        )),
    );
}
```

`server` is `Arc<dyn ResonateServer>`, cloned from the concrete `Server`. `Server` never
holds the router, which is what keeps this a DAG.

One scheme may map to several registrations (`http` and `https` share the push worker), and
a worker may serve every address of its scheme including malformed ones — rejecting those
is the worker's job, not the router's.

## 5. Tests

- **Unit** — address parsing (including every malformed shape), the idempotency key
  (determinism, charset, collision after truncation), param decoding, downstream state
  classification. All pure, all fast.
- **End-to-end** — run the server against an API double for the downstream system, create a
  promise targeted at the new scheme, and assert the promise value.
- **The one that matters** — `SIGKILL` the server between downstream-create and settle,
  restart against the same storage, and assert **exactly one** downstream run exists for
  that promise and the promise still resolves.

## Checklist

- [ ] `src/transport/transport_<name>.rs` implementing `ResonateWorker`
- [ ] `pub mod` in `src/transport/mod.rs`
- [ ] Config struct, field on `TransportsConfig`, `validate` rules
- [ ] `workers.insert("<scheme>", …)` in `src/main.rs`, gated on `enabled`
- [ ] Unit tests for the pure helpers
- [ ] A crash-and-restart test proving one downstream run
- [ ] Address, param and value schemas documented in the module's doc comment
