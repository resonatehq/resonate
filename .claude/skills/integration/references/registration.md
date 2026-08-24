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

The work is in the spawned task, in three layers that each speak one vocabulary. `run` is
the protocol frame — claim, ask, settle — and knows nothing about the downstream system.
`execute` decides what the promise becomes. `work` does the work, with one error channel so
`?` carries the failures.

Keeping `run` at the protocol's altitude is the point of the split: it hands off to
something unspecified and gets back a `Settlement`, so a downstream run, an error kind and
a value schema never appear in it.

`run` and `execute` below are complete — they are the same in every integration. `work` is
a **sketch**: its shape is fixed (resolve, start, watch, report) but every line of it is
yours. `src/transport/transport_airflow.rs` is one filled-in version.

```rust
/// The protocol frame: claim the task, do the work, settle the promise.
///
/// Nothing here is integration-specific. It never sees a downstream run, an
/// error kind or a value schema — `execute` decides all of that and hands
/// back a `Settlement`, and this body's whole job is to apply it.
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
    let Some((state, value)) = self.execute(&promise, version).await else {
        return;
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
/// What the work decided the promise should become.
///
/// `None` leaves it alone — the server settles a timed-out promise itself, and
/// a transient failure must be left for redelivery to retry. This is the only
/// vocabulary `run` needs: it settles, it does not interpret.
type Settlement = Option<(SettleState, PromiseValue)>;

/// Do the work, and decide what the promise becomes.
///
/// This is where the integration's error policy lives: three outcomes —
/// resolve, reject, leave alone — and the mapping from what happened to which
/// one. `run` above only applies the answer.
async fn execute(&self, promise: &PromiseRecord, version: i64) -> Settlement {
    match self.work(promise, version).await {
        Ok(Monitored::Succeeded { run, output }) => {
            Some((SettleState::Resolved, value_of(run, output)))
        }
        Err(MyError::Permanent { kind, message, run }) => {
            tracing::warn!(task_id = %self.task.id, kind, %message, "my: rejecting");
            Some((SettleState::Rejected, error_of(run, kind, message)))
        }
        // Nothing to settle: the server settles a timed-out promise itself, and
        // a transient failure must be left for redelivery to retry.
        other => {
            tracing::warn!(task_id = %self.task.id, ?other, "my: promise left unsettled");
            None
        }
    }
}
```

```rust
/// Do the work. One error channel, so `?` carries the failures out to
/// `execute`, which turns them into a settlement.
///
/// Everything below is integration-specific except its shape, and the shape is
/// the same every time: resolve, start, watch, report.
async fn work(
    &self,
    promise: &PromiseRecord,
    version: i64,
) -> Result<Monitored, MyError> {
    // ── Resolve what to act on ───────────────────────────────────────────
    //
    // Off the promise, not off the message: the promise is the durable record,
    // and a promise that has a task always carries its target tag — that tag is
    // what caused the task to exist.
    //
    // The two ways this fails are not the same failure. A malformed address is
    // the caller's and can never become valid, because promise tags are
    // immutable: reject the promise. An unconfigured deployment is the
    // operator's and a rollout fixes it: leave the task for redelivery.
    let target = self.target(promise)?;
    let input = decode_param(promise.param.data.as_deref())?;

    // ── Start the downstream run ─────────────────────────────────────────
    //
    // This runs on EVERY delivery, not just the first. `execute` messages are
    // at-least-once and are re-sent until the promise settles, and nothing in
    // the message says which delivery this is — so do not try to remember
    // whether you already started it. You cannot: a restart takes your memory
    // with it.
    //
    // Start it again, every time, under a key that is a pure function of the
    // promise id, and let the downstream system reject the duplicate. Its
    // "already exists" is the recovery path, not an error: catch it, look the
    // existing run up, and carry on to monitoring.
    //
    // A downstream system that cannot deduplicate a create cannot be integrated
    // this way. That is the precondition, not a detail — references/idempotency.md.
    let run_id = derive_run_id(&promise.id);        // deterministic: no uuid, no clock
    self.start(&target, &run_id, &input).await?;    // duplicate ⇒ Ok, re-attach

    // ── Watch it, on two independent clocks ──────────────────────────────
    //
    // The LEASE clock: `task.heartbeat` at about a third of the lease TTL, so
    // the server does not conclude this worker died and hand the task to
    // someone else. Its cadence comes from the lease and from nothing else.
    //
    // The DOWNSTREAM clock: ask the external system whether the run has
    // finished, on an interval sized for *that* system — seconds for a cheap
    // status endpoint, minutes for an expensive one, backing off as the run
    // wears on.
    //
    // They answer to different authorities and run at different frequencies.
    // Keep the heartbeat in its own task and the poll interval can back off
    // past the lease TTL without the lease lapsing. Fold them into one loop and
    // you are choosing between hammering the downstream API and silently losing
    // the task.
    let heartbeat = self.spawn_heartbeat(version);  // no `?` past here — see below

    // Poll until the downstream system says the run is done — whatever "done"
    // means there: succeeded, failed, cancelled, expired. Two rules:
    //
    //   * Stop at `promise.timeout_at`. The server settles the promise itself
    //     at that instant whether or not anyone is still watching.
    //   * A state you do not recognise is NOT done. A downstream release that
    //     adds one must not turn healthy promises into rejected ones — keep
    //     waiting and log it.
    let outcome = self.poll_until_done(&target, promise, &run_id).await;

    // The heartbeat has to be stopped on every path out of here, which is why
    // there is no `?` between the spawn and this line: an early return would
    // leave it beating for a lease nobody holds.
    heartbeat.abort();

    // Hand back what happened. `execute` maps it to resolve / reject / leave
    // alone, and `run` settles the task with the answer.
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
- **Three outcomes: resolve, reject, leave alone.** That `match` lives in `execute`, not in
  `run` — the frame applies a decision, it does not make one. It stays three arms only
  because the two enums do not cross-cut; see below.
- **The two hard parts are both in `work`.** Starting the downstream run on every delivery
  under a deterministic key, and running the lease clock and the downstream clock as two
  separate things. Get either wrong and nothing above notices — you get duplicate runs, or
  tasks that quietly get handed to someone else mid-flight.

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
