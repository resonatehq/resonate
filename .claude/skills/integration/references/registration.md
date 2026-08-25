# Registration — plugging a worker into the server

Adding an integration is five edits. Nothing in `src/core/` changes: a new scheme is a
registration, by design.

Names and layout come from `references/structure.md` — follow them rather than inventing, so
this integration reads like every other one. What goes *in* those slots — the address beyond
the scheme, the config fields, the param fields — is yours to decide for your system.

`src/transport/transport_airflow.rs` and its config/registration show that structure filled in
for one system. Read it as an instance, not as a form to copy.

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
something unspecified and gets back one of three answers, so a downstream run, an error kind
and a value schema never appear in it.

What matters is the flow, not the types below. There are **three outcomes** and `run` has to
be able to tell them apart:

| Outcome | `run` does |
|---|---|
| The work produced a result | fulfill the task `resolved`, with a value |
| The work failed in a way that can never succeed | fulfill the task `rejected`, with a value |
| Transient failure, or the deadline passed | fulfill nothing — the delivery is given back, and redelivery retries or the server times the promise out |

Encode that however you like — a three-variant enum, `Option<Result<..>>`, a struct, a
callback. The code below is one way, written out so the flow is readable end to end; treat
it as a sketch to adapt, not a base class to inherit. `work` especially: its rhythm is
fixed (resolve, start, watch, report) but every line of it is yours.
`src/transport/transport_airflow.rs` is one filled-in version.

```rust
/// The protocol frame: claim the task, do the work, settle the promise.
///
/// Nothing here is integration-specific. It never sees a downstream run, an
/// error kind or a value schema — `execute` decides all of that, and this
/// body's whole job is to apply the answer.
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

One way to carry the three outcomes back — pick whatever suits the integration, as long as
"settle nothing" cannot be confused with "settle rejected":

```rust
/// What the work decided the promise should become. `None` leaves it alone.
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
/// the same every time: resolve, validate, start, watch, report.
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

    // ── Read and validate the param ──────────────────────────────────────
    //
    // The param is the request, and its schema is this integration's contract
    // with whoever creates the promise. To the protocol `param.data` is opaque
    // bytes: JSON is the common choice, but it can be protobuf, msgpack, a bare
    // string, a CSV — whatever the integration says it is. Whatever you pick,
    // write it down (README.md, and references/schemas.md).
    //
    // Validate it here, before the first side effect, and reject on violation.
    // A promise's param is immutable, so a request that is malformed now is
    // malformed on every redelivery: retrying cannot help, and leaving it
    // unvalidated means either a confusing downstream error later or — worse —
    // a run started from a request you did not actually understand.
    //
    // Reject with enough detail to fix the caller: which field, what was wrong.
    // The promise value is the only channel back, so a bare "invalid request"
    // strands whoever sent it.
    let request = decode_param(promise.param.data.as_deref())?;   // permanent on violation

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
    self.start(&target, &request, &run_id).await?;   // duplicate ⇒ Ok, re-attach

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
    let outcome = self.poll_until_done(&target, promise, &request, &run_id).await;

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
- **Validation happens once, at the top of `work`.** A param that fails its schema can
  never pass it — promise params are immutable — so it is a rejection, not a retry, and it
  must happen before anything downstream is touched.
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

## 5. README

Write it from `references/readme-template.md` and keep it beside the code —
`docs/integrations/<name>.md` for an in-tree worker.

The address, param and value schemas are what callers write against; they belong somewhere
a caller will look, which is not the middle of a Rust file. Put them in **both** the README
and the worker's module doc comment: the module comment is for whoever changes the worker,
the README for whoever calls it. If they disagree, the code wins and the README is a bug.

## 6. Tests

Four layers, in `references/testing.md`. The short version:

- **Unit** — address parsing (including the shapes you deliberately reject), the idempotency
  key (determinism, charset, collision after truncation), param decoding and validation,
  downstream state mapping (an unknown state is *pending*), and the two clocks. All pure,
  all fast.
- **An API double**, in its own process, that reproduces the downstream system's
  duplicate-create rejection and counts calls. A double that happily creates two runs under
  one key makes every other test meaningless.
- **End to end** — happy path, downstream failure, unknown resource, unknown deployment,
  malformed param, malformed address. Assert the promise outcome *and* the downstream call
  count.
- **Crash and restart** — `SIGKILL` the server between the downstream create and the settle,
  restart against the same storage, and assert exactly one downstream run for that promise.
  Everything above passes on an integration whose idempotency is broken; this is the one
  that does not.

## Checklist

- [ ] `src/transport/transport_<name>.rs` implementing `ResonateWorker`
- [ ] `pub mod` in `src/transport/mod.rs`
- [ ] Config struct, field on `TransportsConfig`, `validate` rules
- [ ] `workers.insert("<scheme>", …)` in `src/main.rs`, gated on `enabled`
- [ ] Param validation at the top of `work`, rejecting on violation with the offending field
- [ ] Unit tests for the pure helpers, including every malformed param and address, and
      the shapes you deliberately reject
- [ ] An API double, in its own process, that rejects duplicate creates and counts calls
- [ ] End-to-end tests asserting the promise outcome *and* the downstream call count
- [ ] A crash-and-restart test proving one downstream run, with `conflicts > 0` asserted so
      it cannot pass without exercising the recovery path
- [ ] Address, param and value schemas in the module's doc comment
- [ ] A README from `references/readme-template.md`
