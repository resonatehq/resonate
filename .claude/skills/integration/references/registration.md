# Registration — plugging a worker into the server

Adding an integration is five edits. Nothing in `src/core/` changes: a new scheme is a
registration, by design.

Use `src/transport/transport_airflow.rs` and its config/registration as the template.

## 1. The worker — `src/transport/transport_<name>.rs`

```rust
/// Everything the worker holds. Shared by every run behind one `Arc`, so a
/// delivery costs a pointer clone rather than a copy of the config.
struct My {
    /// The inbound port: how a run claims its task, heartbeats it, and settles
    /// its promise.
    server: Arc<dyn ResonateServer>,
    client: reqwest::Client,
    deployments: HashMap<String, MyDeployment>,
    lease_timeout: i64,
}

pub struct MyWorker {
    inner: Arc<My>,
}

/// One delivery in flight. Three things and nothing else: the worker it belongs
/// to, the address it was routed to, and the task it was told about.
struct RunContext {
    worker: Arc<My>,
    /// Unparsed — validation happens after the claim.
    address: String,
    task: ExecuteMsgTask,       // { id, version }
}
```

Resist splitting `task` into loose `task_id` and `task_version` fields. It is one thing —
the payload of the `Execute` message — and `ExecuteMsgTask` is the type that says so.

`send` itself does almost nothing. Its whole job is to decide whether this message is
yours and then get off the dispatch thread.

```rust
#[async_trait]
impl ResonateWorker for MyWorker {
    async fn send(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        // ── 1. Which message is this? ────────────────────────────────────────
        // `Unblock` is delivered to workers that *wait* on a promise. An
        // integration never waits, so acknowledge and drop it. Returning `Err`
        // here would log a delivery failure for a message that was never ours.
        let task = match msg {
            Message::Execute(e) => &e.data.task,      // { id, version }
            Message::Unblock(_) => return Ok(()),
        };

        // ── 2. Hand off, and do nothing else here ────────────────────────────
        // The first real step is claiming the task, and that is a round trip to
        // the server. `process_batch` awaits `send` sequentially over the whole
        // batch, so a round trip on this path would stall delivery of every
        // other message in it — including messages for other schemes.
        //
        // Validation waits too. It could run here, but its failures could then
        // only be reported as `Err(Unavailable)`, which the dispatch loop logs
        // and drops. Do it after the claim, where it can settle the promise.
        let ctx = RunContext {
            worker: Arc::clone(&self.inner),
            address: address.to_string(),   // unparsed; owned, the task is `'static`
            task: task.clone(),
        };
        tokio::spawn(async move { ctx.run().await });

        // ── 3. Accepted for delivery — not executed ──────────────────────────
        Ok(())
    }
}
```

The work is in the spawned task, and it starts by claiming the task **through the server
handle the worker holds** — `task.acquire`, over the same `process` entry point a remote
worker's HTTP call would take.

```rust
impl RunContext {
    async fn run(self) {
        // ── 1. Claim the task ────────────────────────────────────────────────
        // Nothing may happen on behalf of a task this worker does not own — and
        // nothing can be *reported* until it does. The claim is the gate that
        // turns "log it and hope" into "settle the promise".
        let acquired = match self.acquire().await {
            Some(a) => a,
            None => return,     // 409: another attempt owns it. Do nothing at all.
        };
        let version = acquired.task.version;   // the RESPONSE version (n+1), from here on
        let promise = acquired.promise;        // param, timeoutAt, createdAt, tags

        // ── 2. Validate, now that failures are reportable ────────────────────
        let target = match self.resolve_target() {
            Ok(t) => t,
            Err(e) => return self.report(version, e).await,
        };

        // ── 3. Monitor, on two independent clocks ────────────────────────────
        let heartbeat = self.spawn_heartbeat(version);                // lease clock
        let outcome = self.create_and_monitor(&target, &promise).await;  // downstream clock
        heartbeat.abort();

        // ── 4. Settle ────────────────────────────────────────────────────────
        match outcome {
            Ok(Monitored::Succeeded(run)) => self.settle(version, "resolved", ...).await,
            Ok(Monitored::Failed(run))    => self.settle(version, "rejected", ...).await,
            // The server settles `rejected_timedout` at `timeoutAt` itself.
            Ok(Monitored::DeadlineReached) => tracing::warn!(...),
            Err(e) => self.report(version, e).await,
        }
    }

    /// The claim. Constructed by hand so it is obvious what crosses the port:
    /// an ordinary protocol request, the same one a remote worker would put on
    /// the wire, handed straight to `process` instead of to a socket.
    async fn acquire(&self) -> Option<TaskAcquireResponseData> {
        let resp = self.worker.server.process(&RequestEnvelope {
            kind: "task.acquire".to_string(),
            head: RequestHead {
                corr_id: format!("my-{}", fastrand::u64(..)),
                version: PROTOCOL_VERSION.to_string(),
                auth: None,          // in process: there is no caller to authenticate
                debug_time: None,
            },
            data: json!({
                "id": self.task.id,
                "version": self.task.version,   // the fencing token from `execute`
                "pid": PID,
                "ttl": self.worker.lease_timeout,
            }),
        }).await.ok()?;                          // Err = Unavailable; redelivery retries

        match resp.head.status {
            200 => serde_json::from_value(resp.data).ok(),
            409 => None,   // another attempt owns it, or the task is no longer pending
            _   => None,   // transient — redelivery will bring us back here
        }
    }
}
```

`PID` is a constant:

```rust
/// The `pid` this worker claims tasks under.
///
/// A constant, and it can be: `pid` only has to match between the `task.acquire`
/// that claims a task and the `task.heartbeat` that refreshes it. The storage
/// guard is `id = ? AND process_id = ?` plus an exists-check on the task's
/// *version* — and the version is the real fence, so two runs sharing a pid
/// cannot touch each other's leases, and a stale attempt heartbeating an old
/// version is a no-op.
const PID: &str = "self";
```

Only `resolve_target` and `create_and_monitor` differ between integrations. Everything
above is the same shape every time.

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
