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

The work is in the spawned task. Below is `RunContext::run` from
`src/transport/transport_airflow.rs`, **verbatim and complete** — claim, resolve, two
clocks, settle, with every protocol request built inline so nothing is hidden behind a
helper. Only the downstream half (`create_and_monitor` and the two HTTP calls under it) is
factored out; that is the part that differs per integration.

```rust
/// The whole lifecycle in one body: claim, resolve, monitor on two clocks,
/// settle. Only the downstream half is factored out — `create_and_monitor`
/// and the two HTTP calls under it are the part that differs per
/// integration; everything here is the same for all of them.
async fn run(self) {
    // ── 1. Claim the task ────────────────────────────────────────────────
    //
    // Nothing may happen on behalf of a task this worker does not own — and
    // nothing can be *reported* until it does. Before the claim the only
    // outcome available is `Err(Unavailable)`, which the dispatch loop logs
    // and drops; after it, every failure can settle the promise. So the
    // claim comes first and validation comes after.
    let claimed = self
        .worker
        .server
        .process(&RequestEnvelope {
            kind: "task.acquire".to_string(),
            head: RequestHead {
                corr_id: format!("airflow-{}", fastrand::u64(..)),
                version: PROTOCOL_VERSION.to_string(),
                // In process: there is no caller to authenticate.
                auth: None,
                debug_time: None,
            },
            data: json!({
                "id": self.task.id,
                "version": self.task.version,   // the fencing token from `execute`
                "pid": PID,
                "ttl": self.worker.lease_timeout,
            }),
        })
        .await;

    let acquired: TaskAcquireResponseData = match claimed {
        Ok(r) if r.head.status == 200 => match serde_json::from_value(r.data) {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(task_id = %self.task.id, error = %e, "airflow: malformed acquire response");
                return;
            }
        },
        // 409: another attempt owns it, or the task is no longer pending.
        // Do nothing at all — in particular, do not touch Airflow.
        Ok(r) if r.head.status == 409 => {
            tracing::debug!(task_id = %self.task.id, "airflow: task not acquired");
            return;
        }
        // Anything else is transient. Drop the task without settling; the
        // lease expires and redelivery brings us back here.
        Ok(r) => {
            tracing::warn!(task_id = %self.task.id, status = r.head.status, "airflow: task acquire rejected");
            return;
        }
        Err(e) => {
            tracing::error!(task_id = %self.task.id, error = %e, "airflow: task acquire failed");
            return;
        }
    };
    let version = acquired.task.version; // the RESPONSE version (n+1), from here on
    let promise = acquired.promise; // param, timeoutAt, createdAt, tags

    // ── 2. Resolve the address, off the promise ──────────────────────────
    //
    // Not off the message: the promise is the durable record, and it is
    // where every other input already comes from. A promise that has a task
    // always carries the target tag — that tag is what caused the task to
    // exist — so the lookup cannot fail in practice.
    //
    // The two ways this *can* fail are not the same failure. A malformed
    // address is the caller's error and can never become valid, because
    // promise tags are immutable; an unconfigured deployment is the
    // operator's error and a rollout fixes it. One rejects, one retries.
    let target = promise
        .tags
        .get(TARGET_TAG)
        .ok_or_else(|| AirflowError::Permanent {
            kind: "invalid_request",
            message: format!("promise has no {TARGET_TAG} tag"),
        })
        .and_then(|address| {
            AirflowAddress::parse(address).map_err(|e| AirflowError::Permanent {
                kind: "invalid_request",
                message: format!("invalid airflow address '{address}': {e}"),
            })
        })
        .and_then(|addr| match self.worker.deployments.get(&addr.deployment) {
            Some(deployment) => Ok(Target {
                deployment: deployment.clone(),
                addr,
            }),
            None => Err(AirflowError::Transient(format!(
                "no deployment '{}' configured (known: {:?})",
                addr.deployment,
                self.worker.deployments.keys().collect::<Vec<_>>()
            ))),
        });

    // ── 3. Monitor, on two independent clocks ────────────────────────────
    let outcome = match target {
        Err(e) => Err(e),
        Ok(target) => {
            // The lease clock. Its own task, on a cadence derived from the
            // lease TTL and nothing else — which is what lets the
            // downstream clock below back off past the lease without the
            // lease lapsing.
            let heartbeat = {
                let server = Arc::clone(&self.worker.server);
                let task_id = self.task.id.clone();
                let beat_ms = heartbeat_interval_ms(self.worker.lease_timeout);
                tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(Duration::from_millis(beat_ms));
                    ticker.tick().await;
                    loop {
                        ticker.tick().await;
                        // Deliberately ignored. `task.heartbeat` answers 200
                        // whether or not it refreshed anything — a heartbeat
                        // for a lease this worker no longer holds is a
                        // silent no-op — so the response carries no signal.
                        // Losing the lease surfaces at `task.fulfill`, as a
                        // 409.
                        let _ = server
                            .process(&RequestEnvelope {
                                kind: "task.heartbeat".to_string(),
                                head: RequestHead {
                                    corr_id: format!("airflow-hb-{}", fastrand::u64(..)),
                                    version: PROTOCOL_VERSION.to_string(),
                                    auth: None,
                                    debug_time: None,
                                },
                                data: json!({
                                    "pid": PID,
                                    "tasks": [{ "id": task_id, "version": version }]
                                }),
                            })
                            .await;
                    }
                })
            };

            // The downstream clock: create once, then poll on an interval
            // sized for Airflow rather than for the lease.
            let outcome = self.create_and_monitor(&target, &promise).await;
            heartbeat.abort();
            outcome
        }
    };

    // ── 4. Settle ────────────────────────────────────────────────────────
    //
    // One exit. Two of the five cases deliberately do not settle: the
    // server itself settles a timed-out promise, and a transient failure
    // must leave the task for redelivery to retry.
    let (state, value) = match outcome {
        Ok(Monitored::Succeeded { run, output }) => {
            ("resolved", json!({ "run": run, "output": output }))
        }
        Ok(Monitored::Failed { run, message }) => (
            "rejected",
            json!({ "run": run, "error": { "kind": "downstream_failed", "message": message } }),
        ),
        Err(AirflowError::Permanent { kind, message }) => {
            tracing::warn!(task_id = %self.task.id, kind, %message, "airflow: permanent failure");
            (
                "rejected",
                json!({ "run": {}, "error": { "kind": kind, "message": message } }),
            )
        }
        // The server settles `rejected_timedout` at `timeoutAt` itself.
        // Stop watching and leave the DAG run alone rather than racing it.
        Ok(Monitored::DeadlineReached) => {
            tracing::warn!(task_id = %self.task.id, "airflow: promise deadline reached, stopped monitoring");
            return;
        }
        Err(AirflowError::Transient(message)) => {
            tracing::warn!(task_id = %self.task.id, %message, "airflow: transient failure, dropping task for redelivery");
            return;
        }
    };

    let settled = self
        .worker
        .server
        .process(&RequestEnvelope {
            kind: "task.fulfill".to_string(),
            head: RequestHead {
                corr_id: format!("airflow-{}", fastrand::u64(..)),
                version: PROTOCOL_VERSION.to_string(),
                auth: None,
                debug_time: None,
            },
            data: json!({
                "id": self.task.id,
                "version": version,
                "action": {
                    "kind": "promise.settle",
                    "head": {},
                    "data": {
                        "id": self.task.id,   // must equal the task id
                        "state": state,
                        "value": {
                            "headers": { "content-type": "application/json" },
                            "data": b64_encode(&value.to_string())
                        }
                    }
                }
            }),
        })
        .await;

    match settled {
        Ok(r) if (200..300).contains(&r.head.status) => {
            tracing::info!(task_id = %self.task.id, state, "airflow: promise settled");
        }
        // 409: the lease was lost, or the promise already settled — almost
        // always a timeout. Retrying cannot help.
        Ok(r) => {
            tracing::warn!(task_id = %self.task.id, status = r.head.status, state, "airflow: task fulfill rejected")
        }
        Err(e) => {
            tracing::error!(task_id = %self.task.id, error = %e, "airflow: task fulfill failed")
        }
    }
}
```

Two things to notice in the shape:

- **Every state change goes through `self.worker.server.process`** — an ordinary protocol
  request, the same one a remote worker would put on the wire, handed to the port instead
  of to a socket. There is no privileged in-process API.
- **Five outcomes, three of which settle.** A timed-out promise is settled by the server
  itself, and a transient failure must be left for redelivery; settling either would be
  wrong. The `match` at step 4 is where an integration's whole error policy lives.

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
