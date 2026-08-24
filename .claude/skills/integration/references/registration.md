# Registration — plugging a worker into the server

Adding an integration is five edits. Nothing in `src/core/` changes: a new scheme is a
registration, by design.

Use `src/transport/transport_airflow.rs` and its config/registration as the template.

## 1. The worker — `src/transport/transport_<name>.rs`

```rust
pub struct MyWorker {
    /// The inbound port. This worker runs in the server's process, so it holds
    /// the port rather than dialling it.
    server: Arc<dyn ResonateServer>,

    /// Everything else is immutable after construction. `send` takes `&self`,
    /// and one instance serves every address of its scheme concurrently, so
    /// any mutable state would need its own synchronisation.
    client: reqwest::Client,
    deployments: HashMap<String, MyDeployment>,
    lease_timeout: i64,
}

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

        // ── 2. Caller errors → reject the promise ────────────────────────────
        // A malformed address can never become valid: promise tags are
        // immutable. Returning `Unavailable` would re-deliver every
        // `tasks.retry_timeout` until the promise times out, and the caller
        // would see `rejected_timedout` instead of the reason. Claim the task
        // and reject it instead — one acquire and one fulfill for a real error.
        let addr = match MyAddress::parse(address) {
            Ok(a) => a,
            Err(e) => {
                let server = Arc::clone(&self.server);
                let (id, version, ttl) = (task.id.clone(), task.version, self.lease_timeout);
                let message = format!("invalid address '{address}': {e}");
                tokio::spawn(async move {
                    reject_permanently(server, id, version, ttl, "invalid_request", message).await
                });
                return Ok(());
            }
        };

        // ── 3. Operator errors → report as undeliverable ─────────────────────
        // An unconfigured deployment is fixable by deploying config, after
        // which redelivery succeeds without touching the promise. That is what
        // `Unavailable` is for.
        let deployment = self.deployments.get(&addr.deployment).cloned().ok_or_else(|| {
            Unavailable::new(format!("my: no deployment '{}' configured", addr.deployment))
        })?;

        // ── 4. Hand off ──────────────────────────────────────────────────────
        // Everything past this point needs a task claim first, so it belongs in
        // the spawned task where it can settle the promise. Clone into an owned
        // context: the task must be `'static`.
        let ctx = RunContext {
            server: Arc::clone(&self.server),
            client: self.client.clone(),
            deployment,
            addr,
            lease_timeout: self.lease_timeout,
            task_id: task.id.clone(),
            task_version: task.version,
        };
        tokio::spawn(async move { ctx.run().await });

        // ── 5. Accepted for delivery — not executed ──────────────────────────
        Ok(())
    }
}
```

That body is almost entirely invariant. Only `MyAddress::parse`, the config lookup, and
`RunContext::run` differ between integrations.

### What must not go in `send`

`process_batch` awaits `route` **sequentially** over the whole batch:

```rust
for msg in execute_msgs {
    if let Err(e) = router.route(&msg.address, &payload).await { tracing::warn!(...) }
}
```

So anything slow in `send` stalls delivery of every other message in that batch —
including messages for other schemes, which have nothing to do with your integration. That
rules out:

| Never in `send` | Why | Where it goes |
|---|---|---|
| The downstream call | Can take hours | The spawned task |
| `task.acquire` | A server round trip per message, serialised | The spawned task |
| Retries or backoff | Blocks the batch for the duration | The spawned task |
| Blocking I/O, `std::fs`, `block_on` | Stalls the runtime thread | Nowhere |

What belongs in `send` is what is cheap *and* worth failing fast on: which message kind it
is, whether the address parses, and whether this worker is configured to serve it.

### The two error classes, side by side

| | Malformed address | Unconfigured deployment |
|---|---|---|
| Whose mistake | The caller's | The operator's |
| Fixable without a new promise | No — tags are immutable | Yes — deploy the config |
| Right response | Claim the task, reject the promise | `Err(Unavailable)` |
| What the caller sees | `rejected`, `kind = "invalid_request"`, with the address quoted | Promise stays pending, then `rejected_timedout` if nobody fixes it |
| Cost of getting it wrong | A parse failure logged every 30 s until timeout, and a useless error for the caller | A promise permanently rejected for a config gap that a rollout would have fixed |

`reject_permanently` is a free function, not a `RunContext` method — it runs before there
is a run to have a context for. See `src/transport/transport_airflow.rs`.

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
