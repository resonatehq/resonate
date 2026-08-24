# Registration — plugging a worker into the server

Adding an integration is five edits. Nothing in `src/core/` changes: a new scheme is a
registration, by design.

Use `src/transport/transport_airflow.rs` and its config/registration as the template.

## 1. The worker — `src/transport/transport_<name>.rs`

```rust
pub struct MyWorker {
    /// In-process, so it holds the port directly. Every state change goes
    /// through `process` — the same path a remote worker's HTTP calls take.
    server: Arc<dyn ResonateServer>,
    client: reqwest::Client,
    deployments: HashMap<String, MyDeployment>,
    lease_timeout: i64,
}

#[async_trait]
impl ResonateWorker for MyWorker {
    async fn send(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let task = match msg {
            Message::Execute(e) => &e.data.task,
            Message::Unblock(_) => return Ok(()),
        };
        let addr = MyAddress::parse(address)
            .map_err(|e| Unavailable::new(format!("my: bad address {address}: {e}")))?;
        let deployment = self.deployments.get(&addr.deployment).cloned()
            .ok_or_else(|| Unavailable::new(format!("my: no deployment '{}'", addr.deployment)))?;

        // `send` means accepted for delivery: spawn the long work and return.
        let ctx = RunContext { /* … */ };
        tokio::spawn(async move { ctx.run().await });
        Ok(())
    }
}
```

Do in `send` only what is cheap and worth failing fast on — parse the address, resolve the
deployment. Everything else belongs in the spawned task, where it can settle the promise
rather than return an error nobody reads.

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
