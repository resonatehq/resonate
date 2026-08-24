---
name: integration
description: Build a Resonate integration — a custom ResonateWorker that makes one durable promise stand for one run in an external system (Apache Airflow, dbt Cloud, Databricks, SageMaker, a batch queue, a partner API). Use when asked to integrate Resonate with a downstream system, add a new address scheme, write a worker/transport that is not application code, or design the create/monitor/settle lifecycle and the promise param/value schemas for such a bridge. Scoped to idempotent downstream systems monitored by polling.
---

# Building a Resonate Integration

## What an integration is

An **integration** is an implementation of the `ResonateWorker` port that does not run
the work itself. It makes one durable promise stand for one run in an external system:

```
promise pending  ──►  downstream run created  ──►  run monitored  ──►  promise settled
```

It lives **in the server process**, registered under its own URL scheme:

```rust
workers.insert("airflow".to_string(), Arc::new(AirflowWorker::new(...)));
```

To the rest of the application it is indistinguishable from any other remote function:
someone creates a promise tagged `resonate:target: airflow://prod/dags/etl_daily`, blocks
on it, and gets back a value.

An integration is **not** a Resonate SDK application, and not a place for business logic.
It maps a request to a run, and a run's state to a promise's state.

## The two in-tree references

| File | What it shows |
|---|---|
| `src/transport/transport_exec_bash.rs` | A worker that runs the work itself, in process |
| `src/transport/transport_airflow.rs` | **An integration**: create / monitor / settle against a foreign API |

Read the Airflow worker before writing a new one. It is the complete worked example —
address schema, idempotency key, error classification, poll loop, settlement — and it is
compiled and unit-tested in this repo, so it cannot drift from the ports it implements.

## The ports

Three traits in `src/core/`, which depends on nothing else in the crate. Verbatim:

```rust
/// The inbound port — src/core/server.rs
#[async_trait]
pub trait ResonateServer: Send + Sync {
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable>;
}

/// The outbound port — src/core/worker.rs
#[async_trait]
pub trait ResonateWorker: Send + Sync {
    async fn send(&self, address: &str, msg: &Message) -> Result<(), Unavailable>;
}

/// The routing port — src/core/router.rs
#[async_trait]
pub trait ResonateRouter: Send + Sync {
    async fn route(&self, address: &str, msg: &Message) -> Result<(), Unavailable>;
}
```

`Message` is what a server emits toward a worker, and `Unavailable` is the only error any
port returns:

```rust
#[derive(Debug, Serialize)]
#[serde(untagged)]                    // each variant carries its own `kind`
pub enum Message {
    Execute(ExecuteMsg),              // { kind, head: { serverUrl }, data: { task: { id, version } } }
    Unblock(UnblockMsg),              // { kind, head: {},            data: { promise } }
}

pub struct Unavailable { pub message: String }
```

### The shape an integration implements

One trait, one method. Everything else is a free choice.

```rust
/// Everything the worker holds, behind one `Arc`. `server` is the inbound port:
/// how a run claims its task, heartbeats it, and settles its promise. `Server`
/// never holds the router, so this stays a DAG.
struct My { server: Arc<dyn ResonateServer>, client: reqwest::Client, /* config */ }

pub struct MyWorker { inner: Arc<My> }

/// One delivery in flight: the worker, the address, the task. Nothing else.
struct RunContext { worker: Arc<My>, address: String, task: ExecuteMsgTask }

#[async_trait]
impl ResonateWorker for MyWorker {
    async fn send(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let task = match msg {
            Message::Execute(e) => &e.data.task,   // { id, version }
            Message::Unblock(_) => return Ok(()),  // for workers that *wait*; not this one
        };

        // Hand off and do nothing else. The first real step is claiming the
        // task through `server`, and that is a round trip — the dispatch loop
        // awaits `send` sequentially over the batch. Validation waits too:
        // until the task is claimed its failures are unreportable.
        let ctx = RunContext {
            worker: Arc::clone(&self.inner),
            address: address.to_string(),          // unparsed
            task: task.clone(),
        };
        tokio::spawn(async move { ctx.run().await });
        Ok(())                                     // accepted for delivery, not executed
    }
}
```

The work lives in `run`, and it starts by claiming the task **through the server handle** —
`task.acquire` via `process`, the same entry point a remote worker's HTTP call takes. That
claim is the gate: before it the only way to report a failure is `Err(Unavailable)`, which
the dispatch loop logs and drops; after it, every failure can settle the promise. So
validation, config lookup and the downstream call all belong on the far side of it. Full
stub in `references/registration.md`.

Registering it is one line, and nothing in `core` changes:

```rust
workers.insert("my".to_string(), Arc::new(MyWorker::new(...)));
```

Four consequences that shape every integration:

1. **`send` means accepted for delivery, not executed.** A DAG run takes hours; the
   dispatch loop must not block on it. Spawn and return `Ok(())`.
2. **The address arrives unparsed, and the worker owns everything past the scheme.**
   `core::address::is_valid_address` only checks that the string is a URI — deliberately,
   so admission is a pure function of the string and identical on every deployment. Syntax
   errors past the scheme surface at delivery, not at `promise.create`.
3. **A new scheme is a registration, never a change to `core`.** That is the whole cost of
   adding an integration.
4. **`Unavailable` is the only error.** Everything the peer can say — not found, conflict,
   forbidden — is an in-band `head.status`. `Unavailable` means the exchange did not
   complete, and *the request may already have been applied*. Retries must be idempotent.

An in-process worker holds `Arc<dyn ResonateServer>` and calls `process` directly: no HTTP
hop, no auth, but the same validation and settlement path a remote worker's calls take.

## The fact that determines the design

**`execute` is at-least-once and is re-sent until the promise settles.** The server
re-enqueues it every `tasks.retry_timeout` (default 30 s) while a task is pending, and
again whenever a lease expires. The message carries `{task: {id, version}}` and nothing
else — no delivery counter, no redelivery flag. A worker cannot tell a first delivery from
a redelivery, and `task.version` counts *acquisitions*, not downstream side effects.

So the integration must not ask "have I already done this?". Build it so asking is
unnecessary:

> **Fire the request again, every time, under a key derived from the promise id, and let
> the downstream system reject the duplicate.**

Airflow's `409` on a duplicate `dag_run_id` is then the *recovery path*: an earlier
attempt already triggered the run, so re-attach and monitor it. If the downstream system
offers no such key, **stop and discuss with the user** — see `references/idempotency.md`.
There is no safe default.

## Scope of this skill

| Dimension | In scope | Out of scope |
|---|---|---|
| Downstream create | **Idempotent** (client-supplied run id or dedupe key) | Non-idempotent create |
| Completion detection | **Polling** the downstream for status | Push / callback from downstream |

For push, see `references/push-vs-poll.md` — the questions to settle before building it.

## The five things every integration defines

1. **Address schema** — the scheme it owns and the syntax past it.
   `references/schemas.md`
2. **Param schema** — the request, in `promise.param.data`.
3. **Value schema** — the outcome, in `promise.value.data`, for both branches.
4. **Create** — idempotently start or re-attach to the downstream run.
5. **Check** — map the downstream state to pending / succeeded / failed.

## The three phases

Phase 0 is `task.acquire`; a `409` means another attempt owns the task, so drop the
message and never touch the downstream system.

1. **Create — runs on every delivery.** Issue the create with a deterministic key derived
   from `promise.id`. Treat "already exists" as success and fall through to monitoring.
   Never a UUID, a clock read, or `task.version`.
2. **Monitor — two independent clocks.** The **lease clock** heartbeats `task.heartbeat`
   at a third of the lease TTL. The **downstream clock** asks the external system for the
   run's state on a backing-off interval sized for *its* cost and latency. They answer to
   different authorities and run at different frequencies; keep the heartbeat in its own
   task so the poll interval can back off past the lease TTL without the lease lapsing.
   Both stop at `promise.timeoutAt` — the server settles `rejected_timedout` there
   regardless.
3. **Settle.** `task.fulfill`, which completes task and promise in one transaction and is
   what finally stops redelivery.

Every downstream error is **permanent** (reject the promise), **transient** (do not settle;
let redelivery re-run the idempotent create), or **ambiguous** — and ambiguous is
transient. `references/lifecycle.md` has the full table.

## Workflow for building one

1. **Check the precondition.** Does downstream create accept a client-supplied id or
   idempotency key? If not → `references/idempotency.md`, then ask the user.
2. **Write the three schemas** and get them reviewed. They are the public contract.
3. **Copy the shape of `src/transport/transport_airflow.rs`**: address type + `parse`,
   worker struct holding `Arc<dyn ResonateServer>`, `send` that spawns, a run context with
   acquire / create / monitor / settle, and pure helpers with unit tests.
4. **Register it** — five edits, listed in `references/registration.md`.
5. **Test the redelivery path.** `SIGKILL` the server between create and settle, restart it
   against the same storage, and assert exactly one downstream run exists.

## Reference material

| File | Contents |
|---|---|
| `references/contract.md` | The ports in detail, `Message`, `Unavailable`, delivery and retry semantics, task state machine |
| `references/lifecycle.md` | Acquire / create / monitor / settle, error classification, timeouts and orphans |
| `references/idempotency.md` | Decision tree, and what to do when the downstream offers no dedupe |
| `references/schemas.md` | Address, param and value schema rules |
| `references/registration.md` | The five edits that plug a worker into the server |
| `references/push-vs-poll.md` | Why push is out of scope and what to settle first |
