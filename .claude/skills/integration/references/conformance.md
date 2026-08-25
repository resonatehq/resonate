# The uniform shape

The point of this skill is that a hundred integrations look like one integration. Someone
who has read `airflow` should be able to open `databricks` and know where everything is,
what the value will look like, and which failures reject versus retry — without reading it
first.

That only works if the shape is **prescribed, not suggested**. What follows is the canon.
Deviate where the downstream system genuinely forces it, and when you do, say so in the
README's *Limitations* — an unexplained deviation is a bug, not a style choice.

---

## File layout

One file, `src/transport/transport_<name>.rs`, in this order. Every integration, same
order, so scrolling finds the same thing in the same place.

```rust
//! <Name> worker — one durable promise ⇄ one <run> in <system>.
//!
//! <what it bridges, and the property that makes it safe>
//! ## Address schema
//! ## Param schema
//! ## Value schema

use …

// ─── Address ─────────────────────────────────────────────
const PID: &str = "self";
fn parse_address(address: &str) -> Result<String, String>

// ─── Worker ──────────────────────────────────────────────
struct <Name>                        // shared state, incl. `server`
pub struct <Name>Worker              // Arc handle
impl <Name>Worker::new
impl ResonateWorker for <Name>Worker // `send`

// ─── Lifecycle ───────────────────────────────────────────
struct RunContext                    // { worker, task }
struct Target                        // { name, deployment }
struct <Op>Request                   // the decoded, validated param
type Settlement
enum RunState                        // what one check found
enum Monitored                       // how monitoring ended
enum <Name>Error                     // Permanent { kind, message, run } | Transient(String)

impl RunContext {
    async fn run(self)               // protocol frame
    async fn execute(…) -> Settlement // decide
    async fn work(…) -> Result<Monitored, <Name>Error>
    async fn start(…)                // idempotent create
    async fn poll_until_done(…)      // the downstream clock
    async fn check(…) -> RunState    // one status check
    async fn request(…)              // one call to the downstream API
    fn run_summary(…) -> Value       // run identity for the value
}

// ─── Pure helpers ────────────────────────────────────────
fn heartbeat_interval_ms(lease_timeout: i64) -> u64
fn classify_run(…) -> RunState
fn derive_run_id(promise_id: &str) -> String
fn decode_param(data: Option<&str>) -> Result<<Op>Request, <Name>Error>

#[cfg(test)] mod tests
```

## Fixed names

These are not suggestions. A reader should be able to grep any integration for `derive_run_id`
and find the idempotency key.

| Name | Is always |
|---|---|
| `parse_address` | Address → deployment name |
| `PID` | The pid this worker claims under, `"self"` |
| `RunContext` | One delivery in flight: `{ worker, task }` |
| `Target` | `{ name, deployment }`, resolved after the claim |
| `Settlement` | `Option<(SettleState, PromiseValue)>` — `None` leaves the promise alone |
| `run` | The protocol frame: claim, ask, settle |
| `execute` | Outcome → `Settlement`; the error policy |
| `work` | Resolve, validate, start, watch, report |
| `start` | The idempotent create. A duplicate is success. |
| `check` | One status check |
| `poll_until_done` | The downstream clock |
| `derive_run_id` | The idempotency key, a pure function of the promise id |
| `decode_param` | Decode **and validate**; every failure permanent |
| `classify_run` | Downstream state → `RunState`; unknown ⇒ `Pending` |
| `heartbeat_interval_ms` | The lease clock's cadence |
| `<Name>Error` | `Permanent { kind, message, run }` \| `Transient(String)` |

## Address

```
<scheme>://<deployment>
```

Scheme routes to the worker; authority is a config key naming the deployment. Nothing else,
unless the path genuinely carries *routing* — different credentials, different endpoints.
Request data never goes here.

## Param

Base64 UTF-8 JSON unless the downstream system forces otherwise, decoded into a typed
`<Op>Request` with `#[serde(deny_unknown_fields)]`. Required fields are required; everything
else has a default. No idempotency key field, no credentials, no deadline — the promise's
`timeoutAt` is the deadline.

## Value

The outer shape is **fixed**. Only what is inside `output` differs between integrations.

```json
{ "run": { "id": "…", "state": "…", "startedAt": …, "endedAt": …, "url": "…" },
  "output": { … } }
```

```json
{ "run": { … }, "error": { "kind": "…", "message": "…" } }
```

- `run` is always present, on both branches. Empty object when the failure happened before
  a run existed.
- `url` is always a link a human can open. On both branches.
- Exactly one of `output` / `error`.

### Error kinds — a closed set

Callers branch on these, so they are the same everywhere. Adding one is a change to this
document, not a local decision.

| kind | Means | Settles as |
|---|---|---|
| `invalid_request` | Param or address failed validation | `rejected` |
| `not_found` | The addressed resource does not exist | `rejected` |
| `unauthorized` | The worker's credentials were rejected | `rejected` |
| `downstream_failed` | The run started and finished in a failure state | `rejected` |
| `canceled` | The run was cancelled out of band | `rejected` |

`rejected_canceled` as a *promise state* is reserved for cancellation initiated through
Resonate. A run cancelled in the downstream system settles `rejected` with
`kind = "canceled"` — one rule, so a caller does not have to know which integration it is
talking to.

## Configuration

Same keys, same meanings, every integration:

```yaml
transports:
  <name>:
    enabled: false              # default; an unregistered scheme is undeliverable, not broken
    lease_timeout: <ms>         # optional; unset follows tasks.lease_timeout
    poll_interval: <ms>         # first downstream-clock interval
    max_poll_interval: <ms>     # its ceiling
    deployments:
      <name>:
        base_url: …
        …                       # per-system: auth, api version, web url
```

Validate in `Config::validate` the failures that would otherwise be silent: a non-positive
lease (every `task.acquire` 400s and the worker never runs anything), a poll interval below
1, a max below the interval, and `enabled` with no deployments.

## Error classification

Two buckets, and the same rule everywhere:

| | Permanent | Transient |
|---|---|---|
| Means | Can never succeed | Might succeed later, or the outcome is unknown |
| Examples | 400, 404, 422, malformed param, malformed address | connection refused, 429, 5xx, timeout |
| Worker does | Settle `rejected` with a kind | Return without settling |
| Then | Caller sees the reason | Lease expires, redelivery retries |

**Ambiguous is transient.** A request that may or may not have been applied is retried,
which is safe only because `start` is idempotent — the same property the whole design rests
on.

## Tests

Same names, so a reader can check any integration has them:

```
address_rejects_wrong_shape
run_id_is_a_pure_function_of_the_promise_id
run_id_stays_unique_after_truncation
param_rejects_unknown_fields
malformed_param_is_permanent_not_transient
unknown_state_is_pending_not_failed
heartbeat_always_fits_inside_the_lease
```

Plus the end-to-end set and the crash-and-restart test — `references/testing.md`.

## README

Same sections, same order — `references/readme-template.md`. Someone comparing two
integrations should be able to diff their READMEs and see only what actually differs.

---

## Deviating

The downstream system sometimes forces it. When it does:

1. Do it in the smallest scope that works — a different `derive_run_id`, not a different
   lifecycle.
2. Say so in the README's *Limitations*, with the reason.
3. If the same deviation shows up in a second integration, it is not a deviation any more.
   Change this document.
