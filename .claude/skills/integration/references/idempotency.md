# Idempotency: the precondition

## The problem, precisely

Two independent mechanisms deliver the same request more than once, and neither is
avoidable.

**Redelivery.** The server re-enqueues `execute` every `tasks.retry_timeout` while a task is
pending, and again on every lease expiry. The message is `{task: {id, version}}` — no
delivery counter, no redelivery flag, and no worker-side state that survives a crash. A
worker that has just received `execute` for promise `P` cannot answer *"did an earlier
attempt already start the downstream run?"*

**`Unavailable`.** From `src/core/mod.rs`, the retry contract on the one out-of-band error:

> The caller must assume the request *may already have been applied*. A connection refused
> before the first byte and a timeout after the last are both `Unavailable` and the caller
> cannot tell them apart, so retries must be idempotent.

`task.version` does not help. It counts acquisitions, and it is bumped by `task.acquire`
*before* any downstream call happens. Version 5 does not mean "four runs were started".

The naive fixes all fail:

| Attempted fix | Why it fails |
|---|---|
| Local file / in-memory map of created ids | Lost on restart; not shared across processes |
| A side database of promise-id → run-id | Correct only if written in the *same transaction* as the downstream create, which foreign APIs do not offer. Otherwise it moves the crash window. |
| "Only act on version 1" | Version 1 delivery can itself be lost before the downstream call |
| Check-then-create | A crash between check and create still duplicates |

## The rule

> **Do not try to remember. Fire the request again on every delivery, with a deterministic
> key derived from the promise id, and let the downstream system reject the duplicate.**

Redelivery stops being a hazard and becomes the recovery mechanism: the second attempt hits
"already exists", learns the run's identity, and proceeds to monitoring.

## Decision tree

Run this before writing any code.

```
Does downstream create accept a client-supplied run/job id, or an idempotency key?
│
├─ YES ──► Tier 1. Use it. Derive it from the promise id. Build the integration.
│
└─ NO
   │
   ├─ Can create attach a searchable, unique label, AND can you look a run up by it?
   │   │
   │   ├─ YES ──► Tier 2. Lookup-then-create, with a narrow race window. Document it.
   │   │          Acceptable only when a duplicate run is cheap and reversible.
   │   │
   │   └─ NO ──► Tier 3. STOP. Discuss with the user. Do not ship "best effort".
```

## Tier 1 — client-supplied id or idempotency key

The key must be a **pure function of the promise id**. From
`src/transport/transport_airflow.rs`:

```rust
fn derive_run_id(promise_id: &str) -> String {
    let digest = fnv1a64_hex(promise_id.as_bytes());
    let safe: String = promise_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' { c } else { '_' })
        .take(100)
        .collect();
    format!("resonate-{safe}-{digest}")
}
```

Three properties, each earning its place:

- **Deterministic.** No UUID, no clock, no hostname, no `task.version`. If the value can
  differ between two deliveries, it is not an idempotency key.
- **Readable prefix**, so the run is findable in the downstream UI from the promise id.
- **Digest over the *whole* id**, so two ids sharing their first 100 characters do not
  collide after sanitising and truncation. There is a unit test pinning exactly that.

The digest is hand-rolled FNV-1a rather than `DefaultHasher`, because the value is written
into a foreign system and must be reproducible by every future build —
`DefaultHasher`'s output is explicitly not stable across Rust releases.

Then treat "already exists" as success:

```rust
match status {
    200 | 201 => Ok(()),
    409 => { /* an earlier delivery got there first — re-attach */ Ok(()) }
    404 => Err(Permanent { kind: "not_found", .. }),
    other => Err(Transient(..)),
}
```

**Systems that fit naturally:** Apache Airflow (`dag_run_id`), Databricks Jobs
(`idempotency_token`), Stripe and most payment APIs (`Idempotency-Key`), Temporal (workflow
id + reuse policy), Kubernetes Jobs (object name).

## Tier 2 — searchable unique label

No id, but create accepts a label you can search on. Search first, create with the label if
absent.

Be explicit about the residual window: attempt A creates a run, its lease expires while the
create is still in flight and the label is not yet visible to search, attempt B searches,
misses, and creates a second run. (`task.acquire` prevents two *concurrent* attempts, so
this is the only remaining hole.)

Small but real. Acceptable only if a duplicate is cheap and reversible. Say so in the
module doc comment, and set the lease `ttl` generously relative to create latency.

## Tier 3 — no idempotency available

The integration **cannot** be made exactly-once, and pretending otherwise is worse than not
shipping. Bring the user these options:

1. **Put an idempotent façade in front of it** — a thin service owning a
   `promise_id → run_id` table with a unique constraint. The only option that restores
   Tier 1 semantics; costs a component and a database.
2. **Change the downstream call.** A non-idempotent "start job" often has an idempotent
   sibling: write to an object-storage key named after the promise, insert a row with a
   unique constraint, publish to a deduplicating queue. Retarget the integration at that.
3. **Accept at-most-once.** Settle `rejected` on any uncertainty instead of retrying.
   Duplicates become impossible; lost work becomes possible and visible.
4. **Accept duplicates explicitly.** Only when the run is idempotent *in effect* — e.g. a
   full-refresh job overwriting the same partition. Confirm with whoever owns the
   downstream system; do not assume it.
5. **Do not integrate.** Have a human trigger the run and use Resonate only to observe it.

Record the decision and its consequences in the module doc comment. The next person to
debug a duplicate run needs to know it was a choice.

## Checking your work

The only test that matters:

1. Create the promise; let the integration create the downstream run.
2. `SIGKILL` the server between create and settle.
3. Restart it against the same storage and wait for redelivery.
4. Assert the downstream system holds **exactly one** run for that promise.
5. Repeat with the kill placed *during* the create request, not just after it.

Also verify the replacement settles the promise from the existing run rather than waiting
forever — re-attachment is half of what idempotent create buys you.
