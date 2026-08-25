# The shape integrations share

Integrations look alike because they *work* alike. Every one of them claims a task, starts
one run downstream, watches two clocks while it runs, and settles one promise. That common
mechanism is what a reader recognises: open `databricks` after reading `airflow` and the
answer to *where does it start the run, where does it decide, where does it settle* should be
in the same place, with the same name, in the same order — "oh yeah, I see".

What they are *not* is the same integration with the names changed. Airflow addresses a
deployment and triggers a DAG; a data warehouse addresses a workspace and submits a
statement; a build system addresses an org and starts a pipeline. Targets differ, config
differs, params differ, "done" differs — each in the way that makes sense for that system.
Forcing those to match would make the integration lie about the system it wraps.

So: **the skeleton is fixed, the flesh is yours.** Two lists follow.

---

## What is the same everywhere

These are the things a reader relies on without checking. Change one and the integration
stops being recognisable — or stops being safe.

### The layering

```
send      hand off, return Ok(())        — never blocks the router
run       claim → execute → settle       — the protocol frame
execute   outcome → Settlement           — the error policy, and nothing else
work      resolve, validate, start, watch — the integration proper
```

`run` never does downstream work; `work` never settles. `execute` is the only place that
decides *whether* a failure settles the promise. Splitting them differently is the one
deviation that always reads as a different integration rather than a variation.

### Claim first

Nothing observable happens before `task.claim` succeeds. Before it, the only outcome a
worker can report is "not mine" — dropped, redelivered later. After it, every failure has
somewhere to go.

### Two clocks

The lease clock (`task.heartbeat`, cadence derived from the lease) and the downstream clock
(the poll interval) run independently and at different rates. One keeps the task alive; the
other asks whether the run is done. Never derive one from the other.

### Idempotent create, duplicate as success

`start` derives its key from the promise id — a pure function, so a restarted worker derives
the same one — and treats "already exists" as success. Every integration rests on this; it is
the precondition the whole skill assumes (`references/idempotency.md`).

### Permanent versus Transient

| | Permanent | Transient |
|---|---|---|
| Means | Can never succeed | Might succeed later, or the outcome is unknown |
| Worker does | Settle `rejected` with a kind | Return without settling |
| Then | Caller sees the reason | Lease expires, redelivery retries |

**Ambiguous is transient.** Retrying is safe precisely because `start` is idempotent.

### The outer value shape

What is inside `output` is yours. What is around it is not:

```json
{ "run": { "id": …, "state": …, "startedAt": …, "endedAt": …, "url": … }, "output": { … } }
{ "run": { … },                                                          "error": { "kind": …, "message": … } }
```

`run` on both branches, a human-openable `url` on both branches, exactly one of
`output` / `error`. A caller that handles two integrations should need one code path for
this, not two.

### One file, sections in this order

`src/transport/transport_<name>.rs`:

```rust
//! <Name> worker — one durable promise ⇄ one <run> in <system>.
//! ## Address schema  ## Param schema  ## Value schema

// ─── Address ───────────  const PID, fn parse_address
// ─── Worker ────────────  struct <Name>, pub struct <Name>Worker, impl ResonateWorker (`send`)
// ─── Lifecycle ─────────  RunContext, Target, the decoded request type, Settlement,
//                          RunState, Monitored, <Name>Error, impl RunContext { … }
// ─── Pure helpers ──────  heartbeat_interval_ms, classify_run, derive_run_id, decode_param
#[cfg(test)] mod tests
```

### Names for the roles

Every integration has these roles because every integration does these things. Use the same
name for the same role — a reader greps `derive_run_id` to find the idempotency key and
should not have to guess what you called it.

| Name | The role |
|---|---|
| `parse_address` | Address → deployment name |
| `PID` | The pid this worker claims under |
| `RunContext` | One delivery in flight: `{ worker, task }` |
| `Target` | The resolved deployment, looked up after the claim |
| `Settlement` | `Option<(SettleState, PromiseValue)>` — `None` leaves the promise alone |
| `run` / `execute` / `work` | The layering above |
| `start` | The idempotent create. A duplicate is success. |
| `check` | One status check |
| `poll_until_done` | The downstream clock |
| `derive_run_id` | The idempotency key, a pure function of the promise id |
| `decode_param` | Decode **and validate**; every failure permanent |
| `classify_run` | Downstream state → `RunState`; unknown ⇒ `Pending` |
| `heartbeat_interval_ms` | The lease clock's cadence |
| `<Name>Error` | `Permanent { kind, message, … }` \| `Transient(…)` |

A role your system does not have needs no function — a system that streams status has no
`poll_until_done`. What it does not get is a *different name for the same role*.

### The README's sections

Same headings, same order (`references/readme-template.md`), so two READMEs diff down to
what actually differs.

### The tests that prove the invariants

Names, so a reader can check any integration has them:

```
address_rejects_wrong_shape
run_id_is_a_pure_function_of_the_promise_id
run_id_stays_unique_after_truncation
param_rejects_unknown_fields
malformed_param_is_permanent_not_transient
unknown_state_is_pending_not_failed
heartbeat_always_fits_inside_the_lease
```

Plus the crash-and-restart test that asserts exactly one downstream run per promise
(`references/testing.md`). Every integration needs that one; it is the only direct proof of
the property the design assumes.

---

## What differs, and should

Decide each of these for your system. Copying Airflow's answer because it is Airflow's
answer is the mistake this section exists to prevent.

| | Fixed | Yours to decide |
|---|---|---|
| **Address** | A scheme that routes to your worker; the authority names a configured deployment; no request data | Whether anything beyond the deployment is *routing* — a region, a workspace, an account — and therefore belongs in the address at all. Airflow's is bare `airflow://<deployment>` and rejects a path; another system may legitimately need more. |
| **Config** | `enabled`, a deployments map, and validation for the failures that would otherwise be silent | Every field under a deployment. Auth is whatever the system uses — token, key pair, OIDC, a signed request. Poll defaults belong to the system's real latency: seconds for a query, minutes for a DAG. |
| **Param** | Decoded into a typed struct, validated on arrival, malformed ⇒ permanent, no credentials and no deadline inside | The fields, and the encoding. JSON is the default because most things speak it, not because it is required. What identifies the thing to run — a DAG id, a job id, a statement — is a param, not part of the address. |
| **"Done"** | `classify_run` maps downstream state to `Pending` / `Succeeded` / `Failed`, and unknown ⇒ `Pending` | Which states those are, and whether "done" is even a state — it may be an exit code, a terminal event, or the absence of a row. |
| **`run` summary** | Present on both branches, carries an id and an openable `url` | Which identifiers make sense. A run id and a dag id; a statement handle; a build number and an attempt. |
| **`output`** | Inside the fixed envelope | Everything. Whatever the run produced that a caller would want. |
| **Error kinds** | The shared vocabulary below means the same everywhere | Which of them apply, and whether your system has a failure none of them describes. |
| **Retries inside `work`** | Ambiguity is transient; redelivery is the outer retry | Whether a fast in-process retry is worth it before giving the delivery back. |

### Error kinds — shared vocabulary, not a closed list

Callers branch on `error.kind`, so these five mean the same thing in every integration:

| kind | Means | Settles as |
|---|---|---|
| `invalid_request` | Param or address failed validation | `rejected` |
| `not_found` | The addressed resource does not exist | `rejected` |
| `unauthorized` | The worker's credentials were rejected | `rejected` |
| `downstream_failed` | The run started and finished in a failure state | `rejected` |
| `canceled` | The run was cancelled out of band | `rejected` |

If your system has a failure none of these describes, add a kind and document it in the
README — do not stretch one of these to cover it, because that is what breaks the caller.
`rejected_canceled` as a *promise state* stays reserved for cancellation initiated through
Resonate; a run cancelled downstream settles `rejected` with `kind = "canceled"`.

---

## When the skeleton itself does not fit

It happens — a system with no create/poll split, a system that will only push. Then:

1. Deviate in the smallest scope that works. A different `derive_run_id` is a variation; a
   different lifecycle is a different kind of worker.
2. Say so in the README's *Limitations*, with the reason. An unexplained deviation reads as
   an oversight.
3. If the same deviation turns up in a second integration, it is not a deviation any more —
   update this document.
