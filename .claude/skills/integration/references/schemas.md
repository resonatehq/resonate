# Schemas: address, param, value

The integration's public contract. Write these first, get them reviewed, then code.

They go in two places, on purpose: the worker's module doc comment, where they sit next to
the code that has to honour them (see the header of `src/transport/transport_airflow.rs`),
and the integration's README, where a caller will actually look — see
`references/readme-template.md`. If the two disagree, the code wins and the README is a bug.

---

## Address schema

An integration **owns a URL scheme**. The router matches on the scheme and hands over the
untouched address; `core` never learns the syntax past it. So a new scheme costs a
registration and nothing else.

```
<scheme>://<deployment>/<collection>/<id>[?<qualifiers>]

airflow://prod.astronomer.io/dags/etl_daily
airflow://airflow.internal:8080/dags/hourly_rollup
dbt://cloud.getdbt.com/accounts/1234/jobs/5678
```

Rules:

1. **Scheme names the integration**, so a mis-addressed promise fails loudly at delivery
   rather than silently doing nothing.
2. **Authority names the *deployment*, not a host.** It is a key into the worker's config
   map, which is where base URLs and credentials live. Take it verbatim — the Airflow
   worker uses `Url::authority()` rather than `host_str()` precisely because `host_str`
   lowercases and strips the port, and a config key is neither a hostname nor
   case-insensitive.
3. **Path names the resource** in the downstream system's own vocabulary. Don't invent
   synonyms for `dags`, `jobs`, `pipelines`.
4. **Query string carries optional qualifiers** with defaults. Anything required goes in the
   path.
5. **No secrets, ever.** Addresses land in logs, promise tags, search results and error
   messages.
6. **Parse strictly, reject early, and unit-test every malformed shape.** `is_valid_address`
   only checks that the string is a URI, so the worker is the *only* thing standing between
   a typo and a promise that never settles.

```rust
impl AirflowAddress {
    pub fn parse(address: &str) -> Result<Self, String> { … }
}
```

Parse it **after the claim**, not in `send`. Before the task is claimed the only way to
report anything is `Err(Unavailable)`, which the dispatch loop logs and drops — so a
permanently malformed address would retry every `retry_timeout` until the promise timed out,
and the caller would see `rejected_timedout` rather than the reason. Past the claim it is an
ordinary permanent error: the promise settles `rejected` with `invalid_request` and the
offending address quoted.

## Deployment configuration

The address authority resolves through config, never through the address:

```yaml
transports:
  airflow:
    enabled: true
    deployments:
      prod.astronomer.io:
        base_url: https://prod.astronomer.io
        api_version: v2
        token: ${AIRFLOW_TOKEN_PROD}
      airflow.internal:8080:
        base_url: http://airflow.internal:8080
        api_version: v1
        username: resonate
        password: ${AF_PASS}
```

In production these arrive as environment variables:
`RESONATE_TRANSPORTS__AIRFLOW__DEPLOYMENTS__PROD__TOKEN=…`.

An address whose authority is not configured must fail with a clear message naming the
known deployments — that is a deployment error, and retrying will not fix it.

---

## Param schema

The request lives in `promise.param`:

```json
{ "headers": { "content-type": "application/json" }, "data": "<encoded request>" }
```

`data` is a string, and to the protocol it is **opaque**. JSON base64-encoded into it is the
common choice — it is what the CLI and SDKs produce (`src/cli.rs::b64_encode_data_field`) —
but an integration is free to carry protobuf, msgpack, a bare string, anything. Two
obligations come with that freedom:

- **Document the encoding**, in the README and in the module doc comment. `headers` is
  where to declare it on the wire; a caller cannot guess it.
- **Validate against the schema before the first side effect**, and reject on violation. A
  promise's param is immutable, so a request that is malformed now is malformed on every
  redelivery: it is a permanent error, never a retry. Name the offending field — the
  promise value is the only channel back to whoever sent it.

### Accept the SDK invocation envelope

A promise created by `resonate invoke` or by an SDK's remote invocation carries:

```json
{ "func": "<function name>", "args": [ … ], "version": 1 }
```

An integration that accepts this is callable from any Resonate application as an ordinary
remote function. Accept a bare object too, for callers using `promise.create` directly:

```rust
let body = if value.get("func").is_some() && value.get("args").is_some() {
    value.get("args").and_then(Value::as_array).and_then(|a| a.first())
         .cloned().unwrap_or_else(|| json!({}))
} else {
    value
};
```

### Request object

Keep it a thin pass-through of the downstream request:

```json
{ "conf": { "date": "2026-08-24" }, "note": "nightly", "logicalDate": null }
```

Rules:

- **No idempotency key field.** The key is derived from the promise id; letting callers
  supply one lets them break exactly-once.
- **No credentials.** They come from deployment config.
- **Keep it a pass-through.** Every field you re-map is a field you re-map again when the
  downstream API changes.
- **An absent param is a valid request** where that makes sense — the Airflow worker treats
  it as a trigger with no conf.
- **Validate before the first side effect**, and classify violations as *permanent*. A
  malformed param will never become valid on retry.
- **Version it** if the schema will evolve — promises can be in flight for days. A
  `schema` discriminant, or the SDK envelope's own `func`/`version`.

The promise's `timeoutAt` is the deadline for the whole operation. Do not duplicate it into
the param.

---

## Value schema

Same encoding. Define **both** branches.

### Resolved

```json
{ "run": { "id": "resonate-airflow.etl.1-1f3a9c02", "state": "success",
           "startedAt": "2026-08-24T10:00:00+00:00",
           "endedAt": "2026-08-24T10:05:00+00:00",
           "url": "https://prod.astronomer.io/dags/etl_daily/runs/resonate-…" },
  "output": { "runType": "manual", "conf": {}, "note": null, "logicalDate": null } }
```

### Rejected

```json
{ "run": { "id": "…", "state": "failed", "url": "https://…" },
  "error": { "kind": "downstream_failed",
             "message": "DAG run finished in state failed" } }
```

Suggested `error.kind` values — keep the set small and stable, since callers branch on it:

| kind | Meaning |
|---|---|
| `invalid_request` | Param or address failed validation. Never retryable. |
| `not_found` | The addressed downstream resource does not exist. |
| `unauthorized` | The worker's credentials were rejected. |
| `downstream_failed` | The run was created and finished in a failure state. The normal failure. |
| `canceled` | The run was cancelled out of band. |

Rules:

- **Include the run identity and a UI link on both branches.** The first question about a
  failed promise is "where do I look?".
- **Distinguish "did not start" from "started and failed".** They have different
  operational responses, and only the value can carry that — the promise state cannot.
- **The server also produces `rejected_timedout` on its own** when `timeoutAt` passes. There
  the value is whatever the server wrote, **not** your schema. Callers must handle a
  timed-out promise with an empty value.
