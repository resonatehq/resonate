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

**Put as little in the address as routing needs.** The scheme selects the worker; the
authority selects which deployment of the downstream system to talk to. That is usually
all:

```
<scheme>://<deployment>

airflow://prod.astronomer.io
dbt://cloud.getdbt.com
```

*What* to do there — which DAG, which job, which pipeline — is the request, and the request
is the param. Pushing it into the path buys nothing and costs two parsers, two sets of
malformed cases, and two places a caller has to look to understand one call. It also
freezes into a routing key something that callers vary per promise.

The exception is a downstream system with several genuinely distinct resource *kinds* that
route differently — different credentials, different endpoints. Then the path is carrying
routing information and belongs in the address.

Rules:

1. **Scheme names the integration**, so a mis-addressed promise fails loudly at delivery
   rather than silently doing nothing.
2. **Authority names the *deployment*, not a host.** It is a key into the worker's config
   map, which is where base URLs and credentials live. Take it verbatim — the Airflow
   worker uses `Url::authority()` rather than `host_str()` precisely because `host_str`
   lowercases and strips the port, and a config key is neither a hostname nor
   case-insensitive.
3. **No secrets, ever.** Addresses land in logs, promise tags, search results and error
   messages.
4. **Parse strictly, reject early, and unit-test every malformed shape** — including the
   shapes you deliberately do *not* accept. The Airflow worker rejects
   `airflow://prod/dags/etl_daily` with "takes no path", so a caller using an older form
   gets an error instead of a surprise.

```rust
fn parse_address(address: &str) -> Result<String, String> { … }
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

### One shape, and only one

Do **not** also accept the SDK invocation envelope `{"func", "args", "version"}`. It is the
SDK's convention for SDK functions, and an integration is not one. Accepting both means two
shapes to document, validate and test, and an ambiguity you cannot resolve: a legitimate
request that happens to carry a `func` field gets silently re-read as an envelope.

A caller reaches an integration by creating a promise with the integration's param schema.
That is the contract; keep it single.

### Request object

Keep it a thin pass-through of the downstream request:

```json
{ "dag": "etl_daily", "conf": { "date": "2026-08-24" },
  "note": "nightly", "logicalDate": null }
```

`dag` — *what to act on* — lives here rather than in the address, for the reasons above.

Rules:

- **No idempotency key field.** The key is derived from the promise id; letting callers
  supply one lets them break exactly-once.
- **No credentials.** They come from deployment config.
- **Keep it a pass-through.** Every field you re-map is a field you re-map again when the
  downstream API changes.
- **Reject unknown fields.** `#[serde(deny_unknown_fields)]` turns a typo into an error
  naming the field instead of a silently ignored setting.
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

`error.kind` is **shared vocabulary**: these five mean the same thing in every integration,
because callers branch on them and should not have to know which integration they are talking
to. Use them where they apply; if your system has a failure none of them describes, add a kind
and document it in the README rather than stretching one of these to cover it
(`references/structure.md`).

| kind | Meaning |
|---|---|
| `invalid_request` | Param or address failed validation. Never retryable. |
| `not_found` | The addressed downstream resource does not exist. |
| `unauthorized` | The worker's credentials were rejected. |
| `downstream_failed` | The run was created and finished in a failure state. The normal failure. |
| `canceled` | The run was cancelled out of band. The promise settles `rejected`, not `rejected_canceled` — that state is reserved for cancellation initiated through Resonate. |

The outer shape is fixed — `run` plus exactly one of `output` / `error`. What is inside
`output` is entirely the integration's own. See `references/structure.md`.

Rules:

- **Include the run identity and a UI link on both branches.** The first question about a
  failed promise is "where do I look?".
- **Distinguish "did not start" from "started and failed".** They have different
  operational responses, and only the value can carry that — the promise state cannot.
- **The server also produces `rejected_timedout` on its own** when `timeoutAt` passes. There
  the value is whatever the server wrote, **not** your schema. Callers must handle a
  timed-out promise with an empty value.
