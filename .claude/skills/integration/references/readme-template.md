# README template

Every integration ships a README. It is the contract: the address, param and value schemas
are what callers write against, and nobody should have to read the worker to learn them.

Copy the skeleton below, fill in every section, and delete nothing — an empty section is
information too ("this integration has no configuration"). Keep it next to the code:
`docs/integrations/<name>.md` for an in-tree worker.

The three schemas must appear here **and** in the worker's module doc comment. That is
deliberate duplication: the module comment is for whoever changes the worker, the README is
for whoever calls it, and they answer different questions. If they disagree, the code wins
and the README is a bug.

---

```markdown
# <Name> integration

One durable promise ⇄ one <run/job/pipeline> in <system>.

<One paragraph: what this bridges, and the one property of the downstream system that
makes it safe to bridge — normally that a create can be deduplicated by a client-supplied
key. If that property is missing or partial, say so here, at the top, not in a footnote.>

## Address

    <scheme>://<deployment>

    <scheme>://prod.example.com

| Part | Meaning |
|---|---|
| scheme | Always `<scheme>`; it is what routes the promise to this worker. |
| authority | A *deployment name*, not a hostname — a key into `deployments` config. Never a credential. |

<The address carries routing and nothing else. What to act on is the param — say so here,
so a caller does not go looking for it in the path. If this integration *does* put more in
the address, this is where to justify it: the path must be carrying routing information
(different credentials, different endpoints), not request data.>

<What a malformed address does: rejected with `invalid_request`, on the first delivery, with
the address quoted. List the shapes you deliberately reject.>

## Param

**Encoding:** <base64 UTF-8 JSON | protobuf `<message>` | plain UTF-8 | …>

<The request schema. A JSON Schema, a proto snippet, or a field table — whatever suits the
encoding. Include which fields are required, which have defaults, and what the defaults
are.>

```json
{ "…": "…" }
```

<Whether the SDK/CLI invocation envelope `{"func","args","version"}` is accepted, and if so
which element is taken as the request.>

**Validation:** <what is checked, and what a violation produces — normally a `rejected`
promise carrying `invalid_request` and the offending field. Say explicitly that a bad param
is never retried, because the param is immutable.>

## Value

**Encoding:** <as above>

Resolved:

```json
{ "…": "…" }
```

Rejected:

```json
{ "error": { "kind": "…", "message": "…" }, "…": "…" }
```

| `error.kind` | Meaning | Caller's move |
|---|---|---|
| `invalid_request` | The param or address failed validation. | Fix the request. |
| `not_found` | The addressed resource does not exist. | Fix the address, or create it. |
| `unauthorized` | The worker's credentials were rejected. | Operational — page someone. |
| `downstream_failed` | The run started and finished in a failure state. | The normal failure. |

<Also: a promise the server times out carries whatever the server wrote, **not** this
schema. Callers must handle a timed-out promise with an empty value.>

## Idempotency

**Key:** <the exact derivation, e.g. `resonate-{sanitised promise id}-{digest}`>

<Why it is safe: it is a pure function of the promise id, so it is identical on every
redelivery, restart and failover.>

**Duplicate behaviour:** <what the downstream system does with a repeated key — e.g.
"returns 409, which this worker treats as success and re-attaches to the existing run".>

**Tier:** <1 = client-supplied id or idempotency key; 2 = searchable unique label, with the
race window described; 3 = none, and then this section explains what was agreed instead and
what can still go wrong. See references/idempotency.md.>

## Configuration

```yaml
transports:
  <name>:
    enabled: true
    lease_timeout: <ms, or omitted to follow tasks.lease_timeout>
    poll_interval: <ms>
    max_poll_interval: <ms>
    deployments:
      prod.example.com:
        base_url: https://prod.example.com
        …
```

| Setting | Default | Notes |
|---|---|---|
| … | … | … |

**Credentials** come from deployment config, never from an address. In production supply
them through the environment: `RESONATE_TRANSPORTS__<NAME>__DEPLOYMENTS__PROD__TOKEN=…`.

## Operating it

- **Timeouts and orphans.** <What happens when `promise.timeoutAt` passes while a run is
  still going: the server settles `rejected_timedout` and this worker <stops watching /
  cancels the run>. Say which, and how an operator finds the orphan.>
- **Two clocks.** Heartbeat at `lease_timeout / 3`; poll the downstream on
  `poll_interval`, backing off to `max_poll_interval`. They are independent, so
  `max_poll_interval` may exceed `lease_timeout`.
- **What to watch.** <Metrics and log fields worth alerting on. The create-conflict rate
  is the interesting one: it should be small and non-zero, and a sudden zero usually means
  the idempotency key stopped being deterministic.>
- **Unrecognised downstream states** are treated as *not finished*, and logged. A
  downstream release that adds a state will show up as a warning, not as rejected promises.

## Versions and compatibility

<Which versions of the downstream system this is written against, and where they differ —
API paths, auth, field names. Anything version-specific should be isolated in config so a
reader can tell what to change.>

## Limitations

<The honest list. Anything a caller would be surprised by: what is not supported, what is
best-effort, what races remain, what is untested. If idempotency is Tier 2 or 3, the
window belongs here as well as above.>

## Testing

```bash
<how to run the unit tests>
<how to run it against a real or mocked downstream>
```

The test that matters: `SIGKILL` the server between the downstream create and the settle,
restart it against the same storage, and assert the downstream holds **exactly one** run
for that promise and the promise still settles from it.
```
