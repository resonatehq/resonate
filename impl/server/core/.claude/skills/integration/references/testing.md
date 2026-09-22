# Testing an integration

Four layers, and they catch different things. The last one is the only one that proves the
design works, and it is the one people skip.

| Layer | Catches | Needs |
|---|---|---|
| Unit | The decisions that carry the design | Nothing |
| API double | — | A separate process |
| End to end | Wiring, config, the value schema | Server + double |
| Crash and restart | **Whether idempotency actually holds** | Server + double + file-backed storage |

---

## 1. Unit tests

Every part of an integration that carries a design decision is a pure function, on purpose.
Test all of them; they need no server, no network, and no async.

**Address parsing.** Every malformed shape, *including the ones you deliberately reject*:

```rust
#[test]
fn address_rejects_a_path() {
    // The old form put the DAG here. Rejecting it makes the move loud
    // instead of silently triggering the wrong thing.
    let err = parse_address("airflow://prod/dags/etl_daily").unwrap_err();
    assert!(err.contains("takes no path"), "{err}");
}
```

Also pin the parse decisions that look like details and are not — the Airflow worker takes
the authority verbatim, so there is a test that `airflow://Airflow.Internal:8080` keeps its
case and its port, because `host_str()` would have quietly destroyed both.

**The idempotency key.** Three properties, and all three matter:

```rust
#[test] fn run_id_is_a_pure_function_of_the_promise_id() { … }   // same id ⇒ same key
#[test] fn run_id_is_readable_and_url_safe() { … }               // charset the downstream accepts
#[test] fn run_id_stays_unique_after_truncation() {
    // Two ids sharing their first 100 characters must not collide: the digest
    // is taken over the whole id, not the truncated prefix.
    assert_ne!(derive_run_id(&format!("{}-a", "x".repeat(120))),
               derive_run_id(&format!("{}-b", "x".repeat(120))));
}
```

**Param decoding and validation.** Required fields, defaults, unknown fields, every
malformed encoding, and the shapes you deliberately do not accept:

```rust
#[test]
fn param_does_not_unwrap_the_sdk_envelope() {
    let envelope = json!({"func": "trigger", "args": [{"dag": "d"}], "version": 1});
    assert!(decode_param(param(envelope).as_deref()).is_err());
}
```

Assert the *classification*, not just that it failed — a malformed param must be
`Permanent`, because a `Transient` here would retry forever.

**Downstream state mapping.** The terminal states, the running states, and — the one that
bites — states you have never heard of:

```rust
#[test]
fn unknown_state_is_pending_not_failed() {
    // A future release adding a state must not reject healthy promises.
    for state in ["deferred", "up_for_retry", "", "SOMETHING_NEW"] {
        assert!(matches!(classify_run(json!({ "state": state }), "r"), RunState::Pending));
    }
}
```

**The clocks.** The heartbeat cadence has to fit inside the lease across the whole
configurable range, and the poll backoff must not overflow:

```rust
#[test]
fn heartbeat_always_fits_inside_the_lease() {
    // A floor added to avoid hammering will push the first beat past a lease
    // shorter than the floor — config allows a lease as short as 1ms.
    for lease in [1_i64, 2, 500, 999, 1_000, 2_999, 3_000, 15_000] {
        let beat = heartbeat_interval_ms(lease) as i64;
        assert!(beat >= 1 && beat <= lease, "lease {lease}: beat {beat}");
    }
}
```

That test found a real bug: `(lease / 3).max(1000)` put the first heartbeat after the lease
had already expired on any lease under a second.

---

## 2. The API double

You need one, and it has to be **a separate process** — an in-process mock dies with the
worker, and the crash test needs it to outlive one.

It only has to implement what the integration touches, plus two things for the tests:

```python
# The property the whole design rests on. Without this, the double proves nothing.
if key in STATE["runs"]:
    STATE["conflicts"] += 1
    return self._send(409, {"detail": "already exists"})
```

- **Duplicate rejection**, exactly as the real system does it. If your double happily
  creates two runs with the same id, every test passes and the integration is broken.
- **Counters** — creates, conflicts, status polls — on a `/__stats` endpoint. Assertions
  about *how many times* the downstream was called are most of what these tests are for.
- **Controllable completion** — `/__finish {"polls": N}` so a test can hold a run open
  while it kills things, then let it finish.

Write it against the real API's shapes, not your worker's expectations. A double that
mirrors your bugs is worse than no double.

---

## 3. End-to-end tests

Run the server with the integration enabled, point it at the double, create a promise, and
assert **both** the promise outcome and the downstream call count. The second half is what
catches "it settled correctly but hammered the API twelve times".

The set worth having, each pinning a different branch of the error policy:

| Test | Asserts |
|---|---|
| Happy path | `resolved`, value schema correct, run id and URL present |
| Downstream failure | `rejected`, `kind = downstream_failed`, run identity still present |
| Unknown resource | `rejected`, `kind = not_found`, and **settles rather than retrying** |
| Unknown deployment | Promise stays **pending** — a transient failure must not settle |
| Malformed param | `rejected` on the first delivery, and **zero downstream calls** |
| Malformed address | `rejected` with the address quoted, not left to time out |

The last three are the ones that catch a misclassified error. A permanent failure that
retries and a transient failure that rejects both look fine in the happy path.

Make them fast with short timings rather than long sleeps:

```bash
RESONATE_TASKS__RETRY_TIMEOUT=5000
RESONATE_TRANSPORTS__AIRFLOW__LEASE_TIMEOUT=4000
RESONATE_TRANSPORTS__AIRFLOW__POLL_INTERVAL=1000
```

Be aware of what that hides: a short lease masks a slow downstream create, which in
production would lapse the lease mid-flight. Run the suite once at production timings too.

---

## 4. Crash and restart — the test that matters

Everything above passes on an integration whose idempotency is broken. This is the one that
does not.

```
1. Hold the downstream run open      (double: finish after a large N)
2. Create the promise; wait until the downstream run exists
3. SIGKILL the whole server
4. Restart it against the same storage
5. Wait for redelivery
6. Let the run finish
7. Assert: exactly one downstream run for this promise, and the promise resolved
```

Five details, each of which will silently defeat the test if you get it wrong:

- **File-backed storage, not in-memory.** `resonate dev` loses every promise on restart, so
  there is nothing to redeliver and the test passes having proved nothing. Use `serve` with
  a sqlite path.
- **`SIGKILL`, not a graceful stop.** A clean shutdown releases the task, which exercises a
  different and much easier path. You want the lease to expire.
- **Scope the assertion to this promise.** Asserting on the double's global run count breaks
  the moment another test runs first — filter by the run ids derived from this promise id.
- **Assert the conflict path actually ran.** `conflicts > 0` is what proves the second
  attempt hit the duplicate. Without it the test passes even if the redelivery never
  happened.
- **Check the restarted server is configured.** A restart that comes up without the
  integration enabled produces no redelivery and looks exactly like a broken idempotency
  key. (This is not hypothetical — it is a real production failure mode too: restart a
  server without the transport enabled and in-flight promises silently stop progressing.)

```python
mine = lambda st: [r for r in st["run_ids"] if r.startswith(f"resonate-{promise_id}-")]

assert promise["state"] == "resolved"
assert len(mine(final)) == 1, f"DUPLICATE RUNS: {mine(final)}"
assert final["conflicts"] > before["conflicts"], "the 409 re-attach path never ran"
```

Then run it again with the kill placed **during** the create request rather than after it.
That is the ambiguous case — the request may or may not have been applied — and it is the
one the whole `Unavailable` retry contract exists for.

---

## What not to test

The server's protocol. Redelivery timing, lease expiry, promise timeouts and settlement are
the server's behaviour and it has its own tests. Your integration's tests should assume
those work and pin **your** decisions: what you send, when you retry, and what you settle.
