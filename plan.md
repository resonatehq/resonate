# Porting the S3 kernel to `resonate-server-blob`

A plan for landing Andres' `s3-kernel` branch on `main` as an independent crate
that implements `ResonateServer` over blob storage.

## Goal and constraints

One crate, `crates/resonate-server-blob`, that:

- implements **`ResonateServer`** (`crates/resonate-core/src/server.rs:40`) — one
  request in, one response out — and nothing lower;
- depends on **`resonate-core` and third-party crates only**. No dependency on
  `resonate-server-dbms`, on `resonate-timer-wheel`, or on anything in the
  binary. It is a complete server, not a storage engine behind one;
- is selected at startup like any other backend, and feature-gated so a
  deployment that does not run it does not compile `object_store`;
- **ships its own oracle.** Every `ResonateServer` gets one: an independent
  in-memory model of that server's observable behaviour, living in the same
  crate as the server it models.

Independence is the point: the blob backend keeps its own state machine, its
own timers, its own outgoing messages, its own snapshot and its own reference
model. It shares the *vocabulary* in `resonate-core` and nothing else.

The oracle is what makes that affordable. A backend held only against other
backends has to behave identically to them in every observable, which is a
claim the storage shapes do not support — the document design aggregates
deadlines per origin, the SQL schema carries them per entity, and neither is
wrong. Pairing each server with its own oracle puts the specification where the
server is: the in-crate differential holds the server to *its* model at full
fidelity, and the cross-server differential holds every model to the protocol.
Divergences that used to have to be argued away become two lines in two
different oracles.

## What is being ported

From `origin/s3-kernel`, ~10.7k lines and 258 unit tests:

| Source | Lines | Tests | What it is |
|---|---|---|---|
| `src/kernel/state.rs` | 614 | 13 | Document types, `Effect`, `apply_effects`, invariants |
| `src/kernel/handle.rs` | 2162 | 71 | Every request transition, as a pure function |
| `src/kernel/drain.rs` | 366 | 15 | Every deadline transition, as a pure function |
| `src/s3/store.rs` | 523 | 11 | The object-store port, six operations, over `object_store` |
| `src/s3/codec.rs` | 1002 | 21 | Document bytes |
| `src/s3/cache.rs` | 283 | 10 | Document cache, read path |
| `src/s3/applier.rs` | 1582 | 29 | One actor per origin: load, decide, perform |
| `src/s3/outbox.rs` | 612 | 13 | Post-commit sends, and the `LateRouter` knot |
| `src/s3/timer_queue.rs` | 205 | 5 | Armed deadlines, in memory |
| `src/s3/timerd.rs` | 844 | 18 | The firing loop; list only to recover |
| `src/s3/schedules.rs` | 808 | 17 | Cron recurrence |
| `src/s3/scan.rs` | 694 | 11 | Whole-store reads: search and `debug.snap` |
| `src/s3/server.rs` | 1069 | 24 | The `ResonateServer` impl and the one constructor |
| `tests/s3_live.rs` | 383 | — | Smoke test against a real bucket |

The design is two layers. The **kernel** is the protocol's state machine as a
pure function — `handle(&doc, req, now) -> (Vec<Effect>, Reply)` and
`drain(&doc, now) -> Vec<Effect>`, reading no clock, generating no ids, touching
no I/O. The **shell** performs what the kernel decides against one CAS'd object
per origin, with one actor per origin so decisions on a document are serialized
and a hot origin costs one write per round rather than one per request.

## Why `ResonateServer` is the right seam for this crate

A survey of what is already written against the port rather than against a
concrete type:

- `resonate-gateway-http` — `Arc<dyn ResonateServer>` (`src/lib.rs:100`, `src/routes.rs:67`)
- `resonate-transport-http-push` — `Weak<dyn ResonateServer>` (`src/lib.rs:265`)
- `resonate-transport-http-poll` — `Weak<dyn ResonateServer>` (`src/lib.rs:120`)
- `resonate-transport-gcps` — `Weak<dyn ResonateServer>` (`src/lib.rs:104`)
- `resonate-worker-bash` — `Weak<dyn ResonateServer>` (`src/lib.rs:161`)

Everything downstream of the server already speaks the port. **One place in the
tree is concrete on `Server`**: `src/processing/processing_timeouts.rs:15`,
whose loop drives `engine.tick`. A blob server owns its own firing loop
(`timerd`), so it does not need that loop and the coupling never bites.

The consequence to be explicit about: a `ResonateServer` implementation is not
compared by `diff/differential.rs`, which is `Arc<dyn ResonateEngine>`
(`diff/differential.rs:105`) and compares responses *plus* returned messages,
armed deadlines and `upcoming()`. The blob backend meets the other servers at
the port instead — responses plus `debug.snap` — and recovers the fidelity it
loses there against its own oracle, in its own crate. `main`'s oracle already
implements `ResonateServer` as well as `ResonateEngine`
(`crates/resonate-server-dbms/src/oracle.rs:2606`), so the port is a seam every
model in the workspace can already meet at. See **Testing** below.

## What the crate must own

Because it is a server rather than an engine, everything `src/server.rs` does
for the SQL engines is the blob crate's own:

| Concern | `Server` does it via | Blob crate |
|---|---|---|
| Debug-time gate | `src/server.rs:214` | `S3Server::process` already does this identically |
| Delivering messages | `Server::deliver` → router | `outbox.rs`, post-commit, at-most-once |
| Arming deadlines | `DeadlineTimer` (`resonate-timer-wheel`) | `timer_queue.rs` + durable timer objects |
| Firing deadlines | `processing_timeouts` sweep + wheel | `timerd.rs` firing loop |
| Readiness | `engine.ping()` | store probe, already present at `s3/server.rs:185` |
| Schedules | engine `ScheduleDue` timeouts | `schedules.rs` |

None of this is new work. It is work Andres already did, which is precisely why
the crate can be independent.

## Target layout

```
crates/resonate-server-blob/
  Cargo.toml
  src/
    lib.rs              // crate doc: the two layers and the graph between them
    kernel/
      mod.rs state.rs handle.rs drain.rs
    store.rs codec.rs cache.rs
    applier.rs outbox.rs
    timer_queue.rs timerd.rs schedules.rs scan.rs
    server.rs           // impl ResonateServer for BlobServer
    oracle.rs           // impl ResonateServer for the model of it
  tests/
    differential.rs     // server vs oracle, in process, no bucket
    live.rs             // #[ignore] unless a bucket is configured
```

Dependencies: `resonate-core`, `object_store = { version = "0.14.1", features = ["aws"] }`,
`async-trait`, `serde`, `serde_json`, `validator`, `tokio`, `tracing`, `cron`,
`chrono`, `prometheus`, `lazy_static`.

Root `Cargo.toml`: add the member, add a `blob` feature forwarding to it, add
the crate as an optional dependency the way the three SQL engines are gated.

## Phases

Each phase ends at a checkpoint that builds and tests green. Do not start the
next one red.

### Phase 0 — Take the code, keep the authorship

```
git checkout origin/s3-kernel -- src/kernel src/s3 tests/s3_live.rs
```

Move into the new crate, then commit with `Co-authored-by: Andres V
<andres.villegas@resonatehq.io>`. One commit that only *moves* code, so the
adaptation commits that follow are readable diffs.

### Phase 1 — Compile the kernel alone

The kernel imports only `crate::core::types`; rewrite to `resonate_core::types`.
**Every symbol it needs exists on `main`** — all 59 were checked against
`crates/resonate-core/src/types.rs` and none are missing. This phase is a
namespace rewrite plus the derive problem in *Mechanical deltas* below.

Checkpoint: `cargo test -p resonate-server-blob kernel::` — 99 tests.

### Phase 2 — Compile the shell

Same rewrite for `s3/*`, plus the metrics decision below. `store.rs` depends on
`object_store` alone and should need nothing.

Checkpoint: `cargo test -p resonate-server-blob` — 258 tests, no bucket needed
(`store.rs` ships an in-process store and a `FaultStore` that can cut the power
between two effects).

### Phase 3 — Implement the port as `main` defines it

1. Add `ready()`. The trait defaults it to `true` (`server.rs:56`); the blob
   server has the store probe already, so this is wiring, not design.
2. Drop `debug.start` / `debug.stop`. `main` removed them deliberately
   (`efceeea`) in favour of one flag set at startup: the clock belongs to the
   caller for the life of the process, or it never does. `S3ServerCfg.debug`
   already exists — make it the only switch, and make the outbox pause
   permanent under it rather than something an operation enters.
3. Keep the envelope checks the blob server does, but return the statuses
   `resonate_core::types::parse_and_validate` returns, since on `main` the
   gateway is the trust boundary and a malformed message never reaches the port.

Checkpoint: the crate's own tests still pass, with `debug.start` removed from
them.

### Phase 4 — Write the oracle

An in-memory model of the blob server, implementing `ResonateServer`, in
`src/oracle.rs`. `crates/resonate-server-dbms/src/oracle.rs` is the worked
example of the form — plain `BTreeMap`s behind a `Mutex`, one `match` on
`kind`, and an `impl ResonateServer` at the end — and 2729 lines is the honest
budget. Less, here, because this one answers only what the port makes
observable.

Three rules, and the value of the whole exercise rests on them:

1. **Write it from the protocol, never from the kernel.** Read `main`'s
   engines, `main`'s oracle and the request types; do not read `handle.rs` while
   writing `oracle.rs`. Two implementations derived from one reading agree on
   its mistakes.
2. **Ordered containers throughout.** `BTreeMap`/`BTreeSet`, for the reason
   stated at the top of the dbms oracle: `HashMap` iteration is randomized per
   process, so a seeded run stops reproducing its trajectory and an unordered
   `preload` differs from `ORDER BY id ASC` at random. Ordering by construction
   removes the class; sorting at the call site removes one instance.
3. **Model this server, not a generic one.** Where the blob backend's behaviour
   is legitimately its own — deadlines aggregated per origin, an at-most-once
   outbox, search behind a flag — the oracle says so. That is the point of a
   per-server oracle: the specification is allowed to be specific.

Checkpoint: `cargo test -p resonate-server-blob --test differential` — the
server and its oracle in lock step over a seeded trajectory.

### Phase 5 — Absorb the semantic drift

The branch forked before ~50 commits on `main`. `main`'s reference model
diverged by ~560 lines and `core::types` by ~380. See *Semantic deltas*.

The oracle is what makes this tractable rather than archaeological. It is
written from `main`'s spec while the kernel was ported from the spec as it stood
in August, so the in-crate differential lights up the drift directly: every
place the two disagree is either drift the kernel has not absorbed or a place
the oracle is wrong, and both are worth finding. Work the failures until they
are gone.

### Phase 6 — Wire it into the binary

`src/main.rs:178` builds `Arc<dyn ResonateEngine>` and hands it to
`Arc::new_cyclic(|weak: &Weak<Server>| ...)` at `src/main.rs:280` — concrete on
`Server`, so a blob backend cannot go through it. It does not need to: the
`LateRouter` in `outbox.rs` exists for exactly this knot, binding the router
after construction instead of during it.

So the selection becomes: build either `Arc<Server>` or `Arc<BlobServer>`, both
as `Arc<dyn ResonateServer>`, then build the transports and router against that
handle, then bind the router into the blob server's `LateRouter` (or close the
cycle the existing way for the SQL path). The gateway, the transports and the
workers need no change at all — they already take the port.

Config: add `storage.type = "blob"` with a `storage.blob` section — bucket, URL,
prefix, timer shards, cache capacity, `search` opt-in.

Checkpoint: `resonate --storage-type blob` serves the protocol against an
in-process store, and against a real bucket.

### Phase 7 — Prove it against the other servers

See **Testing**.

## Mechanical deltas

**1. Namespace.** `crate::core::` → `resonate_core::`, `crate::util` →
`resonate_core::util`, `crate::s3::` → `crate::`, `crate::kernel::` stays.

**2. Derives.** `main` dropped `PartialEq, Eq` from `PromiseValue`,
`PromiseRecord` and `TaskRecord`. The kernel needs them: `PromiseDoc`
(`state.rs:70`) derives `PartialEq` and holds two `PromiseValue`s, and the write
law — *if the decision changed nothing, write nothing* — is a document
comparison. `Reply` holds a `Box<PromiseRecord>` and derives `PartialEq` too.
Restore the three derives in `resonate-core`. It is additive, it is three lines,
and the alternative is hand-written structural comparison in the blob crate for
no gain.

**3. Metrics collide.** `src/metrics.rs` registers `resonate_messages_total`
(`:29`) and `resonate_schedule_promises_total` (`:41`) into prometheus' global
default registry with `.unwrap()`. The blob crate needs both names plus
`TIMER_QUEUE_LEN`, `DOC_CACHE_HITS_TOTAL` and `DOC_CACHE_MISSES_TOTAL`. Two
registrations of one name in one process is an `Err` that a `lazy_static`
`.unwrap()` turns into a panic on first use.

Decide before Phase 2. The cheap answer: the blob crate declares only its three
new names, and counts delivered messages nowhere — `Server::deliver` counts them
for the SQL path and the blob path has its own outbox, so the counter would be
recorded in two places anyway. The clean answer: hoist the shared names into a
small module both can use. Do not let two crates register the same string.

**4. `object_store` version.** The branch pins `0.14.1`; it is current. Nothing
else in the workspace uses it, so there is no version to reconcile.

**5. `Store` requires real conditional writes.** S3, R2, GCS and Azure have
them; MinIO, B2 and Spaces do not, and lose writes silently if pointed at. This
belongs in the config docs and in a startup check, not only in a module comment.

## Semantic deltas the kernel must absorb

Ordered by how much they change:

**1. Awaitability.** `main` added `OType` / `OKind` and `is_external`
(`crates/resonate-core/src/types.rs:602-624`): a promise is external if any of
`resonate:scope = global`, `resonate:external = true`, `resonate:target`
present, or `resonate:timer = true`. Two rules follow from that one predicate —
**an external promise may be awaited, and an external promise is armed**. The
kernel has no notion of it: it arms every promise and refuses no await.

`promise.register_callback`, `promise.register_listener` and `task.suspend` must
refuse a non-external awaited promise with the same 422 the engines return
("Awaited promise is not awaitable"). And only external promises get a deadline
— which is a real efficiency win here, because an internal promise then costs no
timer object at all.

Replace the kernel's private `TAG_TARGET` / `TAG_TIMER` constants with
`resonate_core::types::{is_timer, is_external, otype, okind}` so the
classification has one home.

**2. Duplicate awaited ids.** `task.suspend` now refuses a repeated awaited id
at validation. The blob server uses core's `*Data` types and `validator`, so
this arrives free — but the kernel must not also silently deduplicate, or a
request the caller did not mean becomes a success.

**3. Preload limit and ordering.** `task.suspend`'s preload response truncates
at a configured `preload_limit` and is ordered by id. The kernel's documents are
`BTreeMap`s already, so ordering holds by construction; the limit is new config.

**4. `debug.start` / `debug.stop`.** Removed on `main`; see Phase 3.

**5. Search.** Every search reads every document, so it stays opt-in
(`S3ServerCfg.search`). Enable it in the differential, where stores are small,
and leave it off by default in production.

**6. Everything else in the ~560-line oracle diff.** Phases 4 and 5 are how
these are found — write the oracle from `main`'s spec and let the differential
name the disagreements. Do not try to enumerate them by reading.

## Testing

Three levels, and each answers a different question.

**Unit — does the piece do what its header says?** 258 tests come with the
code, including `FaultStore`, which cuts the power between two effects and
inspects exactly what landed. The crash-window table in `applier.rs`'s header is
tested, not asserted.

**In-crate differential — does the server match its own specification?** The
blob server and the blob oracle, both `Arc<dyn ResonateServer>`, driven through
a seeded trajectory and compared at every step: response, then `debug.snap`.
This runs in process against the in-memory store, needs no bucket and no
database, and is the loop to develop in. It is full fidelity for this backend,
because the oracle is free to model the backend exactly — including what it
arms, what it queues, and when it fires.

**Cross-server differential — do the servers agree on the protocol?** The blob
server against the SQL engines composed into servers, at the port. Responses and
`debug.snap`, which is what the port makes observable; messages and armed
deadlines are not returned through `ResonateServer` and must be read out of the
snapshot instead. `resonate_core::types::Snapshot`, `SnapshotMessage`,
`SnapshotCallback`, `SnapshotListener`, `SnapshotPromiseTimeout` and
`SnapshotTaskTimeout` all still exist on `main`, so every side can answer it.

This level is deliberately the weaker one, and the division of labour is the
reason it can be: what is genuinely protocol lives here, and what is genuinely
this backend's lives one level up, in an oracle that can state it. A divergence
here is a real disagreement about the protocol rather than an artifact of two
storage shapes, which is what makes it worth chasing.

Two notes on mechanism. The harness for both differentials is the same — drive
`Arc<dyn ResonateServer>` implementations in lock step and diff — so write it
once, in `diff/`, where a test target may depend on every crate without
compromising any crate's independence. And under the startup debug flag the
blob outbox pauses, which is what makes the queued messages in `debug.snap`
mean anything.

**Live.** `tests/live.rs`, `#[ignore]` unless a bucket is configured, in CI
against one real store with conditional writes.

## Open questions

**1. Timers are single-node.** `timerd.rs` states the assumption: the in-memory
queue and the durable timer objects "can only disagree across a crash" *because*
the deployment is a single node. The document design is genuinely multi-writer
safe — CAS per origin — but a second instance's armed deadlines are invisible to
this one until it seeds. `seed`-once must become periodic backfill before the
blob backend is described as horizontally scalable. Worth deciding now whether
that is in scope for the first landing or explicitly deferred with the
limitation documented.

**2. Two pure models in one crate, on purpose.** The kernel and the oracle are
both complete in-memory implementations of the protocol, sitting side by side.
That looks like duplication and is not: the kernel is written for the document
shape and runs in production, the oracle is written from the protocol and runs
only in tests, and the whole value of the pair is that neither was derived from
the other. Say so in both module comments, or someone will helpfully unify them
and delete the evidence.

**3. Firing granularity, now specified rather than argued.** The kernel
aggregates deadlines to one per origin; `main`'s SQL engines carry them per
entity, so `drain(origin)` fires everything due in that origin where an engine
fires exactly the one named. Nothing at the `ResonateServer` port names a
timeout, so this is invisible there — and where it does surface, in what
`debug.snap` shows after time moves, the blob oracle models the blob behaviour
and the cross-server differential is where the two must be reconciled against
the protocol. If it ever has to be reconciled in code instead, the fix is a
narrow `drain_one(doc, timeout, now)` in the kernel, which is pure-function
work.

**4. Does the dbms oracle stay shared?** "Every `ResonateServer` gets an oracle"
raises the question of what `crates/resonate-server-dbms/src/oracle.rs` becomes:
today one model serves three SQL engines, which is right while those three are
meant to be indistinguishable. Nothing in this port needs that to change — but
if the three ever diverge legitimately the way blob and SQL do, the same split
applies there. Out of scope here; worth naming so the pattern is deliberate.

## Not in scope

- Changing `ResonateServer` or `ResonateEngine`.
- Touching the three SQL engines.
- Making the blob backend the default for anything.
