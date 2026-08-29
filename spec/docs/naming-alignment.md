# Naming alignment across the implementations

Five things model this protocol. `spec/02-abstract` (Lean) is the reference;
`tlap/` (TLA+), `valid/porc` (Go, the linearizability checker), the server
(Rust, `resonatehq/resonate`) and the conformance model (C#,
`resonatehq/resonate-accordant`) each restate it.

This is a naming audit, not a semantic one — for semantic differences see
`resonate-accordant/docs/spec-divergences.md`. Established by reading the
declarations in each, not by inference.

## How closely the linearizability checker tracks the spec

Closely, and deliberately. `valid/porc` is the only implementation that
mirrors the Lean machine name for name.

| | Lean | Go |
|---|---|---|
| objects | `PromiseObject` / `TaskObject` / `Object` / `ServerState` | `Promise` / `Task` / `Object` / `ServerState` |
| promise fields | state, param, value, tags, timeoutAt, createdAt, settledAt, callbacks, listeners | identical (Go casing) |
| task fields | state, version, ttl, pid, leaseTimeoutAt, retryTimeoutAt, resumes | identical (`TTL`, `PID` per Go initialisms) |
| handlers | `promiseCreate`, `taskAcquire`, `taskSuspend`, … | `PromiseCreate`, `TaskAcquire`, `TaskSuspend`, … |
| internal steps | `processPromiseTimeout` (R1), `processListener` (R3), `processCallback` (R4), `processLeaseTimeout` (R5), `processRetryTimeout` (R6), `resumeOne` | identical, R-numbers in the comments |
| predicates | `settable`, `isTimer`, `timerTargeted`, `external`, `project`, `addressValid` | `Settable`, `IsTimer`, `TimerTargeted`, `External`, `Project`, `addressValid` |

Three differences, each already argued in the Go source:

- **schedules are absent** — no `schedules` on `ServerState`, no R7
  `processSchedule`, because `nextCron` is not something the checker can
  decide.
- **R2 is absent** because it is absent from the spec.
- **`targeted` has no Go counterpart.** Lean names the predicate and uses it
  10 times; Go inlines `Tags.Has("resonate:target")` at 5 call sites. The one
  place the checker states a spec concept without naming it.

One more, structural rather than named: Lean carries materialisation as
`Env.mat : Bool`, Go as a `Discipline` enum threaded through every handler.
Same parameter, different shape.

## Holdouts on the lease/retry rename

`expiresAt`/`retryAt` became `leaseTimeoutAt`/`retryTimeoutAt`, which is what
the internal steps acting on them were always called — `processLeaseTimeout`
reads the lease deadline, `processRetryTimeout` the retry one. The fields had
simply never been named after their timeouts. Holdouts found afterwards:

| holdout | where | status |
|---|---|---|
| `well_formed_task_acquired_iff_has_expires_at`, `well_formed_task_pending_iff_has_retry_at` | 43 spellings across Lean properties and theorems, `tlap/*.tla`, four `.cfg` invariant lists, `tlap/README.md` | renamed to `…_iff_has_lease_timeout_at` / `…_iff_has_retry_timeout_at` |
| R5 called `leaseExpiry` | `valid/porc/handlers.go`, `partition.go` comments | a third name for R5, matching no identifier anywhere — now `processLeaseTimeout` |
| trigger label `lease-expiry:{id}` | accordant `Spec/InternalSteps.cs` | now `lease-timeout:{id}` |
| `expires_at` / `retry_at` | the Rust engines — 85 and 83 occurrences across the SQLite, Postgres and MySQL backends | **left alone**: these are SQL columns, so aligning them is a migration, not a rename |

The property names were the interesting find: they are a cross-implementation
contract, spelled identically in Lean, in TLA+, and in the TLC config files
that name the invariants to check, so the old field name was pinned in four
places at once.

## Same concept, different name

Neither the wire nor the Rust records drift: `PromiseRecord` and `TaskRecord`
carry exactly the spec's fields, and neither deadline appears on either — the
lease and retry deadlines are server-internal in every implementation.

The conformance model is where the vocabulary diverges:

| concept | Lean | Go | Rust | C# accordant |
|---|---|---|---|---|
| a promise's state | `state` | `State` | `state` | `State` ✔ |
| a promise's param | `param` | `Param` | `param` | **`ParamData`** |
| carries a target | `targeted` | *(inlined)* | — | **`HasTarget`** |
| is a timer | `isTimer` | `IsTimer` | — | **`TimerTag`** |
| awaitable | `external` | `External` | — | `IsExternal` ✔ |
| address validity | `addressValid` | `addressValid` | `is_valid_address` ✔ | `AddressValid` ✔ |
| lease deadline | `leaseTimeoutAt` | `LeaseTimeoutAt` | `expires_at` | `LeaseTimeoutAt` ✔ |
| retry deadline | `retryTimeoutAt` | `RetryTimeoutAt` | `retry_at` | `RetryTimeoutAt` ✔ |

`Status` was the one worth deciding, and it is decided: accordant now says
`State`, leaving `Status` to mean the HTTP status, which is a different
thing. `HasTarget` and `TimerTag` remain a dialect — they name the tag rather
than the predicate — but they read as different concepts when they are the
same one.

Case conventions are not divergences: `TTL`/`PID` in Go, snake_case in Rust,
PascalCase in C# are each idiomatic and should stay.

## Structural gaps, for completeness

- accordant models no `listeners` and no `resumes`, and no schedules. It now
  carries both task deadlines, though only the lease one is observable: R6's
  re-arm and its `execute` are both invisible over this wire, so the model
  folds R6 into the clock step rather than branching on a choice no read can
  ever collapse.
- the Rust engines do not decompose the internal steps the way the spec does:
  `process_timeouts`, `process_callbacks` and `process_schedule_timeout`
  group what the spec separates into R1, R3, R4, R5 and R6. Nothing there is
  misnamed; the rules simply have no individual names to align.
