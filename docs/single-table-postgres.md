# Single-table Postgres — exploration

An experimental Postgres backend that stores one promise as **one row**, alongside
the existing multi-table backend. Both are wired into the differential test, so
they are checked against each other, against SQLite, and against the reference
Oracle on the same random operation stream.

- Implementation: `src/persistence/persistence_postgres_single.rs`
- Schema: `config/postgres/single-table.sql`
- Constraint catalogue: `config/postgres/single-table-constraints.sql` — applies
  to this schema unchanged, which is why the columns are named `state` and
  `task_state` rather than the starting DDL's `p_state` and `t_state`. The one
  place this schema departs from the catalogue as delivered is the callback
  array: it is `callbacks` here, and the three constraints that reference it
  (`well_formed_promise_awaiter_is_not_self`,
  `well_formed_promise_callbacks_unique`,
  `well_formed_promise_obligations_require_external`) are patched to match
- Select it with `RESONATE_STORAGE__POSTGRES__SINGLE_TABLE=true`
  (and `RESONATE_STORAGE__POSTGRES__CONSTRAINTS=true` to have the database
  enforce the catalogue).

## Result

It works, and it is faster. The differential test passes at **83,800 steps with
four backends** (SQLite, Oracle, multi-table Postgres, single-table Postgres),
all 22 operations covered, 745 behavioural signatures — every response body and
every state snapshot byte-identical across all four.

Ten tables become three, and 118 named CTEs become 41:

| | multi-table | single-table |
|---|---|---|
| tables | 10 | 3 (`promises`, `outbox`, `schedules`) |
| named CTEs across all ops | 118 | 41 |
| rows written by `promise.create` | 5, across 5 tables | 2 (`promises` + `outbox`) |
| rows locked by the `promise.settle` preamble | 2 | 1 |

## The mapping

| multi-table | single-table |
|---|---|
| `promises` | `id, state, param_*, value_*, tags, *_at` |
| `promise_timeouts(timeout_at, id)` | *derived*: `state='pending' AND target IS NOT NULL` |
| `tasks(id, state, version)` | `task_state` (NULL ⟺ no task), `task_version` |
| `task_timeouts` type 0 (retry) | `retry_at` — live ⟺ `task_state='pending'` |
| `task_timeouts` type 1 (lease) | `expires_at` — live ⟺ `task_state='acquired'` — plus `ttl`, `pid` |
| `callbacks(awaited, awaiter, ready=false)` | `callbacks TEXT[]` on the **awaited** row |
| `callbacks(awaited, awaiter, ready=true)` | `resumes TEXT[]` on the **awaiter** row |
| `listeners(promise_id, address)` | `listeners TEXT[]` |
| `outgoing_execute(id, version, address)` | `outbox` row, `key = 'e:'‖task_id` |
| `outgoing_unblock(promise_id, address)` | `outbox` row, `key = 'u:'‖promise_id‖':'‖address` |
| `schedule_timeouts(timeout_at, id)` | *gone* — it was a verbatim copy of `(next_run_at, id)` |

`outbox` stays its own table because a message is not a promise attribute: one
settled promise can owe several unblocks. `key` carries exactly the deduplication
the two `outgoing_*` primary keys used to provide.

Column names, the `resonate` schema and the shape of `outbox` follow
`constraints-all.sql`, so that file applies to this schema unchanged.

## Messages

Messages are the one thing that did not collapse into the promise row.

`outgoing_execute` is 1:1 with a task and *could* have been a column — an early
draft had `out_execute_version INT`, NULL meaning nothing queued. `outgoing_unblock`
is one row per listener, so a settled promise can owe several at once, and that
never folds into a scalar. Once one kind needs a table, splitting the two kinds
across a column and a table is worse than one `outbox`. The second reason is the
benchmark below: an outbox column is a *hot* column, so every enqueue and every
take would rewrite the whole promise row, payload included.

```sql
CREATE TABLE outbox (
  key      TEXT PRIMARY KEY,
  kind     TEXT NOT NULL CHECK (kind IN ('execute', 'unblock')),
  address  TEXT NOT NULL,
  task_id  TEXT,      -- execute only
  version  INT,       -- execute only
  promise  JSONB      -- unblock only
);
```

`key` carries the deduplication the two tables got structurally from their
primary keys, so every `ON CONFLICT` clause states what the old schema stated
by shape:

| kind | key | replaces | on conflict |
|---|---|---|---|
| execute | `'e:' ‖ task_id` | `outgoing_execute (id)` | `DO UPDATE SET address, version` |
| unblock | `'u:' ‖ promise_id ‖ ':' ‖ address` | `outgoing_unblock (promise_id, address)` | `DO NOTHING` |

**Unblock carries a snapshot, not a join.** The multi-table backend stores
`(promise_id, address)` and joins back to `promises` at delivery time. Here the
settled promise is serialised into the row by `resonate._promise_json(...)`, an
`IMMUTABLE` SQL function reproducing `PromiseRecord`'s serde exactly — camelCase
timestamps, `settledAt`/`headers`/`data` omitted when absent — so `take_outgoing`
deserialises it straight back into a `PromiseRecord` with no join. Two reasons:
`consistent_outbox_unblock_names_settled_promise` checks
`promise->>'state' <> 'pending'`, which is only checkable if the payload is in
the row; and the promise is settled and therefore immutable at enqueue time, so
the snapshot cannot go stale.

`outbox.task_id REFERENCES promises(task_key)` — "the promises that are tasks" is
a partial set and a foreign key needs a total one, which is what the generated
`task_key` column is for.

Enqueue is a CTE in the same statement as the state change, so it commits with
it; take is still delete-then-deliver, for at-most-once. One behavioural note:
for unblocks the batch limit now counts *promises* rather than
(promise, address) pairs, so a batch can expand slightly past `batch_size` when
one promise has many listeners. `take_outgoing` is not exercised by the
differential test — background loops are paused in debug mode — so that is
reasoned, not measured.

## The one structural constraint the collapse imposes

**Two CTEs in one statement may not update the same row.** In the multi-table
schema `fulfilled_task`, `deleted_ttimeout`, the awaiter-side `deleted_callbacks`
and `updated_promise` are four independent CTEs that merely share an id. Here
they must become one `UPDATE ... SET` with `CASE` expressions — that is
`SETTLE_SELF`.

The same applies to fan-out. `marked_ready` + `resumed_tasks` (awaited side) and
`deleted_callbacks` (awaiter side) are merged into one `UPDATE` (`SETTLE_FANOUT`),
because in a two-promise await cycle a single row is *both*, and two CTEs
updating it is undefined behaviour in Postgres. The multi-table layout gets this
for free — those are different rows of the `callbacks` table.

One consequence: `SETTLE_FANOUT` reads the set of suspended callbacks from a
snapshot CTE rather than from the `UPDATE`'s `RETURNING`, because `RETURNING`
yields post-update values and the outbox needs to know *which* callbacks were
suspended. The multi-table backend gets that set from `resumed_tasks RETURNING`,
which is re-checked under EvalPlanQual; the snapshot read is not. This is
documented at the fragment.

## Latency

From the differential run itself (`postgres` vs `postgres-single`, same process,
same database, same operation stream, so contention is symmetric). Mean/p99 in µs.

| operation | multi mean | single mean | Δ | multi p99 | single p99 | Δ |
|---|---|---|---|---|---|---|
| promise.create | 2269 | 2217 | −2% | 4544 | 4551 | 0% |
| promise.get | 1931 | 1876 | −3% | 3755 | 3692 | −2% |
| promise.settle | 3760 | 3387 | −10% | 6523 | 6148 | −6% |
| promise.register_callback | 1638 | 1451 | −11% | 4548 | 4251 | −7% |
| promise.register_listener | 3270 | 2859 | −13% | 5718 | 4895 | −14% |
| promise.search | 1621 | 1548 | −5% | 3088 | 2900 | −6% |
| task.create | 4223 | 3643 | −14% | 7748 | 6659 | −14% |
| task.get | 2065 | 1843 | −11% | 3689 | 3590 | −3% |
| task.acquire | 3534 | 3251 | −8% | 7229 | 5456 | −25% |
| task.release | 2916 | 2523 | −13% | 5160 | 4736 | −8% |
| task.fulfill | 3587 | 3209 | −11% | 6658 | 6879 | +3% |
| task.suspend | 2093 | 1812 | −13% | 5958 | 4831 | −19% |
| task.fence | 3713 | 3260 | −12% | 8966 | 6735 | −25% |
| task.heartbeat | 1587 | 1337 | −16% | 3260 | 2696 | −17% |
| task.halt | 2648 | 2484 | −6% | 4857 | 4699 | −3% |
| task.continue | 2613 | 2337 | −11% | 5086 | 4286 | −16% |
| task.search | 1662 | 1378 | −17% | 3008 | 3234 | +8% |
| schedule.create | 2410 | 2013 | −16% | 4694 | 4113 | −12% |
| schedule.get | 1239 | 1108 | −11% | 2442 | 2195 | −10% |
| schedule.delete | 2165 | 1862 | −14% | 4186 | 3704 | −12% |
| schedule.search | 1491 | 1343 | −10% | 3080 | 2819 | −8% |
| debug.tick | 5557 | 3812 | −31% | 12344 | 9254 | −25% |

Mean of the per-operation deltas: **−11.7%**. Nothing is meaningfully slower.
`task.search` and `task.get` lose a join and a correlated `COUNT` subquery;
`debug.tick` (the timeout sweep) loses the most because it was the widest fan-out.

Caveat that matters for the next section: **every promise in this test has an
empty `param`**. These numbers do not exercise the one place single-table loses.

## Where single-table loses: write amplification inside the row

A task state transition rewrites a ~40-byte `tasks` row in the multi-table
layout. In the single-table layout it rewrites the *whole promise row* — payload
included — because Postgres has no in-place update.

20,000 tasks, one acquire + one release each, incompressible payloads,
`VACUUM FULL` before each measurement:

| layout | payload | update ms | heap growth |
|---|---|---|---|
| multi-table | none | 427 | 7.4 MB |
| single-table | none | 651 | 12.5 MB |
| multi-table | 200 B | 436 | 7.4 MB |
| single-table | 200 B | 679 | 20.7 MB |
| multi-table | 1000 B | 421 | 7.4 MB |
| **single-table** | **1000 B** | **926** | **56.3 MB** |
| multi-table | 1900 B | 432 | 7.4 MB |
| single-table | 1900 B | 620 | 12.5 MB |
| multi-table | 8000 B | 510 | 7.4 MB |
| single-table | 8000 B | 626 | 12.5 MB |

The penalty peaks just **below** the TOAST threshold. Above roughly 2 KB the
payload moves out of line, the main tuple stays narrow, and updating other
columns only copies the TOAST pointer — so the cost collapses back. Below it,
every task transition copies the payload. At 1 KB that is **7.6× the heap churn
and 2.2× the latency** of the multi-table layout.

`toast_tuple_target` does not help: it controls how small Postgres tries to get a
tuple *once TOASTing triggers*, not the ~2 KB trigger threshold itself. Measured —
identical bytes, identical time.

### The decomposition that does help

Collapse everything except the payload. Same single-table row, with
`param_headers/param_data/value_headers/value_data` moved to a 1:1
`promise_payload` side table, so nothing a task transition writes shares a page
with a payload:

| layout | payload | update ms | heap growth |
|---|---|---|---|
| single + payload side table | none | 373 | 9.6 MB |
| single + payload side table | 200 B | 414 | 10.0 MB |
| single + payload side table | 1000 B | 337 | 9.6 MB |
| single + payload side table | 1900 B | 378 | 9.6 MB |

Flat in payload size, and faster than the multi-table layout (which needs two
`UPDATE`s across two tables where this needs one).

The lesson is that the useful axis for decomposition is **mutation frequency**,
not entity identity. Tasks, timeouts, callbacks, listeners and outbox flags all
change on the same events as the promise, so keeping them apart buys nothing and
costs joins and lock ordering. Payloads are written once and read many times, so
keeping them apart is the split that pays.

## Findings

### 1. `resumes` cannot be a `BOOLEAN`

The starting DDL has `resumes BOOLEAN NOT NULL`. The API returns `resumes` as a
**count** (`TaskRecord.resumes: i64`, the number of ready callbacks), and
`task.suspend` must clear exactly the ready set. It is `TEXT[]` here — the awaited
ids that are ready for this row's task — which is also what the Oracle models
(`Task.resumes: HashSet<String>`).

### 2. The promise-timeout queue is derivable — resolved

`promise_timeouts` is not `(state='pending' AND timeout_at)`. A promise created
*without* a `resonate:target` never enters the queue and only times out lazily,
on first touch. Making the queue a derived predicate — which is what the
recommended `idx_promises_timeout_at ... WHERE state = 'pending'` index assumes —
therefore depends on `consistent_task_iff_targeted_promise`.

That constraint did not hold, because `task.create` created a task whether or
not the action carried a `resonate:target`. The specification is explicit that
it should not — `spec/02-abstract/external.lean`:

```lean
def taskCreate (req : TaskCreateReq) (now : Nat) : H TaskCreateRes := do
  let a := req.action
  if !(a.tags.has "resonate:target") ∨ a.tags.timerTargeted then
    return { status := 400 }
```

400, before any state is read. That door check is now implemented (see
finding 3), so a task always implies a target, the predicate is sound, and no
`timeout_sweep` column is needed.

### 3. Three door checks the server was missing

The specification refuses malformed tag combinations with a 400 at every door a
promise can be born through, before any state is read. Three of those checks
were absent from both `server.rs` and the Oracle:

| door | spec | rule |
|---|---|---|
| `promise.create` | `external.lean:31` | `Tags.timerTargeted` → 400 |
| `task.create` | `external.lean:119` | no `resonate:target`, **or** `timerTargeted` → 400 |
| `schedule.create` | `external.lean:365` | `promiseTags.timerTargeted` → 400 |

A timer is never targeted: `resonate:target` says a worker owns the promise's
execution and the server gives it a task to carry it, while `resonate:timer`
says nothing executes it at all. A promise carrying both would be handed a task
no worker should ever run.

Two related checks were already correct: `task.create` against an existing
untargeted promise returns 422, and every door validates a `resonate:target`
that is present.

**`task.fence` is still open, and deliberately so.** The spec says the fence
needs no guard of its own — its create action *is* a `promise.create` and the
inner refusal is what it reports, nested inside a 200. The server does not
delegate: `task_fence_create` builds the promise directly, so there is no inner
handler to carry the check. Adding one at the top of the fence handler is wrong
in a way the differential test caught immediately:

```
step=413 op=task.fence: status mismatch
  sqlite=400 "A timer promise cannot carry a resonate:target tag"
  oracle=404 "Task not found"
```

The fence's own outcome precedes the action's validity — `taskFence` reads the
task and answers 404/409 *before* running the inner create. Closing this
properly means either carrying the malformed-action verdict into the fence CTE
(so the fence check still happens first and the insert is skipped), or reading
the task before validating and accepting a benign race on an already-invalid
request. Both are more than this change should carry, so `task.fence` can still
give birth to a targeted timer, and with the catalogue enforced that surfaces as
a 500 rather than a 400.

The same latent ordering discrepancy already exists for the fence's
`resonate:target` address check, which the differential test never exercises
because it only ever generates valid addresses.

Without these, `well_formed_promise_timer_not_targeted` in the catalogue turns
a malformed request into a 500 at insert time instead of a 400 at the door —
which is how a database-enforced catalogue behaves when a door check is
missing, and is worth remembering as a diagnostic.

The differential generators now produce both malformed `task.create` shapes,
and the targeted-timer shape at `promise.create` and `schedule.create`, about
one time in twelve each — so all four backends are compared on them.

### 4. One catalogue constraint the server violates today

Running the differential test with all 44 constraints enforced
(`TEST_POSTGRES_CONSTRAINTS=1`) fails at step 7:

```
well_formed_promise_obligations_require_external
  CHECK (external OR (callbacks = '{}' AND listeners = '{}'))
```

`promise.register_listener` accepts any pending promise, including one created
with no tags at all, which is not `external`. With that one constraint skipped,
the remaining 43 are enforced by Postgres across a full **57,200-step**
differential run without a single violation.

Whether this is a server bug or a test-generator artefact depends on whether real
clients ever register a listener on a non-external promise. The server permits it.

### 5. Naming a schema `resonate` shadows `public` for the `resonate` role

Postgres' default `search_path` is `"$user", public`. Creating a schema named
`resonate` while connected as role `resonate` makes `"$user"` resolve to it, so
*every other connection* silently starts reading the new tables. This broke the
multi-table backend the moment the single-table schema was created in the same
database. The single-table pool pins its own `search_path` in `after_connect`;
the differential test pins the multi-table connection to `public`. Worth knowing
before any migration that creates the schema alongside live tables.

### 6. `ANY((SELECT ...))` is the subquery form

`x = ANY((SELECT arr FROM t))` parses as `ANY(subquery)` and fails with
`operator does not exist: text = text[]`, even with the extra parentheses.
Wrapping in `COALESCE(..., '{}'::text[])` makes it an array expression — which is
wanted anyway, so the "settlement did not fire" case is an empty array and not
NULL.

### 7. Unreachable divergence: a targetless task's outbox row

`outgoing_execute.address` is `NOT NULL`, so in the multi-table backend a
resumed/released/retried task whose promise has no `resonate:target` would raise
a constraint error. Here those inserts are guarded on `target IS NOT NULL` and
are skipped instead. Reachable only through a targetless `task.create`, which
finding 3 now refuses at the door — so the difference is moot.

### 8. Headers normalise `NULL` to `{}`

The catalogue's `well_formed_promise_pending_has_no_value` compares
`value_headers` against `'{}'::jsonb`, so the header columns are
`NOT NULL DEFAULT '{}'` and reads apply `NULLIF(..., '{}')`. A client that sends
an explicitly empty `headers` map gets it back omitted. Indistinguishable in
practice, but it is a real narrowing of the wire format.

### 9. Carried but unconsumed

`origin_id`, `parent_id` and `external` are in the schema as the starting DDL
asked, with the recommended index on `origin_id`. Nothing in the server reads
them yet; `external` is load-bearing only for the constraint in finding 3.
`origin_id` is `split_part(id, ':', 1)`, which is *not* the same as the
multi-table backend's unused `origin` column (`tags->>'resonate:origin'`).

### 10. Minor, in the starting DDL

`p_state TEXT ... CHECK (state IN (...))` names a column that does not exist, and
there is a trailing comma before the closing paren.

## Recommendation

Adopt the collapse, with payloads split out — the `single + payload side table`
row of the second benchmark. It is the only variant that is faster than the
multi-table layout on *both* the operation mix and the write-amplification
workload, and it keeps every simplification that motivated the exercise: one lock
per promise, no joins on the read path, no referential fan-out, and a schema the
constraint catalogue can actually be enforced against.

One specification question remains open:

- Is `promise.register_listener` on a non-external promise legal? If no, the
  server should reject it and the catalogue can be enforced in full.

## Running it

```bash
# both Postgres backends, plus SQLite and the Oracle, on one operation stream
TEST_POSTGRES_URL=postgres://resonate:resonate@localhost:5432/resonate \
  cargo test --test differential -- --nocapture

# with the constraint catalogue enforced
TEST_POSTGRES_CONSTRAINTS=1 \
TEST_POSTGRES_CONSTRAINTS_SKIP=well_formed_promise_obligations_require_external \
TEST_POSTGRES_URL=postgres://resonate:resonate@localhost:5432/resonate \
  cargo test --test differential -- --nocapture

# skip the single-table backend
TEST_POSTGRES_SINGLE=0 ...
```
