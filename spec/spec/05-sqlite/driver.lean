import «05-sqlite».sql
import «03-concrete».«state»

/-!  # The driver — schema, decoding, and the abstraction

What the handlers need that is not SQL: the schema they run against, the
row→wire decoding the driver does in Rust (`row_to_promise`), and
`alpha`, which reads the tables as a `ServerState`.

The schema below is `persistence_sqlite.rs`'s, transcribed. It is worth
listing the four places the first cut of this experiment — a typed effect
interface with the SQL in docstrings — got it wrong, because every one of
them was invisible there and is load-bearing here:

* `tasks` is `(id, state, version)`. It has NO `pid` and NO `ttl`.
* `pid` and `ttl` live on `task_timeouts` as `process_id` and `ttl`,
  so deleting a task's timeout row is what clears them.
* There is no resume table. A resume IS a `callbacks` row with
  `ready = true`; registration is `ready = false`.
* There is no messages table. `outgoing_execute (id, version, address)`
  and `outgoing_unblock (promise_id, address)` are separate, and the
  latter stores no promise — the reader joins it back.

A typed interface asserts a schema; text runs against one. -/

namespace SqliteDriver

open Sql ServerModel

/-! ## Schema -/

def schema : List String :=
  [ "promises", "promise_timeouts", "tasks", "task_timeouts",
    "callbacks", "listeners", "outgoing_execute", "outgoing_unblock" ]

def emptyStore : Store := schema.map fun t => (t, [])

/-! ## Column readers -/

def txt? (r : Row) (c : String) : Option String :=
  match Row.get? r c with | some (.text s) => some s | _ => none

def txt (r : Row) (c : String) : String := (txt? r c).getD ""

def int? (r : Row) (c : String) : Option Int :=
  match Row.get? r c with | some (.int n) => some n | _ => none

def int (r : Row) (c : String) : Int := (int? r c).getD 0

def bool (r : Row) (c : String) : Bool :=
  match Row.get? r c with | some (.int n) => n != 0 | _ => false

/-- JSON objects as the driver stores them: `{"k":"v",…}`. Pairs up the
    quoted strings in order. Not a JSON parser — it handles flat objects
    of string values, which is what `tags` and the header columns are.
    The encoding itself is a DECLARED boundary: nothing here tests that
    serde round-trips. -/
def jsonTags : Option String → Tags
  | none   => []
  | some s =>
    let rec pair : List String → Tags
      | k :: v :: rest => (k, v) :: pair rest
      | _              => []
    pair (Sql.jsonStringsOf s)

def jsonArray (ids : List String) : String :=
  "[" ++ String.intercalate "," (ids.map fun i => "\"" ++ i ++ "\"") ++ "]"

def promiseStateOf : String → PromiseState
  | "resolved"          => .resolved
  | "rejected"          => .rejected
  | "rejected_canceled" => .rejectedCanceled
  | "rejected_timedout" => .rejectedTimedout
  | _                   => .pending

def taskStateOf : String → TaskState
  | "acquired"  => .acquired
  | "suspended" => .suspended
  | "halted"    => .halted
  | "fulfilled" => .fulfilled
  | _           => .pending

/-- `row_to_promise` (`persistence_sqlite.rs:437`). -/
def rowToPromise (r : Row) : PromiseRecord :=
  { id        := txt r "id"
    state     := promiseStateOf (txt r "state")
    param     := { headers := jsonTags (txt? r "param_headers"), data := txt? r "param_data" }
    value     := { headers := jsonTags (txt? r "value_headers"), data := txt? r "value_data" }
    tags      := jsonTags (txt? r "tags")
    timeoutAt := (int r "timeout_at").toNat
    createdAt := (int r "created_at").toNat
    settledAt := (int? r "settled_at").map Int.toNat }

/-! ## α : Store → ServerState

Four representation differences to reconcile, all of them real:

1. The specification keeps `callbacks` and `listeners` ON the promise;
   SQLite normalises them into tables.
2. A resume is a `callbacks` row with `ready = true`, so the SAME table
   feeds `PromiseObject.callbacks` (`ready = false`) and
   `TaskObject.resumes` (`ready = true`).
3. `pid` and `ttl` are on `task_timeouts`, not `tasks`.
4. `outgoing_unblock` stores no promise; the reader joins it back from
   `promises` to rebuild the `unblock` message the specification carries.

**One deliberate relaxation**, in the spirit of `TRACES.md`'s: `ttl` and
`pid` are read from `task_timeouts` only when `timeout_type = 1` (lease).
resonate reuses the same two columns to park the RETRY cadence on a
type-0 row, where the specification has no field at all. Reading a retry
cadence as a lease TTL would be a representation artifact reported as a
behavioural divergence. `timeout_type` is compared in every state, so
"this task has no lease" is still checked. -/

/-- resonate's retry cadence: `PENDING_RETRY_TTL` (`oracle.rs:21`),
    `TASK_RETRY_TIMEOUT_MS` (`diff/differential.rs:48`), and the
    `task_timeouts.ttl` column default, all 30 000 ms. The
    specification's `ServerConfig.retryTimeout` defaults to 5 000.

    That difference is CONFIGURATION, not behaviour — the concrete
    machine takes the cadence as a config field precisely so it is not a
    protocol constant — so `alpha` carries it across rather than letting
    the sweep report `now + 30000 ≠ now + 5000` as a divergence. It is
    the same constant the handler passes to `task_timeouts`, so the two
    sides cannot drift apart silently. -/
def retryCadence : Nat := 30000

def alpha (st : Store) : ServerState :=
  let promises  := st.table "promises"
  let callbacks := st.table "callbacks"
  let listeners := st.table "listeners"
  let tasks     := st.table "tasks"
  let tmo       := st.table "task_timeouts"
  { config := { retryTimeout := retryCadence }
    promises := promises.map fun r =>
      { id        := txt r "id"
        state     := promiseStateOf (txt r "state")
        param     := { headers := jsonTags (txt? r "param_headers"), data := txt? r "param_data" }
        value     := { headers := jsonTags (txt? r "value_headers"), data := txt? r "value_data" }
        tags      := jsonTags (txt? r "tags")
        timeoutAt := (int r "timeout_at").toNat
        createdAt := (int r "created_at").toNat
        settledAt := (int? r "settled_at").map Int.toNat
        callbacks := (callbacks.filter fun c =>
                        txt c "awaited_id" == txt r "id" && !bool c "ready").map (txt · "awaiter_id")
        listeners := (listeners.filter fun l =>
                        txt l "promise_id" == txt r "id").map (txt · "address") }
    tasks := tasks.map fun r =>
      let lease := tmo.find? fun t => txt t "id" == txt r "id" && int t "timeout_type" == 1
      { id      := txt r "id"
        state   := taskStateOf (txt r "state")
        version := (int r "version").toNat
        ttl     := lease.bind fun t => (int? t "ttl").map Int.toNat
        pid     := lease.bind fun t => txt? t "process_id"
        resumes := (callbacks.filter fun c =>
                      txt c "awaiter_id" == txt r "id" && bool c "ready").map (txt · "awaited_id") }
    promiseTimeouts := (st.table "promise_timeouts").map fun r =>
      { id := txt r "id", timeout := (int r "timeout_at").toNat }
    taskTimeouts := tmo.map fun r =>
      { id := txt r "id", kind := (int r "timeout_type").toNat,
        timeout := (int r "timeout_at").toNat }
    -- SQLite discharges resumes inline; it has no deferred queue. The
    -- specification's eager schedule (`Materialized.step`) is therefore
    -- the one to compare against, and `deferred` is empty on this side
    -- by construction rather than by mapping.
    deferred := []
    outbox :=
      ((st.table "outgoing_execute").map fun r =>
        { address := txt r "address",
          message := .execute (txt r "id") (int r "version").toNat })
      ++ ((st.table "outgoing_unblock").filterMap fun r =>
        (promises.find? fun p => txt p "id" == txt r "promise_id").map fun p =>
          { address := txt r "address", message := .unblock (rowToPromise p) }) }

end SqliteDriver
