import «05-sqlite».sql
import «03-concrete».«state»

/-!  # Schema, decoding, and α

## The read discipline: `-m`

Materialization, not projection. The reasons are the driver's, not
aesthetic: a storage layer's `promise_get` takes no `now`
(`persistence_sqlite.rs:432`), so a read cannot project — it must
materialize first or serve a stale row. resonate's `op_promise_get`
does exactly that (`try_timeout` then read), and the specification's
`-m` machine is the one that names the discipline. Every handler below
therefore begins with `touchPromise` or `touchTask`, and `touchPromise`
IS the promise-timeout τ, run eagerly.

## The schema is this file's, not resonate's

The earlier transcription inherited four representation choices that
made α do work it should not have to. Changed, deliberately:

* **`pid` and `ttl` live on `tasks`.** resonate parks them on
  `task_timeouts` and reuses the same `ttl` column to carry the RETRY
  CADENCE on a type-0 row. That overloading produced a divergence report
  that was not behavioural. They are task state; they belong on the task.
* **`callbacks`, `deferred` and `resumes` are three tables.** resonate
  folds all three into `callbacks.ready`. The specification distinguishes
  a registered callback, a deferred resume obligation, and a buffered
  resume on a task; collapsing them means α has to invent the difference
  back.
* **One `outbox`, keyed by the specification's own `OutboxEntry.key`.**
  resonate has `outgoing_execute` and `outgoing_unblock`, and the latter
  stores no promise, so a reader must join it back.
* **`target`, `timer` and `external` are ordinary columns** the driver
  maintains alongside `tags`, not `GENERATED ALWAYS AS … STORED`. The
  engine has no generated-column support; making them plain columns that
  every INSERT sets is a real schema choice rather than a modelling gap.

The result is that α is a transcription rather than a reconstruction,
which is the point: an abstraction function doing clever work is an
abstraction function that can hide a divergence. -/

namespace Sqlite

open Sql ServerModel

/-! ## Schema -/

def schema : List String :=
  [ "promises", "promise_timeouts", "tasks", "task_timeouts",
    "callbacks", "listeners", "resumes", "deferred", "outbox",
    "schedules", "schedule_timeouts" ]

def emptyStore : Store := schema.map fun t => (t, [])

/-- The retry cadence. The specification's `ServerConfig.retryTimeout`
    default; since this schema is ours, the two simply agree, and the
    configuration mismatch that the earlier transcription reported as a
    divergence cannot arise. -/
def retryTimeout : Nat := 5000

/-! ## Column readers -/

def txt? (r : Row) (c : String) : Option String :=
  match Row.get? r c with | some (.text s) => some s | _ => none
def txt (r : Row) (c : String) : String := (txt? r c).getD ""
def int? (r : Row) (c : String) : Option Int :=
  match Row.get? r c with | some (.int n) => some n | _ => none
def int (r : Row) (c : String) : Int := (int? r c).getD 0
def nat (r : Row) (c : String) : Nat := (int r c).toNat
def bool (r : Row) (c : String) : Bool :=
  match Row.get? r c with | some (.int n) => n != 0 | _ => false

/-- Flat JSON objects of string values — what `tags` and the header
    columns hold. The encoding is a DECLARED boundary: nothing here tests
    that a real serde round-trip agrees. -/
def jsonTags : Option String → Tags
  | none   => []
  | some s =>
    let rec pair : List String → Tags
      | k :: v :: rest => (k, v) :: pair rest
      | _              => []
    pair (Sql.jsonStringsOf s)

def tagsJson (t : Tags) : String :=
  "{" ++ String.intercalate ","
    (t.map fun (k, v) => "\"" ++ k ++ "\":\"" ++ v ++ "\"") ++ "}"

def optText : Option String → SqlValue
  | none   => .null
  | some s => .text s
def optInt : Option Nat → SqlValue
  | none   => .null
  | some n => .int (Int.ofNat n)

def promiseStateText : PromiseState → String
  | .pending          => "pending"
  | .resolved         => "resolved"
  | .rejected         => "rejected"
  | .rejectedCanceled => "rejected_canceled"
  | .rejectedTimedout => "rejected_timedout"

def promiseStateOf : String → PromiseState
  | "resolved"          => .resolved
  | "rejected"          => .rejected
  | "rejected_canceled" => .rejectedCanceled
  | "rejected_timedout" => .rejectedTimedout
  | _                   => .pending

def taskStateText : TaskState → String
  | .pending   => "pending"
  | .acquired  => "acquired"
  | .suspended => "suspended"
  | .halted    => "halted"
  | .fulfilled => "fulfilled"

def taskStateOf : String → TaskState
  | "acquired"  => .acquired
  | "suspended" => .suspended
  | "halted"    => .halted
  | "fulfilled" => .fulfilled
  | _           => .pending

/-! ## Rows to protocol records -/

def rowToPromise (r : Row) : PromiseRecord :=
  { id        := txt r "id"
    state     := promiseStateOf (txt r "state")
    param     := { headers := jsonTags (txt? r "param_headers"), data := txt? r "param_data" }
    value     := { headers := jsonTags (txt? r "value_headers"), data := txt? r "value_data" }
    tags      := jsonTags (txt? r "tags")
    timeoutAt := nat r "timeout_at"
    createdAt := nat r "created_at"
    settledAt := (int? r "settled_at").map Int.toNat }

/-- `TaskRecord.resumes` is a COUNT on the wire, so the caller supplies
    the row count rather than the engine growing an aggregate. -/
def rowToTask (r : Row) (resumeCount : Nat) : TaskRecord :=
  { id      := txt r "id"
    state   := taskStateOf (txt r "state")
    version := nat r "version"
    resumes := resumeCount
    ttl     := (int? r "ttl").map Int.toNat
    pid     := txt? r "pid" }

def rowToSchedule (r : Row) : Schedule :=
  { id             := txt r "id"
    cron           := txt r "cron"
    promiseId      := txt r "promise_id"
    promiseTimeout := nat r "promise_timeout"
    promiseParam   := { headers := jsonTags (txt? r "promise_param_headers"),
                        data    := txt? r "promise_param_data" }
    promiseTags    := jsonTags (txt? r "promise_tags")
    nextRunAt      := nat r "next_run_at"
    lastRunAt      := (int? r "last_run_at").map Int.toNat
    createdAt      := nat r "created_at" }

/-! ## α : Store → ServerState

A transcription. Every component is one table; the only work is
re-attaching `callbacks` and `listeners` to their promise, which is the
one place the specification's shape (collections ON the object) and a
relational shape (collections in their own tables) genuinely differ. -/

def alpha (st : Store) : ServerState :=
  let promises  := st.table "promises"
  let callbacks := st.table "callbacks"
  let listeners := st.table "listeners"
  let resumes   := st.table "resumes"
  { config := { retryTimeout := retryTimeout }
    promises := promises.map fun r =>
      { id        := txt r "id"
        state     := promiseStateOf (txt r "state")
        param     := { headers := jsonTags (txt? r "param_headers"), data := txt? r "param_data" }
        value     := { headers := jsonTags (txt? r "value_headers"), data := txt? r "value_data" }
        tags      := jsonTags (txt? r "tags")
        timeoutAt := nat r "timeout_at"
        createdAt := nat r "created_at"
        settledAt := (int? r "settled_at").map Int.toNat
        callbacks := (callbacks.filter (txt · "awaited_id" == txt r "id")).map (txt · "awaiter_id")
        listeners := (listeners.filter (txt · "promise_id" == txt r "id")).map (txt · "address") }
    tasks := (st.table "tasks").map fun r =>
      { id      := txt r "id"
        state   := taskStateOf (txt r "state")
        version := nat r "version"
        ttl     := (int? r "ttl").map Int.toNat
        pid     := txt? r "pid"
        resumes := (resumes.filter (txt · "task_id" == txt r "id")).map (txt · "awaited_id") }
    schedules := (st.table "schedules").map rowToSchedule
    deferred  := (st.table "deferred").map fun r =>
      { awaited := txt r "awaited_id", awaiter := txt r "awaiter_id" }
    promiseTimeouts := (st.table "promise_timeouts").map fun r =>
      { id := txt r "id", timeout := nat r "timeout_at" }
    taskTimeouts := (st.table "task_timeouts").map fun r =>
      { id := txt r "id", kind := nat r "kind", timeout := nat r "timeout_at" }
    scheduleTimeouts := (st.table "schedule_timeouts").map fun r =>
      { id := txt r "id", timeout := nat r "timeout_at" }
    outbox := (st.table "outbox").filterMap fun r =>
      if txt r "kind" == "execute" then
        some { address := txt r "address",
               message := .execute (txt r "task_id") (nat r "version") }
      else
        (promises.find? (txt · "id" == txt r "promise_id")).map fun p =>
          { address := txt r "address", message := .unblock (rowToPromise p) } }

end Sqlite
