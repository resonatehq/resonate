import «03-concrete».«state»

/-!  # The SQLite machine — store, statements, and the abstraction

A THIRD machine over the same wire surface. `-p` and `-m` differ in read
discipline over one state type; this one differs in STATE REPRESENTATION:
the objects live in relational tables with SQLite's own typing, and the
handlers reach them only through the statements a driver actually issues.
The statements are transcribed from `resonatehq/resonate`'s
`src/persistence/persistence_sqlite.rs`, quoted verbatim above each
effect, so the correspondence is auditable line by line rather than
asserted.

The point of the exercise: a handler here has the SAME TYPE as a handler
there — `Req → (now) → m Res` — and differs only in the monad. `-p` and
`-m` run in `StateM ServerState`; this one runs in any `m` with a
`MonadSql` instance. Two instances matter:

* `SqlM = StateM Db` — a PURE interpreter of the statements. Being pure,
  the whole handler is a closed Lean term, so `decide` reaches it and
  conformance at small scope is a kernel-checked theorem.
* `IO` via `leanprover/leansqlite` — the same statements against a real
  `:memory:` database, for randomized runs past the exhaustible scope.

One handler text, two interpretations, one conformance statement. The
pure one is what makes `decide` possible at all: `IO` is opaque to the
kernel, `StateM Db` is not.

## Declared boundaries

Two things this model does NOT check, stated rather than implied:

1. **JSON encoding.** `tags`, `param_headers` and `value_headers` are
   `TEXT` holding JSON in the real schema. Here they are the DECODED
   assoc-lists, and `json_extract` is modelled as `Tags.get?`. The
   serde round-trip is assumed correct; nothing below tests it.
2. **Statement semantics.** Each effect below is a Lean function that
   the author asserts implements the quoted SQL. `decide` checks the
   handler against the specification given those functions; it cannot
   check that SQLite agrees with them. That is precisely what the `IO`
   instance is for — the same theorem, re-run against the real engine.
-/

namespace SqliteModel

open ServerModel

/-! ## SQLite column types

SQLite's `INTEGER` is signed 64-bit; the specification's clock is `Nat`.
Keeping the store's integers `Int` means the gap is visible instead of
assumed away, and `wf` below carries the non-negativity that `alpha`
relies on. -/

abbrev SqlText := Option String
abbrev SqlInt  := Int

/-! ## `state` as TEXT

    state TEXT NOT NULL DEFAULT 'pending'
      CHECK (state IN ('pending','resolved','rejected',
                       'rejected_canceled','rejected_timedout'))

The CHECK constraint is a decidable predicate on the column, so it is
modelled as one (`stateOfText?.isSome`) rather than being enforced by
construction — an implementation that wrote a bad string would be caught
by `wf`, and by SQLite. -/

def stateText : PromiseState → String
  | .pending          => "pending"
  | .resolved         => "resolved"
  | .rejected         => "rejected"
  | .rejectedCanceled => "rejected_canceled"
  | .rejectedTimedout => "rejected_timedout"

def stateOfText? : String → Option PromiseState
  | "pending"           => some .pending
  | "resolved"          => some .resolved
  | "rejected"          => some .rejected
  | "rejected_canceled" => some .rejectedCanceled
  | "rejected_timedout" => some .rejectedTimedout
  | _                   => none

def taskStateText : TaskState → String
  | .pending   => "pending"
  | .acquired  => "acquired"
  | .suspended => "suspended"
  | .halted    => "halted"
  | .fulfilled => "fulfilled"

def taskStateOfText? : String → Option TaskState
  | "pending"   => some .pending
  | "acquired"  => some .acquired
  | "suspended" => some .suspended
  | "halted"    => some .halted
  | "fulfilled" => some .fulfilled
  | _           => none

/-! ## Generated columns

    target  TEXT    GENERATED ALWAYS AS (json_extract(tags,'$.resonate:target')) STORED
    timer BOOLEAN NOT NULL GENERATED ALWAYS AS
            (COALESCE(json_extract(tags,'$.resonate:timer'),'') = 'true') STORED

`STORED`, so they are columns, not views — modelled as fields, with `wf`
asserting they equal their generators. That keeps a row that violates the
generator OUT of the swept scope instead of silently admitting states
SQLite cannot produce. -/

def genTarget (tags : Tags) : SqlText := tags.get? "resonate:target"
def genOrigin (tags : Tags) : SqlText := tags.get? "resonate:origin"
def genBranch (tags : Tags) : SqlText := tags.get? "resonate:branch"
def genTimer  (tags : Tags) : Bool    := tags.get? "resonate:timer" == some "true"

/-! ## Rows -/

structure PromiseRow where
  id           : String
  state        : String
  paramHeaders : Option Tags
  paramData    : SqlText
  valueHeaders : Option Tags
  valueData    : SqlText
  tags         : Tags
  target       : SqlText
  origin       : SqlText
  branch       : SqlText
  timer        : Bool
  timeoutAt    : SqlInt
  createdAt    : SqlInt
  settledAt    : Option SqlInt
  deriving Repr, DecidableEq

structure PromiseTimeoutRow where
  timeoutAt : SqlInt
  id        : String
  deriving Repr, DecidableEq

structure TaskRow where
  id      : String
  state   : String
  version : SqlInt
  pid     : SqlText
  ttl     : Option SqlInt
  deriving Repr, DecidableEq

structure TaskTimeoutRow where
  id        : String
  kind      : SqlInt      -- 0 = pending retry, 1 = lease expiration
  timeoutAt : SqlInt
  deriving Repr, DecidableEq

structure CallbackRow where
  awaitedId : String
  awaiterId : String
  ready     : Bool
  deriving Repr, DecidableEq

structure ListenerRow where
  promiseId : String
  address   : String
  deriving Repr, DecidableEq

structure ResumeRow where
  taskId    : String
  awaitedId : String
  deriving Repr, DecidableEq

structure MessageRow where
  address : String
  kind    : String        -- 'execute' | 'unblock'
  taskId  : SqlText
  version : Option SqlInt
  promise : Option PromiseRow
  deriving Repr, DecidableEq

structure Db where
  promises        : List PromiseRow        := []
  promiseTimeouts : List PromiseTimeoutRow  := []
  tasks           : List TaskRow            := []
  taskTimeouts    : List TaskTimeoutRow     := []
  callbacks       : List CallbackRow        := []
  listeners       : List ListenerRow        := []
  resumes         : List ResumeRow          := []
  messages        : List MessageRow         := []
  deriving Repr, DecidableEq

def Db.init : Db := {}

/-! ## Well-formedness

The schema's own constraints, as a predicate. The sweeps quantify over
well-formed databases only: a row violating a CHECK or a generated column
is not a state SQLite can be in, so a divergence found there would be
noise. Stating it as a predicate rather than baking it into the types is
deliberate — it is the list of assumptions the theorem rests on, and a
reader can audit it. -/

def PromiseRow.wf (r : PromiseRow) : Bool :=
  (stateOfText? r.state).isSome
    && r.target == genTarget r.tags
    && r.origin == genOrigin r.tags
    && r.branch == genBranch r.tags
    && r.timer  == genTimer  r.tags
    && decide (0 ≤ r.timeoutAt)
    && decide (0 ≤ r.createdAt)
    && (match r.settledAt with | none => true | some v => decide (0 ≤ v))
    -- settled_at is set exactly on the settled states
    && ((r.state == "pending") == r.settledAt.isNone)

def TaskRow.wf (r : TaskRow) : Bool :=
  (taskStateOfText? r.state).isSome
    && decide (0 ≤ r.version)
    && (match r.ttl with | none => true | some v => decide (0 ≤ v))

/-- `PRIMARY KEY` uniqueness, spelled out rather than via `List.Nodup`:
    the specification declares itself dependency-free, and `Nodup` lives
    outside core. -/
def distinct (ids : List String) : Bool :=
  ids.all fun i => (ids.filter (· == i)).length == 1

def Db.wf (db : Db) : Bool :=
  db.promises.all PromiseRow.wf
    && db.tasks.all TaskRow.wf
    -- PRIMARY KEY (id)
    && distinct (db.promises.map (·.id))
    && distinct (db.tasks.map (·.id))
    -- FOREIGN KEY: callbacks/listeners reference promises(id)
    && db.callbacks.all (fun c => db.promises.any (·.id == c.awaitedId))
    && db.listeners.all (fun l => db.promises.any (·.id == l.promiseId))

/-! ## The abstraction α : Db → ServerState

Where the structural difference actually lives. The specification keeps
`callbacks` and `listeners` ON the promise object; SQLite normalises them
into their own tables with a foreign key back. α un-normalises. Nothing
else about the promise moves — which is the claim this file exists to
make checkable.

α is total: it reads a malformed row as `pending` and clamps negative
integers, because a partial α would make the conformance statement
vacuous on exactly the inputs worth checking. `wf` is what keeps those
inputs out of the swept scope instead. -/

def valueOf (h : Option Tags) (d : SqlText) : Value :=
  { headers := h.getD [], data := d }

def PromiseRow.toObject (db : Db) (r : PromiseRow) : PromiseObject :=
  { id        := r.id
    state     := (stateOfText? r.state).getD .pending
    param     := valueOf r.paramHeaders r.paramData
    value     := valueOf r.valueHeaders r.valueData
    tags      := r.tags
    timeoutAt := r.timeoutAt.toNat
    createdAt := r.createdAt.toNat
    settledAt := r.settledAt.map Int.toNat
    callbacks := (db.callbacks.filter (·.awaitedId == r.id)).map (·.awaiterId)
    listeners := (db.listeners.filter (·.promiseId == r.id)).map (·.address) }

/-- `row_to_promise` (`persistence_sqlite.rs:437`) — the row as it goes on
    the wire. No `Db` needed: `PromiseRecord` carries neither callbacks nor
    listeners, so the un-normalisation α performs is invisible on the
    response channel. That asymmetry is why the response half of
    conformance is checkable well before the state half. -/
def PromiseRow.toRecord (r : PromiseRow) : PromiseRecord :=
  { id        := r.id
    state     := (stateOfText? r.state).getD .pending
    param     := valueOf r.paramHeaders r.paramData
    value     := valueOf r.valueHeaders r.valueData
    tags      := r.tags
    timeoutAt := r.timeoutAt.toNat
    createdAt := r.createdAt.toNat
    settledAt := r.settledAt.map Int.toNat }

def TaskRow.toObject (db : Db) (r : TaskRow) : TaskObject :=
  { id      := r.id
    state   := (taskStateOfText? r.state).getD .pending
    version := r.version.toNat
    ttl     := r.ttl.map Int.toNat
    pid     := r.pid
    resumes := (db.resumes.filter (·.taskId == r.id)).map (·.awaitedId) }

/-- The `messages` table back to `OutboxEntry`. Partial by construction —
    a row whose `kind` is neither tag, or an `execute` row missing its
    task, has no image — so `filterMap` drops it. `wf` does not currently
    exclude such rows; that is a gap, and naming it here is cheaper than
    pretending the abstraction is total. -/
def MessageRow.toEntry (r : MessageRow) : Option OutboxEntry :=
  if r.kind == "unblock" then
    r.promise.map fun p => { address := r.address, message := .unblock p.toRecord }
  else
    match r.taskId, r.version with
    | some tid, some v => some { address := r.address, message := .execute tid v.toNat }
    | _,        _      => none

def alpha (db : Db) : ServerState :=
  { promises        := db.promises.map (PromiseRow.toObject db)
    tasks           := db.tasks.map (TaskRow.toObject db)
    promiseTimeouts := db.promiseTimeouts.map fun t =>
                         { id := t.id, timeout := t.timeoutAt.toNat }
    taskTimeouts    := db.taskTimeouts.map fun t =>
                         { id := t.id, kind := t.kind.toNat,
                           timeout := t.timeoutAt.toNat }
    -- the specification's `deferred` queue is resonate's `resumes` table
    deferred        := db.resumes.map fun r =>
                         { awaited := r.awaitedId, awaiter := r.taskId }
    outbox          := db.messages.filterMap MessageRow.toEntry }

/-! ## `MonadSql` — the statement interface

THE SEAM. Every method is one statement (or one small transcribed helper)
from `persistence_sqlite.rs`, quoted above it. A handler may touch the
store only through these, which is what makes the pure interpreter and
the `leansqlite` interpreter interchangeable: they implement the same
class, and the handler cannot tell them apart. -/

class MonadSql (m : Type → Type) where
  /-- `SELECT id, state, param_headers, param_data, value_headers,
       value_data, tags, timeout_at, created_at, settled_at
       FROM promises WHERE id = ?1`  (`persistence_sqlite.rs:432`) -/
  selectPromise : String → m (Option PromiseRow)

  /-- `SELECT id, timer, timeout_at FROM promises
       WHERE id IN (SELECT value FROM json_each(?1))
         AND state = 'pending' AND timeout_at <= ?2`
      (`persistence_sqlite.rs:374`, single-id instance) -/
  selectExpired : String → Int → m (Option (String × Bool × Int))

  /-- `UPDATE promises SET state = ?2, settled_at = ?3
       WHERE id = ?1 AND state = 'pending'`  (`:400`) -/
  updateSettle : String → String → Int → m Unit

  /-- `DELETE FROM promise_timeouts WHERE id = ?1`  (`:404`) -/
  deletePromiseTimeout : String → m Unit

  /-- `settlement_enqueued` (`:408`): the settled promise's own task, if
      any, becomes `fulfilled` and drops its lease and its timer. -/
  settlementEnqueued : String → m Bool

  /-- `resumption_enqueued` (`:415`): each registered callback becomes a
      buffered resume on the awaiter's task. -/
  resumptionEnqueued : String → Int → m Unit

  /-- `listener_unblocked` (`:426`): each registered listener gets an
      `unblock` message carrying the settled promise. -/
  listenerUnblocked : String → m Unit

export MonadSql (selectPromise selectExpired updateSettle deletePromiseTimeout
                 settlementEnqueued resumptionEnqueued listenerUnblocked)

/-! ## The pure interpreter

`StateM Db`. Every method below is a total function on the store, so a
handler built from them is a closed term and `decide` can run it. -/

abbrev SqlM := StateM Db

namespace Pure

def selectPromise (id : String) : SqlM (Option PromiseRow) :=
  return (← get).promises.find? (·.id == id)

def selectExpired (id : String) (now : Int) : SqlM (Option (String × Bool × Int)) := do
  match (← get).promises.find? (·.id == id) with
  | none   => return none
  | some r =>
      if r.state == "pending" && decide (r.timeoutAt ≤ now) then
        return some (r.id, r.timer, r.timeoutAt)
      else
        return none

def updateSettle (id : String) (st : String) (settledAt : Int) : SqlM Unit :=
  modify fun db =>
    { db with promises := db.promises.map fun r =>
        if r.id == id && r.state == "pending" then
          { r with state := st, settledAt := some settledAt }
        else r }

def deletePromiseTimeout (id : String) : SqlM Unit :=
  modify fun db =>
    { db with promiseTimeouts := db.promiseTimeouts.filter (·.id != id) }

def settlementEnqueued (id : String) : SqlM Bool := do
  match (← get).tasks.find? (·.id == id) with
  | none   => return false
  | some _ =>
      modify fun db =>
        { db with
            tasks := db.tasks.map fun t =>
              if t.id == id then
                { t with state := "fulfilled", pid := none, ttl := none }
              else t
            resumes      := db.resumes.filter (·.taskId != id)
            taskTimeouts := db.taskTimeouts.filter (·.id != id) }
      return true

def resumptionEnqueued (awaitedId : String) (_now : Int) : SqlM Unit := do
  let db ← get
  let awaiters := (db.callbacks.filter (·.awaitedId == awaitedId)).map (·.awaiterId)
  modify fun db =>
    { db with
        resumes := awaiters.foldl (init := db.resumes) fun acc awaiter =>
          -- PRIMARY KEY (task_id, awaited_id): INSERT OR IGNORE
          if acc.any (fun r => r.taskId == awaiter && r.awaitedId == awaitedId) then acc
          else acc ++ [{ taskId := awaiter, awaitedId := awaitedId }]
        callbacks := db.callbacks.filter (·.awaitedId != awaitedId) }

def listenerUnblocked (promiseId : String) : SqlM Unit := do
  let db ← get
  let row       := db.promises.find? (·.id == promiseId)
  let addresses := (db.listeners.filter (·.promiseId == promiseId)).map (·.address)
  modify fun db =>
    { db with
        messages := addresses.foldl (init := db.messages) fun acc address =>
          -- collapse-on-key, matching the specification's `setMessage`
          acc.filter (fun e => !(e.address == address && e.kind == "unblock"
                                 && e.promise.map (·.id) == some promiseId))
          ++ [{ address, kind := "unblock", taskId := none,
                version := none, promise := row }]
        listeners := db.listeners.filter (·.promiseId != promiseId) }

end Pure

instance : MonadSql SqlM where
  selectPromise        := Pure.selectPromise
  selectExpired        := Pure.selectExpired
  updateSettle         := Pure.updateSettle
  deletePromiseTimeout := Pure.deletePromiseTimeout
  settlementEnqueued   := Pure.settlementEnqueued
  resumptionEnqueued   := Pure.resumptionEnqueued
  listenerUnblocked    := Pure.listenerUnblocked

end SqliteModel
