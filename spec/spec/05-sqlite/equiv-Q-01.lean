import «05-sqlite».«Q-01-promise.get»
import «03-concrete».«m».«P-01-promise.get»
import «03-concrete».«m».«03-resume»

/-!  # Q-01 · conformance of the SQL text to `-m`

Same claim as `equiv-P-01.lean`, with one difference that is the whole
point: what runs on the implementation side is the STATEMENT TEXT, so a
divergence between the specification and the SQL is now reachable.

The specification side is `Materialized.step` — the handler followed by
the resume drain — because SQLite discharges the wake inline
(`resumption_enqueued` sets the awaiter `pending` in the same
transaction) and the specification's concrete machine admits both the
eager and the lazy schedule. Comparing against the un-drained handler
would be comparing schedules, not behaviour; `04-theorems/eager.lean`
is the same choice for the same reason.

## What the first run of this file found

Two things, both of which the typed-interface version (`equiv-P-01.lean`)
was structurally unable to report, because there both sides of the
comparison were the author's paraphrase of the SQL rather than the SQL.

1. **A configuration mismatch, 144/1920 points.** `resumption_enqueued`
   arms the retry at `now + 30000`; the specification's
   `ServerConfig.retryTimeout` defaults to 5 000. Fixed at the source:
   `alpha` carries `retryCadence` — the same constant the handler passes
   to `task_timeouts` — into `ServerConfig`, so the two cannot drift.

2. **A comparison defect, the remaining 72.** Every surviving failure
   differed only in LIST ORDER: the specification's `setTask` prepends
   (`t :: s.tasks.filter …`), so the most recently written object floats
   to the front, while a SQL table preserves insertion order. Nothing
   about either machine's behaviour differs. `ServerState`'s components
   are lists representing KEYED COLLECTIONS — `setPromise`, `setTask`,
   `setMessage` and `defer` all filter-then-prepend on a key — and a SQL
   table without `ORDER BY` is a bag. So list equality was the wrong
   relation and `bagEq` below is the right one.

   Stating that as a relaxation rather than silently sorting: order IS
   observable in a list, and if some future handler's response depended
   on it, this comparison would not catch it. Nothing in the protocol
   does — no response projects a component's order — but the assumption
   is here to be audited rather than buried. -/

namespace SqliteDriver.Conformance

open Sql ServerModel SqliteDriver

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

deriving instance DecidableEq for ServerModel.Value
deriving instance DecidableEq for ServerModel.PromiseRecord
deriving instance DecidableEq for ServerModel.TaskRecord
deriving instance DecidableEq for ServerModel.PromiseGetRes
deriving instance DecidableEq for ServerModel.PromiseObject
deriving instance DecidableEq for ServerModel.TaskObject
deriving instance DecidableEq for ServerModel.Schedule
deriving instance DecidableEq for ServerModel.ResumeReq
deriving instance DecidableEq for ServerModel.PromiseTimeout
deriving instance DecidableEq for ServerModel.TaskTimeout
deriving instance DecidableEq for ServerModel.ScheduleTimeout
deriving instance DecidableEq for ServerModel.Message
deriving instance DecidableEq for ServerModel.OutboxEntry
deriving instance DecidableEq for ServerModel.ServerConfig
deriving instance DecidableEq for ServerModel.ServerState

/-! ## Bag equality

Equal lengths plus matching multiplicities for every element of the left
list. That is enough: with equal lengths, an element unmatched on the
right would force some other multiplicity to differ. -/

def bagEq {α} [BEq α] (a b : List α) : Bool :=
  a.length == b.length &&
    a.all fun x => a.countP (· == x) == b.countP (· == x)

/-- Objects carry keyed collections of their own (`callbacks`,
    `listeners`, `resumes`), so they are compared the same way. -/
def promiseEq (p q : PromiseObject) : Bool :=
  p.id == q.id && p.state == q.state && p.param == q.param
    && p.value == q.value && bagEq p.tags q.tags
    && p.timeoutAt == q.timeoutAt && p.createdAt == q.createdAt
    && p.settledAt == q.settledAt
    && bagEq p.callbacks q.callbacks && bagEq p.listeners q.listeners

def taskEq (t u : TaskObject) : Bool :=
  t.id == u.id && t.state == u.state && t.version == u.version
    && t.ttl == u.ttl && t.pid == u.pid && bagEq t.resumes u.resumes

def bagEqBy {α} (eq : α → α → Bool) (a b : List α) : Bool :=
  a.length == b.length && a.all fun x => b.any (eq x)

/-- `ServerState` compared as the keyed collections it represents. -/
def stateEquiv (s t : ServerState) : Bool :=
  s.config == t.config
    && bagEqBy promiseEq s.promises t.promises
    && bagEqBy taskEq    s.tasks    t.tasks
    && bagEq s.schedules        t.schedules
    && bagEq s.deferred         t.deferred
    && bagEq s.promiseTimeouts  t.promiseTimeouts
    && bagEq s.taskTimeouts     t.taskTimeouts
    && bagEq s.scheduleTimeouts t.scheduleTimeouts
    && bagEq s.outbox           t.outbox

/-! ## The two machines, run -/

def runSql (st : Store) (id : String) (now : Nat) : PromiseGetRes × Store :=
  Id.run ((SqliteDriver.promiseGet ⟨id⟩ now : Sql.SqlM PromiseGetRes).run st)

/-- Handler then drain: the eager schedule, matching what the driver
    does inside one transaction. -/
def runSpec (s : ServerState) (id : String) (now : Nat) : PromiseGetRes × ServerState :=
  Id.run ((Materialized.step (Materialized.promiseGet ⟨id⟩ now) now).run s)

/-! ## Scope

`"a"` is the promise read. `"b"` is a targeted awaiter whose task may be
suspended on it, so the cascade has work to do: settle → fulfil own task
→ mark the callback ready → resume `b` → arm its retry → emit its
`execute` → unblock the listener. Every branch in `try_timeout` is
straddled. -/

def tagsOf : String → String × SqlValue × SqlValue
  | "timer"  => ("{\"resonate:timer\":\"true\"}",    .null,        .int 1)
  | "target" => ("{\"resonate:target\":\"w1\"}",     .text "w1",   .int 0)
  | "ext"    => ("{\"resonate:external\":\"true\"}", .null,        .int 0)
  | _        => ("{}",                                .null,        .int 0)

def scopeTagKinds : List String := ["plain", "timer", "target", "ext"]
def scopeStates   : List String :=
  ["pending", "resolved", "rejected", "rejected_canceled", "rejected_timedout"]
def scopeDeadlines : List Int := [0, 1, 2]
def scopeNows      : List Nat := [0, 1, 2, 3]

def mkStore (pstate tagKind : String) (deadline : Int)
            (ownTask awaiter listener : Bool) : Store :=
  let (tags, target, timer) := tagsOf tagKind
  [ ("promises",
     [ [("id", .text "a"), ("state", .text pstate),
        ("param_headers", .null), ("param_data", .null),
        ("value_headers", .null), ("value_data", .null),
        ("tags", .text tags), ("target", target), ("origin", .null), ("branch", .null),
        ("timer", timer), ("timeout_at", .int deadline), ("created_at", .int 0),
        ("settled_at", if pstate == "pending" then .null else .int deadline)]
     , [("id", .text "b"), ("state", .text "pending"),
        ("param_headers", .null), ("param_data", .null),
        ("value_headers", .null), ("value_data", .null),
        ("tags", .text "{\"resonate:target\":\"w2\"}"), ("target", .text "w2"),
        ("origin", .null), ("branch", .null),
        ("timer", .int 0), ("timeout_at", .int 99), ("created_at", .int 0),
        ("settled_at", .null)] ])
  , ("promise_timeouts",
     (if pstate == "pending" then [[("timeout_at", .int deadline), ("id", .text "a")]] else [])
     ++ [[("timeout_at", .int 99), ("id", .text "b")]])
  , ("tasks",
     (if ownTask then [[("id", .text "a"), ("state", .text "acquired"), ("version", .int 1)]] else [])
     ++ (if awaiter then [[("id", .text "b"), ("state", .text "suspended"), ("version", .int 1)]] else []))
  , ("task_timeouts",
     if ownTask then
       [[("timeout_at", .int deadline), ("id", .text "a"),
         ("timeout_type", .int 1), ("process_id", .text "p0"), ("ttl", .int 100)]]
     else [])
  , ("callbacks",
     if awaiter then
       [[("awaited_id", .text "a"), ("awaiter_id", .text "b"), ("ready", .int 0)]]
     else [])
  , ("listeners",
     if listener then [[("promise_id", .text "a"), ("address", .text "poll://w@1")]] else [])
  , ("outgoing_execute", [])
  , ("outgoing_unblock", []) ]

def scopeStores : List Store :=
  scopeStates.flatMap fun ps =>
  scopeTagKinds.flatMap fun tk =>
  scopeDeadlines.flatMap fun d =>
  [true, false].flatMap fun ownTask =>
  [true, false].flatMap fun awaiter =>
  [true, false].map fun listener =>
    mkStore ps tk d ownTask awaiter listener

/-! ## Every statement the handler issues is inside the engine's subset

`parse` returning `none` yields `QueryResult.error`, and the handler
ignores that exactly as `rusqlite` would not — so an unparseable
statement would silently do nothing and the 404 path could pass for the
wrong reason. This pins the sixteen statements down instead. -/

def statements : List String :=
  [ "SELECT id, timer, timeout_at FROM promises WHERE id IN (SELECT value FROM json_each(?1)) AND state = 'pending' AND timeout_at <= ?2"
  , "UPDATE promises SET state = ?2, settled_at = ?3 WHERE id = ?1 AND state = 'pending'"
  , "DELETE FROM promise_timeouts WHERE id = ?1"
  , "UPDATE tasks SET state = 'fulfilled' WHERE id = ?1 AND state != 'fulfilled'"
  , "DELETE FROM task_timeouts WHERE id = ?1"
  , "DELETE FROM callbacks WHERE awaiter_id = ?1"
  , "UPDATE callbacks SET ready = true WHERE awaited_id = ?1"
  , "SELECT DISTINCT c.awaiter_id FROM callbacks c JOIN tasks t ON t.id = c.awaiter_id WHERE c.awaited_id = ?1 AND c.ready = true AND t.state = 'suspended'"
  , "UPDATE tasks SET state = 'pending' WHERE id = ?1 AND state = 'suspended'"
  , "SELECT version FROM tasks WHERE id = ?1"
  , "INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl) VALUES (?1, ?2, 0, ?3) ON CONFLICT (id) DO UPDATE SET timeout_at = ?1, timeout_type = 0, process_id = NULL, ttl = ?3"
  , "SELECT target FROM promises WHERE id = ?1"
  , "INSERT INTO outgoing_execute (id, version, address) VALUES (?1, ?2, ?3) ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address"
  , "INSERT INTO outgoing_unblock (promise_id, address) SELECT l.promise_id, l.address FROM listeners l WHERE l.promise_id = ?1 ON CONFLICT DO NOTHING"
  , "DELETE FROM listeners WHERE promise_id = ?1"
  , "SELECT id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at FROM promises WHERE id = ?1" ]

theorem allStatementsParse : statements.all (fun s => (Sql.parse s).isSome) = true := by
  native_decide

/-! ## Response channel

The channel `alts/unified/TRACES.md` records as unobserved by every
existing harness — "no harness records what an RPC returned". -/

def responseAgrees (st : Store) (now : Nat) : Bool :=
  decide ((runSql st "a" now).1 = (runSpec (alpha st) "a" now).1)

theorem responseConformance :
    scopeStores.all (fun st => scopeNows.all (responseAgrees st)) = true := by
  native_decide

/-! ## State channel, under α -/

def stateAgrees (st : Store) (now : Nat) : Bool :=
  stateEquiv (alpha (runSql st "a" now).2) ((runSpec (alpha st) "a" now).2)

theorem stateConformance :
    scopeStores.all (fun st => scopeNows.all (stateAgrees st)) = true := by
  native_decide

end SqliteDriver.Conformance
