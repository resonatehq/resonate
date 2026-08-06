import «05-sqlite».«P-01-promise.get»
import «03-concrete».«m».«P-01-promise.get»

/-!  # P-01 · conformance of the SQL machine to `-m`

The claim, for `promise.get`:

> For every well-formed database `db` and every instant `now`, the SQL
> handler and `Materialized.promiseGet` return the same response, and
> the databases they leave behind agree under `alpha`.

Both sides are closed Lean terms — the SQL side because `SqlM` is
`StateM Db`, a pure interpreter — so the claim is `Decidable` at each
point and the sweep below is discharged by kernel `decide`. No
`native_decide`: nothing here is trusted beyond the kernel.

This is the square's third corner. `-p` and `-m` are related by a change
of READ DISCIPLINE over one state type; `-m` and this machine are
related by a change of STATE REPRESENTATION over one read discipline.
`alpha` is the second change, exactly as `ConcreteRefinesAbstract`'s
`alpha` is the first.

**Scope, stated rather than implied.** One promise id under observation
plus one awaiter, every promise state, four tag shapes (internal, timer,
targeted, explicitly external), three deadlines, a task present or not, a
callback present or not, a listener present or not, four instants —
1 920 points. That is a sweep, not a proof: it says the machines agree
everywhere in this box, and says nothing outside it. The box is chosen to
straddle every branch in `try_timeout` — expired vs not, timer vs not,
task vs none, callback vs none, listener vs none — so a divergence in any
one of them lands inside it. -/

namespace SqliteModel.Conformance

open ServerModel SqliteModel

/-! ## Decidable equality on the compared channels

Following `04-theorems/trace.lean`, which derives `BEq` post hoc for the
same reason: the protocol types carry `Repr` only, because the machines
themselves never compare them — only the theorems do. -/

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

/-! Kernel reduction over a 1 920-point scope, at the budgets the
existing sweeps use (`04-theorems/lazy.lean:215`). -/

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

/-! ## The two machines, run -/

def runSql (db : Db) (req : PromiseGetReq) (now : Nat) : PromiseGetRes × Db :=
  Id.run ((SqliteModel.promiseGet req now : SqlM PromiseGetRes).run db)

def runSpec (s : ServerState) (req : PromiseGetReq) (now : Nat) :
    PromiseGetRes × ServerState :=
  Id.run ((Materialized.promiseGet req now).run s)

/-! ## The swept scope -/

def scopeTags : List Tags :=
  [ []                                -- internal: no target, no timer
  , [("resonate:timer",    "true")]   -- timer
  , [("resonate:target",   "w1")]     -- targeted
  , [("resonate:external", "true")] ] -- explicitly external

def scopeStates : List String :=
  ["pending", "resolved", "rejected", "rejected_canceled", "rejected_timedout"]

def scopeDeadlines : List Int := [0, 1, 2]
def scopeNows      : List Nat := [0, 1, 2, 3]

def mkPromise (id : String) (st : String) (tg : Tags) (deadline : Int) : PromiseRow :=
  { id           := id
    state        := st
    paramHeaders := none
    paramData    := none
    valueHeaders := none
    valueData    := none
    tags         := tg
    target       := genTarget tg
    origin       := genOrigin tg
    branch       := genBranch tg
    timer        := genTimer tg
    timeoutAt    := deadline
    createdAt    := 0
    settledAt    := if st == "pending" then none else some deadline }

/-- `"a"` is the promise under observation; `"b"` is a live awaiter, so a
    `callbacks` row has a referent and the foreign key holds. -/
def mkDb (st : String) (tg : Tags) (deadline : Int)
         (hasTask hasCallback hasListener : Bool) : Db :=
  { promises        := [ mkPromise "a" st tg deadline
                       , mkPromise "b" "pending" [("resonate:target", "w2")] 9 ]
    promiseTimeouts := if st == "pending" then [{ timeoutAt := deadline, id := "a" }] else []
    tasks           := if hasTask then
                         [{ id := "a", state := "acquired", version := 1,
                            pid := some "p0", ttl := some 100 }]
                       else []
    taskTimeouts    := if hasTask then [{ id := "a", kind := 1, timeoutAt := deadline }] else []
    callbacks       := if hasCallback then
                         [{ awaitedId := "a", awaiterId := "b", ready := false }]
                       else []
    listeners       := if hasListener then [{ promiseId := "a", address := "poll://w@1" }] else []
    resumes         := []
    messages        := [] }

def scopeDbs : List Db :=
  scopeStates.flatMap fun st =>
  scopeTags.flatMap fun tg =>
  scopeDeadlines.flatMap fun d =>
  [true, false].flatMap fun hasTask =>
  [true, false].flatMap fun hasCallback =>
  [true, false].map fun hasListener =>
    mkDb st tg d hasTask hasCallback hasListener

/-- Every database in the scope is one SQLite could actually be in. If
    this were false the sweeps below would be checking phantoms. -/
theorem scope_wellFormed : scopeDbs.all Db.wf = true := by decide

/-! ## The response channel

The channel `alts/unified/TRACES.md` records as unobserved by every
existing harness — "no harness records what an RPC returned" — and the
one the specification fully determines. -/

def responseAgrees (db : Db) (now : Nat) : Bool :=
  decide ((runSql db ⟨"a"⟩ now).1 = (runSpec (alpha db) ⟨"a"⟩ now).1)

theorem responseConformance :
    scopeDbs.all (fun db => scopeNows.all (responseAgrees db)) = true := by
  decide

/-! ## The state channel

Strictly stronger, and the one that pins `alpha` down: it asserts that
un-normalising the `callbacks` and `listeners` tables back onto the
promise object, and reading `resumes`/`messages` as `deferred`/`outbox`,
reproduces the specification's successor state exactly — including the
ordering difference between `try_timeout`'s cascade order and
`touchPromise`'s, which this is what settles. -/

def stateAgrees (db : Db) (now : Nat) : Bool :=
  decide (alpha (runSql db ⟨"a"⟩ now).2 = (runSpec (alpha db) ⟨"a"⟩ now).2)

theorem stateConformance :
    scopeDbs.all (fun db => scopeNows.all (stateAgrees db)) = true := by
  decide

/-! ## The generated column, as a lemma

    timer BOOLEAN NOT NULL GENERATED ALWAYS AS
      (COALESCE(json_extract(tags,'$.resonate:timer'),'') = 'true') STORED

against the specification's `Tags.isTimer`. A one-line correspondence
that would otherwise be an assumption buried in `wf`. -/

theorem genTimer_eq_isTimer :
    scopeTags.all (fun tg => genTimer tg == tg.isTimer) = true := by decide

end SqliteModel.Conformance
