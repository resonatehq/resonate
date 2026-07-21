import «01-objects».«state»
import «02-actions».«P-03-promise.settle»
import «02-actions».«02-timeouts»
import «02-actions».«03-resume»
import «02-actions».«T-01-task.get»

open ServerModel

/-! ## The durable-execution guarantee, on the τ-quotient

The central safety property of the Resonate protocol:

> If a task is suspended awaiting promises, and an awaited promise's
settlement is determined -- by a client settle, a worker fulfill, or the
passage of the awaited's deadline -- while the task's own promise is
still logically pending, then the wake of that task is a CONSERVED
OBLIGATION: some stage of the wake pipeline holds it in every state,
until the wake itself materializes.

The pipeline has three stages, connected by internal (τ) steps whose
schedule the spec deliberately does not fix:

  stage 0  callback registered on the awaited.
           Settlement may be merely logical: the awaited is past its
           deadline and the timeout τ (`onPromiseTimeout`) is pending.
  stage 1  resume deferred.
           Settlement materialized: `promiseSettle` / `taskFulfill` /
           `onPromiseTimeout` moved the callback into `deferred`;
           the drain τ (`onResume`) is pending.
  stage 2  wake materialized.
           `onResume` flipped the task to `.pending` (emitting an
           execute if targeted); the task has left `.suspended`.

TIMEOUT ALWAYS WINS: if the task's OWN promise is past its deadline the
obligation is void -- the timeout path owns the task's cleanup, and a
drained resume lands in `.expired`.

An eager schedule (the `step` combinator; resonate-pg's inline
`_cascade_settle`) passes through stages 0-2 within one action; a lazy
schedule dwells in 0 and 1 across wall-clock time. Both are admitted.
The stage is invisible at the promise interface (every promise response
projects) and visible at the task interface, deliberately: task state
is material -- claims, leases, fencing -- and is not projected, because
a projected `.pending` would report an affordance (acquirable-now) the
material machine does not yet offer.

### Two defects of the previous revision, for the record

1. Its antecedent read the task's await set from `t.resumes` -- which
   `taskSuspend` clears. Every REACHABLE `.suspended` task has
   `resumes = []`, so the old invariant was vacuous exactly where it
   mattered; it only bit on hand-built states. The await graph of a
   suspended task lives in `callbacks` and `deferred` (stages 0 and 1);
   the antecedent now reads from there.
2. Its conclusion demanded stage 2 (`hasExecuteInOutbox`) as a
   between-states predicate -- true only under the eager schedule
   (and, per defect 1, vacuously elsewhere). The quotient statement
   below is schedule-independent.

### Why the guarantee is stated over TWO states

Once the antecedent reads the true await graph, a single-state
implication "awaits ∧ settled → some stage holds" is a TAUTOLOGY: for a
suspended task, the await evidence IS stage 0 or stage 1. The content
of the guarantee is therefore not a state predicate but a conservation
law across transitions -- no handler may DROP a rung without producing
the next stage or waking the task -- plus one non-trivial state
invariant: no suspended task with a live own promise is orphaned
(rungless). Formally:

  * `noOrphanedSuspension` (one state): every live suspended task holds
    at least one rung. This is what a drop-bug violates.
  * `wakeConserved` (two states): across any action or τ-step, an
    awaited rung either persists (possibly advanced a stage) or the
    task has left `.suspended`. This is the guarantee proper; its
    per-handler proof obligations are discharged below by EXECUTING the
    handlers on concrete states -- the spec is an abstract machine, so
    the scenarios drive `promiseSettle`, `onPromiseTimeout`, `drain`,
    and `step` directly rather than hand-building post-states.

Under any schedule that eventually runs every enabled τ (weak
fairness), conservation plus stage-advancement yields the liveness
reading: a determined wake is eventually materialized. The spec states
the safety half; fairness is an obligation on schedules, not on the
machine.
-/

/-- Task `t` awaits promise `a`: a callback is registered (stage 0) or
    the settlement already moved it into the deferred set (stage 1). -/
def awaitsOn (s : ServerState) (t : TaskObject) (a : PromiseObject) : Bool :=
  a.callbacks.contains t.id
    || s.deferred.any (fun d => d.awaited == a.id && d.awaiter == t.id)

/-- Whether an `execute` message for task `taskId` is present in the
    outbox (stage 2, targeted case). -/
def hasExecuteInOutbox (taskId : String) (outbox : List OutboxEntry) : Bool :=
  outbox.any (fun e =>
    match e with
    | { address := _, message := .execute id _ } => id == taskId
    | _ => false)

/-- **No orphaned suspension** (single-state half of the guarantee).
    A suspended task whose own promise is logically pending holds at
    least one rung: some promise it awaits, at stage 0 or stage 1.
    `taskSuspend` establishes this (it 400s on an empty awaited set and
    registers a callback per awaited); every other handler preserves it
    by advancing rungs rather than dropping them. -/
def noOrphanedSuspension (s : ServerState) (now : Nat) : Bool :=
  s.tasks.all fun t =>
    match t.state with
    | .pending | .acquired | .halted | .fulfilled => true
    | .suspended =>
      match s.promises.find? (·.id == t.id) with
      | none => true
      | some tP =>
        if (tP.project now).state != .pending then
          true  -- TIMEOUT ALWAYS WINS: cleanup owned by the timeout path
        else
          s.promises.any (fun a => awaitsOn s t a)

/-- **Wake conservation** (two-state half of the guarantee). For every
    task suspended in `pre` with a live own promise, and every promise
    it awaits there: after the step, either the task has left
    `.suspended` (the wake materialized, or its own settlement/timeout
    fulfilled it), or a rung for that awaited persists. Instantiate
    `post` with the result of any handler or τ-step. -/
def wakeConserved (pre post : ServerState) (now : Nat) : Bool :=
  pre.tasks.all fun t =>
    match t.state with
    | .pending | .acquired | .halted | .fulfilled => true
    | .suspended =>
      match pre.promises.find? (·.id == t.id) with
      | none => true
      | some tP =>
        if (tP.project now).state != .pending then
          true  -- TIMEOUT ALWAYS WINS
        else
          pre.promises.all fun a =>
            if awaitsOn pre t a then
              match post.tasks.find? (·.id == t.id),
                    post.promises.find? (·.id == a.id) with
              | some t', some a' =>
                  t'.state != .suspended || awaitsOn post t' a'
              | _, _ => true
            else true

/-! ## Executable verification

The scenarios below run the actual handlers. `sBase` is the reachable
shape `taskSuspend` produces: task `t1` suspended with `resumes = []`,
its await recorded as a callback on external promise `a` (stage 0,
awaited still pending). The trace then advances the pipeline:

  sBase --promiseSettle a--> sStage1 --drain--> sStage2

and eagerly in one step via `step` (the sync schedule), checking
conservation across every edge, the projection's stage-independence,
and the stage-1 state on which the previous revision's between-states
demand (`hasExecuteInOutbox`) fails -- the lazy state it could not
admit.
-/

/-- Run an action for its post-state. -/
def runM {α} (act : M α) (s : ServerState) : ServerState :=
  (Id.run (act.run s)).2

/-- Run an action for its response. -/
def runRes {α} (act : M α) (s : ServerState) : α :=
  (Id.run (act.run s)).1

def pAwaited : PromiseObject :=
  { id := "a", state := .pending, param := {},
    tags := [("resonate:external", "true")],
    timeoutAt := 1000, createdAt := 0, callbacks := ["t1"] }

def pOwn : PromiseObject :=
  { id := "t1", state := .pending, param := {},
    tags := [("resonate:target", "w1")],
    timeoutAt := 2000, createdAt := 0 }

def tSusp : TaskObject :=
  { id := "t1", state := .suspended, version := 1 }

/-- Stage 0: the post-`taskSuspend` shape. -/
def sBase : ServerState :=
  { promises := [pAwaited, pOwn], tasks := [tSusp],
    promiseTimeouts := [{ id := "a", timeout := 1000 }] }

/-- Stage 1: the awaited settles; the callback becomes a deferred
    resume. No drain has run. -/
def sStage1 : ServerState :=
  runM (promiseSettle { id := "a", state := .resolved, value := {} } 500) sBase

/-- Stage 2: the drain τ fires; the task wakes, the execute is queued. -/
def sStage2 : ServerState :=
  runM (drain 500) sStage1

/-- The eager (sync) schedule reaches stage 2 in one step. -/
def sEager : ServerState :=
  runM (step (promiseSettle { id := "a", state := .resolved, value := {} } 500) 500) sBase

-- No orphaned suspension, at stage 0 and stage 1 (stage 2 is vacuous:
-- the task is `.pending`).
example : noOrphanedSuspension sBase 500 := by decide
example : noOrphanedSuspension sStage1 500 := by decide
example : noOrphanedSuspension sStage2 500 := by decide
example : noOrphanedSuspension sEager 500 := by decide

-- Wake conservation across every edge of the trace, lazy and eager.
example : wakeConserved sBase sStage1 500 := by decide
example : wakeConserved sStage1 sStage2 500 := by decide
example : wakeConserved sBase sEager 500 := by decide

-- The stage-1 rung is the deferred set; the previous revision's
-- between-states demand fails here. This is the lazy state the
-- τ-quotient statement exists to admit.
example : sStage1.deferred.any
    (fun d => d.awaited == "a" && d.awaiter == "t1") := by decide
example : hasExecuteInOutbox "t1" sStage1.outbox = false := by decide
example : hasExecuteInOutbox "t1" sStage2.outbox = true := by decide

-- The stage is visible at the task interface, by design: at stage 1
-- the task still reads `.suspended` (material truth -- it is not yet
-- acquirable); after the drain it reads `.pending`. Task state reports
-- the machine, not the forecast.
example :
    (runRes (taskGet { id := "t1" } 500) sStage1).task.map (·.state)
      = some .suspended := by decide
example :
    ((sStage2.tasks.find? (·.id == "t1")).map
      (fun t => t.state == .pending && t.resumes == ["a"])) = some true := by
  decide

/-! ### Timeout always wins -/

/-- The awaiter's own promise is past its deadline; a deferred resume
    for it is dead weight. -/
def sExpiredOwn : ServerState :=
  { promises :=
      [{ pAwaited with state := .resolved, callbacks := [] },
       { pOwn with timeoutAt := 300 }],
    tasks := [tSusp],
    deferred := [{ awaited := "a", awaiter := "t1" }] }

-- The drain discards the resume (`.expired`); the task never wakes.
-- Both halves of the guarantee hold vacuously under the own-deadline
-- guard -- the timeout path owns this task's cleanup.
example : noOrphanedSuspension sExpiredOwn 500 := by decide
example : wakeConserved sExpiredOwn (runM (drain 500) sExpiredOwn) 500 := by
  decide
-- The one task projection that exists -- `taskGet` serving
-- `.fulfilled` for a logically settled own promise -- already reports
-- it: inert state, so no guard can contradict the report.
example :
    (runRes (taskGet { id := "t1" } 500) sExpiredOwn).task.map (·.state)
      = some .fulfilled := by decide

/-! ### The logical stage: settlement by deadline, nothing materialized -/

/-- The awaited is stored-pending but past its deadline. No timeout τ
    has fired: the callback is intact (stage 0), the deferred set is
    empty. The settlement -- and therefore the wake -- is logically
    determined. -/
def sLogical : ServerState :=
  { promises :=
      [{ pAwaited with timeoutAt := 300 }, pOwn],
    tasks := [tSusp],
    promiseTimeouts := [{ id := "a", timeout := 300 }] }

-- The rung is the callback; the state is orphan-free.
example : noOrphanedSuspension sLogical 500 := by decide

-- The split, in one state: the promise interface reports the awaited
-- settled before ANY materialization (projection); the task interface
-- reports the awaiter still `.suspended` (material). Two τ-steps of
-- lag, invisible to promise-facing observers, visible to the
-- coordination layer whose job it is to see it.
example :
    ((sLogical.promises.find? (·.id == "a")).map
      (fun a => (a.project 500).state == .rejectedTimedout)) = some true := by
  decide
example :
    (runRes (taskGet { id := "t1" } 500) sLogical).task.map (·.state)
      = some .suspended := by decide

/-- Eager materialization of the determined closure: the timeout τ then
    the drain τ at zero delay -- "depth 2" as two internal steps. -/
def sLogicalEager : ServerState :=
  runM (step (Timeouts.onPromiseTimeout "a" 500) 500) sLogical

example : wakeConserved sLogical sLogicalEager 500 := by decide
example : hasExecuteInOutbox "t1" sLogicalEager.outbox = true := by decide
