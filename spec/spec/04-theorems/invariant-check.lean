import «04-theorems».«lazy»
import «03-concrete».«invariant»

namespace Equivalence

open ConcreteModel (inv uniqueTimeouts timeoutsNameTasks leaseDiscipline retryDiscipline
                    deferredMoved promiseTimeoutsMirror scheduleTimeoutsMirror
                    coKeyed settledCoupled tagsWellFormed)

/-!  # `inv`, evaluated

Before proving the invariant preserved, evaluate it: run each script and
check `inv` at EVERY state it passes through, not only the last. A
conjunct that is too strong shows up here in seconds instead of after a
day of a failed preservation proof.

The states are taken under both read disciplines, since the components
`inv` constrains — the timeout sets, the deferred queue — are written by
handlers that differ between them. -/

def statesOf (handle : Request → Nat → ConcreteModel.H Response) :
    List (Request × Nat) → ConcreteModel.ServerState → List ConcreteModel.ServerState
  | [], s => [s]
  | (rq, n) :: w, s =>
      let (_, s') := Id.run ((handle rq n).run s)
      s :: statesOf handle w s'

def invHolds (w : List (Request × Nat)) : Bool :=
  (statesOf handleP w ConcreteModel.ServerState.init).all inv
    && (statesOf handleM w ConcreteModel.ServerState.init).all inv

/-- Which conjunct fails, for a script that fails — the shape a
    counterexample report needs. -/
def invReport (w : List (Request × Nat)) : List (String × Bool) :=
  let ss := statesOf handleP w ConcreteModel.ServerState.init
          ++ statesOf handleM w ConcreteModel.ServerState.init
  [ ("uniqueTimeouts",        ss.all uniqueTimeouts),
    ("timeoutsNameTasks",     ss.all timeoutsNameTasks),
    ("leaseDiscipline",       ss.all leaseDiscipline),
    ("retryDiscipline",       ss.all retryDiscipline),
    ("deferredMoved",         ss.all deferredMoved),
    ("promiseTimeoutsMirror", ss.all promiseTimeoutsMirror),
    ("scheduleTimeoutsMirror",ss.all scheduleTimeoutsMirror),
    ("coKeyed",               ss.all coKeyed),
    ("settledCoupled",        ss.all settledCoupled),
    ("tagsWellFormed",        ss.all tagsWellFormed) ]

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

example : invHolds t1 := by decide
example : invHolds t2 := by decide
example : invHolds t3 := by decide
example : invHolds t4 := by decide
example : invHolds t5 := by decide
example : invHolds t7 := by decide
example : invHolds t8 := by decide
example : invHolds t9 := by decide
example : invHolds t6 := by decide
example : invHolds lockstepCex := by decide

/-- Every script of length ≤ 3 over the alphabet, at every state it
    passes through, under both disciplines — 820 scripts, 6 356 states. -/
theorem invSweep : ((seqsUpTo 3).map instantiate).all invHolds = true := by decide

/-! ### Which conjuncts this actually exercises

A conjunct nothing reaches is not checked by passing; it is checked by
nothing. So the reach is pinned here as theorems rather than claimed in a
comment, and one of them cannot be pinned at all.

The sweep alphabet creates tasks only through `task.create`, which makes
them ACQUIRED, and it never creates a schedule. So the states it reaches
have no pending task, no deferred entry and no armed schedule — three
conjuncts would be vacuous on the sweep alone. The hand-built battery
reaches two of them. -/

def reaches (p : ConcreteModel.ServerState → Bool) (ws : List (List (Request × Nat))) : Bool :=
  ws.any fun w =>
    (statesOf handleP w ConcreteModel.ServerState.init).any p
      || (statesOf handleM w ConcreteModel.ServerState.init).any p

def battery : List (List (Request × Nat)) := [lockstepCex, t1, t2, t3, t4, t5, t6, t7, t8, t9]

theorem reachesArmedLease :
    reaches (fun s => s.taskTimeouts.any (·.kind == 1)) battery = true := by decide

theorem reachesPendingTask :
    reaches (fun s => s.tasks.any (·.state == .pending)) battery = true := by decide

theorem reachesDeferred :
    reaches (fun s => !s.deferred.isEmpty) battery = true := by decide

theorem reachesSettledOverTask :
    reaches (fun s => s.promises.any fun p =>
      p.state != .pending && s.tasks.any (·.id == p.id)) battery = true := by decide

/-- `scheduleTimeoutsMirror` is exercised by NOTHING — no script in the
    tree creates a schedule, because `nextCron` and `occurrences` are
    `opaque` with no value and a schedule cannot be run under `decide`.
    It is in `inv` because `alpha` drops `scheduleTimeouts` and something
    has to justify that; it is not evidence of anything until R7 has a
    calendar. The Go checker rejects traces mentioning schedules for the
    same reason. -/
theorem scheduleTimeoutsUnreached :
    reaches (fun s => !s.scheduleTimeouts.isEmpty) battery = false := by decide

end Equivalence
