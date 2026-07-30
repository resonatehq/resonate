import «04-theorems».«lazy»

/-!  # Refinement — the concrete machine into the abstract one, executed

`MRefinesP` (`02-theorem.lean`): every behavior of the materialized
(concrete) machine is a behavior of the projected (abstract) machine.
The constructive witness is the EAGER schedule:

    eagerSchedule  —  before each step of an -m script, insert an
                      explicit `τPromiseTimeout` for every promise the
                      step's request names.

The insertions are purely syntactic — `touched` reads the request, not
the run — because -m's touches are keyed by the ids the request names.
Each inserted τ is obligation-guarded, so it fires exactly where -m's
touch would have materialized (armed and due) and is a silent no-op
everywhere else: -p may over-insert freely, never over-fire. Inserted
steps carry the instant of the step they precede — validity's clock
conjunct is `≤`, so ties are ordinary.

Under the eager schedule, -p performs as explicit internal transitions
exactly the work -m performs inside its reads. The check per script
compares the EXTERNAL response subsequences (the insertion shifts
indices, so this direction genuinely exercises the re-indexing φ of
`SameObservation`) and quiescence-equality of the final states.

The same script that refutes the shared schedule (`03-lockstep.lean`)
is the centerpiece here, from the other side: read as an -m behavior —
the touch defers, the `τResume` wakes — the eager -p trace inserts
`τPromiseTimeout a` before the read, defers the same resume, wakes the
same task, and answers `pending` right along with -m.

One scoping note, inherited from the machines themselves: an INTERNAL
promise past its deadline is settled by -m's touch but has no armed τ
for -p to fire — its settlement is projection-only in -p, forever.
Responses cannot tell (projection and materialization serve the same
bytes), but stored states differ there. Listeners and callbacks
attach only to external promises — enforced by `422` in every machine
— so the fact-lag on internal promises carries no obligations and is
invisible to both channels.  -/

namespace Equivalence

/-- The promise ids a request's handler may touch — a static function
    of the request. -/
def touched : Request → List String
  | .promiseGet r              => [r.id]
  | .promiseCreate r           => [r.id]
  | .promiseSettle r           => [r.id]
  | .promiseRegisterCallback r => [r.awaited, r.awaiter]
  | .promiseRegisterListener r => [r.awaited]
  | .promiseSearch _           => []
  | .scheduleGet _             => []
  | .scheduleCreate _          => []
  | .scheduleDelete _          => []
  | .scheduleSearch _          => []
  | .taskGet r                 => [r.id]
  | .taskCreate r              => [r.action.id]
  | .taskAcquire r             => [r.id]
  | .taskFence r               => [r.id, r.action.targetId]
  | .taskHeartbeat r           => r.tasks.map (·.id)
  | .taskSuspend r             => r.id :: r.actions.map (·.awaited)
  | .taskFulfill r             => [r.id]
  | .taskRelease r             => [r.id]
  | .taskHalt r                => [r.id]
  | .taskContinue r            => [r.id]
  | .taskSearch _              => []
  | .τResume r                 => [r.awaited, r.awaiter]
  | .τPromiseTimeout _         => []
  | .τTaskRetryTimeout _       => []
  | .τTaskLeaseTimeout _       => []
  | .τScheduleTimeout _        => []
  | .idle                      => []

/-- The eager schedule: -m's script with -m's touch-work made explicit
    as -p transitions. External steps are never touched, only preceded. -/
def eagerSchedule (w : List (Request × Nat)) : List (Request × Nat) :=
  w.flatMap fun (rq, n) =>
    ((touched rq).map (fun id => (Request.τPromiseTimeout id, n))) ++ [(rq, n)]

/-- The external response subsequence — the observation, extracted.
    Insertions shift indices, so this direction compares filtered
    subsequences: the executable shadow of `SameObservation`'s φ. -/
def extResponses (w : List (Request × Nat)) (rs : List Response) : List Response :=
  ((w.map (·.1)).zip rs).filterMap
    (fun (rq, r) => if rq.isExternal then some r else none)

/-- The executable refinement check for one -m script: the abstract
    machine, eagerly scheduled, exhibits the same external observation
    and a quiescence-equal final state. -/
def refCheck (w : List (Request × Nat)) (horizon : Nat) : Bool :=
  let (rsM, sM) := runFin handleM w
  let wP := eagerSchedule w
  let (rsP, sP) := runFin handleP wP
  extResponses w rsM == extResponses wP rsP
    && stateEq (quiesced horizon sP) (quiesced horizon sM)

/-! ### The refuting trace, refined -/

example : refCheck lockstepCex 1200 := by decide

/-! ### The battery, read as concrete behaviors -/

example : refCheck t1 500 := by decide
example : refCheck t2 500 := by decide
example : refCheck t3 240 := by decide
example : refCheck t4 600 := by decide
example : refCheck t5 500 := by decide
example : refCheck t6 5500 := by decide
example : refCheck t7 2500 := by decide
example : refCheck t8 900 := by decide
example : refCheck t9 100 := by decide

/-! ### The small scope, exhausted

Same alphabet and clock as `04-simulation.lean`: every script up to
length 4 over the 9-request alphabet — 7 381 scripts, each read as a
concrete (-m) behavior and refined into an eager abstract (-p) one. -/

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

theorem smallScopeRefinement :
    ((seqsUpTo 4).map instantiate).all (fun w => refCheck w 500) = true := by
  decide

end Equivalence
