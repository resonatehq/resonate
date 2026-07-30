import «04-theorems».«equivalence»
import «04-theorems».«lockstep»

/-!  # Equivalence — the simulation, executed

The -m-simulates--p construction of `02-theorem.lean`, made executable:

    lazySchedule  —  replay a -p script, replacing every internal step
                     that no-opped in -p with `idle`.

Under the lazy schedule -m tracks -p step for step (φ is the identity —
this direction needs no re-indexing), so the check per script is:

  * every external response equal, pointwise, and
  * the final states equal after quiescence — which subsumes
    `SameMessages` (the outbox is a component) and is the induction
    invariant of the open full proof.

Checked on: the lockstep counterexample (the trace that REFUTES the
shared schedule is SIMULATED by the lazy one), a battery of targeted
deep traces through every projection-sensitive path, and exhaustively
on every script up to length 4 over a 9-request alphabet that includes
the dangerous ones — the touching read, the promise τ, and the
per-pair resume (7 381 scripts, none hand-picked).  -/

namespace Equivalence

open ServerModel (PromiseState)

/-- The lazy schedule: -p's script with its silent internal steps made
    literally silent. External steps are never touched. -/
def lazySchedule (w : List (Request × Nat)) : List (Request × Nat) :=
  go w ServerModel.ServerState.init
where
  go : List (Request × Nat) → ServerModel.ServerState → List (Request × Nat)
    | [], _ => []
    | (rq, n) :: rest, s =>
        let s' := (stepOf handleP rq n s).2
        let rq' := if rq.isExternal then rq
                   else if s == s' then Request.idle else rq
        (rq', n) :: go rest s'

/-- The executable simulation check for one script: external responses
    pointwise equal under the lazy schedule, states quiescence-equal. -/
def simCheck (w : List (Request × Nat)) (horizon : Nat) : Bool :=
  let (rsP, sP) := runFin handleP w
  let (rsM, sM) := runFin handleM (lazySchedule w)
  ((w.map (·.1)).zip (rsP.zip rsM)).all
      (fun (rq, rp, rm) => !rq.isExternal || rp == rm)
    && stateEq (quiesced horizon sP) (quiesced horizon sM)

/-! ### The refuting trace, simulated

The exact script that distinguishes the machines under the shared
schedule (`03-lockstep.lean`) is indistinguishable under the lazy one:
the no-op `τResume` becomes `idle`, -m leaves the wake pipeline where
-p left it, and the task interface shows the same stage. -/

example : simCheck lockstepCex 1200 := by decide

/-! ### The battery — targeted deep traces -/

def extTags : ServerModel.Tags := [("resonate:external", "true")]
def tgtTags : ServerModel.Tags := [("resonate:target", "w1")]
def timerTags : ServerModel.Tags := [("resonate:timer", "true")]

-- Observe a dead task, then halt it (the former D1 shape).
def t1 : List (Request × Nat) :=
  [ (.promiseCreate { id := "a", timeoutAt := 1000, param := {}, tags := extTags }, 100),
    (.taskCreate { pid := "p0", ttl := 100,
                   action := { id := "x", timeoutAt := 300, param := {}, tags := tgtTags } }, 100),
    (.taskSuspend { id := "x", version := 1,
                    actions := [{ awaited := "a", awaiter := "x" }] }, 120),
    (.taskGet { id := "x" }, 500),
    (.taskHalt { id := "x" }, 500) ]

example : simCheck t1 500 := by decide

-- task.create against a logically dead promise (the former D2 shape).
def t2 : List (Request × Nat) :=
  [ (.taskCreate { pid := "p0", ttl := 100,
                   action := { id := "y", timeoutAt := 300, param := {}, tags := tgtTags } }, 100),
    (.taskRelease { id := "y", version := 1 }, 150),
    (.taskCreate { pid := "p1", ttl := 100,
                   action := { id := "y", timeoutAt := 300, param := {}, tags := tgtTags } }, 500) ]

example : simCheck t2 500 := by decide

-- The live settle pipeline: suspend, settle, resume, re-acquire, fulfill.
def t3 : List (Request × Nat) :=
  [ (.promiseCreate { id := "a", timeoutAt := 1000, param := {}, tags := extTags }, 100),
    (.taskCreate { pid := "p0", ttl := 100,
                   action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } }, 100),
    (.taskSuspend { id := "x", version := 1,
                    actions := [{ awaited := "a", awaiter := "x" }] }, 120),
    (.promiseSettle { id := "a", state := .resolved, value := {} }, 200),
    (.τResume { awaited := "a", awaiter := "x" }, 200),
    (.taskGet { id := "x" }, 210),
    (.taskAcquire { id := "x", version := 1, pid := "p2", ttl := 50 }, 220),
    (.taskFulfill { id := "x", version := 2,
                    action := { id := "x", state := .resolved, value := {} } }, 230),
    (.taskGet { id := "x" }, 240) ]

example : simCheck t3 240 := by decide

-- A timer promise past its deadline: read, late listener registration,
-- late settle, duplicate create.
def t4 : List (Request × Nat) :=
  [ (.promiseCreate { id := "tm", timeoutAt := 300, param := {}, tags := timerTags }, 100),
    (.promiseGet { id := "tm" }, 500),
    (.promiseRegisterListener { awaited := "tm", address := "https://l" }, 500),
    (.promiseSettle { id := "tm", state := .rejected, value := {} }, 500),
    (.promiseCreate { id := "tm", timeoutAt := 9999, param := {}, tags := [] }, 600) ]

example : simCheck t4 600 := by decide

-- Registration and suspension against an expired awaited.
def t5 : List (Request × Nat) :=
  [ (.promiseCreate { id := "a", timeoutAt := 300, param := {}, tags := extTags }, 100),
    (.taskCreate { pid := "p0", ttl := 100,
                   action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } }, 100),
    (.promiseRegisterCallback { awaited := "a", awaiter := "x" }, 500),
    (.taskSuspend { id := "x", version := 1,
                    actions := [{ awaited := "a", awaiter := "x" }] }, 500) ]

example : simCheck t5 500 := by decide

-- The material task lifecycle: heartbeat, early lease τ (refused, NOT
-- BEFORE), due lease τ, re-acquire, halt, continue, early retry τ
-- (refused), due retry τ against a by-then dead promise, read across
-- the deadline.
def t6 : List (Request × Nat) :=
  [ (.taskCreate { pid := "p0", ttl := 100,
                   action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } }, 100),
    (.taskHeartbeat { pid := "p0", tasks := [{ id := "x", version := 1 }] }, 150),
    (.τTaskLeaseTimeout "x", 200),
    (.τTaskLeaseTimeout "x", 300),
    (.taskAcquire { id := "x", version := 1, pid := "p1", ttl := 100 }, 310),
    (.taskHalt { id := "x" }, 320),
    (.taskContinue { id := "x" }, 330),
    (.τTaskRetryTimeout "x", 340),
    (.τTaskRetryTimeout "x", 5400),
    (.taskGet { id := "x" }, 5500) ]

example : simCheck t6 5500 := by decide

-- Fencing: fenced create, fenced settle, self-target 400, fenced
-- action after the fence's own promise dies.
def t7 : List (Request × Nat) :=
  [ (.taskCreate { pid := "p0", ttl := 1000,
                   action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } }, 100),
    (.taskFence { id := "x", version := 1,
                  action := .create { id := "c", timeoutAt := 3000, param := {}, tags := extTags } }, 200),
    (.taskFence { id := "x", version := 1,
                  action := .settle { id := "c", state := .resolved, value := {} } }, 300),
    (.taskFence { id := "x", version := 1,
                  action := .settle { id := "x", state := .resolved, value := {} } }, 400),
    (.taskFence { id := "x", version := 1,
                  action := .settle { id := "c", state := .resolved, value := {} } }, 2500) ]

example : simCheck t7 2500 := by decide

-- Delayed dispatch: retry τ refused before the delay, fired at it.
def t8 : List (Request × Nat) :=
  [ (.promiseCreate { id := "d", timeoutAt := 9000, param := {},
                      tags := [("resonate:target", "wd"), ("resonate:delay", "800")] }, 100),
    (.τTaskRetryTimeout "d", 500),
    (.τTaskRetryTimeout "d", 800),
    (.taskGet { id := "d" }, 900) ]

example : simCheck t8 900 := by decide

-- Absences and request-level validation.
def t9 : List (Request × Nat) :=
  [ (.promiseGet { id := "z" }, 100),
    (.taskGet { id := "z" }, 100),
    (.taskHalt { id := "z" }, 100),
    (.promiseSettle { id := "z", state := .pending, value := {} }, 100) ]

example : simCheck t9 100 := by decide

/-! ### The small scope, exhausted

Alphabet: two promises — `a` (external, awaited) and `x` (targeted,
with task), both deadline 250 — with creation, suspension, settlement,
BOTH reads (the promise read is -m's toucher), halt, the promise τ,
and the per-pair resume τ. Clock: position `i` runs at `100·(i+1)`, so
length-3 scripts cross the deadlines between their second and third
steps. Every script up to length 4 is checked — 7 381 scripts, none
hand-picked. -/

def kernels : List Request :=
  [ .promiseCreate { id := "a", timeoutAt := 250, param := {}, tags := extTags },
    .taskCreate { pid := "p0", ttl := 100,
                  action := { id := "x", timeoutAt := 250, param := {}, tags := tgtTags } },
    .taskSuspend { id := "x", version := 1,
                   actions := [{ awaited := "a", awaiter := "x" }] },
    .promiseSettle { id := "a", state := .resolved, value := {} },
    .promiseGet { id := "a" },
    .taskGet { id := "x" },
    .taskHalt { id := "x" },
    .τPromiseTimeout "a",
    .τResume { awaited := "a", awaiter := "x" } ]

def seqsLen : Nat → List (List Request)
  | 0 => [[]]
  | n + 1 => (seqsLen n).flatMap (fun s => kernels.map (fun k => s ++ [k]))

def seqsUpTo (n : Nat) : List (List Request) :=
  (List.range (n + 1)).flatMap seqsLen

def instantiate (ks : List Request) : List (Request × Nat) :=
  ks.mapIdx (fun i rq => (rq, 100 * (i + 1)))

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

theorem smallScopeSimulation :
    ((seqsUpTo 4).map instantiate).all (fun w => simCheck w 500) = true := by
  decide

end Equivalence
