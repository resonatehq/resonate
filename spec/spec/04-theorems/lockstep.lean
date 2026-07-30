import «04-theorems».«trace»

/-!  # Equivalence — why the τ schedule must be the machine's own

The tempting stronger statement — run BOTH machines under the SAME
full request sequence, τs included, and demand equal responses — is
FALSE, and this file is the machine-checked witness.

The trace: external promise `a` (deadline 1000) awaited by suspended
task `x` (deadline 2000). At 1100, `a` is logically dead and its τ has
not fired.

    promiseGet a @1100     -p: serves the projection, writes nothing
                           -m: the touch IS the τ — settles `a` and
                               DEFERS the resume (a,x)
    τResume (a,x) @1101    -p: (a,x) not in deferred → silent no-op
                           -m: (a,x) in deferred → wakes x
    taskGet x @1102        -p: suspended.  -m: pending.  ≠

The obligation records are the τs' enabledness state, and the touch
moves them: -m's reads advance the wake pipeline a stage, and the base
spec itself declares the stage deliberately visible at the task
interface (`04-guarantee`). So under a shared internal schedule the
machines MUST differ — asking them to fire "the same" τ presumes they
have the same pending obligations, which is precisely what
materialization changes. Hence the ∃-quantified schedules in
`Indistinguishable`; here, -m simulates this -p trace by replacing the
(no-op) τResume with `idle`, which `04-simulation.lean` checks.  -/

namespace Equivalence

open ServerModel (PromiseState)

def lockstepCex : List (Request × Nat) :=
  [ (.promiseCreate { id := "a", timeoutAt := 1000, param := {},
                      tags := [("resonate:external", "true")] }, 100),
    (.taskCreate { pid := "p0", ttl := 100,
                   action := { id := "x", timeoutAt := 2000, param := {},
                               tags := [("resonate:target", "w1")] } }, 100),
    (.taskSuspend { id := "x", version := 1,
                    actions := [{ awaited := "a", awaiter := "x" }] }, 120),
    (.promiseGet { id := "a" }, 1100),
    (.τResume { awaited := "a", awaiter := "x" }, 1101),
    (.taskGet { id := "x" }, 1102) ]

-- Under the SHARED schedule the response streams differ …
example : ((runFin handleP lockstepCex).1 == (runFin handleM lockstepCex).1)
    = false := by decide

-- … at exactly the last step, and in exactly the stage-visible way:
-- -p still shows the suspension, -m has already woken the task.
example :
    ((runFin handleP lockstepCex).1.getLast?.map fun r =>
      match r with
      | .taskGet r => r.task.map (·.state)
      | _ => none)
    = some (some .suspended) := by decide

example :
    ((runFin handleM lockstepCex).1.getLast?.map fun r =>
      match r with
      | .taskGet r => r.task.map (·.state)
      | _ => none)
    = some (some .pending) := by decide

end Equivalence
