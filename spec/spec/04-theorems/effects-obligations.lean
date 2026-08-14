import «04-theorems».«effects-equiv»
import «04-theorems».«properties-step»

/-!  # The effects machine — what must be true of it

Everything the effects machine (`02-abstract/effects.lean`) has to
carry that is a claim about THE MACHINE ITSELF, stated and not proved.
Each theorem is `sorry`, so the build names it on every run.

## Why this file is short

Because it is the half that survives. The obligations relating `E` to
the StateM twins — the read layer, the 21 external handlers, the 6
internal steps, and the step from those to `EqualsM`/`EqualsP` — are 32
statements, and every one of them mentions a machine that is scheduled
for deletion. They live in `effects-equiv.lean`, which dies whole when
the twins do. Nothing here has to be picked apart on that day.

The division is worth stating as a rule, because it decides where
proof effort is worth spending. An obligation that mentions StateM is
TRANSITIONAL: its only job is to license the deletion, and once the
deletion happens the statement is unstatable. An obligation that
mentions only `E` is PERMANENT: it is what the specification actually
owes, and it is still owed after the twins are gone. Proving a
transitional obligation rigorously is proving something about code you
are about to delete.

## What these four say

The 94-entry catalogue (`02-abstract/properties.lean`) holds over `E`
at every state and every consecutive pair, under both disciplines, for
scripts of ANY length. `properties-check` and `properties-step`
establish the bounded versions — every script of length ≤ 3 — and they
establish them against the twins, not against `E`. These are the
unbounded statements against `E`, which is what a specification owes
and what evaluation cannot give.

The fourth is the strongest and the least obvious:
`effects_properties_are_discipline_blind` says `failures` is pointwise
identical between a materialized run and a projected one. That is the
formal content of "the read discipline is an implementation choice, not
a protocol decision" — and it is the one claim here that would still
mean something if both twins had never existed.

## Not here

The four known gaps (`02-abstract/properties.lean`, `## Known gaps`)
are true of the protocol and FALSE of this machine, with witnesses.
Stating them as obligations would state a falsehood. Liveness is
`liveness.lean`, unchanged by the discipline. Refinement still relates
the concrete machine to `M`; until `03-concrete` moves to effects, that
result is not about `E`. -/

namespace Abstraction
namespace Obligations

open AbstractModel
open ServerModel

def statesE (mat : Bool) :
    List (AStep × Nat) → ServerState → List (Nat × ServerState)
  | [], _ => []
  | (st, n) :: w, s =>
      let (_, s') := E.run mat (handleE st n) s
      (n, s') :: statesE mat w s'

def stepsE (mat : Bool) :
    List (AStep × Nat) → ServerState → List (Nat × ServerState × ServerState)
  | [], _ => []
  | (st, n) :: w, s =>
      let (_, s') := E.run mat (handleE st n) s
      (n, s, s') :: stepsE mat w s'

def internalStepsE (mat : Bool) (w : List (AStep × Nat)) :
    List (Nat × ServerState × ServerState) :=
  ((stepsE mat w ServerState.init).zip (w.map Prod.fst)).filterMap
    fun (t, st) => if isInternalStep st then some t else none

/-- Every `.state` property in the catalogue, at every state `E` reaches,
    under both disciplines, for a script of any length. -/
theorem effects_state_properties (mat : Bool) (w : List (AStep × Nat)) :
    (statesE mat w ServerState.init).all
      (fun (n, s) => Properties.well_formed n s) = true := sorry

/-- Every `.trans` property, at every consecutive pair. -/
theorem effects_step_properties (mat : Bool) (w : List (AStep × Nat)) :
    (stepsE mat w ServerState.init).all
      (fun (n, a, b) => Properties.stepWellFormed n a b) = true := sorry

/-- The sweeper properties, on internal steps only: a background job may
    re-pend, fulfil, resume or refresh; it may never acquire, suspend,
    halt or continue a task, and it may settle a promise only by its
    deadline. False on request steps, which is what makes it a
    constraint rather than a restatement. -/
theorem effects_internal_properties (mat : Bool) (w : List (AStep × Nat)) :
    (internalStepsE mat w).all
      (fun (n, a, b) => Properties.internalWellFormed n a b) = true := sorry

/-- The catalogue is discipline-blind: nothing in it distinguishes a
    materialized run from a projected one. Stronger than the two above
    taken together, and the reason `Env.mat` is an implementation
    choice rather than a protocol decision. -/
theorem effects_properties_are_discipline_blind (w : List (AStep × Nat)) :
    ((statesE true w ServerState.init).map (fun (n, s) => Properties.failures n s)
      = (statesE false w ServerState.init).map (fun (n, s) => Properties.failures n s)) := sorry

end Obligations
end Abstraction
