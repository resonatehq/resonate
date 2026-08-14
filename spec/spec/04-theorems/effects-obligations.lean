import «04-theorems».«effects-equiv»
import «04-theorems».«properties-step»

/-!  # The effects discipline — what must be true

Every claim the effects machine (`02-abstract/effects.lean`) has to
carry, written down and NOT proved. Each theorem below is `sorry`, so
the build names every one of them on every run. This file is a review
artifact: read it as the specification of the proof work, and disagree
with it here rather than after a month of tactics.

## The shape of the argument

`E` replaces two machines with one. `AbstractModel.M` materializes on
touch, `AbstractModel.P` serves the same facts as views and persists
nothing, and they are 387 and 386 lines that differ in one word per
read. In `E` that word is a value — `Env.mat` — so `run true` is M and
`run false` is P.

The obligations come in four layers, and the order is the dependency
order:

  1. **The read layer.** Four statements. The read primitives are
     shared — they live in `02-abstract/state.lean`, and the twins are
     distinguished by WHICH they call. `readPromise`/`readTask` at
     `mat := true` are the `touch` pair, the ones M calls; at
     `mat := false` they are the `view` pair, the ones P calls. Everything
     else is downstream of these, because the handler bodies are
     otherwise the same code — they were derived by substituting the
     reads.
  2. **The steps.** 21 external handlers and 6 internal steps, each
     agreeing with its StateM counterpart. These should follow from
     layer 1 structurally; they are stated separately because a
     structural argument that skips a handler is a proof of nothing,
     and 27 named obligations make the omission visible.
  3. **The machine.** `EqualsM`/`EqualsP` in `effects-equiv.lean`,
     already stated. Layer 2 implies them; that implication is stated
     here as `machine_agreement_follows_from_steps`.
  4. **The properties.** The 94-entry catalogue
     (`02-abstract/properties.lean`) holds over `E` — at every state,
     at every step, under both disciplines, for scripts of ANY length.

## What agreement means

`AgreesOn` compares the returned value with `=` and the resulting state
with `absStateEq`, which is set equality per table. Not structural
equality: `E` accumulates writes and folds them at the end, so its
lists come out in a different ORDER than the StateM machine's. Order
was never observable — `getPromise` is `find?` by id — so set equality
is the right relation and structural equality would be a false
obligation.

## The internal steps are not discipline-parametric

Layer 2 states the six internal steps at both values of `mat` and
expects agreement with the SAME StateM internal step both times.
Internal steps materialize under both disciplines: they take the forced
reads (`touchPromise`, `touchTask`, `viewPromise`), never the
parametric ones. This is not a detail. The first derivation of `E`
substituted the parametric read into the internal steps as well, and
the sweep refuted it — 60 of 1 331 scripts, `eqM` true throughout and
`eqP` false. `withMat` is the fix, and these six obligations are where
a regression would surface.

## What is NOT here

  * **The gaps.** The four constraints in `02-abstract/properties.lean`
    under `## Known gaps` are true of the protocol and FALSE of this
    machine, with witnesses. They are not obligations; making them
    obligations would be stating a falsehood.
  * **Liveness.** `liveness.lean`, unchanged by the discipline.
  * **Refinement.** `refinement.lean` relates the abstract machine to
    the concrete one, and `03-concrete` is still StateM. Porting it is
    a separate project, and until it happens the refinement result is
    about `M`, not about `E`.
  * **Provenance.** `internalChecks` is stated below over internal
    steps only, which is the one provenance claim a walk can carry. -/

namespace Abstraction
namespace Obligations

open AbstractModel
open ServerModel

/-! ## Agreement

The value is equal; the state is equal as a set of rows. -/

def AgreesOn {α : Type} (mat : Bool) (e : E.H α) (m : AbstractModel.H α) : Prop :=
  ∀ s : ServerState,
    (E.run mat e s).1 = (Id.run (m.run s)).1
      ∧ absStateEq (E.run mat e s).2 (Id.run (m.run s)).2 = true

/-! ## Layer 1 — the read layer

The four statements everything else rests on. `E` has no `viewTask` of its
own because `readTask` at `mat := false` already IS `viewTask` — the
one P calls ten times and M never calls. That is exactly what the
fourth obligation says. -/

theorem read_promise_is_touch_when_materialized (id : String) (now : Nat) :
    AgreesOn true (E.readPromise id now) (touchPromise id now) := sorry

theorem read_promise_is_view_when_projected (id : String) (now : Nat) :
    AgreesOn false (E.readPromise id now) (viewPromise id now) := sorry

theorem read_task_is_touch_when_materialized (id : String) (now : Nat) :
    AgreesOn true (E.readTask id now) (touchTask id now) := sorry

theorem read_task_is_view_when_projected (id : String) (now : Nat) :
    AgreesOn false (E.readTask id now) (viewTask id now) := sorry

/-! ## Layer 2a — the external steps

21 handlers, each at both disciplines. The StateM side is selected by
the same bit that parameterizes `E`, which is the whole claim: one
machine, two readings. -/

theorem promiseGet_agrees (mat : Bool) (req : PromiseGetReq) (now : Nat) :
    AgreesOn mat (E.promiseGet req now)
      (if mat then M.promiseGet req now else P.promiseGet req now) := sorry

theorem promiseCreate_agrees (mat : Bool) (req : PromiseCreateReq) (now : Nat) :
    AgreesOn mat (E.promiseCreate req now)
      (if mat then M.promiseCreate req now else P.promiseCreate req now) := sorry

theorem promiseSettle_agrees (mat : Bool) (req : PromiseSettleReq) (now : Nat) :
    AgreesOn mat (E.promiseSettle req now)
      (if mat then M.promiseSettle req now else P.promiseSettle req now) := sorry

theorem promiseRegisterCallback_agrees
    (mat : Bool) (req : PromiseRegisterCallbackReq) (now : Nat) :
    AgreesOn mat (E.promiseRegisterCallback req now)
      (if mat then M.promiseRegisterCallback req now
       else P.promiseRegisterCallback req now) := sorry

theorem promiseRegisterListener_agrees
    (mat : Bool) (req : PromiseRegisterListenerReq) (now : Nat) :
    AgreesOn mat (E.promiseRegisterListener req now)
      (if mat then M.promiseRegisterListener req now
       else P.promiseRegisterListener req now) := sorry

theorem promiseSearch_agrees (mat : Bool) (req : PromiseSearchReq) (now : Nat) :
    AgreesOn mat (E.promiseSearch req now)
      (if mat then M.promiseSearch req now else P.promiseSearch req now) := sorry

theorem taskGet_agrees (mat : Bool) (req : TaskGetReq) (now : Nat) :
    AgreesOn mat (E.taskGet req now)
      (if mat then M.taskGet req now else P.taskGet req now) := sorry

theorem taskCreate_agrees (mat : Bool) (req : TaskCreateReq) (now : Nat) :
    AgreesOn mat (E.taskCreate req now)
      (if mat then M.taskCreate req now else P.taskCreate req now) := sorry

theorem taskAcquire_agrees (mat : Bool) (req : TaskAcquireReq) (now : Nat) :
    AgreesOn mat (E.taskAcquire req now)
      (if mat then M.taskAcquire req now else P.taskAcquire req now) := sorry

theorem taskFence_agrees (mat : Bool) (req : TaskFenceReq) (now : Nat) :
    AgreesOn mat (E.taskFence req now)
      (if mat then M.taskFence req now else P.taskFence req now) := sorry

theorem taskHeartbeat_agrees (mat : Bool) (req : TaskHeartbeatReq) (now : Nat) :
    AgreesOn mat (E.taskHeartbeat req now)
      (if mat then M.taskHeartbeat req now else P.taskHeartbeat req now) := sorry

theorem taskSuspend_agrees (mat : Bool) (req : TaskSuspendReq) (now : Nat) :
    AgreesOn mat (E.taskSuspend req now)
      (if mat then M.taskSuspend req now else P.taskSuspend req now) := sorry

theorem taskFulfill_agrees (mat : Bool) (req : TaskFulfillReq) (now : Nat) :
    AgreesOn mat (E.taskFulfill req now)
      (if mat then M.taskFulfill req now else P.taskFulfill req now) := sorry

theorem taskRelease_agrees (mat : Bool) (req : TaskReleaseReq) (now : Nat) :
    AgreesOn mat (E.taskRelease req now)
      (if mat then M.taskRelease req now else P.taskRelease req now) := sorry

theorem taskHalt_agrees (mat : Bool) (req : TaskHaltReq) (now : Nat) :
    AgreesOn mat (E.taskHalt req now)
      (if mat then M.taskHalt req now else P.taskHalt req now) := sorry

theorem taskContinue_agrees (mat : Bool) (req : TaskContinueReq) (now : Nat) :
    AgreesOn mat (E.taskContinue req now)
      (if mat then M.taskContinue req now else P.taskContinue req now) := sorry

theorem taskSearch_agrees (mat : Bool) (req : TaskSearchReq) (now : Nat) :
    AgreesOn mat (E.taskSearch req now)
      (if mat then M.taskSearch req now else P.taskSearch req now) := sorry

theorem scheduleGet_agrees (mat : Bool) (req : ScheduleGetReq) (now : Nat) :
    AgreesOn mat (E.scheduleGet req now)
      (if mat then M.scheduleGet req now else P.scheduleGet req now) := sorry

theorem scheduleCreate_agrees (mat : Bool) (req : ScheduleCreateReq) (now : Nat) :
    AgreesOn mat (E.scheduleCreate req now)
      (if mat then M.scheduleCreate req now else P.scheduleCreate req now) := sorry

theorem scheduleDelete_agrees (mat : Bool) (req : ScheduleDeleteReq) (now : Nat) :
    AgreesOn mat (E.scheduleDelete req now)
      (if mat then M.scheduleDelete req now else P.scheduleDelete req now) := sorry

theorem scheduleSearch_agrees (mat : Bool) (req : ScheduleSearchReq) (now : Nat) :
    AgreesOn mat (E.scheduleSearch req now)
      (if mat then M.scheduleSearch req now else P.scheduleSearch req now) := sorry

/-! ## Layer 2b — the internal steps

Six, each at BOTH values of `mat`, each against the SAME StateM internal
step. The quantifier over `mat` with no `if` on the right is the
statement that internal steps are not discipline-parametric — the claim
the first derivation of `E` got wrong. -/

theorem processPromiseTimeout_agrees (mat : Bool) (id : String) (now : Nat) :
    AgreesOn mat (E.Internal.processPromiseTimeout id now)
      (Internal.processPromiseTimeout id now) := sorry

theorem processListener_agrees
    (mat : Bool) (id : String) (address : String) (now : Nat) :
    AgreesOn mat (E.Internal.processListener id address now)
      (Internal.processListener id address now) := sorry

theorem processCallback_agrees
    (mat : Bool) (id : String) (awaiter : String) (now : Nat) :
    AgreesOn mat (E.Internal.processCallback id awaiter now)
      (Internal.processCallback id awaiter now) := sorry

theorem processLeaseTimeout_agrees (mat : Bool) (id : String) (now : Nat) :
    AgreesOn mat (E.Internal.processLeaseTimeout id now)
      (Internal.processLeaseTimeout id now) := sorry

theorem processRetryTimeout_agrees
    (mat : Bool) (id : String) (next : Nat) (now : Nat) :
    AgreesOn mat (E.Internal.processRetryTimeout id next now)
      (Internal.processRetryTimeout id next now) := sorry

theorem processSchedule_agrees (mat : Bool) (id : String) (now : Nat) :
    AgreesOn mat (E.Internal.processSchedule id now)
      (Internal.processSchedule id now) := sorry

/-! ## Layer 3 — the machine

`EqualsM`/`EqualsP` live in `effects-equiv.lean` and are stated there.
What belongs here is the step from 27 per-step obligations to the whole
machine: agreement composes along a script, because each step starts
from the state the last one left and `absStateEq` is preserved by every
handler. -/

theorem machine_agreement_follows_from_steps :
    (∀ (st : AStep) (now : Nat) (mat : Bool),
        AgreesOn mat (handleE st now) (if mat then handleA st now else handleAP st now))
    → (∀ w, eqM w = true) ∧ (∀ w, eqP w = true) := sorry

/-! ## Layer 4 — the properties

The catalogue, over `E`, for scripts of any length. `properties-check`
and `properties-step` establish the bounded versions against the StateM
twins — every script of length ≤ 3, both disciplines. These are the
unbounded statements against `E`, which is what a specification owes and
what evaluation cannot give. -/

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
