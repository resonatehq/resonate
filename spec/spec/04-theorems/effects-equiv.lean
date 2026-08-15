import «04-theorems».«abstract-twins»
import «02-abstract».«effects»

namespace Abstraction

open Equivalence (Request Response extTags tgtTags timerTags)

def handleE (st : Step) (now : Nat) : AbstractModel.E.H Response :=
  match st with
  | .api (.promiseGet req)              => Response.promiseGet <$> AbstractModel.E.promiseGet req now
  | .api (.promiseCreate req)           => Response.promiseCreate <$> AbstractModel.E.promiseCreate req now
  | .api (.promiseSettle req)           => Response.promiseSettle <$> AbstractModel.E.promiseSettle req now
  | .api (.promiseRegisterCallback req) => Response.promiseRegisterCallback <$> AbstractModel.E.promiseRegisterCallback req now
  | .api (.promiseRegisterListener req) => Response.promiseRegisterListener <$> AbstractModel.E.promiseRegisterListener req now
  | .api (.promiseSearch req)           => Response.promiseSearch <$> AbstractModel.E.promiseSearch req now
  | .api (.scheduleGet req)             => Response.scheduleGet <$> AbstractModel.E.scheduleGet req now
  | .api (.scheduleCreate req)          => Response.scheduleCreate <$> AbstractModel.E.scheduleCreate req now
  | .api (.scheduleDelete req)          => Response.scheduleDelete <$> AbstractModel.E.scheduleDelete req now
  | .api (.scheduleSearch req)          => Response.scheduleSearch <$> AbstractModel.E.scheduleSearch req now
  | .api (.taskGet req)                 => Response.taskGet <$> AbstractModel.E.taskGet req now
  | .api (.taskCreate req)              => Response.taskCreate <$> AbstractModel.E.taskCreate req now
  | .api (.taskAcquire req)             => Response.taskAcquire <$> AbstractModel.E.taskAcquire req now
  | .api (.taskFence req)               => Response.taskFence <$> AbstractModel.E.taskFence req now
  | .api (.taskHeartbeat req)           => Response.taskHeartbeat <$> AbstractModel.E.taskHeartbeat req now
  | .api (.taskSuspend req)             => Response.taskSuspend <$> AbstractModel.E.taskSuspend req now
  | .api (.taskFulfill req)             => Response.taskFulfill <$> AbstractModel.E.taskFulfill req now
  | .api (.taskRelease req)             => Response.taskRelease <$> AbstractModel.E.taskRelease req now
  | .api (.taskHalt req)                => Response.taskHalt <$> AbstractModel.E.taskHalt req now
  | .api (.taskContinue req)            => Response.taskContinue <$> AbstractModel.E.taskContinue req now
  | .api (.taskSearch req)              => Response.taskSearch <$> AbstractModel.E.taskSearch req now
  | .api _                              => return .τ
  | .r1 id      => do AbstractModel.E.Internal.processPromiseTimeout id now; return .τ
  | .r3 id a    => do AbstractModel.E.Internal.processListener id a now; return .τ
  | .r4 id x    => do AbstractModel.E.Internal.processCallback id x now; return .τ
  | .r5 id      => do AbstractModel.E.Internal.processLeaseTimeout id now; return .τ
  | .r6 id next => do AbstractModel.E.Internal.processRetryTimeout id next now; return .τ
  | .r7 id      => do AbstractModel.E.Internal.processSchedule id now; return .τ
  | .idle       => return .τ

def runFinE (mat : Bool) :
    List (Step × Nat) → AbstractModel.ServerState →
    List Response × AbstractModel.ServerState
  | [], s => ([], s)
  | (st, n) :: w, s =>
      let (r, s') := AbstractModel.E.run mat (handleE st n) s
      let (rs, s'') := runFinE mat w s'
      (r :: rs, s'')

def eqM (w : List (Step × Nat)) : Bool :=
  let (rs, s) := runFinE true w AbstractModel.ServerState.init
  let (rs', s') := runFinA w
  rs == rs' && stateEq s s'

def eqP (w : List (Step × Nat)) : Bool :=
  let (rs, s) := runFinE false w AbstractModel.ServerState.init
  let (rs', s') := runFinAP w
  rs == rs' && stateEq s s'

def eqBoth (w : List (Step × Nat)) : Bool := eqM w && eqP w

/-! ### The bridge

`E` and the two StateM twins agree — on the response stream and on the
quiesced state, for every script, under both disciplines. Stated as
theorems rather than as `Prop` definitions, and left unproved.

The change from `def … : Prop` to `theorem … := sorry` is not cosmetic.
An unconsumed `Prop` is a claim nobody makes: it compiles green, no
build reports it, and nothing anywhere depends on it. A `sorry` is a
claim the build names every time it runs. These two are the reason a
result about `E` is a result about the specification, so they should be
loud.

The evidence beneath them is `effectSweep₀..₂`: every script of length
≤ 3 over the adversarial alphabet — 133 + 1 331 = 1 464 — both
disciplines, responses and state, by `decide`. That is a refutation
engine, not a proof; it is what caught the `withMat` error (60 of 1 331
scripts, `eqM` true throughout and `eqP` false). What it cannot do is
quantify over scripts of length 4. -/

theorem EqualsM : ∀ w, eqM w = true := sorry
theorem EqualsP : ∀ w, eqP w = true := sorry

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

example : eqBoth wLag := by decide
example : eqBoth b1 := by decide
example : eqBoth b2 := by decide
example : eqBoth b3 := by decide
example : eqBoth b4 := by decide
example : eqBoth b5 := by decide

def wTimerE : List (Step × Nat) :=
  [ (.api (.promiseCreate { id := "tm", timeoutAt := 250, param := {}, tags := timerTags }), 300),
    (.api (.promiseGet { id := "tm" }), 300),
    (.r1 "tm", 400) ]

example : eqBoth wTimerE := by decide

theorem effectSweep₀ :
    ((seqsUpToA kernelsResp 2).map instantiateA).all eqBoth = true := by decide

theorem effectSweep₁ :
    (((seqsLenA kernelsResp 3).take 700).map instantiateA).all eqBoth = true := by decide

theorem effectSweep₂ :
    ((((seqsLenA kernelsResp 3).drop 700).take 631).map instantiateA).all eqBoth = true := by decide

section Agreement
open AbstractModel ServerModel

/-! ## Per-step agreement

The 32 obligations that relate `E` to the StateM twins, stated and not
proved. They live HERE, in the file that dies with StateM, rather than
beside the claims about `E` itself — when the twins go, this file goes
whole, and nothing has to be picked apart.

`AgreesOn` compares the returned value with `=` and the resulting state
with `stateEq`, set equality per table. Not structural equality: `E`
accumulates its writes and folds them at the end, so its lists come out
in a different ORDER. Order was never observable — every read is
`find?` by id — so set equality is the right relation, and structural
equality would be a false obligation. -/

def AgreesOn {α : Type} (mat : Bool) (e : E.H α) (m : AbstractModel.H α) : Prop :=
  ∀ s : ServerState,
    (E.run mat e s).1 = (Id.run (m.run s)).1
      ∧ stateEq (E.run mat e s).2 (Id.run (m.run s)).2 = true

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
from the state the last one left and `stateEq` is preserved by every
handler. -/

theorem machine_agreement_follows_from_steps :
    (∀ (st : Step) (now : Nat) (mat : Bool),
        AgreesOn mat (handleE st now) (if mat then handleA st now else handleAP st now))
    → (∀ w, eqM w = true) ∧ (∀ w, eqP w = true) := sorry

end Agreement

end Abstraction
