import «04-theorems».«abstract-twins»
import «02-abstract».«effects»

namespace Abstraction

open Equivalence (Request Response extTags tgtTags timerTags)

def handleE (st : AStep) (now : Nat) : AbstractModel.E.H Response :=
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
    List (AStep × Nat) → AbstractModel.ServerState →
    List Response × AbstractModel.ServerState
  | [], s => ([], s)
  | (st, n) :: w, s =>
      let (r, s') := AbstractModel.E.run mat (handleE st n) s
      let (rs, s'') := runFinE mat w s'
      (r :: rs, s'')

def eqM (w : List (AStep × Nat)) : Bool :=
  let (rs, s) := runFinE true w AbstractModel.ServerState.init
  let (rs', s') := runFinA w
  rs == rs' && absStateEq s s'

def eqP (w : List (AStep × Nat)) : Bool :=
  let (rs, s) := runFinE false w AbstractModel.ServerState.init
  let (rs', s') := runFinAP w
  rs == rs' && absStateEq s s'

def eqBoth (w : List (AStep × Nat)) : Bool := eqM w && eqP w

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

def wTimerE : List (AStep × Nat) :=
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

end Abstraction
