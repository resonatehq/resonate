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

def EqualsM : Prop := ∀ w, eqM w = true
def EqualsP : Prop := ∀ w, eqP w = true

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
