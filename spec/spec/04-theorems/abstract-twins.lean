import «04-theorems».«refinement»
import «02-abstract».«p»

/-!  # The abstract twins — projected vs materialized, coalesced state

The square, completed: the abstract machine in both read disciplines.
`AbstractModel` (in `m.lean`) materializes on touch; here its projected
twin (`p.lean`) serves the same facts as views and persists nothing —
the shared rules (`rules.lean`, material transitions both) do all fact
writing at the environment's pace.

The situation is SHARPER than at the concrete level, in both
directions:

  * **Response lockstep holds.** At the concrete level, a shared τ
    schedule distinguishes the twins (`lockstep.lean`) because touches
    move obligation records. At the abstract level obligations are
    retained on the objects — touches move nothing — and, after the
    halt fix, no handler reads raw state: every response goes through
    the view, and the view of a fact-lagged object equals the touched
    object byte for byte. So the two disciplines answer identically
    under ANY shared schedule — checked exhaustively below, with the
    adversarial rules (R5, R6) in the alphabet.

  * **Message lockstep still fails.** R5 and R6 read raw task state —
    lease expiry and dispatch are choices, not facts, and are never
    applied on touch. On a fact-lagged task (promise dead, fulfillment
    unpersisted) the projected machine's stored `.acquired` lets R5
    re-pend and R6 emit a doomed `execute` that the materialized
    machine — whose touch already fulfilled the task — can no longer
    produce at that point. The witness is below; the repair is the
    usual one: under its OWN schedule the materialized machine fires
    R5 and R6 at the same instant BEFORE the touching read, and the
    channels agree again. Hence the ∃-quantified schedules in the
    statement, needed here for the message channel alone.  -/

namespace Abstraction

open Equivalence (Request Response extTags tgtTags timerTags eqSet)

/-- The projected abstract machine's step: `Projected` handlers, the
    SAME rules — the read discipline concerns handlers only. -/
def handleAP (st : AStep) (now : Nat) : AbstractModel.M Response :=
  match st with
  | .api (.promiseGet req)              => Response.promiseGet <$> AbstractModel.Projected.promiseGet req now
  | .api (.promiseCreate req)           => Response.promiseCreate <$> AbstractModel.Projected.promiseCreate req now
  | .api (.promiseSettle req)           => Response.promiseSettle <$> AbstractModel.Projected.promiseSettle req now
  | .api (.promiseRegisterCallback req) => Response.promiseRegisterCallback <$> AbstractModel.Projected.promiseRegisterCallback req now
  | .api (.promiseRegisterListener req) => Response.promiseRegisterListener <$> AbstractModel.Projected.promiseRegisterListener req now
  | .api (.promiseSearch req)           => Response.promiseSearch <$> AbstractModel.Projected.promiseSearch req now
  | .api (.scheduleGet req)             => Response.scheduleGet <$> AbstractModel.Projected.scheduleGet req now
  | .api (.scheduleCreate req)          => Response.scheduleCreate <$> AbstractModel.Projected.scheduleCreate req now
  | .api (.scheduleDelete req)          => Response.scheduleDelete <$> AbstractModel.Projected.scheduleDelete req now
  | .api (.scheduleSearch req)          => Response.scheduleSearch <$> AbstractModel.Projected.scheduleSearch req now
  | .api (.taskGet req)                 => Response.taskGet <$> AbstractModel.Projected.taskGet req now
  | .api (.taskCreate req)              => Response.taskCreate <$> AbstractModel.Projected.taskCreate req now
  | .api (.taskAcquire req)             => Response.taskAcquire <$> AbstractModel.Projected.taskAcquire req now
  | .api (.taskFence req)               => Response.taskFence <$> AbstractModel.Projected.taskFence req now
  | .api (.taskHeartbeat req)           => Response.taskHeartbeat <$> AbstractModel.Projected.taskHeartbeat req now
  | .api (.taskSuspend req)             => Response.taskSuspend <$> AbstractModel.Projected.taskSuspend req now
  | .api (.taskFulfill req)             => Response.taskFulfill <$> AbstractModel.Projected.taskFulfill req now
  | .api (.taskRelease req)             => Response.taskRelease <$> AbstractModel.Projected.taskRelease req now
  | .api (.taskHalt req)                => Response.taskHalt <$> AbstractModel.Projected.taskHalt req now
  | .api (.taskContinue req)            => Response.taskContinue <$> AbstractModel.Projected.taskContinue req now
  | .api (.taskSearch req)              => Response.taskSearch <$> AbstractModel.Projected.taskSearch req now
  | .api _                              => return .τ
  | .r1 id      => do AbstractModel.Rules.promiseTimeout id now; return .τ
  | .r2 id      => do AbstractModel.Rules.taskFulfillment id now; return .τ
  | .r3 id a    => do AbstractModel.Rules.notify id a now; return .τ
  | .r4 id x    => do AbstractModel.Rules.resume id x now; return .τ
  | .r5 id      => do AbstractModel.Rules.leaseExpiry id now; return .τ
  | .r6 id next => do AbstractModel.Rules.dispatch id next now; return .τ
  | .r7 id      => do AbstractModel.Rules.scheduleFire id now; return .τ
  | .idle       => return .τ

def stepOfAP (st : AStep) (now : Nat) (s : AbstractModel.ServerState) :
    Response × AbstractModel.ServerState :=
  Id.run ((handleAP st now).run s)

/-- Validity at the projected abstract machine. -/
def ValidAP (tr : ATrace) : Prop :=
  ∀ t : Nat,
    (tr t).res = (stepOfAP (tr t).req (tr t).now (tr t).state).1 ∧
    (tr (t + 1)).state = (stepOfAP (tr t).req (tr t).now (tr t).state).2 ∧
    (tr t).now ≤ (tr (t + 1)).now

/-- Same externalized behavior between two abstract traces. -/
def SameObservationAA (tr tr' : ATrace) : Prop :=
  ∃ φ : Nat → Nat,
    (∀ s t, s < t → φ s < φ t) ∧
    (∀ t, (tr t).req.isExternal = true →
      (tr' (φ t)).req = (tr t).req ∧
      (tr' (φ t)).now = (tr t).now ∧
      (tr' (φ t)).res = (tr t).res) ∧
    (∀ s, (tr' s).req.isExternal = true →
      ∃ t, (tr t).req.isExternal = true ∧ φ t = s)

def SameMessagesAA (tr tr' : ATrace) : Prop :=
  ∀ N N',
    (∀ t, N ≤ t → (tr t).req = .idle) →
    (∀ t, N' ≤ t → (tr' t).req = .idle) →
    (tr N).now = (tr' N').now →
    eqSet (absQuiesced (tr N).now (tr N).state).outbox
          (absQuiesced (tr' N').now (tr' N').state).outbox = true

/-- **The abstract twins are weakly bisimilar.** -/
def APRefinesAM : Prop :=
  ∀ tr, ValidAP tr → (tr 0).state = AbstractModel.ServerState.init →
    ∃ tr', ValidA tr' ∧ (tr' 0).state = AbstractModel.ServerState.init ∧
      SameObservationAA tr tr' ∧ SameMessagesAA tr tr'

def AMRefinesAP : Prop :=
  ∀ tr, ValidA tr → (tr 0).state = AbstractModel.ServerState.init →
    ∃ tr', ValidAP tr' ∧ (tr' 0).state = AbstractModel.ServerState.init ∧
      SameObservationAA tr tr' ∧ SameMessagesAA tr tr'

def IndistinguishableAbstract : Prop := APRefinesAM ∧ AMRefinesAP

/-! ### Instruments -/

def runFinAP (w : List (AStep × Nat)) : List Response × AbstractModel.ServerState :=
  Id.run ((w.mapM (fun (st, n) => handleAP st n)).run AbstractModel.ServerState.init)

/-- Response lockstep: SAME script, responses pointwise equal — the
    strong form that is false at the concrete level. -/
def respLockstepA (w : List (AStep × Nat)) : Bool :=
  (runFinAP w).1 == (runFinA w).1

/-- Full lockstep: responses plus quiesced states. -/
def twinCheckA (w : List (AStep × Nat)) (horizon : Nat) : Bool :=
  respLockstepA w
    && absStateEq (absQuiesced horizon (runFinAP w).2)
                  (absQuiesced horizon (runFinA w).2)

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

/-! ### The message-channel witness

Task `x`, deadline 250, lease 200. At 300 the read runs: the
materialized machine persists facts, the projected one serves them.
Then R5 and R6 fire on the fact-lagged task: the projected machine's
stored `.acquired` re-pends and emits; the materialized machine's
fulfilled task cannot. Responses never differ — only the outbox. -/

def wLag : List (AStep × Nat) :=
  [ (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := "x", timeoutAt := 250, param := {}, tags := tgtTags } }), 100),
    (.api (.taskGet { id := "x" }), 300),
    (.r5 "x", 300),
    (.r6 "x" 5300, 300) ]

example : respLockstepA wLag = true := by decide
example : twinCheckA wLag 400 = false := by decide

/-- The repair, under the materialized machine's own schedule: R5 and
    R6 fire at the same instant BEFORE the touching read. External
    observations and quiesced states agree again. -/
def wLag' : List (AStep × Nat) :=
  [ (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := "x", timeoutAt := 250, param := {}, tags := tgtTags } }), 100),
    (.r5 "x", 300),
    (.r6 "x" 5300, 300),
    (.api (.taskGet { id := "x" }), 300) ]

example :
    (extResponsesA wLag (runFinAP wLag).1 == extResponsesA wLag' (runFinA wLag').1
      && absStateEq (absQuiesced 400 (runFinAP wLag).2)
                    (absQuiesced 400 (runFinA wLag').2)) = true := by decide

/-! ### The battery -/

def b1 : List (AStep × Nat) :=
  [ (.api (.promiseCreate { id := "a", timeoutAt := 1000, param := {}, tags := extTags }), 100),
    (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } }), 100),
    (.api (.taskSuspend { id := "x", version := 1, actions := [{ awaited := "a", awaiter := "x" }] }), 120),
    (.api (.promiseSettle { id := "a", state := .resolved, value := {} }), 200),
    (.r4 "a" "x", 200),
    (.api (.taskGet { id := "x" }), 210),
    (.r6 "x" 5200, 210),
    (.api (.taskAcquire { id := "x", version := 1, pid := "p2", ttl := 50 }), 220),
    (.api (.taskFulfill { id := "x", version := 2, action := { id := "x", state := .resolved, value := {} } }), 230) ]

example : twinCheckA b1 300 := by decide

def b2 : List (AStep × Nat) :=
  [ (.api (.promiseCreate { id := "a", timeoutAt := 1000, param := {}, tags := extTags }), 100),
    (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := "x", timeoutAt := 300, param := {}, tags := tgtTags } }), 100),
    (.api (.taskSuspend { id := "x", version := 1, actions := [{ awaited := "a", awaiter := "x" }] }), 120),
    (.api (.taskGet { id := "x" }), 500),
    (.api (.taskHalt { id := "x" }), 500) ]

example : twinCheckA b2 500 := by decide

def b3 : List (AStep × Nat) :=
  [ (.api (.promiseCreate { id := "tm", timeoutAt := 300, param := {}, tags := timerTags }), 100),
    (.api (.promiseGet { id := "tm" }), 500),
    (.api (.promiseRegisterListener { awaited := "tm", address := "https://l" }), 500),
    (.api (.promiseSettle { id := "tm", state := .rejected, value := {} }), 500),
    (.api (.promiseCreate { id := "tm", timeoutAt := 9999, param := {}, tags := [] }), 600) ]

example : twinCheckA b3 600 := by decide

def b4 : List (AStep × Nat) :=
  [ (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := "y", timeoutAt := 300, param := {}, tags := tgtTags } }), 100),
    (.api (.taskRelease { id := "y", version := 1 }), 150),
    (.api (.taskCreate { pid := "p1", ttl := 100, action := { id := "y", timeoutAt := 300, param := {}, tags := tgtTags } }), 500) ]

example : twinCheckA b4 500 := by decide

def b5 : List (AStep × Nat) :=
  [ (.api (.taskCreate { pid := "p0", ttl := 1000, action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } }), 100),
    (.api (.taskFence { id := "x", version := 1, action := .create { id := "c", timeoutAt := 3000, param := {}, tags := extTags } }), 200),
    (.api (.taskFence { id := "x", version := 1, action := .settle { id := "c", state := .resolved, value := {} } }), 300),
    (.api (.taskFence { id := "x", version := 1, action := .settle { id := "x", state := .resolved, value := {} } }), 400),
    (.api (.taskFence { id := "x", version := 1, action := .settle { id := "c", state := .resolved, value := {} } }), 2500) ]

example : twinCheckA b5 2500 := by decide

/-! ### The sweeps

Response lockstep is checked over the ADVERSARIAL alphabet — the raw
choice-rules R5 and R6 included — because the claim is that no shared
schedule, however hostile, splits the responses. Full lockstep
(quiesced states) is checked over the fact-rule alphabet, where it
holds; R5/R6 on fact-lagged tasks are exactly the witnessed exception,
owned by the ∃-schedule in the statement. -/

def kernelsResp : List AStep :=
  [ .api (.promiseCreate { id := "a", timeoutAt := 250, param := {}, tags := extTags }),
    .api (.taskCreate { pid := "p0", ttl := 100, action := { id := "x", timeoutAt := 250, param := {}, tags := tgtTags } }),
    .api (.taskSuspend { id := "x", version := 1, actions := [{ awaited := "a", awaiter := "x" }] }),
    .api (.promiseSettle { id := "a", state := .resolved, value := {} }),
    .api (.promiseGet { id := "a" }),
    .api (.taskGet { id := "x" }),
    .api (.taskHalt { id := "x" }),
    .r1 "a",
    .r4 "a" "x",
    .r5 "x",
    .r6 "x" 9000 ]

def kernelsState : List AStep := kernelsResp.take 9

def seqsLenA (ks : List AStep) : Nat → List (List AStep)
  | 0 => [[]]
  | n + 1 => (seqsLenA ks n).flatMap (fun s => ks.map (fun k => s ++ [k]))

def seqsUpToA (ks : List AStep) (n : Nat) : List (List AStep) :=
  (List.range (n + 1)).flatMap (seqsLenA ks)

def instantiateA (ks : List AStep) : List (AStep × Nat) :=
  ks.mapIdx (fun i st => (st, 100 * (i + 1)))

/-- Every script up to length 3 over the 11-request adversarial
    alphabet: the twins' responses never split — 1 464 scripts. -/
theorem respLockstepSweep :
    ((seqsUpToA kernelsResp 3).map instantiateA).all respLockstepA = true := by
  decide

/-- Every script up to length 3 over the 9-request fact-rule alphabet:
    full lockstep, quiesced states included — 820 scripts. -/
theorem twinLockstepSweep :
    ((seqsUpToA kernelsState 3).map instantiateA).all
      (fun w => twinCheckA w 500) = true := by
  decide

end Abstraction
