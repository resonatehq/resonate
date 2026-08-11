import «04-theorems».«refinement»
import «02-abstract».«external-steps-p»

/-!  # The abstract twins — projected vs materialized, coalesced state

The square, completed: the abstract machine in both read disciplines.
`AbstractModel` (in `external-steps-m.lean`) materializes on touch;
here its projected twin (`external-steps-p.lean`) serves the same facts
as views and persists nothing — the shared internal steps
(`internal-steps.lean`, material transitions both) do all fact
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

  * **Message lockstep holds too.** R5 and R6 read the TASK raw
    (choices are never forced by observation) but DECIDE through the
    view — no rule creates new work for a logically dead task. With
    that, no rule's effect depends on fact-lag at all, and the twins
    lockstep on both channels: `LockstepAbstract`. The script that
    refuted message lockstep when R5/R6 decided raw (`wLag` — the
    projected machine re-pended a fact-lagged task and emitted a
    doomed `execute` the materialized machine could not) now
    locksteps; it is kept below as the regression witness.  -/

namespace Abstraction

open Equivalence (Request Response extTags tgtTags timerTags eqSet)

/-- The projected abstract machine's step: `Projected` handlers, the
    SAME rules — the read discipline concerns handlers only. -/
def handleAP (st : AStep) (now : Nat) : AbstractModel.H Response :=
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
  | .r1 id      => do AbstractModel.Internal.processPromiseTimeout id now; return .τ
  | .r3 id a    => do AbstractModel.Internal.processListener id a now; return .τ
  | .r4 id x    => do AbstractModel.Internal.processCallback id x now; return .τ
  | .r5 id      => do AbstractModel.Internal.processLeaseTimeout id now; return .τ
  | .r6 id next => do AbstractModel.Internal.processRetryTimeout id next now; return .τ
  | .r7 id      => do AbstractModel.Internal.processSchedule id now; return .τ
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
    eqSet (absQuiesced defaultRetry (tr N).now (tr N).state).outbox
          (absQuiesced defaultRetry (tr' N').now (tr' N').state).outbox = true

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

/-- **The sharper theorem, unique to the abstract level: response
    lockstep.** Feed the SAME schedule — every step, adversarial rule
    firings included — to both disciplines, and the response streams
    are pointwise identical. No ∃, no re-indexing, no silence: the
    universal synchronous form, strictly stronger than the response
    half of the bisimulation, and exactly the statement that is FALSE
    for the concrete twins (`lockstep.lean`). PROVEN — the full
    mechanized induction is `Proof.responseLockstepAbstract`
    (`proof.lean`); the exhaustive adversarial sweep (`lockstepSweep`)
    and the battery remain as executable spot-checks. -/
def ResponseLockstepAbstract : Prop :=
  ∀ (trP trM : ATrace),
    ValidAP trP → ValidA trM →
    (trP 0).state = AbstractModel.ServerState.init →
    (trM 0).state = AbstractModel.ServerState.init →
    (∀ t, (trP t).req = (trM t).req ∧ (trP t).now = (trM t).now) →
    ∀ t, (trP t).res = (trM t).res

/-- **Full lockstep: both channels, any shared schedule.** Responses
    pointwise identical AND messages equal at quiescence. Holds because
    every decision in either machine — handler or rule — goes through
    the view, so no effect depends on fact-lag; only the stored bytes
    differ, and quiescence closes them. -/
def LockstepAbstract : Prop :=
  ∀ (trP trM : ATrace),
    ValidAP trP → ValidA trM →
    (trP 0).state = AbstractModel.ServerState.init →
    (trM 0).state = AbstractModel.ServerState.init →
    (∀ t, (trP t).req = (trM t).req ∧ (trP t).now = (trM t).now) →
    (∀ t, (trP t).res = (trM t).res) ∧ SameMessagesAA trP trM

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
    && absStateEq (absQuiesced defaultRetry horizon (runFinAP w).2)
                  (absQuiesced defaultRetry horizon (runFinA w).2)

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

/-! ### The regression witness

Task `x`, deadline 250, lease 200. At 300 the read runs (the
materialized machine persists facts, the projected one serves them),
then R5 and R6 fire on the fact-lagged task. When R5/R6 decided on raw
task state alone, the projected machine re-pended and emitted a doomed
`execute` here while the materialized one could not — the script
refuted message lockstep. With decisions through the view, both
machines refuse (no new work for the dead), and the script locksteps
on both channels. -/

def wLag : List (AStep × Nat) :=
  [ (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := "x", timeoutAt := 250, param := {}, tags := tgtTags } }), 100),
    (.api (.taskGet { id := "x" }), 300),
    (.r5 "x", 300),
    (.r6 "x" 5300, 300) ]

example : respLockstepA wLag = true := by decide
example : twinCheckA wLag 400 = true := by decide

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

/-! ### The sweep

Full lockstep — responses pointwise, quiesced states and outboxes —
over the ADVERSARIAL alphabet, the choice-rules R5 and R6 included:
no shared schedule, however hostile, splits the twins on either
channel. -/

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

def seqsLenA (ks : List AStep) : Nat → List (List AStep)
  | 0 => [[]]
  | n + 1 => (seqsLenA ks n).flatMap (fun s => ks.map (fun k => s ++ [k]))

def seqsUpToA (ks : List AStep) (n : Nat) : List (List AStep) :=
  (List.range (n + 1)).flatMap (seqsLenA ks)

def instantiateA (ks : List AStep) : List (AStep × Nat) :=
  ks.mapIdx (fun i st => (st, 100 * (i + 1)))

/-- Every script up to length 3 over the 11-request adversarial
    alphabet: full lockstep — 1 464 scripts, none hand-picked. -/
theorem lockstepSweep :
    ((seqsUpToA kernelsResp 3).map instantiateA).all
      (fun w => twinCheckA w 500) = true := by
  decide

end Abstraction
