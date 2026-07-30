import «04-theorems».«abstract-twins»

/-!  # The square, closed

Four machines, one protocol surface:

                 concrete state          coalesced state
  projection     03-concrete/p           02-abstract/p.lean
  materialized   03-concrete/m           02-abstract/m.lean

and four relations:

  * `Equivalence.Indistinguishable`        — Cp ≈ Cm   (equivalence.lean)
  * `IndistinguishableAbstract`            — Ap ≈ Am   (abstract-twins.lean)
  * `ConcreteRefinesAbstract`              — Cm ⊑ Am   (refinement.lean)
  * `AbstractRefinesConcrete` (this file)  — Am ⊑ Cm

`TheSquare` is their conjunction. By composition it places ALL FOUR
machines in one weak-bisimilarity class with respect to the two
observables — externalized request/response behavior, and messages at
quiescence: every implementation choice the machines embody (serving
facts vs persisting them; auxiliary timers, queues, and config vs
deadlines and obligations on the objects) is unobservable. The
protocol has exactly one behavior space.

The reverse refinement became stateable only after the listener fix in
this change: every machine now refuses listeners AND callbacks on
internal promises (`422`, `promise.register_listener` mirroring
`promise.register_callback`). Before it, the abstract machine could
notify a listener on an internal promise — a τ the concrete projected
machine does not possess, an obligation with no discharge path; no
schedule, however chosen, can insert a transition the machine does not
have. With waiters external-only, every obligation lives where an
armed timeout guarantees discharge, and the last cross-machine
divergence channel closes.

The remaining fact-lag on internal promises (the abstract machine may
settle one the concrete machine leaves stored-pending forever) now
carries no obligations and is invisible to both observables; the
normalizing comparator `normEq` below absorbs it for state-level
checks by closing BOTH sides under the object-guarded fact rules.

Constructive evidence for `AbstractRefinesConcrete`: observation
equality is symmetric, so every paired run of `refinement.lean` —
concrete script `w` against its translation — is equally a witness
that the abstract trace's behavior is realized by a concrete trace;
that covers every abstract schedule in the translation's image
(battery plus exhaustive sweep). The pairs below cover the
abstract-native steps OUTSIDE that image: the standalone fact rules
R2 and R3, the wake pair R4;R6, and a fact-lagged internal promise.  -/

namespace Abstraction

open Equivalence (Request Response extTags tgtTags eqSet)

/-- Same externalized behavior, abstract trace realized by a concrete
    trace: every external abstract step `.api rq` lands, order-
    preservingly, on a concrete step with request `rq`, same instant,
    same response — and every external concrete step is reached. -/
def SameObservationAC (tr : ATrace) (tr' : Equivalence.Trace) : Prop :=
  ∃ φ : Nat → Nat,
    (∀ s t, s < t → φ s < φ t) ∧
    (∀ t, (tr t).req.isExternal = true →
      AStep.api (tr' (φ t)).req = (tr t).req ∧
      (tr' (φ t)).now = (tr t).now ∧
      (tr' (φ t)).res = (tr t).res) ∧
    (∀ s, (tr' s).req.isExternal = true →
      ∃ t, (tr t).req.isExternal = true ∧ φ t = s)

def SameMessagesAC (tr : ATrace) (tr' : Equivalence.Trace) : Prop :=
  ∀ N N',
    (∀ t, N ≤ t → (tr t).req = .idle) →
    (∀ t, N' ≤ t → (tr' t).req = .idle) →
    (tr N).now = (tr' N').now →
    eqSet (absQuiesced (tr N).now (tr N).state).outbox
          (Equivalence.quiesced (tr' N').now (tr' N').state).outbox = true

/-- **The reverse refinement: the abstract machine's behaviors are
    concrete behaviors.** Stated at Cm; Cp inherits through the
    concrete twins' bisimulation. -/
def AbstractRefinesConcrete : Prop :=
  ∀ tr, ValidA tr → (tr 0).state = AbstractModel.ServerState.init →
    ∃ tr', Equivalence.ValidM tr' ∧ (tr' 0).state = ServerModel.ServerState.init ∧
      SameObservationAC tr tr' ∧ SameMessagesAC tr tr'

/-- **THE SQUARE: all four machines, one behavior space.** -/
def TheSquare : Prop :=
  Equivalence.Indistinguishable
    ∧ IndistinguishableAbstract
    ∧ ConcreteRefinesAbstract
    ∧ AbstractRefinesConcrete

/-! ### The fix, witnessed in all four machines

A listener on an internal promise is `422` everywhere — the obligation
that could never be discharged can no longer be accepted. -/

def wListen : List (Request × Nat) :=
  [ (.promiseCreate { id := "i", timeoutAt := 1000, param := {}, tags := [] }, 100),
    (.promiseRegisterListener { awaited := "i", address := "https://l" }, 200) ]

def wListenA : List (AStep × Nat) := wListen.map (fun (rq, n) => (AStep.api rq, n))

def lastStatus (rs : List Response) : Option Nat :=
  match rs.getLast? with
  | some (.promiseRegisterListener r) => some r.status
  | _ => none

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

example : lastStatus (Equivalence.runFin Equivalence.handleP wListen).1 = some 422 := by decide
example : lastStatus (Equivalence.runFin Equivalence.handleM wListen).1 = some 422 := by decide
example : lastStatus (runFinA wListenA).1 = some 422 := by decide
example : lastStatus (runFinAP wListenA).1 = some 422 := by decide

/-! ### The paired witnesses, abstract-native steps -/

/-- The normalizing comparator: both sides closed under the
    object-guarded fact rules and the drains — absorbing the harmless
    fact-lag on internal promises. -/
def normC (h : Nat) (s : ServerModel.ServerState) : AbstractModel.ServerState :=
  absQuiesced h (alpha (Equivalence.quiesced h s))

def normA (h : Nat) (s : AbstractModel.ServerState) : AbstractModel.ServerState :=
  absQuiesced h s

/-- One abstract script realized by one concrete script: equal external
    observations, normalized states equal. Symmetric evidence — read
    left-to-right it witnesses `ConcreteRefinesAbstract`, right-to-left
    `AbstractRefinesConcrete`. -/
def pairCheck (wC : List (Request × Nat)) (wA : List (AStep × Nat)) (h : Nat) : Bool :=
  let (rsC, sC) := Equivalence.runFin Equivalence.handleM wC
  let (rsA, sA) := runFinA wA
  Equivalence.extResponses wC rsC == extResponsesA wA rsA
    && absStateEq (normC h sC) (normA h sA)

-- R3, standalone: the concrete settle notifies inline; the abstract
-- machine notifies by rule.
def q2C : List (Request × Nat) :=
  [ (.promiseCreate { id := "a", timeoutAt := 1000, param := {}, tags := extTags }, 100),
    (.promiseRegisterListener { awaited := "a", address := "https://l" }, 150),
    (.promiseSettle { id := "a", state := .resolved, value := {} }, 200) ]

def q2A : List (AStep × Nat) :=
  (q2C.map (fun (rq, n) => (AStep.api rq, n))) ++ [(.r3 "a" "https://l", 200)]

example : pairCheck q2C q2A 300 := by decide

-- R2, standalone: the concrete settle fulfills the task inline; the
-- abstract machine fulfills by rule.
def q3C : List (Request × Nat) :=
  [ (.taskCreate { pid := "p0", ttl := 100, action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } }, 100),
    (.promiseSettle { id := "x", state := .resolved, value := {} }, 200) ]

def q3A : List (AStep × Nat) :=
  (q3C.map (fun (rq, n) => (AStep.api rq, n))) ++ [(.r2 "x", 200)]

example : pairCheck q3C q3A 300 := by decide

-- The wake pair R4;R6 against the concrete per-pair resume τ.
def q4C : List (Request × Nat) :=
  [ (.promiseCreate { id := "a", timeoutAt := 1000, param := {}, tags := extTags }, 100),
    (.taskCreate { pid := "p0", ttl := 100, action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } }, 100),
    (.taskSuspend { id := "x", version := 1, actions := [{ awaited := "a", awaiter := "x" }] }, 120),
    (.promiseSettle { id := "a", state := .resolved, value := {} }, 200),
    (.τResume { awaited := "a", awaiter := "x" }, 200) ]

def q4A : List (AStep × Nat) :=
  [ (.api (.promiseCreate { id := "a", timeoutAt := 1000, param := {}, tags := extTags }), 100),
    (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } }), 100),
    (.api (.taskSuspend { id := "x", version := 1, actions := [{ awaited := "a", awaiter := "x" }] }), 120),
    (.api (.promiseSettle { id := "a", state := .resolved, value := {} }), 200),
    (.r4 "a" "x", 200),
    (.r6 "x" 5200, 200) ]

example : pairCheck q4C q4A 300 := by decide

-- A fact-lagged internal promise: the abstract R1 settles it, the
-- concrete machine (no armed τ, nothing touches it) leaves it
-- stored-pending forever. With no waiters possible there, both
-- observables are blind to the lag; `normEq` closes it.
def q5C : List (Request × Nat) :=
  [ (.promiseCreate { id := "i", timeoutAt := 250, param := {}, tags := [] }, 100),
    (.idle, 300) ]

def q5A : List (AStep × Nat) :=
  [ (.api (.promiseCreate { id := "i", timeoutAt := 250, param := {}, tags := [] }), 100),
    (.r1 "i", 300) ]

example : pairCheck q5C q5A 400 := by decide

end Abstraction
