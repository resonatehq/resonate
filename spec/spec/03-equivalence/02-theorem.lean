import «03-equivalence».«01-trace»

/-!  # Equivalence — the statement

What we want to say, derived from the observer:

The world interacts with the machine through two channels — it sends
requests and reads responses (synchronous), and it eventually receives
the messages the machine emits (asynchronous). Internal transitions are
the machine's own business: their scheduling is not part of the
contract, and — decisively — it CANNOT be shared between the machines,
because the obligation records that enable a τ are moved by -m's
touches. Holding the τ schedule fixed across both machines makes them
distinguishable (`03-lockstep.lean` is the machine-checked witness).
The honest claim is therefore the textbook one:

> **The twin machines are weakly bisimilar.** For every valid trace of
> one machine there is a valid trace of the other, from the same start,
> with the same externalized behavior — the same external requests, at
> the same instants, with the same responses, in the same order —
> internal steps silent, each machine scheduling its own. And at
> matching quiescent points the machines have emitted the same
> messages.

Monotonicity of the clock is not a separate hypothesis here: it is the
third conjunct of `Valid`.

The simulations are constructive, in both directions:

  * **-p simulates -m** by running eagerly: before each step whose
    touch materialized something in -m, insert the corresponding τ
    (same instant — the clock conjunct is `≤`). The projection then
    serves byte-identically what the materialized read serves.
  * **-m simulates -p** by running lazily: replay the trace, replacing
    each τ that no-opped in -p with `idle`. A τ that acted in -p either
    acts identically in -m or is a no-op because -m's touch already
    discharged it — silent either way.

`04-simulation.lean` executes the second construction; the first needs
genuine step insertion and is exercised there through the re-indexing
in the statement. The full proof of `Indistinguishable` is the open
obligation; its shape is an induction over the trace with the invariant
`stateEq (quiesced now sP) (quiesced now sM)` and one commutation lemma
per handler.  -/

namespace Equivalence

/-- `tr'` exhibits the same externalized behavior as `tr`: a strictly
    monotone re-indexing φ carries every external step of `tr` to a
    step of `tr'` with the same request, instant, and response — and
    every external step of `tr'` is reached this way, so neither trace
    has external steps the other lacks. Internal steps are unconstrained:
    they may differ, move, multiply, or vanish. -/
def SameObservation (tr tr' : Trace) : Prop :=
  ∃ φ : Nat → Nat,
    (∀ s t, s < t → φ s < φ t) ∧
    (∀ t, (tr t).req.isExternal = true →
      (tr' (φ t)).req = (tr t).req ∧
      (tr' (φ t)).now = (tr t).now ∧
      (tr' (φ t)).res = (tr t).res) ∧
    (∀ s, (tr' s).req.isExternal = true →
      ∃ t, (tr t).req.isExternal = true ∧ φ t = s)

/-- The asynchronous channel: once both traces have gone quiet at the
    same instant, the settlement pipeline discharged, the machines must
    have emitted the same messages. (Timing within the quiet period is
    not observable — delivery is asynchronous by contract.) -/
def SameMessages (tr tr' : Trace) : Prop :=
  ∀ N N',
    (∀ t, N ≤ t → (tr t).req = .idle) →
    (∀ t, N' ≤ t → (tr' t).req = .idle) →
    (tr N).now = (tr' N').now →
    eqSet (quiesced (tr N).now (tr N).state).outbox
          (quiesced (tr' N').now (tr' N').state).outbox = true

/-- **THE CLAIM: the projected and the materialized machine are weakly
    bisimilar.** Every behavior of one is a behavior of the other, up
    to the machine's own internal scheduling. -/
def Indistinguishable : Prop :=
  (∀ tr, ValidP tr → (tr 0).state = ServerModel.ServerState.init →
     ∃ tr', ValidM tr' ∧ (tr' 0).state = ServerModel.ServerState.init ∧
       SameObservation tr tr' ∧ SameMessages tr tr') ∧
  (∀ tr, ValidM tr → (tr 0).state = ServerModel.ServerState.init →
     ∃ tr', ValidP tr' ∧ (tr' 0).state = ServerModel.ServerState.init ∧
       SameObservation tr tr' ∧ SameMessages tr tr')

end Equivalence
