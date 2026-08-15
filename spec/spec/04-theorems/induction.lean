import «04-theorems».«system»
import «02-abstract».«properties»

/-!  # Inductive invariance

Is the state half of the catalogue closed under stepping? Each of the
45 `.state` properties either is preserved by every step — in which
case 1 464 sampled scripts become all scripts — or it is not, and the
failure names a strengthening the catalogue is missing.

## The lever

Writes are DATA here, which changes the shape of the work. A step is

    stepOf mat st now s = (_, applyAll s w)

for the effect list `w` the handler emitted. So a property that
survives every SINGLE write survives every step, whatever the handler
was, without a case split over the 28 steps at all:

    EffectStable P  →  ∀ mat st now s, P s → P (stepOf mat st now s).2

That is the strongest tier, and it costs five cases — one per `Effect`
constructor — instead of twenty-eight. Not every property reaches it:
`EffectStable` says the property survives an ARBITRARY write, which is
false for anything whose truth depends on what the handler chose to
write. Those need the weaker per-handler argument. Which tier a
property lands in is itself the interesting output. -/

namespace Abstraction
namespace Induction

open AbstractModel

/-- A property no single write can break. -/
def EffectStable (P : ServerState → Bool) : Prop :=
  ∀ (e : Effect) (s : ServerState), P s = true → P (e.apply s) = true

theorem applyAll_preserves {P : ServerState → Bool} (h : EffectStable P) :
    ∀ (w : List Effect) (s : ServerState), P s = true → P (applyAll s w) = true
  | [],      _, hs => hs
  | e :: es, s, hs => applyAll_preserves h es (e.apply s) (h e s hs)

/-- The lever: effect-stability gives preservation by every step of the
    machine, for both read disciplines, at every instant. -/
theorem step_preserves {P : ServerState → Bool} (h : EffectStable P)
    (mat : Bool) (st : Step) (now : Nat) (s : ServerState) :
    P s = true → P (stepOf mat st now s).2 = true := by
  intro hs
  show P (applyAll s ((handle st now) { state := s, mat := mat }).2) = true
  exact applyAll_preserves h _ s hs

/-! ## The per-object tier

`EffectStable` turns out to be a very small club. An effect carries a
WHOLE object, so an arbitrary `.setPromise` can break any property of
promises; only the structural ones (id uniqueness) survive an arbitrary
write, and those need `eraseDups` lemmas core does not ship.

The tier that actually covers the catalogue is one step weaker. Most
`.state` properties have the shape `s.promises.all q` — 36 of the 45
are `.all` over one table — and such a property survives a step exactly
when every object the step WRITES satisfies `q`. The state it started
from stops mattering: the surviving rows were already good, and the
written rows are the whole obligation.

That is the reduction worth having. It turns "is this property
inductive?" into "what does each handler write?", which is a question
about 28 handlers rather than about all states.

The 9 that are NOT per-object are exactly the two families that talk
about relationships rather than rows: the four `_ids_unique` /
`_keys_unique` properties, and the five `consistent_outbox_*` ones,
which relate an outbox entry to the task or promise it names. Those
need a different argument, and the outbox five are the harder half
because a write to `promises` can invalidate a claim about `outbox`
without touching it. -/

def PerPromise (q : PromiseObject → Bool) (s : ServerState) : Bool :=
  s.promises.all q

theorem all_filter {α} (q : α → Bool) (f : α → Bool) :
    ∀ l : List α, l.all q = true → (l.filter f).all q = true
  | [],     _ => rfl
  | a :: l, h => by
      simp only [List.all_cons, Bool.and_eq_true] at h
      by_cases hf : f a = true <;>
        simp [hf, List.all_cons, h.1, all_filter q f l h.2]

/-- One write preserves a per-promise property when the promise it
    writes satisfies it. Writes to the other tables cannot touch it. -/
theorem perPromise_apply (q : PromiseObject → Bool) (e : Effect) (s : ServerState)
    (hs : PerPromise q s = true)
    (he : ∀ p, e = .setPromise p → q p = true) :
    PerPromise q (e.apply s) = true := by
  cases e with
  | setPromise p =>
      have hq : q p = true := he p rfl
      simp [PerPromise, Effect.apply, List.all_cons, hq,
            all_filter q _ s.promises hs]
  | setTask _     => simpa [PerPromise, Effect.apply] using hs
  | setSchedule _ => simpa [PerPromise, Effect.apply] using hs
  | delSchedule _ => simpa [PerPromise, Effect.apply] using hs
  | setMessage _ _ => simpa [PerPromise, Effect.apply] using hs

theorem perPromise_applyAll (q : PromiseObject → Bool) :
    ∀ (w : List Effect) (s : ServerState), PerPromise q s = true →
      (∀ e ∈ w, ∀ p, e = .setPromise p → q p = true) →
      PerPromise q (applyAll s w) = true
  | [],      _, hs, _  => hs
  | e :: es, s, hs, hw =>
      perPromise_applyAll q es (e.apply s)
        (perPromise_apply q e s hs (hw e (by simp)))
        (fun f hf => hw f (by simp [hf]))

end Induction
end Abstraction
