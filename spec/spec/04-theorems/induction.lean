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

/-! ## Threading the hypothesis through a read

The cost centre. Almost every `setPromise` in the machine writes `f p`
for a `p` that came out of the state a moment earlier — `readPromise`
then `setSettled`, `getPromise` then `addCallback`, and so on. So the
per-write obligation is never "this promise is good" outright; it is
"this promise is good BECAUSE the one it was derived from was, and `f`
kept it good".

That splits cleanly, and the split is why this is one lemma rather than
thirty-six. `getPromise_sound` says a promise read out of the state
inherits the state's per-promise property — once, for all `q`. What is
left per property is a statement about the four transformations the
machine applies, and those are about `PromiseObject` alone: no monad,
no state, no handler. -/

theorem getPromise_sound {q : PromiseObject → Bool} {s : ServerState}
    {id : String} {p : PromiseObject}
    (hs : PerPromise q s = true)
    (h : s.promises.find? (·.id == id) = some p) : q p = true :=
  List.all_eq_true.mp hs p (List.mem_of_find?_eq_some h)

/-! ### The transformations

Four functions produce every promise the machine ever stores, and each
one is a statement about a `PromiseObject`. `project` is Fact P;
`addCallback` and `addListener` are the obligation ledgers; a settle
rewrites the verdict.

Stated for `created_at ≤ timeout_at`, the first property to go through
end to end. All four are `rfl`-shaped, because none of them touches
either field — which is itself the content: the birth fields are
immutable, and that is why the property is inductive. -/

def qCreatedLeTimeout (p : PromiseObject) : Bool := p.createdAt ≤ p.timeoutAt

theorem project_preserves_created_le_timeout (p : PromiseObject) (now : Nat) :
    qCreatedLeTimeout p = true → qCreatedLeTimeout (p.project now) = true := by
  intro h
  unfold PromiseObject.project
  split
  · split <;> simpa [qCreatedLeTimeout] using h
  · simpa [qCreatedLeTimeout] using h

theorem addCallback_preserves_created_le_timeout (p : PromiseObject) (a : String) :
    qCreatedLeTimeout p = true → qCreatedLeTimeout (p.addCallback a) = true := by
  intro h
  unfold PromiseObject.addCallback
  split <;> simpa [qCreatedLeTimeout] using h

theorem addListener_preserves_created_le_timeout (p : PromiseObject) (a : String) :
    qCreatedLeTimeout p = true → qCreatedLeTimeout (p.addListener a) = true := by
  intro h
  unfold PromiseObject.addListener
  split <;> simpa [qCreatedLeTimeout] using h

/-- A settle rewrites `state`, `value` and `settledAt` and nothing else,
    so the birth fields ride through untouched. -/
theorem settle_preserves_created_le_timeout
    (p : PromiseObject) (st : ServerModel.PromiseState)
    (v : ServerModel.Value) (t : Option Nat) :
    qCreatedLeTimeout p = true →
    qCreatedLeTimeout { p with state := st, value := v, settledAt := t } = true := by
  intro h; simpa [qCreatedLeTimeout] using h

/-- And the one promise the machine builds from nothing rather than
    from another promise: `createPromise`'s two birth shapes. Live,
    `createdAt := now < req.timeoutAt`; born dead, both fields are
    `req.timeoutAt` and the inequality is an equality. Neither needs a
    hypothesis, which is what makes the property inductive rather than
    merely true. -/
theorem birth_satisfies_created_le_timeout
    (req : ServerModel.PromiseCreateReq) (now : Nat) :
    qCreatedLeTimeout
      (if req.timeoutAt > now then
        { id := req.id, state := .pending, param := req.param, tags := req.tags,
          timeoutAt := req.timeoutAt, createdAt := now }
       else
        { id := req.id, state := if req.tags.isTimer then .resolved else .rejectedTimedout,
          param := req.param, tags := req.tags, timeoutAt := req.timeoutAt,
          createdAt := req.timeoutAt, settledAt := some req.timeoutAt }) = true := by
  split
  · rename_i h; simp [qCreatedLeTimeout]; omega
  · simp [qCreatedLeTimeout]

/-! ### The two ledger removals

Missed on the first pass, and the enumeration is what found them. The
internal drains do not go through `addCallback`/`addListener`'s
inverses — there are none — they rewrite the field directly:

    setPromise { p with listeners := p.listeners.filter (· != address) }
    setPromise { p with callbacks := p.callbacks.filter (· != awaiter) }

So nine functions produce stored promises, not four. -/

theorem removeListener_preserves_created_le_timeout
    (p : PromiseObject) (address : String) :
    qCreatedLeTimeout p = true →
    qCreatedLeTimeout { p with listeners := p.listeners.filter (· != address) } = true := by
  intro h; simpa [qCreatedLeTimeout] using h

theorem removeCallback_preserves_created_le_timeout
    (p : PromiseObject) (awaiter : String) :
    qCreatedLeTimeout p = true →
    qCreatedLeTimeout { p with callbacks := p.callbacks.filter (· != awaiter) } = true := by
  intro h; simpa [qCreatedLeTimeout] using h

/-- `task.create` does NOT call `createPromise`; it inlines its own
    birth, so there are two copies of promise birth in the machine and
    both need discharging. This copy has no timer branch, and that is
    correct rather than an omission: `task.create` 400s on
    `timerTargeted`, so a timer never reaches here and the born-dead
    verdict is `rejectedTimedout` with no `isTimer` case to answer for.
    The enumeration is what makes that reasoning checkable instead of a
    remark in a docstring. -/
theorem task_birth_satisfies_created_le_timeout
    (a : ServerModel.PromiseCreateReq) (now : Nat) :
    qCreatedLeTimeout
      (if a.timeoutAt > now then
        { id := a.id, state := .pending, param := a.param, tags := a.tags,
          timeoutAt := a.timeoutAt, createdAt := now }
       else
        { id := a.id, state := .rejectedTimedout, param := a.param, tags := a.tags,
          timeoutAt := a.timeoutAt, createdAt := a.timeoutAt,
          settledAt := some a.timeoutAt }) = true := by
  split
  · rename_i h; simp [qCreatedLeTimeout]; omega
  · simp [qCreatedLeTimeout]

/-! ## Making the enumeration checkable

The step above — "those nine are ALL the sites" — is the one claim that
was outside Lean, and it is the one most likely to rot: a handler added
next year writes a promise and nothing complains.

`WritesGood` closes it. It says every promise a computation writes is
good, and it composes, so a handler's obligation is built from its
parts rather than asserted about its whole. The reason it composes is
the reader discipline: `bind` hands the SAME environment to its
continuation, so `s` does not move under the binder and the predicate
is about one fixed state throughout. In a state monad this would not
factor — the continuation would run against a state the first half had
already changed, and there would be nothing to induct on. -/

def WritesGood (q : PromiseObject → Bool) (s : ServerState) (mat : Bool)
    {α : Type} (act : H α) : Prop :=
  ∀ e ∈ (act { state := s, mat := mat }).2, ∀ p, e = .setPromise p → q p = true

theorem writesGood_pure {α} (q : PromiseObject → Bool) (s : ServerState)
    (mat : Bool) (a : α) : WritesGood q s mat (pure a) := by
  intro e he; simp [pure] at he

theorem writesGood_bind {α β} (q : PromiseObject → Bool) (s : ServerState) (mat : Bool)
    (x : H α) (f : α → H β)
    (hx : WritesGood q s mat x) (hf : ∀ a, WritesGood q s mat (f a)) :
    WritesGood q s mat (x >>= f) := by
  intro e he p hp
  simp only [bind, List.mem_append] at he
  cases he with
  | inl h => exact hx e h p hp
  | inr h => exact hf _ e h p hp

theorem writesGood_setPromise (q : PromiseObject → Bool) (s : ServerState)
    (mat : Bool) (p : PromiseObject) (h : q p = true) :
    WritesGood q s mat (setPromise p) := by
  intro e he p' hp'
  simp [setPromise, emit] at he
  subst he; cases hp'; exact h

theorem writesGood_setTask (q : PromiseObject → Bool) (s : ServerState)
    (mat : Bool) (t : TaskObject) : WritesGood q s mat (setTask t) := by
  intro e he p hp; simp [setTask, emit] at he; subst he; cases hp

theorem writesGood_setMessage (q : PromiseObject → Bool) (s : ServerState)
    (mat : Bool) (a : String) (m : ServerModel.Message) :
    WritesGood q s mat (setMessage a m) := by
  intro e he p hp; simp [setMessage, emit] at he; subst he; cases hp

theorem writesGood_ask (q : PromiseObject → Bool) (s : ServerState) (mat : Bool) :
    WritesGood q s mat ask := by
  intro e he; simp [ask] at he

end Induction
end Abstraction
