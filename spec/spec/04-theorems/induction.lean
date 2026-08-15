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

set_option maxHeartbeats 400000

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

/-! ### What is left per property

Nine functions produce every promise the machine ever stores, and each
one is a statement about a `PromiseObject`: no monad, no state, no
handler. `project` is Fact P; `addCallback` and `addListener` are the
obligation ledgers, and the internal drains remove from them by
rewriting the field directly, since there are no inverses; a settle
rewrites the verdict; and two births — `createPromise`'s and the copy
`task.create` inlines — build a promise from nothing.

`Hereditary` below packages them. The instances live in
`entries.lean`. -/

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

/-- Every promise this computation writes is good. Indexed by the whole
    environment rather than by `(state, mat)`, because `withMat` moves
    `mat` and the internal steps use it. -/
def WritesGood (q : PromiseObject → Bool) (e : Env) {α : Type} (act : H α) : Prop :=
  ∀ f ∈ (act e).2, ∀ p, f = .setPromise p → q p = true

/-- And the companion: the promise it hands BACK is good. Needed
    because handlers write promises they have just read — the obligation
    is never about writes alone. -/
def ReturnsGood (q : PromiseObject → Bool) (e : Env)
    (act : H (Option PromiseObject)) : Prop :=
  ∀ p, (act e).1 = some p → q p = true

theorem writesGood_pure {α} (q : PromiseObject → Bool) (e : Env) (a : α) :
    WritesGood q e (pure a) := by
  intro f hf; simp [pure] at hf

theorem writesGood_bind {α β} (q : PromiseObject → Bool) (e : Env)
    (x : H α) (f : α → H β)
    (hx : WritesGood q e x) (hf : ∀ a, WritesGood q e (f a)) :
    WritesGood q e (x >>= f) := by
  intro g hg p hp
  simp only [bind, List.mem_append] at hg
  cases hg with
  | inl h => exact hx g h p hp
  | inr h => exact hf _ g h p hp

theorem writesGood_map {α β} (q : PromiseObject → Bool) (e : Env)
    (g : α → β) (x : H α) (hx : WritesGood q e x) : WritesGood q e (g <$> x) :=
  writesGood_bind q e x _ hx (fun a => writesGood_pure q e (g a))

theorem writesGood_setPromise (q : PromiseObject → Bool) (e : Env)
    (p : PromiseObject) (h : q p = true) : WritesGood q e (setPromise p) := by
  intro f hf p' hp'
  simp [setPromise, emit] at hf
  subst hf; cases hp'; exact h

theorem writesGood_setTask (q : PromiseObject → Bool) (e : Env) (t : TaskObject) :
    WritesGood q e (setTask t) := by
  intro f hf p hp; simp [setTask, emit] at hf; subst hf; cases hp

theorem writesGood_setMessage (q : PromiseObject → Bool) (e : Env)
    (a : String) (m : ServerModel.Message) : WritesGood q e (setMessage a m) := by
  intro f hf p hp; simp [setMessage, emit] at hf; subst hf; cases hp

theorem writesGood_setSchedule (q : PromiseObject → Bool) (e : Env)
    (c : ServerModel.Schedule) : WritesGood q e (setSchedule c) := by
  intro f hf p hp; simp [setSchedule, emit] at hf; subst hf; cases hp

theorem writesGood_delSchedule (q : PromiseObject → Bool) (e : Env) (id : String) :
    WritesGood q e (delSchedule id) := by
  intro f hf p hp; simp [delSchedule, emit] at hf; subst hf; cases hp

theorem writesGood_ask (q : PromiseObject → Bool) (e : Env) : WritesGood q e ask := by
  intro f hf; simp [ask] at hf

theorem writesGood_getPromise (q : PromiseObject → Bool) (e : Env) (id : String) :
    WritesGood q e (getPromise id) := by
  intro f hf; simp [getPromise, bind, ask, pure] at hf

theorem writesGood_getTask (q : PromiseObject → Bool) (e : Env) (id : String) :
    WritesGood q e (getTask id) := by
  intro f hf; simp [getTask, bind, ask, pure] at hf

theorem writesGood_getSchedule (q : PromiseObject → Bool) (e : Env) (id : String) :
    WritesGood q e (getSchedule id) := by
  intro f hf; simp [getSchedule, bind, ask, pure] at hf

/-- `withMat` changes which environment the body sees, and nothing else.
    The one place the reader indexing earns its keep. -/
theorem writesGood_withMat {α} (q : PromiseObject → Bool) (e : Env) (b : Bool)
    (act : H α) (h : WritesGood q { e with mat := b } act) :
    WritesGood q e (withMat b act) := h

theorem returnsGood_withMat (q : PromiseObject → Bool) (e : Env) (b : Bool)
    (act : H (Option PromiseObject)) (h : ReturnsGood q { e with mat := b } act) :
    ReturnsGood q e (withMat b act) := h

/-! ## The obligation set

`WritesGood` reduces "is this property inductive?" to "what does each
handler write?", and the answer is always: a promise built by one of
nine functions from a promise that was already in the state, or one of
two births built from nothing. `Hereditary` names exactly those eleven
obligations.

The payoff is that the 28-handler argument is made ONCE, generically in
`q`. A new property costs its eleven `PromiseObject` lemmas — no monad,
no handlers, no states — and a property that fails to be inductive
fails at a named transformation rather than in the middle of a case
split. -/

structure Hereditary (q : PromiseObject → Bool) : Prop where
  project      : ∀ (p : PromiseObject) (n : Nat), q p = true → q (p.project n) = true
  addCallback  : ∀ (p : PromiseObject) (a : String), q p = true → q (p.addCallback a) = true
  addListener  : ∀ (p : PromiseObject) (a : String), q p = true → q (p.addListener a) = true
  settle       : ∀ (p : PromiseObject) (st : ServerModel.PromiseState)
                   (v : ServerModel.Value) (t : Nat),
                   st.settable = true → q p = true →
                   q { p with state := st, value := v, settledAt := some t } = true
  dropListener : ∀ (p : PromiseObject) (a : String), q p = true →
                   q { p with listeners := p.listeners.filter (· != a) } = true
  dropCallback : ∀ (p : PromiseObject) (a : String), q p = true →
                   q { p with callbacks := p.callbacks.filter (· != a) } = true
  live         : ∀ (id : String) (param : ServerModel.Value) (tags : ServerModel.Tags)
                   (timeoutAt createdAt : Nat), createdAt < timeoutAt →
                   q { id := id, state := .pending, param := param, tags := tags,
                       timeoutAt := timeoutAt, createdAt := createdAt } = true
  dead         : ∀ (id : String) (st : ServerModel.PromiseState)
                   (param : ServerModel.Value) (tags : ServerModel.Tags) (timeoutAt : Nat),
                   st ≠ .pending →
                   q { id := id, state := st, param := param, tags := tags,
                       timeoutAt := timeoutAt, createdAt := timeoutAt,
                       settledAt := some timeoutAt } = true

/-! ## Reducing a transition

Two lemmas, and the reader discipline is visible in both: the value a
bind produces is the continuation's value at the SAME environment, and
the writes are the two halves concatenated. Nothing threads. -/

theorem pure_fst {α} (a : α) (e : Env) : ((pure a : H α) e).1 = a := rfl
theorem pure_snd {α} (a : α) (e : Env) : ((pure a : H α) e).2 = [] := rfl

theorem bind_fst {α β} (x : H α) (f : α → H β) (e : Env) :
    ((x >>= f) e).1 = (f (x e).1 e).1 := by
  simp only [bind]

theorem bind_snd {α β} (x : H α) (f : α → H β) (e : Env) :
    ((x >>= f) e).2 = (x e).2 ++ (f (x e).1 e).2 := by
  simp only [bind]

/-- The bind rule that keeps the connection to the state. `writesGood_bind`
    asks for the continuation at EVERY value; this asks for it only at the
    value the first half actually produced — which is the one that came out
    of `e.state`, and therefore the one `getPromise_sound` says something
    about. Without this the read hypothesis is lost at the first `←`. -/
theorem writesGood_bind' {α β} (q : PromiseObject → Bool) (e : Env)
    (x : H α) (f : α → H β)
    (hx : WritesGood q e x) (hf : WritesGood q e (f (x e).1)) :
    WritesGood q e (x >>= f) := by
  intro g hg p hp
  rw [bind_snd, List.mem_append] at hg
  cases hg with
  | inl h => exact hx g h p hp
  | inr h => exact hf g h p hp

/-- `if guard then return e` leaves a join point behind: the rest of the
    handler becomes `pure () >>= jp`. This steps over it. -/
theorem writesGood_pureBind {α β} (q : PromiseObject → Bool) (e : Env)
    (a : α) (f : α → H β) (h : WritesGood q e (f a)) :
    WritesGood q e (pure a >>= f) := by
  intro g hg p hp
  rw [bind_snd] at hg
  simp only [pure_snd, pure_fst, List.nil_append] at hg
  exact h g hg p hp

/-- Targeted `if`. `split` picks whatever match it finds first, which
    inside a handler is usually the innermost one; this addresses the
    head. -/
theorem writesGood_ite {α} (q : PromiseObject → Bool) (e : Env)
    (c : Prop) [Decidable c] (x y : H α)
    (hx : WritesGood q e x) (hy : WritesGood q e y) :
    WritesGood q e (if c then x else y) := by
  by_cases h : c
  · simpa only [if_pos h] using hx
  · simpa only [if_neg h] using hy

/-- The same, keeping the branch condition. Needed only where a branch
    is good BECAUSE of the test — the two births, where `now <
    timeoutAt` is what `Hereditary.live` asks for. -/
theorem writesGood_iteH {α} (q : PromiseObject → Bool) (e : Env)
    (c : Prop) [Decidable c] (x y : H α)
    (hx : c → WritesGood q e x) (hy : ¬c → WritesGood q e y) :
    WritesGood q e (if c then x else y) := by
  by_cases h : c
  · simpa only [if_pos h] using hx h
  · simpa only [if_neg h] using hy h

theorem getPromise_fst (id : String) (e : Env) :
    (getPromise id e).1 = e.state.promises.find? (·.id == id) := rfl

theorem getTask_fst (id : String) (e : Env) :
    (getTask id e).1 = e.state.tasks.find? (·.id == id) := rfl

theorem ask_fst (e : Env) : (ask e).1 = e := rfl

/-! ## The derived reads and writes

Each of these is used by many handlers, so each is proved once. The
shape is always the same: split on what came out of the state, and in
the `some` branch pull `q` across with `getPromise_sound` and one
`Hereditary` field. -/

section Derived

variable {q : PromiseObject → Bool}

theorem writesGood_setSettled (e : Env) (p : PromiseObject) (h : q p = true) :
    WritesGood q e (setSettled p) := by
  unfold setSettled
  refine writesGood_bind' _ _ _ _ (writesGood_setPromise _ _ _ h) ?_
  split
  · refine writesGood_bind' _ _ _ _ (writesGood_getTask _ _ _) ?_
    split
    · split
      · exact writesGood_setTask _ _ _
      · exact writesGood_pure _ _ _
    · exact writesGood_pure _ _ _
  · exact writesGood_pure _ _ _

/-- What `readPromise` hands back: the stored promise, projected. Fact P
    as an equation. -/
theorem readPromise_fst (id : String) (now : Nat) (e : Env) :
    (readPromise id now e).1 =
      (e.state.promises.find? (·.id == id)).map (·.project now) := by
  unfold readPromise
  rw [bind_fst, getPromise_fst]
  cases h : e.state.promises.find? (·.id == id) with
  | none => rfl
  | some p => rw [bind_fst]; split <;> rw [bind_fst] <;> rfl

theorem writesGood_readPromise (hq : Hereditary q) {e : Env}
    (hs : PerPromise q e.state = true) (id : String) (now : Nat) :
    WritesGood q e (readPromise id now) := by
  unfold readPromise
  refine writesGood_bind' _ _ _ _ (writesGood_getPromise _ _ _) ?_
  rw [getPromise_fst]
  split
  · exact writesGood_pure _ _ _
  · rename_i p₀ h
    have hp₀ : q p₀ = true := getPromise_sound hs h
    refine writesGood_bind' _ _ _ _ (writesGood_ask _ _) ?_
    split
    · exact writesGood_bind' _ _ _ _
        (writesGood_setSettled _ _ (hq.project p₀ now hp₀)) (writesGood_pure _ _ _)
    · exact writesGood_bind' _ _ _ _ (writesGood_pure _ _ _) (writesGood_pure _ _ _)

theorem returnsGood_readPromise (hq : Hereditary q) {e : Env}
    (hs : PerPromise q e.state = true) (id : String) (now : Nat) :
    ReturnsGood q e (readPromise id now) := by
  intro p hp
  rw [readPromise_fst] at hp
  cases h : e.state.promises.find? (fun x => x.id == id) with
  | none => rw [h] at hp; simp at hp
  | some p₀ =>
      rw [h] at hp
      simp only [Option.map_some] at hp
      obtain rfl := Option.some.inj hp
      exact hq.project p₀ now (getPromise_sound hs h)

/-- The combinator the handlers are actually written against: after a
    read, either there was nothing, or there was a promise AND it is
    good. This is where the reader discipline pays — `writesGood_bind'`
    keeps the continuation pinned to the value the read produced, so the
    `q p` fact survives the `←`. -/
theorem writesGood_afterReadPromise {α} (hq : Hereditary q) {e : Env}
    (hs : PerPromise q e.state = true) (id : String) (now : Nat)
    (f : Option PromiseObject → H α)
    (hnone : WritesGood q e (f none))
    (hsome : ∀ p, q p = true → WritesGood q e (f (some p))) :
    WritesGood q e (readPromise id now >>= f) := by
  refine writesGood_bind' _ _ _ _ (writesGood_readPromise hq hs id now) ?_
  cases h : (readPromise id now e).1 with
  | none => exact hnone
  | some p => exact hsome p (returnsGood_readPromise hq hs id now p h)

/-- Same, through `withMat` — which is how the internal steps read.
    `touchPromise` is `withMat true`, `viewPromise` is `withMat false`. -/
theorem writesGood_afterMatReadPromise {α} (hq : Hereditary q) {e : Env} (b : Bool)
    (hs : PerPromise q e.state = true) (id : String) (now : Nat)
    (f : Option PromiseObject → H α)
    (hnone : WritesGood q e (f none))
    (hsome : ∀ p, q p = true → WritesGood q e (f (some p))) :
    WritesGood q e (withMat b (readPromise id now) >>= f) := by
  refine writesGood_bind' _ _ _ _
    (writesGood_readPromise (e := { e with mat := b }) hq hs id now) ?_
  show WritesGood q e (f ((readPromise id now { e with mat := b }).1))
  cases h : (readPromise id now { e with mat := b }).1 with
  | none => exact hnone
  | some p =>
      exact hsome p (returnsGood_readPromise (e := { e with mat := b }) hq hs id now p h)

theorem writesGood_readTask (hq : Hereditary q) {e : Env}
    (hs : PerPromise q e.state = true) (id : String) (now : Nat) :
    WritesGood q e (readTask id now) := by
  unfold readTask
  refine writesGood_bind' _ _ _ _ (writesGood_getTask _ _ _) ?_
  rw [getTask_fst]
  split
  · exact writesGood_pure _ _ _
  · refine writesGood_bind' _ _ _ _ (writesGood_readPromise hq hs _ _) ?_
    split
    · exact writesGood_pure _ _ _
    · refine writesGood_bind' _ _ _ _ (writesGood_ask _ _) ?_
      split
      · exact writesGood_bind' _ _ _ _ (writesGood_setTask _ _ _) (writesGood_pure _ _ _)
      · exact writesGood_bind' _ _ _ _ (writesGood_pure _ _ _) (writesGood_pure _ _ _)

/-- The promise `readTask` hands back is one `readPromise` handed back,
    so it inherits `ReturnsGood` rather than needing its own argument. -/
theorem readTask_promise (id : String) (now : Nat) (e : Env)
    (t : TaskObject) (p : PromiseObject) :
    (readTask id now e).1 = some (t, some p) →
    ∃ i, (readPromise i now e).1 = some p := by
  unfold readTask
  rw [bind_fst, getTask_fst]
  cases h : e.state.tasks.find? (·.id == id) with
  | none => intro hh; simp [pure] at hh
  | some t₀ =>
      rw [bind_fst]
      cases hr : (readPromise t₀.id now e).1 with
      | none => intro hh; simp [pure] at hh
      | some p₀ =>
          refine fun hh => ⟨t₀.id, ?_⟩
          rw [hr]
          revert hh
          rw [bind_fst]
          split <;> · rw [bind_fst]; intro hh; simp [pure] at hh; rw [hh.2]

theorem writesGood_afterReadTask {α} (hq : Hereditary q) {e : Env}
    (hs : PerPromise q e.state = true) (id : String) (now : Nat)
    (f : Option (TaskObject × Option PromiseObject) → H α)
    (hnone : WritesGood q e (f none))
    (hbare : ∀ t, WritesGood q e (f (some (t, none))))
    (hsome : ∀ t p, q p = true → WritesGood q e (f (some (t, some p)))) :
    WritesGood q e (readTask id now >>= f) := by
  refine writesGood_bind' _ _ _ _ (writesGood_readTask hq hs id now) ?_
  cases h : (readTask id now e).1 with
  | none => exact hnone
  | some tp =>
      obtain ⟨t, po⟩ := tp
      cases po with
      | none => exact hbare t
      | some p =>
          obtain ⟨i, hi⟩ := readTask_promise id now e t p h
          exact hsome t p (returnsGood_readPromise hq hs i now p hi)

theorem writesGood_afterTouchTask {α} (hq : Hereditary q) {e : Env}
    (hs : PerPromise q e.state = true) (id : String) (now : Nat)
    (f : Option (TaskObject × Option PromiseObject) → H α)
    (hnone : WritesGood q e (f none))
    (hbare : ∀ t, WritesGood q e (f (some (t, none))))
    (hsome : ∀ t p, q p = true → WritesGood q e (f (some (t, some p)))) :
    WritesGood q e (withMat true (readTask id now) >>= f) := by
  refine writesGood_bind' _ _ _ _
    (writesGood_readTask (e := { e with mat := true }) hq hs id now) ?_
  show WritesGood q e (f ((readTask id now { e with mat := true }).1))
  cases h : (readTask id now { e with mat := true }).1 with
  | none => exact hnone
  | some tp =>
      obtain ⟨t, po⟩ := tp
      cases po with
      | none => exact hbare t
      | some p =>
          obtain ⟨i, hi⟩ := readTask_promise id now { e with mat := true } t p h
          exact hsome t p
            (returnsGood_readPromise (e := { e with mat := true }) hq hs i now p hi)

end Derived

/-! ## The goal

Everything above is scaffolding for this. `Legal` says the catalogue
holds along a trace; `valid_implies_legal` says every run of the machine
is Legal. That is the statement the whole conformance story rests on —
not because it constrains an implementation (it does not: it mentions
no implementation) but because it is what makes a `Legal` violation
UNAMBIGUOUS when an implementation is checked against the catalogue.
Without it, a failing property leaves two live hypotheses: the
implementation is wrong, or the property is. With it, the failure is a
bug report.

Stated with `sorry`, so the build names it on every run. What backs it
today is `stage1_sweep` and `stage3_sweep` — 1 464 scripts, both
readings, `decide` at build time — which is a finite fact and not this.

Note the second hypothesis. `Valid` alone is not enough: a trace that
starts in an arbitrary state and steps correctly from there satisfies
`Valid` and can violate anything. Reachability is what the catalogue is
about, and `(tr 0).state = init` is where it enters. -/

/-! ### What it decomposes into

Four obligations, and only the second needs an induction. Each is a
statement about ONE step applied to ONE state, and the second is where a
missing strengthening would surface as a failure. -/

/-- The empty store satisfies the catalogue. Proved, not assumed: every
    `.state` entry is an `.all` over a list that is empty at `init`. -/
theorem stateHolds_init (now : Nat) :
    Properties.stateHolds now ServerState.init = true := rfl

/-- **The induction.** Every step preserves the `.state` half. -/
theorem stateHolds_step (mat : Bool) (st : Step) (now : Nat) (s : ServerState) :
    Properties.stateHolds now s = true →
    Properties.stateHolds now (stepOf mat st now s).2 = true := sorry

/-- **The clock.** A state satisfying the `.state` half at one instant
    satisfies it at every later one. Needed because a step lands a state
    at `(tr t).now` and the trace checks it again at `(tr (t+1)).now`.
    Not bookkeeping: `consistent_suspended_task_holds_rung` reads
    `project now`, so advancing changes what it says about a state that
    did not move. -/
theorem stateHolds_clock (n n' : Nat) (s : ServerState) :
    Properties.stateHolds n s = true → n ≤ n' →
    Properties.stateHolds n' s = true := sorry

/-- **The whole fold at one step**, which is `Legal`'s body. Given the
    `.state` half at the pre-state, every entry — both kinds — holds of
    the step. The `.trans` half needs no induction: it is a claim about
    an arbitrary state and the step out of it. -/
theorem legalAt_step (mat : Bool) (st : Step) (now : Nat) (s : ServerState) :
    Properties.stateHolds now s = true →
    Properties.legalAt now s (stepOf mat st now s).2 = true := sorry

/-- **Provenance**, and NOT part of `Legal`. The sweeper properties hold
    on internal steps only: a background job may re-pend, fulfil, resume
    or refresh, and may never acquire, suspend, halt or continue a task.
    The guard is necessary rather than cosmetic —
    `internal_laws_are_strictly_stronger` proves by `decide` that some
    request step in the corpus violates them, which is what makes them a
    constraint rather than a restatement of the general edge tables. A
    `task.acquire` is the witness: `legalAt` accepts it,
    `internalWellFormed` refuses it. -/
theorem internal_well_formed (mat : Bool) (st : Step) (now : Nat) (s : ServerState) :
    st.isInternal = true → Properties.stateHolds now s = true →
    Properties.internalWellFormed now s (stepOf mat st now s).2 = true := sorry

end Induction
end Abstraction
