import «04-theorems».«system»
import «04-theorems».«lookup»
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

/-- The three row predicates a step has to respect, bundled. Bundling
    rather than three parameters because every lemma below would
    otherwise carry three, and because the handler discharge is one
    argument about all three tables rather than three arguments about
    one each. -/
structure Q where
  promise  : PromiseObject → Bool
  task     : TaskObject → Bool
  schedule : ServerModel.Schedule → Bool

def PerStore (g : Q) (s : ServerState) : Bool :=
  s.promises.all g.promise && s.tasks.all g.task && s.schedules.all g.schedule

theorem perStore_promises {g : Q} {s : ServerState} (h : PerStore g s = true) :
    s.promises.all g.promise = true := by
  simp only [PerStore, Bool.and_eq_true] at h; exact h.1.1

theorem perStore_tasks {g : Q} {s : ServerState} (h : PerStore g s = true) :
    s.tasks.all g.task = true := by
  simp only [PerStore, Bool.and_eq_true] at h; exact h.1.2

theorem perStore_schedules {g : Q} {s : ServerState} (h : PerStore g s = true) :
    s.schedules.all g.schedule = true := by
  simp only [PerStore, Bool.and_eq_true] at h; exact h.2

theorem perStore_mk {g : Q} {s : ServerState}
    (h1 : s.promises.all g.promise = true) (h2 : s.tasks.all g.task = true)
    (h3 : s.schedules.all g.schedule = true) : PerStore g s = true := by
  simp [PerStore, h1, h2, h3]

/-- What one effect has to satisfy. `delSchedule` and `setMessage` write
    no row into a table this tier is about, so they are unconstrained —
    the outbox is a different argument. -/
def GoodEffect (g : Q) : Effect → Prop
  | .setPromise p   => g.promise p = true
  | .setTask t      => g.task t = true
  | .setSchedule c  => g.schedule c = true
  | .delSchedule _  => True
  | .setMessage _ _ => True

theorem all_filter {α} (q : α → Bool) (f : α → Bool) :
    ∀ l : List α, l.all q = true → (l.filter f).all q = true
  | [],     _ => rfl
  | a :: l, h => by
      simp only [List.all_cons, Bool.and_eq_true] at h
      by_cases hf : f a = true <;>
        simp [hf, List.all_cons, h.1, all_filter q f l h.2]

theorem all_upsert {α} (q : α → Bool) (x : α) (f : α → Bool) (l : List α)
    (hl : l.all q = true) (hx : q x = true) : ((x :: l.filter f).all q) = true := by
  simp [List.all_cons, hx, all_filter q f l hl]

/-- One write preserves the store predicate when the row it writes
    satisfies it. Writes to the other tables cannot touch the rest. -/
theorem perStore_apply (g : Q) (f : Effect) (s : ServerState)
    (hs : PerStore g s = true) (hf : GoodEffect g f) :
    PerStore g (f.apply s) = true := by
  have h1 := perStore_promises hs
  have h2 := perStore_tasks hs
  have h3 := perStore_schedules hs
  cases f with
  | setPromise p =>
      exact perStore_mk (all_upsert _ p _ _ h1 hf) h2 h3
  | setTask t =>
      exact perStore_mk h1 (all_upsert _ t _ _ h2 hf) h3
  | setSchedule c =>
      exact perStore_mk h1 h2 (all_upsert _ c _ _ h3 hf)
  | delSchedule i =>
      exact perStore_mk h1 h2 (all_filter _ _ _ h3)
  | setMessage a m =>
      exact perStore_mk h1 h2 h3

theorem perStore_applyAll (g : Q) :
    ∀ (w : List Effect) (s : ServerState), PerStore g s = true →
      (∀ f ∈ w, GoodEffect g f) → PerStore g (applyAll s w) = true
  | [],      _, hs, _  => hs
  | f :: fs, s, hs, hw =>
      perStore_applyAll g fs (f.apply s)
        (perStore_apply g f s hs (hw f (by simp)))
        (fun k hk => hw k (by simp [hk]))

/-! ## Threading the hypothesis through a read

The cost centre. Almost every write in the machine writes `f x` for an
`x` that came out of the state a moment earlier — `readPromise` then
`setSettled`, `getTask` then `t.fulfill`, and so on. So the per-write
obligation is never "this row is good" outright; it is "this row is
good BECAUSE the one it was derived from was, and `f` kept it good".

That splits cleanly. `getPromise_sound` and `getTask_sound` say a row
read out of the state inherits the store's predicate — once, for all
`g`. What is left per property is a statement about the transformations
the machine applies, and those are about one object: no monad, no
state, no handler. -/

/-- Weakening a row predicate. Used where an entry is inductive only
    relative to a strengthening: prove the stronger predicate, then
    project onto the catalogue's. -/
theorem all_mono {α} {q r : α → Bool} (h : ∀ x, q x = true → r x = true)
    (l : List α) : l.all q = true → l.all r = true := fun hs =>
  List.all_eq_true.mpr (fun x hx => h x (List.all_eq_true.mp hs x hx))

theorem getPromise_sound {g : Q} {s : ServerState} {id : String} {p : PromiseObject}
    (hs : PerStore g s = true)
    (h : s.promises.find? (·.id == id) = some p) : g.promise p = true :=
  List.all_eq_true.mp (perStore_promises hs) p (List.mem_of_find?_eq_some h)

theorem getTask_sound {g : Q} {s : ServerState} {id : String} {t : TaskObject}
    (hs : PerStore g s = true)
    (h : s.tasks.find? (·.id == id) = some t) : g.task t = true :=
  List.all_eq_true.mp (perStore_tasks hs) t (List.mem_of_find?_eq_some h)

theorem getSchedule_sound {g : Q} {s : ServerState} {id : String}
    {c : ServerModel.Schedule} (hs : PerStore g s = true)
    (h : s.schedules.find? (·.id == id) = some c) : g.schedule c = true :=
  List.all_eq_true.mp (perStore_schedules hs) c (List.mem_of_find?_eq_some h)

/-! ## Making the enumeration checkable

The claim "those are ALL the write sites" is the one that was outside
Lean, and the one most likely to rot: a handler added next year writes
a row and nothing complains.

`WritesGood` closes it. It says every row a computation writes is good,
and it composes, so a handler's obligation is built from its parts
rather than asserted about its whole. The reason it composes is the
reader discipline: `bind` hands the SAME environment to its
continuation, so `s` does not move under the binder and the predicate
is about one fixed state throughout. In a state monad this would not
factor — the continuation would run against a state the first half had
already changed, and there would be nothing to induct on. -/

def WritesGood (g : Q) (e : Env) {α : Type} (act : H α) : Prop :=
  ∀ f ∈ (act e).2, GoodEffect g f

/-- The companion: the promise it hands BACK is good. Needed because
    handlers write rows they have just read. -/
def ReturnsGood (g : Q) (e : Env) (act : H (Option PromiseObject)) : Prop :=
  ∀ p, (act e).1 = some p → g.promise p = true

theorem writesGood_pure {α} (g : Q) (e : Env) (a : α) : WritesGood g e (pure a) := by
  intro f hf; simp [pure] at hf

theorem writesGood_setPromise (g : Q) (e : Env) (p : PromiseObject)
    (h : g.promise p = true) : WritesGood g e (setPromise p) := by
  intro f hf; simp [setPromise, emit] at hf; subst hf; exact h

theorem writesGood_setTask (g : Q) (e : Env) (t : TaskObject)
    (h : g.task t = true) : WritesGood g e (setTask t) := by
  intro f hf; simp [setTask, emit] at hf; subst hf; exact h

theorem writesGood_setSchedule (g : Q) (e : Env) (c : ServerModel.Schedule)
    (h : g.schedule c = true) : WritesGood g e (setSchedule c) := by
  intro f hf; simp [setSchedule, emit] at hf; subst hf; exact h

theorem writesGood_setMessage (g : Q) (e : Env) (a : String) (m : ServerModel.Message) :
    WritesGood g e (setMessage a m) := by
  intro f hf; simp [setMessage, emit] at hf; subst hf; trivial

theorem writesGood_delSchedule (g : Q) (e : Env) (id : String) :
    WritesGood g e (delSchedule id) := by
  intro f hf; simp [delSchedule, emit] at hf; subst hf; trivial

theorem writesGood_ask (g : Q) (e : Env) : WritesGood g e ask := by
  intro f hf; simp [ask] at hf

theorem writesGood_getPromise (g : Q) (e : Env) (id : String) :
    WritesGood g e (getPromise id) := by
  intro f hf; simp [getPromise, bind, ask, pure] at hf

theorem writesGood_getTask (g : Q) (e : Env) (id : String) :
    WritesGood g e (getTask id) := by
  intro f hf; simp [getTask, bind, ask, pure] at hf

theorem writesGood_getSchedule (g : Q) (e : Env) (id : String) :
    WritesGood g e (getSchedule id) := by
  intro f hf; simp [getSchedule, bind, ask, pure] at hf

/-- `withMat` changes which environment the body sees, and nothing else.
    The one place the reader indexing earns its keep. -/
theorem writesGood_withMat {α} (g : Q) (e : Env) (b : Bool)
    (act : H α) (h : WritesGood g { e with mat := b } act) :
    WritesGood g e (withMat b act) := h

/-! ## The obligation set

`WritesGood` reduces "is this property inductive?" to "what does each
handler write?", and the answer is always: a row built by one of a
fixed list of functions from a row that was already in the store, or
one of the births built from nothing. `Hereditary` names exactly those.

The payoff is that the 28-handler argument is made ONCE, generically in
`g`. A new property costs its obligations — no monad, no handlers, no
states — and a property that fails to be inductive fails at a NAMED
obligation rather than in the middle of a case split.

Several obligations carry a guard. Those are not conveniences: they are
the prechecks the handler performs, and they are what makes a precheck
a state invariant. `tRearm` may only re-arm a PENDING task; delete the
`t.state == .pending` check in the retry step and this is the
obligation that fails. -/

structure Hereditary (g : Q) (a : ServerState) : Prop where
  -- promises
  project      : ∀ (p : PromiseObject) (n : Nat),
                   g.promise p = true → g.promise (p.project n) = true
  addCallback  : ∀ (p : PromiseObject) (a : String),
                   g.promise p = true → g.promise (p.addCallback a) = true
  addListener  : ∀ (p : PromiseObject) (a : String),
                   g.promise p = true → g.promise (p.addListener a) = true
  settle       : ∀ (p : PromiseObject) (st : ServerModel.PromiseState)
                   (v : ServerModel.Value) (t : Nat),
                   st.settable = true → p.state = .pending → t < p.timeoutAt →
                   g.promise p = true →
                   g.promise { p with state := st, value := v, settledAt := some t } = true
  dropListener : ∀ (p : PromiseObject) (a : String), g.promise p = true →
                   g.promise { p with listeners := p.listeners.filter (· != a) } = true
  dropCallback : ∀ (p : PromiseObject) (a : String), g.promise p = true →
                   g.promise { p with callbacks := p.callbacks.filter (· != a) } = true
  live         : ∀ (id : String) (param : ServerModel.Value) (tags : ServerModel.Tags)
                   (timeoutAt createdAt : Nat), createdAt < timeoutAt →
                   a.promises.find? (·.id == id) = none →
                   g.promise { id := id, state := .pending, param := param, tags := tags,
                               timeoutAt := timeoutAt, createdAt := createdAt } = true
  dead         : ∀ (id : String) (st : ServerModel.PromiseState)
                   (param : ServerModel.Value) (tags : ServerModel.Tags) (timeoutAt : Nat),
                   st = (if tags.isTimer then .resolved else .rejectedTimedout) →
                   a.promises.find? (·.id == id) = none →
                   g.promise { id := id, state := st, param := param, tags := tags,
                               timeoutAt := timeoutAt, createdAt := timeoutAt,
                               settledAt := some timeoutAt } = true
  -- tasks
  tFulfill     : ∀ (t : TaskObject), g.task t = true → g.task t.fulfill = true
  tBornPending : ∀ (id : String) (due : Nat),
                   g.task { id := id, state := .pending, version := 0,
                            retryAt := some due } = true
  tBornDone    : ∀ (id : String), g.task { id := id, state := .fulfilled, version := 0 } = true
  tBornHeld    : ∀ (id pid : String) (ttl now : Nat),
                   g.task { id := id, state := .acquired, version := 1, ttl := some ttl,
                            pid := some pid, expiresAt := some (now + ttl) } = true
  tAcquire     : ∀ (t : TaskObject) (pid : String) (ttl now : Nat), g.task t = true →
                   g.task { t with state := .acquired, version := t.version + 1, ttl := some ttl, pid := some pid, expiresAt := some (now + ttl), retryAt := none, resumes := [] } = true
  tHeartbeat   : ∀ (t : TaskObject) (x : Nat), (t.state == .acquired) = true →
                   g.task t = true → g.task { t with expiresAt := some x } = true
  tClearResumes : ∀ (t : TaskObject), g.task t = true →
                   g.task { t with resumes := [] } = true
  tSuspend     : ∀ (t : TaskObject), g.task t = true →
                   g.task { t with state := .suspended, pid := none, ttl := none, expiresAt := none, retryAt := none, resumes := [] } = true
  tRepend      : ∀ (t : TaskObject) (n : Nat), g.task t = true →
                   g.task { t with state := .pending, pid := none, ttl := none, expiresAt := none, retryAt := some n } = true
  tHalt        : ∀ (t : TaskObject), g.task t = true →
                   g.task { t with state := .halted, pid := none, ttl := none, expiresAt := none, retryAt := none } = true
  tContinue    : ∀ (t : TaskObject) (n : Nat), (t.state == .halted) = true →
                   g.task t = true →
                   g.task { t with state := .pending, retryAt := some n } = true
  tResume      : ∀ (t : TaskObject) (a : String) (n : Nat), t.state = .suspended →
                   g.task t = true →
                   g.task { t with state := .pending, resumes := [a], retryAt := some n } = true
  tAddResume   : ∀ (t : TaskObject) (a : String), t.state ≠ .suspended →
                   t.state ≠ .fulfilled → (t.resumes.contains a) = false → g.task t = true →
                   g.task { t with resumes := t.resumes ++ [a] } = true
  tRearm       : ∀ (t : TaskObject) (n : Nat), (t.state == .pending) = true →
                   g.task t = true → g.task { t with retryAt := some n } = true
  -- schedules
  cBorn        : ∀ (id cron promiseId : String) (promiseTimeout : Nat)
                   (promiseParam : ServerModel.Value) (promiseTags : ServerModel.Tags)
                   (now : Nat), promiseTags.timerTargeted = false →
                   g.schedule { id := id, cron := cron, promiseId := promiseId,
                                promiseTimeout := promiseTimeout,
                                promiseParam := promiseParam, promiseTags := promiseTags,
                                createdAt := now,
                                nextRunAt := ServerModel.nextCron cron now,
                                lastRunAt := none } = true
  cAdvance     : ∀ (c : ServerModel.Schedule) (last : Nat), g.schedule c = true →
                   g.schedule { c with lastRunAt := some last, nextRunAt := ServerModel.nextCron c.cron last } = true

/-- `TaskObject.view` is `fulfill` behind a test, so it is derived
    rather than assumed. -/
theorem Hereditary.tView {g : Q} {a : ServerState} (h : Hereditary g a) (t : TaskObject) (p : PromiseObject) :
    g.task t = true → g.task (t.view p) = true := by
  intro ht
  unfold TaskObject.view
  split
  · exact h.tFulfill t ht
  · exact ht

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

/-- The bind rule that keeps the connection to the state: it asks for
    the continuation only at the value the first half actually produced
    — which is the one that came out of `e.state`, and therefore the one
    `getPromise_sound` says something about. Without this the read
    hypothesis is lost at the first `←`. -/
theorem writesGood_bind' {α β} (g : Q) (e : Env) (x : H α) (f : α → H β)
    (hx : WritesGood g e x) (hf : WritesGood g e (f (x e).1)) :
    WritesGood g e (x >>= f) := by
  intro k hk
  rw [bind_snd, List.mem_append] at hk
  cases hk with
  | inl h => exact hx k h
  | inr h => exact hf k h

/-- `if guard then return e` leaves a join point behind: the rest of the
    handler becomes `pure () >>= jp`. This steps over it. -/
theorem writesGood_pureBind {α β} (g : Q) (e : Env) (a : α) (f : α → H β)
    (h : WritesGood g e (f a)) : WritesGood g e (pure a >>= f) := by
  intro k hk
  rw [bind_snd] at hk
  simp only [pure_snd, pure_fst, List.nil_append] at hk
  exact h k hk

theorem writesGood_map {α β} (g : Q) (e : Env) (k : α → β) (x : H α)
    (hx : WritesGood g e x) : WritesGood g e (k <$> x) :=
  writesGood_bind' g e x _ hx (writesGood_pure g e _)

/-- Targeted `if`. `split` picks whatever match it finds first, which
    inside a handler is usually the innermost one; this addresses the
    head. -/
theorem writesGood_ite {α} (g : Q) (e : Env) (c : Prop) [Decidable c] (x y : H α)
    (hx : WritesGood g e x) (hy : WritesGood g e y) :
    WritesGood g e (if c then x else y) := by
  by_cases h : c
  · simpa only [if_pos h] using hx
  · simpa only [if_neg h] using hy

/-- The same, keeping the branch condition. Needed wherever a branch is
    good BECAUSE of the test — which is every place a precheck becomes
    an invariant. -/
theorem writesGood_iteH {α} (g : Q) (e : Env) (c : Prop) [Decidable c] (x y : H α)
    (hx : c → WritesGood g e x) (hy : ¬c → WritesGood g e y) :
    WritesGood g e (if c then x else y) := by
  by_cases h : c
  · simpa only [if_pos h] using hx h
  · simpa only [if_neg h] using hy h

theorem getPromise_fst (id : String) (e : Env) :
    (getPromise id e).1 = e.state.promises.find? (·.id == id) := rfl

theorem getTask_fst (id : String) (e : Env) :
    (getTask id e).1 = e.state.tasks.find? (·.id == id) := rfl

theorem getSchedule_fst (id : String) (e : Env) :
    (getSchedule id e).1 = e.state.schedules.find? (·.id == id) := rfl

theorem ask_fst (e : Env) : (ask e).1 = e := rfl

/-! ## The derived reads and writes

Each of these is used by many handlers, so each is proved once. The
shape is always the same: split on what came out of the state, and in
the `some` branch pull the predicate across with `getPromise_sound` /
`getTask_sound` and one `Hereditary` field. -/

section Derived

variable {g : Q}

theorem writesGood_setSettled {e : Env} (hq : Hereditary g e.state)
    (hs : PerStore g e.state = true) (p : PromiseObject) (h : g.promise p = true) :
    WritesGood g e (setSettled p) := by
  unfold setSettled
  refine writesGood_bind' _ _ _ _ (writesGood_setPromise _ _ _ h) ?_
  refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
  refine writesGood_bind' _ _ _ _ (writesGood_getTask _ _ _) ?_
  rw [getTask_fst]
  split
  · rename_i t ht
    refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
    exact writesGood_setTask _ _ _ (hq.tFulfill t (getTask_sound hs ht))
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

theorem writesGood_readPromise {e : Env} (hq : Hereditary g e.state)
    (hs : PerStore g e.state = true) (id : String) (now : Nat) :
    WritesGood g e (readPromise id now) := by
  unfold readPromise
  refine writesGood_bind' _ _ _ _ (writesGood_getPromise _ _ _) ?_
  rw [getPromise_fst]
  split
  · exact writesGood_pure _ _ _
  · rename_i p₀ h
    have hp₀ : g.promise p₀ = true := getPromise_sound hs h
    refine writesGood_bind' _ _ _ _ (writesGood_ask _ _) ?_
    split
    · exact writesGood_bind' _ _ _ _
        (writesGood_setSettled hq hs _ (hq.project p₀ now hp₀)) (writesGood_pure _ _ _)
    · exact writesGood_bind' _ _ _ _ (writesGood_pure _ _ _) (writesGood_pure _ _ _)

theorem returnsGood_readPromise {e : Env} (hq : Hereditary g e.state)
    (hs : PerStore g e.state = true) (id : String) (now : Nat) :
    ReturnsGood g e (readPromise id now) := by
  intro p hp
  rw [readPromise_fst] at hp
  cases h : e.state.promises.find? (fun x => x.id == id) with
  | none => rw [h] at hp; simp at hp
  | some p₀ =>
      rw [h] at hp
      simp only [Option.map_some] at hp
      obtain rfl := Option.some.inj hp
      exact hq.project p₀ now (getPromise_sound hs h)

/-- Fact P, in the form the handlers can use: a promise that reads
    pending is not yet due. Every settle in the machine happens behind a
    `state == pending` guard, so every stamp the machine writes is
    strictly below the promise's own deadline — and that is a fact about
    the READ, not about the promise, which is why it travels with the
    read combinator rather than with `Hereditary`. -/
def NotDue (now : Nat) (p : PromiseObject) : Prop :=
  (p.state == ServerModel.PromiseState.pending) = true → now < p.timeoutAt

theorem readPromise_notDue (id : String) (now : Nat) (e : Env) (p : PromiseObject)
    (h : (readPromise id now e).1 = some p) : NotDue now p := by
  intro hst
  rw [readPromise_fst] at h
  cases hf : e.state.promises.find? (fun x => x.id == id) with
  | none => rw [hf] at h; simp at h
  | some p₀ =>
      rw [hf] at h
      simp only [Option.map_some] at h
      obtain rfl := Option.some.inj h
      exact Lookup.project_pending_not_due hst

/-- The combinator the handlers are written against: after a read,
    either there was nothing, or there was a promise AND it is good AND
    it is not yet due. This is where the reader discipline pays —
    `writesGood_bind'` keeps the continuation pinned to the value the
    read produced, so both facts survive the `←`. -/
theorem writesGood_afterReadPromise {α} {e : Env} (hq : Hereditary g e.state)
    (hs : PerStore g e.state = true) (id : String) (now : Nat)
    (f : Option PromiseObject → H α)
    (hnone : e.state.promises.find? (·.id == id) = none → WritesGood g e (f none))
    (hsome : ∀ p, g.promise p = true → NotDue now p → WritesGood g e (f (some p))) :
    WritesGood g e (readPromise id now >>= f) := by
  refine writesGood_bind' _ _ _ _ (writesGood_readPromise hq hs id now) ?_
  cases h : (readPromise id now e).1 with
  | none =>
      refine hnone ?_
      rw [readPromise_fst] at h
      cases hf : e.state.promises.find? (·.id == id) with
      | none => rfl
      | some p₀ => rw [hf] at h; simp at h
  | some p =>
      exact hsome p (returnsGood_readPromise hq hs id now p h)
        (readPromise_notDue id now e p h)

/-- Same, through `withMat` — which is how the internal steps read.
    `touchPromise` is `withMat true`, `viewPromise` is `withMat false`. -/
theorem writesGood_afterMatReadPromise {α} {e : Env} (hq : Hereditary g e.state) (b : Bool)
    (hs : PerStore g e.state = true) (id : String) (now : Nat)
    (f : Option PromiseObject → H α)
    (hnone : e.state.promises.find? (·.id == id) = none → WritesGood g e (f none))
    (hsome : ∀ p, g.promise p = true → NotDue now p → WritesGood g e (f (some p))) :
    WritesGood g e (withMat b (readPromise id now) >>= f) := by
  refine writesGood_bind' _ _ _ _
    (writesGood_readPromise (e := { e with mat := b }) hq hs id now) ?_
  show WritesGood g e (f ((readPromise id now { e with mat := b }).1))
  cases h : (readPromise id now { e with mat := b }).1 with
  | none =>
      refine hnone ?_
      rw [readPromise_fst] at h
      cases hf : e.state.promises.find? (·.id == id) with
      | none => rfl
      | some p₀ => rw [hf] at h; simp at h
  | some p =>
      exact hsome p
        (returnsGood_readPromise (e := { e with mat := b }) hq hs id now p h)
        (readPromise_notDue id now { e with mat := b } p h)

theorem writesGood_readTask {e : Env} (hq : Hereditary g e.state)
    (hs : PerStore g e.state = true) (id : String) (now : Nat) :
    WritesGood g e (readTask id now) := by
  unfold readTask
  refine writesGood_bind' _ _ _ _ (writesGood_getTask _ _ _) ?_
  rw [getTask_fst]
  split
  · exact writesGood_pure _ _ _
  · rename_i t ht
    refine writesGood_bind' _ _ _ _ (writesGood_readPromise hq hs _ _) ?_
    split
    · exact writesGood_pure _ _ _
    · rename_i p _
      refine writesGood_bind' _ _ _ _ (writesGood_ask _ _) ?_
      refine writesGood_ite _ _ _ _ _ ?_ ?_
      · exact writesGood_bind' _ _ _ _
          (writesGood_setTask _ _ _ (hq.tView t p (getTask_sound hs ht)))
          (writesGood_pure _ _ _)
      · exact writesGood_bind' _ _ _ _ (writesGood_pure _ _ _) (writesGood_pure _ _ _)

/-- What `readTask` hands back, in the two forms the handlers use. The
    task is `t.view p` for a `t` that was in the store; the promise is
    one `readPromise` handed back. Both inherit their facts rather than
    needing their own argument. -/
theorem readTask_good {e : Env} (hq : Hereditary g e.state) (hs : PerStore g e.state = true)
    (id : String) (now : Nat) (t : TaskObject) (po : Option PromiseObject)
    (h : (readTask id now e).1 = some (t, po)) :
    g.task t = true ∧ ∀ p, po = some p → g.promise p = true ∧ NotDue now p := by
  revert h
  unfold readTask
  rw [bind_fst, getTask_fst]
  cases ht : e.state.tasks.find? (·.id == id) with
  | none => intro hh; simp [pure] at hh
  | some t₀ =>
      have hgt₀ : g.task t₀ = true := getTask_sound hs ht
      rw [bind_fst]
      cases hr : (readPromise t₀.id now e).1 with
      | none =>
          intro hh
          simp only [pure_fst, Option.some.injEq, Prod.mk.injEq] at hh
          obtain ⟨rfl, rfl⟩ := hh
          exact ⟨hgt₀, fun p hp => by simp at hp⟩
      | some p₀ =>
          have hgp₀ : g.promise p₀ = true := returnsGood_readPromise hq hs t₀.id now p₀ hr
          have hnd : NotDue now p₀ := readPromise_notDue t₀.id now e p₀ hr
          rw [bind_fst]
          intro hh
          revert hh
          split <;>
          · rw [bind_fst]
            intro hh
            simp only [pure_fst, Option.some.injEq, Prod.mk.injEq] at hh
            obtain ⟨rfl, rfl⟩ := hh
            refine ⟨hq.tView t₀ p₀ hgt₀, fun p hp => ?_⟩
            simp only [Option.some.injEq] at hp
            subst hp
            exact ⟨hgp₀, hnd⟩

theorem writesGood_afterReadTask {α} {e : Env} (hq : Hereditary g e.state)
    (hs : PerStore g e.state = true) (id : String) (now : Nat)
    (f : Option (TaskObject × Option PromiseObject) → H α)
    (hnone : WritesGood g e (f none))
    (hbare : ∀ t, g.task t = true → WritesGood g e (f (some (t, none))))
    (hsome : ∀ t p, g.task t = true → g.promise p = true → NotDue now p →
               WritesGood g e (f (some (t, some p)))) :
    WritesGood g e (readTask id now >>= f) := by
  refine writesGood_bind' _ _ _ _ (writesGood_readTask hq hs id now) ?_
  cases h : (readTask id now e).1 with
  | none => exact hnone
  | some tp =>
      obtain ⟨t, po⟩ := tp
      obtain ⟨hgt, hgp⟩ := readTask_good hq hs id now t po h
      cases po with
      | none => exact hbare t hgt
      | some p =>
          obtain ⟨h1, h2⟩ := hgp p rfl
          exact hsome t p hgt h1 h2

theorem writesGood_afterTouchTask {α} {e : Env} (hq : Hereditary g e.state)
    (hs : PerStore g e.state = true) (id : String) (now : Nat)
    (f : Option (TaskObject × Option PromiseObject) → H α)
    (hnone : WritesGood g e (f none))
    (hbare : ∀ t, g.task t = true → WritesGood g e (f (some (t, none))))
    (hsome : ∀ t p, g.task t = true → g.promise p = true → NotDue now p →
               WritesGood g e (f (some (t, some p)))) :
    WritesGood g e (withMat true (readTask id now) >>= f) := by
  refine writesGood_bind' _ _ _ _
    (writesGood_readTask (e := { e with mat := true }) hq hs id now) ?_
  show WritesGood g e (f ((readTask id now { e with mat := true }).1))
  cases h : (readTask id now { e with mat := true }).1 with
  | none => exact hnone
  | some tp =>
      obtain ⟨t, po⟩ := tp
      obtain ⟨hgt, hgp⟩ :=
        readTask_good (e := { e with mat := true }) hq hs id now t po h
      cases po with
      | none => exact hbare t hgt
      | some p =>
          obtain ⟨h1, h2⟩ := hgp p rfl
          exact hsome t p hgt h1 h2

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
