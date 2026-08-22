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
  /-- KEYED by the row's id. The promise no longer carries one, and the
      relational tier in `trans.lean` genuinely needs it: its predicate
      is "relate this row to the row that was at the SAME id before the
      step", which is a claim about a keyed row, not about a promise.
      `.setPromise` names the id, so a per-effect obligation can still
      be stated locally. -/
  promise  : String → PromiseObject → Bool
  task     : TaskObject → Bool
  schedule : ServerModel.Schedule → Bool

/-- `Q` applied to ONE ROW. The promise face always, the task face if
    the row has one — which is the whole of what fusing the row changed
    here: two walks over two lists became one walk over one.

    `Q` itself keeps its three fields. The entries in `entries.lean` are
    still written against one face at a time, and a fourth field for
    predicates that read BOTH faces is the follow-up this makes
    possible, not something this change needs. -/
def QObj (g : Q) (o : Object) : Bool := g.promise o.id o.promise && o.task.all g.task

theorem QObj_promise {g : Q} {o : Object} (h : QObj g o = true) :
    g.promise o.id o.promise = true := by
  simp only [QObj, Bool.and_eq_true] at h; exact h.1

theorem QObj_task {g : Q} {o : Object} {t : TaskObject} (h : QObj g o = true)
    (ht : o.task = some t) : g.task t = true := by
  simp only [QObj, Bool.and_eq_true] at h
  simpa [ht] using h.2

theorem all_filterMap {α β} (q : β → Bool) (m : α → Option β) :
    ∀ l : List α, (l.filterMap m).all q = l.all (fun a => (m a).all q)
  | [] => rfl
  | a :: l => by
      cases h : m a with
      | none   => simp [List.filterMap_cons, h, all_filterMap q m l]
      | some b => simp [List.filterMap_cons, h, List.all_cons, all_filterMap q m l]

theorem all_and {α} (p q : α → Bool) :
    ∀ l : List α, l.all (fun a => p a && q a) = (l.all p && l.all q)
  | [] => rfl
  | a :: l => by
      simp only [List.all_cons, all_and p q l]
      cases p a <;> cases q a <;> simp

theorem allObj_split (g : Q) (s : ServerState) :
    s.objects.all (QObj g)
      = ((s.objects.all fun o => g.promise o.id o.promise) && s.tasks.all g.task) := by
  simp only [ServerState.tasks, all_filterMap, ← all_and]
  rfl

def PerStore (g : Q) (s : ServerState) : Bool :=
  s.objects.all (QObj g) && s.schedules.all g.schedule

theorem perStore_promises {g : Q} {s : ServerState} (h : PerStore g s = true) :
    (s.objects.all fun o => g.promise o.id o.promise) = true := by
  simp only [PerStore, allObj_split, Bool.and_eq_true] at h; exact h.1.1

theorem perStore_tasks {g : Q} {s : ServerState} (h : PerStore g s = true) :
    s.tasks.all g.task = true := by
  simp only [PerStore, allObj_split, Bool.and_eq_true] at h; exact h.1.2

theorem perStore_objects {g : Q} {s : ServerState} (h : PerStore g s = true) :
    s.objects.all (QObj g) = true := by
  simp only [PerStore, Bool.and_eq_true] at h; exact h.1

theorem perStore_schedules {g : Q} {s : ServerState} (h : PerStore g s = true) :
    s.schedules.all g.schedule = true := by
  simp only [PerStore, Bool.and_eq_true] at h; exact h.2

theorem perStore_mk {g : Q} {s : ServerState}
    (h1 : (s.objects.all fun o => g.promise o.id o.promise) = true)
    (h2 : s.tasks.all g.task = true)
    (h3 : s.schedules.all g.schedule = true) : PerStore g s = true := by
  simp [PerStore, allObj_split, h1, h2, h3]

theorem perStore_mkObj {g : Q} {s : ServerState}
    (h1 : s.objects.all (QObj g) = true) (h3 : s.schedules.all g.schedule = true) :
    PerStore g s = true := by
  simp [PerStore, h1, h3]

/-- What one effect has to satisfy. `delSchedule` and `setMessage` write
    no row into a table this tier is about, so they are unconstrained —
    the outbox is a different argument. -/
def GoodEffect (g : Q) : Effect → Prop
  | .setPromise id p => g.promise id p = true
  | .setTask _ t    => g.task t = true
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

/-- A task write is a `map`, and a map preserves the predicate rowwise.
    This is where the asymmetry pays: `.setTask` cannot add, remove or
    reorder a row, so there is nothing here about the shape of the
    list — only about the one field it touches. -/
theorem all_map_of {α} (q : α → Bool) (m : α → α)
    (hm : ∀ a, q a = true → q (m a) = true) :
    ∀ l : List α, l.all q = true → (l.map m).all q = true
  | [],     _ => rfl
  | a :: l, h => by
      simp only [List.all_cons, Bool.and_eq_true] at h
      simp [List.map_cons, List.all_cons, hm a h.1, all_map_of q m hm l h.2]

/-- One write preserves the store predicate when the row it writes
    satisfies it. Writes to the other tables cannot touch the rest. -/
theorem perStore_apply (g : Q) (f : Effect) (s : ServerState)
    (hs : PerStore g s = true) (hf : GoodEffect g f) :
    PerStore g (f.apply s) = true := by
  have ho := perStore_objects hs
  have h3 := perStore_schedules hs
  cases f with
  | setPromise id p =>
      have hf' : g.promise id p = true := hf
      refine perStore_mkObj (all_upsert _ _ _ _ ho ?_) h3
      cases hfind : s.objects.find? (·.id == id) with
      | none => simpa [Object.withPromise, QObj] using hf'
      | some o =>
          have hoid : o.id = id := eq_of_beq (by simpa using List.find?_some hfind)
          have hq : QObj g o = true :=
            List.all_eq_true.mp ho o (List.mem_of_find?_eq_some hfind)
          simp only [QObj, Bool.and_eq_true] at hq ⊢
          exact ⟨by simpa [Object.withPromise, hoid] using hf',
                 by simpa [Object.withPromise] using hq.2⟩
  | setTask id t =>
      refine perStore_mkObj (all_map_of _ _ (fun o hq => ?_) _ ho) h3
      by_cases hid : (o.id == id) = true
      · simp only [QObj, Bool.and_eq_true] at hq ⊢
        simpa [hid] using ⟨hq.1, hf⟩
      · simpa [hid] using hq
  | setSchedule c =>
      exact perStore_mkObj ho (all_upsert _ c _ _ h3 hf)
  | delSchedule i =>
      exact perStore_mkObj ho (all_filter _ _ _ h3)
  | setMessage a m =>
      exact perStore_mkObj ho h3

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

theorem getObject_sound {g : Q} {s : ServerState} {id : String} {o : Object}
    (hs : PerStore g s = true)
    (h : s.objects.find? (·.id == id) = some o) : QObj g o = true :=
  List.all_eq_true.mp (perStore_objects hs) o (List.mem_of_find?_eq_some h)

theorem getPromise_sound {g : Q} {s : ServerState} {id : String} {o : Object}
    (hs : PerStore g s = true)
    (h : s.objects.find? (·.id == id) = some o) : g.promise o.id o.promise = true :=
  QObj_promise (getObject_sound hs h)

theorem getTask_sound {g : Q} {s : ServerState} {id : String} {t : TaskObject}
    (hs : PerStore g s = true)
    (h : s.task? id = some t) : g.task t = true := by
  unfold ServerState.task? at h
  cases hfind : s.objects.find? (·.id == id) with
  | none => rw [hfind] at h; simp at h
  | some o =>
      rw [hfind] at h
      simp only [Option.bind_some] at h
      have hq := getObject_sound hs hfind
      simp only [QObj, Bool.and_eq_true] at hq
      simpa [h] using hq.2

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
def ReturnsGood (g : Q) (e : Env) (act : H (Option Object)) : Prop :=
  ∀ o, (act e).1 = some o → QObj g o = true

theorem writesGood_pure {α} (g : Q) (e : Env) (a : α) : WritesGood g e (pure a) := by
  intro f hf; simp [pure] at hf

theorem writesGood_setPromise (g : Q) (e : Env) (id : String) (p : PromiseObject)
    (h : g.promise id p = true) : WritesGood g e (setPromise id p) := by
  intro f hf; simp [setPromise, emit] at hf; subst hf; exact h

theorem writesGood_setTask (g : Q) (e : Env) (id : String) (t : TaskObject)
    (h : g.task t = true) : WritesGood g e (setTask id t) := by
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

theorem writesGood_getObject (g : Q) (e : Env) (id : String) :
    WritesGood g e (getObject id) := by
  intro f hf; simp [getObject, bind, ask, pure] at hf

theorem writesGood_getSchedule (g : Q) (e : Env) (id : String) :
    WritesGood g e (getSchedule id) := by
  intro f hf; simp [getSchedule, bind, ask, pure] at hf

/-- `withMat` changes which environment the body sees, and nothing else.
    The one place the reader indexing earns its keep. -/
theorem writesGood_withMat {α} (g : Q) (e : Env) (b : Bool)
    (act : H α) (h : WritesGood g { e with mat := b } act) :
    WritesGood g e (withMat b act) := h

/-- The row came out of the store. Every one of the six transformations
    is applied to a promise the handler READ, so this is always
    available — and it is what makes a relational obligation's birth
    case vacuous rather than unprovable: a birth is by construction the
    one write with no row behind it. -/
def Stored (a : ServerState) (id : String) : Prop :=
  a.objects.find? (·.id == id) ≠ none

theorem stored_of_find? {a : ServerState} {id : String} {o : Object}
    (h : a.objects.find? (·.id == id) = some o) : Stored a id := by
  unfold Stored; rw [h]; simp

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
  project      : ∀ (id : String) (p : PromiseObject) (n : Nat), Stored a id →
                   g.promise id p = true → g.promise id (p.project n) = true
  addCallback  : ∀ (id : String) (p : PromiseObject) (c : String), Stored a id →
                   g.promise id p = true → g.promise id (p.addCallback c) = true
  addListener  : ∀ (id : String) (p : PromiseObject) (c : String), Stored a id →
                   g.promise id p = true → g.promise id (p.addListener c) = true
  settle       : ∀ (id : String) (p : PromiseObject) (st : ServerModel.PromiseState)
                   (v : ServerModel.Value) (t : Nat), Stored a id →
                   st.settable = true → p.state = .pending → t < p.timeoutAt →
                   g.promise id p = true →
                   g.promise id { p with state := st, value := v, settledAt := some t } = true
  dropListener : ∀ (id : String) (p : PromiseObject) (c : String), Stored a id →
                   p.state ≠ .pending → g.promise id p = true →
                   g.promise id { p with listeners := p.listeners.filter (· != c) } = true
  dropCallback : ∀ (id : String) (p : PromiseObject) (c : String), Stored a id →
                   p.state ≠ .pending → g.promise id p = true →
                   g.promise id { p with callbacks := p.callbacks.filter (· != c) } = true
  live         : ∀ (id : String) (param : ServerModel.Value) (tags : ServerModel.Tags)
                   (timeoutAt createdAt : Nat), createdAt < timeoutAt →
                   a.objects.find? (·.id == id) = none →
                   g.promise id { state := .pending, param := param, tags := tags,
                                  timeoutAt := timeoutAt, createdAt := createdAt } = true
  dead         : ∀ (id : String) (st : ServerModel.PromiseState)
                   (param : ServerModel.Value) (tags : ServerModel.Tags) (timeoutAt : Nat),
                   st = (if tags.isTimer then .resolved else .rejectedTimedout) →
                   a.objects.find? (·.id == id) = none →
                   g.promise id { state := st, param := param, tags := tags,
                                  timeoutAt := timeoutAt, createdAt := timeoutAt,
                                  settledAt := some timeoutAt } = true
  -- tasks
  tFulfill     : ∀ (t : TaskObject), g.task t = true → g.task t.fulfill = true
  tBornPending : ∀ (due : Nat),
                   g.task { state := .pending, version := 0,
                            retryAt := some due } = true
  tBornDone    : g.task { state := .fulfilled, version := 0 } = true
  tBornHeld    : ∀ (pid : String) (ttl now : Nat),
                   g.task { state := .acquired, version := 1, ttl := some ttl,
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

theorem getObject_fst (id : String) (e : Env) :
    (getObject id e).1 = e.state.objects.find? (·.id == id) := rfl

theorem getSchedule_fst (id : String) (e : Env) :
    (getSchedule id e).1 = e.state.schedules.find? (·.id == id) := rfl

theorem ask_fst (e : Env) : (ask e).1 = e := rfl

/-! ## The derived reads and writes

Each of these is used by many handlers, so each is proved once. The
shape is always the same: split on what came out of the state, and in
the `some` branch pull the predicate across with `getObject_sound` and
one `Hereditary` field.

There is ONE read here now. `readPromise` and `readTask` were two, and
`readTask` was the awkward one: it returned a task and MAYBE its
promise, so every combinator built on it carried a third case for a
task whose promise was missing, and every handler had to be handed an
obligation for it. Fusing the row deletes that case at the source —
`writesGood_afterReadObject` has two branches where
`writesGood_afterReadTask` had three. -/

section Derived

variable {g : Q}

/-- Fact P and fact T at the row level: projecting an object keeps both
    faces good. The promise half is `Hereditary.project`, the task half
    is `Hereditary.tView` — the same two obligations as before, now
    discharged together because they move together. -/
theorem QObj_project {a : ServerState} (hq : Hereditary g a) {o : Object}
    (hst : Stored a o.id) (h : QObj g o = true) (n : Nat) :
    QObj g (o.project n) = true := by
  have h1 : g.promise o.id o.promise = true := QObj_promise h
  simp only [QObj, Bool.and_eq_true]
  refine ⟨hq.project _ _ n hst h1, ?_⟩
  show (o.task.map (·.view (o.promise.project n))).all g.task = true
  cases hto : o.task with
  | none   => rfl
  | some t => exact hq.tView t _ (QObj_task h hto)

theorem writesGood_setSettled {e : Env} (hq : Hereditary g e.state)
    (o : Object) (ho : QObj g o = true) (p : PromiseObject) (h : g.promise o.id p = true) :
    WritesGood g e (setSettled o p) := by
  unfold setSettled
  refine writesGood_bind' _ _ _ _ (writesGood_setPromise _ _ _ _ h) ?_
  refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
  split
  · rename_i t ht
    refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
    exact writesGood_setTask _ _ _ _ (hq.tFulfill t (QObj_task ho ht))
  · exact writesGood_pure _ _ _

/-- The only write a read performs. Both faces come from the PROJECTED
    object, so both are good by `QObj_project` — one obligation where
    the two reads used to need one each. -/
theorem writesGood_materialise {e : Env} {o' : Object} (h : QObj g o' = true)
    (id : String) (hid : o'.id = id) (o : Object) :
    WritesGood g e (materialise id o o') := by
  unfold materialise
  refine writesGood_bind' _ _ _ _ ?_ ?_
  · exact writesGood_ite _ _ _ _ _
      (writesGood_setPromise _ _ _ _ (hid ▸ QObj_promise h)) (writesGood_pure _ _ _)
  · split
    · rename_i t u _ hu
      exact writesGood_ite _ _ _ _ _
        (writesGood_setTask _ _ _ _ (QObj_task h hu)) (writesGood_pure _ _ _)
    · exact writesGood_pure _ _ _

/-- What `readObject` hands back: the stored object, projected. Facts P
    and T as one equation. -/
theorem readObject_fst (id : String) (now : Nat) (e : Env) :
    (readObject id now e).1 =
      (e.state.objects.find? (·.id == id)).map (·.project now) := by
  unfold readObject
  rw [bind_fst, getObject_fst]
  cases h : e.state.objects.find? (·.id == id) with
  | none => rfl
  | some o =>
      rw [bind_fst]
      split <;> rw [bind_fst] <;> rfl

theorem writesGood_readObject {e : Env} (hq : Hereditary g e.state)
    (hs : PerStore g e.state = true) (id : String) (now : Nat) :
    WritesGood g e (readObject id now) := by
  unfold readObject
  refine writesGood_bind' _ _ _ _ (writesGood_getObject _ _ _) ?_
  rw [getObject_fst]
  split
  · exact writesGood_pure _ _ _
  · rename_i o h
    have ho : QObj g o = true := getObject_sound hs h
    have hst : Stored e.state o.id := by
      have : o.id = id := eq_of_beq (by simpa using List.find?_some h)
      rw [this]; exact stored_of_find? h
    have hpr : QObj g (o.project now) = true := QObj_project hq hst ho now
    refine writesGood_bind' _ _ _ _ (writesGood_ask _ _) ?_
    split
    · have hid : (o.project now).id = id :=
        (Lookup.project_id o now).trans (eq_of_beq (by simpa using List.find?_some h))
      exact writesGood_bind' _ _ _ _ (writesGood_materialise hpr id hid o)
        (writesGood_pure _ _ _)
    · exact writesGood_bind' _ _ _ _ (writesGood_pure _ _ _) (writesGood_pure _ _ _)

theorem returnsGood_readObject {e : Env} (hq : Hereditary g e.state)
    (hs : PerStore g e.state = true) (id : String) (now : Nat) (o : Object)
    (h : (readObject id now e).1 = some o) : QObj g o = true := by
  rw [readObject_fst] at h
  cases hf : e.state.objects.find? (fun x => x.id == id) with
  | none => rw [hf] at h; simp at h
  | some o₀ =>
      rw [hf] at h
      simp only [Option.map_some] at h
      obtain rfl := Option.some.inj h
      have hoid : o₀.id = id := eq_of_beq (by simpa using List.find?_some hf)
      exact QObj_project hq (by rw [hoid]; exact stored_of_find? hf)
        (getObject_sound hs hf) now

/-- Fact P, in the form the handlers can use: a promise that reads
    pending is not yet due. Every settle in the machine happens behind a
    `state == pending` guard, so every stamp the machine writes is
    strictly below the promise's own deadline — and that is a fact about
    the READ, not about the promise, which is why it travels with the
    read combinator rather than with `Hereditary`. -/
def NotDue (now : Nat) (p : PromiseObject) : Prop :=
  (p.state == ServerModel.PromiseState.pending) = true → now < p.timeoutAt

theorem readObject_notDue (id : String) (now : Nat) (e : Env) (o : Object)
    (h : (readObject id now e).1 = some o) : NotDue now o.promise := by
  intro hst
  rw [readObject_fst] at h
  cases hf : e.state.objects.find? (fun x => x.id == id) with
  | none => rw [hf] at h; simp at h
  | some o₀ =>
      rw [hf] at h
      simp only [Option.map_some] at h
      obtain rfl := Option.some.inj h
      exact Lookup.project_pending_not_due hst

/-- The id the read was made at is the id of the row it returned —
    projection cannot move a row. So the caller gets `Stored` at the
    OBJECT's id, which is the form every `Hereditary` obligation wants. -/
theorem readObject_id (id : String) (now : Nat) (e : Env) (o : Object)
    (h : (readObject id now e).1 = some o) : o.id = id := by
  rw [readObject_fst] at h
  cases hf : e.state.objects.find? (fun x => x.id == id) with
  | none => rw [hf] at h; simp at h
  | some o₀ =>
      rw [hf] at h
      simp only [Option.map_some] at h
      obtain rfl := Option.some.inj h
      exact (Lookup.project_id o₀ now).trans
        (eq_of_beq (by simpa using List.find?_some hf))

theorem readObject_stored (id : String) (now : Nat) (e : Env) (o : Object)
    (h : (readObject id now e).1 = some o) : Stored e.state o.id := by
  rw [readObject_id id now e o h]
  rw [readObject_fst] at h
  cases hf : e.state.objects.find? (fun x => x.id == id) with
  | none => rw [hf] at h; simp at h
  | some o₀ => exact stored_of_find? hf

/-- The combinator every handler is written against: after a read,
    either there was nothing, or there was an object AND both its faces
    are good AND its promise is not yet due. This is where the reader
    discipline pays — `writesGood_bind'` keeps the continuation pinned
    to the value the read produced, so the facts survive the `←`.

    Two branches, not three. The missing one is the task whose promise
    was gone, and it is missing because the state can no longer hold
    it. -/
theorem writesGood_afterReadObject {α} {e : Env} (hq : Hereditary g e.state)
    (hs : PerStore g e.state = true) (id : String) (now : Nat)
    (f : Option Object → H α)
    (hnone : e.state.objects.find? (·.id == id) = none → WritesGood g e (f none))
    (hsome : ∀ o, QObj g o = true → NotDue now o.promise → Stored e.state o.id →
               WritesGood g e (f (some o))) :
    WritesGood g e (readObject id now >>= f) := by
  refine writesGood_bind' _ _ _ _ (writesGood_readObject hq hs id now) ?_
  cases h : (readObject id now e).1 with
  | none =>
      refine hnone ?_
      rw [readObject_fst] at h
      cases hf : e.state.objects.find? (·.id == id) with
      | none => rfl
      | some o₀ => rw [hf] at h; simp at h
  | some o =>
      exact hsome o (returnsGood_readObject hq hs id now o h)
        (readObject_notDue id now e o h) (readObject_stored id now e o h)

/-- The promise-only face of the combinator, for the handlers that
    never look at the task. Same read, weaker hypothesis. -/
theorem writesGood_afterReadObjectP {α} {e : Env} (hq : Hereditary g e.state)
    (hs : PerStore g e.state = true) (id : String) (now : Nat)
    (f : Option Object → H α)
    (hnone : e.state.objects.find? (·.id == id) = none → WritesGood g e (f none))
    (hsome : ∀ o, g.promise o.id o.promise = true → NotDue now o.promise →
               Stored e.state o.id → WritesGood g e (f (some o))) :
    WritesGood g e (readObject id now >>= f) :=
  writesGood_afterReadObject hq hs id now f hnone
    (fun o ho => hsome o (QObj_promise ho))

/-- Same, through `withMat` — which is how the internal steps read.
    `touchObject` is `withMat true`, `viewObject` is `withMat false`. -/
theorem writesGood_afterMatReadObject {α} {e : Env} (hq : Hereditary g e.state) (b : Bool)
    (hs : PerStore g e.state = true) (id : String) (now : Nat)
    (f : Option Object → H α)
    (hnone : e.state.objects.find? (·.id == id) = none → WritesGood g e (f none))
    (hsome : ∀ o, QObj g o = true → NotDue now o.promise → Stored e.state o.id →
               WritesGood g e (f (some o))) :
    WritesGood g e (withMat b (readObject id now) >>= f) := by
  refine writesGood_bind' _ _ _ _
    (writesGood_readObject (e := { e with mat := b }) hq hs id now) ?_
  show WritesGood g e (f ((readObject id now { e with mat := b }).1))
  cases h : (readObject id now { e with mat := b }).1 with
  | none =>
      refine hnone ?_
      rw [readObject_fst] at h
      cases hf : e.state.objects.find? (·.id == id) with
      | none => rfl
      | some o₀ => rw [hf] at h; simp at h
  | some o =>
      exact hsome o
        (returnsGood_readObject (e := { e with mat := b }) hq hs id now o h)
        (readObject_notDue id now { e with mat := b } o h)
        (readObject_stored id now { e with mat := b } o h)

theorem writesGood_afterMatReadObjectP {α} {e : Env} (hq : Hereditary g e.state) (b : Bool)
    (hs : PerStore g e.state = true) (id : String) (now : Nat)
    (f : Option Object → H α)
    (hnone : e.state.objects.find? (·.id == id) = none → WritesGood g e (f none))
    (hsome : ∀ o, g.promise o.id o.promise = true → NotDue now o.promise →
               Stored e.state o.id → WritesGood g e (f (some o))) :
    WritesGood g e (withMat b (readObject id now) >>= f) :=
  writesGood_afterMatReadObject hq b hs id now f hnone
    (fun o ho => hsome o (QObj_promise ho))

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
