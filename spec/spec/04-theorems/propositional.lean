import «04-theorems».«abstract-twins»

/-!  # The propositional twin

The abstract machine, stated a second time — as a RELATION between a
state `s`, a successor `s'` and the response the step externalizes,
with no monad, no `bind`, and nothing to run. Every definition below is
`Prop`-valued; every one of them is joined to the executable machine of
`02-abstract` by a bridging theorem `↔`.

Why bother, when the executable machine already exists and `decide`
already sweeps it?

  * A `Prop` reads as a SPECIFICATION. `promiseSettle` in
    `external-steps-m.lean` is a sequence of early returns whose
    meaning is the order they are written in; here it is a disjunction
    of five guarded cases, each self-contained, each stating its own
    frame. Nothing is implied by falling through.
  * Frames become visible. The executable machine never says what a
    step LEAVES ALONE — `setPromise` silently preserves tasks,
    schedules and the outbox. `wrotePromise` says it in three
    equations, so a reviewer can check the frame instead of trusting
    the combinator.
  * The intermediate states a handler passes through become explicit
    `∃ s₁`. In the do-block they are anonymous, threaded by `>>=`;
    here they are named, and the read-then-write structure that the
    two disciplines differ on is right there in the binder.
  * Relations compose with quantifiers, which the state monad does not.
    A liveness or fairness statement quantifies over successors; it
    cannot be phrased against `H α` without first peeling it.

None of this replaces the executable machine. The point of the pair is
the `↔`: state the property against whichever half suits it, transport
it across.

This file STATES the twin. Most of the bridge is `sorry` — discharging
it is a separate body of work, and the statements are the part that has
to be right first. What IS discharged is the bottom layer: the three
read lemmas by `rfl`, all five frame bridges, `init`, and the three
`501` handlers. That layer is not interesting on its own; it is here
because it validates the SHAPE — the `Id.run (h.run s) = (res, s')`
form, the four-equation frame, the `↔` direction — against the real
machine rather than against my reading of it. Each frame bridge is
`ServerState.ext'` and `rfl`, and the same skeleton scales to the
handlers; what does not scale is the case analysis, which is where the
38 remaining `sorry`s live.

## The two disciplines, once

The -m and -p handlers are IDENTICAL modulo the read: replace
`touchPromise`/`touchTask` by `viewPromise`/`viewTask` and one becomes
the other, byte for byte. So the twin is written once, against a
`Discipline` — a pair of read relations — and instantiated twice.
That is the same parametrization the effects machine takes as its
`mat` flag (`02-abstract/effects.lean`), arrived at from the other
side.

The INTERNAL steps take no discipline. They read `touchPromise` and
`touchTask` under both, which is the fact `effects.lean` had to
discover the hard way when a sweep refuted 60 scripts: materialization
is scoped to the handler, and internal steps materialize regardless. Here it is
not a parameter to get wrong — R1 through R7 simply mention the
touching relations.
-/

namespace Propositional

open ServerModel
open AbstractModel (PromiseObject TaskObject ServerState)

/-! ## 1. Reads, as lookups

The store is a list and the machine finds by id. Nothing below ever
mentions the list again. -/

def promiseOf (s : ServerState) (id : String) : Option PromiseObject :=
  s.promises.find? (·.id == id)

def taskOf (s : ServerState) (id : String) : Option TaskObject :=
  s.tasks.find? (·.id == id)

def scheduleOf (s : ServerState) (id : String) : Option Schedule :=
  s.schedules.find? (·.id == id)

theorem promiseOf_correct (s : ServerState) (id : String) :
    Id.run ((AbstractModel.getPromise id).run s) = (promiseOf s id, s) := rfl

theorem taskOf_correct (s : ServerState) (id : String) :
    Id.run ((AbstractModel.getTask id).run s) = (taskOf s id, s) := rfl

theorem scheduleOf_correct (s : ServerState) (id : String) :
    Id.run ((AbstractModel.getSchedule id).run s) = (scheduleOf s id, s) := rfl

/-! ## 2. Writes, as frames

Each write names what moved AND what did not. The three trailing
equations are the frame the executable machine leaves implicit. -/

def wrotePromise (s s' : ServerState) (p : PromiseObject) : Prop :=
  s'.promises = p :: s.promises.filter (·.id != p.id)
    ∧ s'.tasks = s.tasks ∧ s'.schedules = s.schedules ∧ s'.outbox = s.outbox

def wroteTask (s s' : ServerState) (t : TaskObject) : Prop :=
  s'.tasks = t :: s.tasks.filter (·.id != t.id)
    ∧ s'.promises = s.promises ∧ s'.schedules = s.schedules ∧ s'.outbox = s.outbox

def wroteSchedule (s s' : ServerState) (sch : Schedule) : Prop :=
  s'.schedules = sch :: s.schedules.filter (·.id != sch.id)
    ∧ s'.promises = s.promises ∧ s'.tasks = s.tasks ∧ s'.outbox = s.outbox

def deletedSchedule (s s' : ServerState) (id : String) : Prop :=
  s'.schedules = s.schedules.filter (·.id != id)
    ∧ s'.promises = s.promises ∧ s'.tasks = s.tasks ∧ s'.outbox = s.outbox

def wroteMessage (s s' : ServerState) (address : String) (m : Message) : Prop :=
  let e : OutboxEntry := { address := address, message := m }
  s'.outbox = e :: s.outbox.filter (fun x => x.key != e.key)
    ∧ s'.promises = s.promises ∧ s'.tasks = s.tasks ∧ s'.schedules = s.schedules

def wrotePromiseTask (s s' : ServerState) (p : PromiseObject) (t : TaskObject) : Prop :=
  ∃ s₁, wrotePromise s s₁ p ∧ wroteTask s₁ s' t

/-- Four components, so four equations determine the state. Every frame
    bridge below is this lemma plus `rfl`. -/
theorem ServerState.ext' {a b : ServerState}
    (hp : a.promises = b.promises) (ht : a.tasks = b.tasks)
    (hs : a.schedules = b.schedules) (ho : a.outbox = b.outbox) : a = b := by
  cases a; cases b; simp_all

theorem wrotePromise_correct (s s' : ServerState) (p : PromiseObject) :
    wrotePromise s s' p ↔ Id.run ((AbstractModel.setPromise p).run s) = ((), s') := by
  constructor
  · rintro ⟨h1, h2, h3, h4⟩
    have h : s' = { s with promises := p :: s.promises.filter (·.id != p.id) } :=
      ServerState.ext' h1 h2 h3 h4
    subst h; rfl
  · intro h
    have hs : ({ s with promises := p :: s.promises.filter (·.id != p.id) } : ServerState) = s' :=
      congrArg Prod.snd h
    subst hs; exact ⟨rfl, rfl, rfl, rfl⟩

theorem wroteTask_correct (s s' : ServerState) (t : TaskObject) :
    wroteTask s s' t ↔ Id.run ((AbstractModel.setTask t).run s) = ((), s') := by
  constructor
  · rintro ⟨h1, h2, h3, h4⟩
    have h : s' = { s with tasks := t :: s.tasks.filter (·.id != t.id) } :=
      ServerState.ext' h2 h1 h3 h4
    subst h; rfl
  · intro h
    have hs : ({ s with tasks := t :: s.tasks.filter (·.id != t.id) } : ServerState) = s' :=
      congrArg Prod.snd h
    subst hs; exact ⟨rfl, rfl, rfl, rfl⟩

theorem wroteSchedule_correct (s s' : ServerState) (sch : Schedule) :
    wroteSchedule s s' sch ↔ Id.run ((AbstractModel.setSchedule sch).run s) = ((), s') := by
  constructor
  · rintro ⟨h1, h2, h3, h4⟩
    have h : s' = { s with schedules := sch :: s.schedules.filter (·.id != sch.id) } :=
      ServerState.ext' h2 h3 h1 h4
    subst h; rfl
  · intro h
    have hs : ({ s with schedules := sch :: s.schedules.filter (·.id != sch.id) } : ServerState) = s' :=
      congrArg Prod.snd h
    subst hs; exact ⟨rfl, rfl, rfl, rfl⟩

theorem deletedSchedule_correct (s s' : ServerState) (id : String) :
    deletedSchedule s s' id ↔ Id.run ((AbstractModel.delSchedule id).run s) = ((), s') := by
  constructor
  · rintro ⟨h1, h2, h3, h4⟩
    have h : s' = { s with schedules := s.schedules.filter (·.id != id) } :=
      ServerState.ext' h2 h3 h1 h4
    subst h; rfl
  · intro h
    have hs : ({ s with schedules := s.schedules.filter (·.id != id) } : ServerState) = s' :=
      congrArg Prod.snd h
    subst hs; exact ⟨rfl, rfl, rfl, rfl⟩

theorem wroteMessage_correct (s s' : ServerState) (address : String) (m : Message) :
    wroteMessage s s' address m ↔
      Id.run ((AbstractModel.setMessage address m).run s) = ((), s') := by
  constructor
  · rintro ⟨h1, h2, h3, h4⟩
    have h : s' = { s with outbox := OutboxEntry.mk address m :: s.outbox.filter (fun x => x.key != (OutboxEntry.mk address m).key) } := ServerState.ext' h2 h3 h4 h1
    subst h; rfl
  · intro h
    have hs : ({ s with outbox := OutboxEntry.mk address m :: s.outbox.filter (fun x => x.key != (OutboxEntry.mk address m).key) } : ServerState) = s' := congrArg Prod.snd h
    subst hs; exact ⟨rfl, rfl, rfl, rfl⟩

/-- What the frame equations MEAN, and the only consequence of them the
    handlers below actually use: the written key reads back, every other
    key is untouched. Stated separately because the equation is the
    definition and this is the specification. -/
theorem wrotePromise_lookup {s s' : ServerState} {p : PromiseObject}
    (h : wrotePromise s s' p) :
    promiseOf s' p.id = some p ∧ ∀ k, k ≠ p.id → promiseOf s' k = promiseOf s k := by
  sorry

theorem wroteTask_lookup {s s' : ServerState} {t : TaskObject}
    (h : wroteTask s s' t) :
    taskOf s' t.id = some t ∧ ∀ k, k ≠ t.id → taskOf s' k = taskOf s k := by
  sorry

/-! ## 3. Fact P

The verdict a deadline passes on a promise. Named once and used twice —
by the view of a live promise past its deadline, and by the birth of a
promise created past it. The executable machine writes the same `if`
in `PromiseObject.project` and in `createPromise` and nothing connects
them; here the sharing is the definition. -/

def verdict (tags : Tags) : PromiseState :=
  if tags.isTimer then .resolved else .rejectedTimedout

/-- `q` is what `p` reads as at `now`: itself, unless it is pending and
    its deadline has passed, in which case it is settled AT the deadline
    — never at `now`. -/
def isView (now : Nat) (p q : PromiseObject) : Prop :=
  (p.state = .pending ∧ p.timeoutAt ≤ now ∧
     q = { p with state := verdict p.tags, settledAt := some p.timeoutAt })
  ∨ (¬ (p.state = .pending ∧ p.timeoutAt ≤ now) ∧ q = p)

theorem isView_correct (now : Nat) (p q : PromiseObject) :
    isView now p q ↔ p.project now = q := by
  sorry

/-! ## 4. The coupled write

Storing a settled promise stores its co-keyed task fulfilled, in the
same step. The disjunction is disjoint: the second case is the negation
of the first's guard, so exactly one holds. -/

def wroteSettled (s s' : ServerState) (p : PromiseObject) : Prop :=
  (∃ t, p.state ≠ .pending ∧ taskOf s p.id = some t ∧ t.state ≠ .fulfilled
        ∧ wrotePromiseTask s s' p t.fulfill)
  ∨ (¬ (∃ t, p.state ≠ .pending ∧ taskOf s p.id = some t ∧ t.state ≠ .fulfilled)
        ∧ wrotePromise s s' p)

theorem wroteSettled_correct (s s' : ServerState) (p : PromiseObject) :
    wroteSettled s s' p ↔ Id.run ((AbstractModel.setSettled p).run s) = ((), s') := by
  sorry

/-! ## 5. Birth -/

/-- When a targeted promise's task first becomes due: its delay tag, or
    now, whichever is later. -/
def dueAt (tags : Tags) (now : Nat) : Nat :=
  match tags.get? "resonate:delay" with
  | some d => max (parseNat d) now
  | none => now

def createdPromise (s s' : ServerState) (req : PromiseCreateReq) (now : Nat)
    (p : PromiseObject) : Prop :=
  (req.timeoutAt > now ∧
    p = { id := req.id, state := .pending, param := req.param, tags := req.tags,
          timeoutAt := req.timeoutAt, createdAt := now } ∧
    ( (req.tags.has "resonate:target" = false ∧ wrotePromise s s' p)
    ∨ (req.tags.has "resonate:target" = true ∧
        wrotePromiseTask s s' p
          { id := p.id, state := .pending, version := 0,
            retryAt := some (dueAt req.tags now) }) ))
  ∨ (¬ (req.timeoutAt > now) ∧
    p = { id := req.id, state := verdict req.tags, param := req.param, tags := req.tags,
          timeoutAt := req.timeoutAt, createdAt := req.timeoutAt,
          settledAt := some req.timeoutAt } ∧
    ( (req.tags.has "resonate:target" = false ∧ wrotePromise s s' p)
    ∨ (req.tags.has "resonate:target" = true ∧
        wrotePromiseTask s s' p { id := p.id, state := .fulfilled, version := 0 }) ))

theorem createdPromise_correct (s s' : ServerState) (req : PromiseCreateReq)
    (now : Nat) (p : PromiseObject) :
    createdPromise s s' req now p ↔
      Id.run ((AbstractModel.createPromise req now).run s) = (p, s') := by
  sorry

/-! ## 6. The read disciplines

`touchedPromise` persists fact P when it fires; `viewedPromise` serves
it and stores nothing. The difference is one clause: whether the state
moves. Everything downstream is written against the pair. -/

def touchedPromise (s s' : ServerState) (id : String) (now : Nat)
    (r : Option PromiseObject) : Prop :=
  (promiseOf s id = none ∧ r = none ∧ s' = s)
  ∨ (∃ p q, promiseOf s id = some p ∧ isView now p q ∧ r = some q ∧
      ((q.state = p.state ∧ s' = s) ∨ (q.state ≠ p.state ∧ wroteSettled s s' q)))

def viewedPromise (s s' : ServerState) (id : String) (now : Nat)
    (r : Option PromiseObject) : Prop :=
  s' = s ∧
    ( (promiseOf s id = none ∧ r = none)
    ∨ (∃ p q, promiseOf s id = some p ∧ isView now p q ∧ r = some q) )

/-- Fact T rides on the promise read: the task of a settled promise
    reads fulfilled. Under -m the coupled write has already stored it and
    this write is a no-op on content; under -p there is no write at all,
    and the `s' = s₁` branch is taken because `viewedPromise` pins
    `s₁ = s`. Both are covered by the same shape, so the two task reads
    below differ only in the promise read they are given. -/
def touchedTask (s s' : ServerState) (id : String) (now : Nat)
    (r : Option (TaskObject × Option PromiseObject)) : Prop :=
  (taskOf s id = none ∧ r = none ∧ s' = s)
  ∨ (∃ t, taskOf s id = some t ∧ touchedPromise s s' t.id now none ∧ r = some (t, none))
  ∨ (∃ t p s₁, taskOf s id = some t ∧ touchedPromise s s₁ t.id now (some p) ∧
      ( (p.state ≠ .pending ∧ t.state ≠ .fulfilled ∧
          wroteTask s₁ s' t.fulfill ∧ r = some (t.fulfill, some p))
      ∨ (¬ (p.state ≠ .pending ∧ t.state ≠ .fulfilled) ∧
          s' = s₁ ∧ r = some (t, some p)) ))

def viewedTask (s s' : ServerState) (id : String) (now : Nat)
    (r : Option (TaskObject × Option PromiseObject)) : Prop :=
  s' = s ∧
    ( (taskOf s id = none ∧ r = none)
    ∨ (∃ t, taskOf s id = some t ∧ promiseOf s t.id = none ∧ r = some (t, none))
    ∨ (∃ t p q, taskOf s id = some t ∧ promiseOf s t.id = some p ∧ isView now p q ∧
        ( (q.state ≠ .pending ∧ t.state ≠ .fulfilled ∧ r = some (t.fulfill, some q))
        ∨ (¬ (q.state ≠ .pending ∧ t.state ≠ .fulfilled) ∧ r = some (t, some q)) )) )

theorem touchedPromise_correct (s s' : ServerState) (id : String) (now : Nat)
    (r : Option PromiseObject) :
    touchedPromise s s' id now r ↔
      Id.run ((AbstractModel.touchPromise id now).run s) = (r, s') := by
  sorry

theorem viewedPromise_correct (s s' : ServerState) (id : String) (now : Nat)
    (r : Option PromiseObject) :
    viewedPromise s s' id now r ↔
      Id.run ((AbstractModel.viewPromise id now).run s) = (r, s') := by
  sorry

theorem touchedTask_correct (s s' : ServerState) (id : String) (now : Nat)
    (r : Option (TaskObject × Option PromiseObject)) :
    touchedTask s s' id now r ↔
      Id.run ((AbstractModel.touchTask id now).run s) = (r, s') := by
  sorry

theorem viewedTask_correct (s s' : ServerState) (id : String) (now : Nat)
    (r : Option (TaskObject × Option PromiseObject)) :
    viewedTask s s' id now r ↔
      Id.run ((AbstractModel.viewTask id now).run s) = (r, s') := by
  sorry

/-- A read discipline: how a handler learns a promise, and how it learns
    a task. The only thing -m and -p disagree about. -/
structure Discipline where
  readP : ServerState → ServerState → String → Nat → Option PromiseObject → Prop
  readT : ServerState → ServerState → String → Nat →
            Option (TaskObject × Option PromiseObject) → Prop

def materialized : Discipline := { readP := touchedPromise, readT := touchedTask }
def projected    : Discipline := { readP := viewedPromise,  readT := viewedTask }

/-! ## 7. The promise handlers -/

def promiseGet (d : Discipline) (s s' : ServerState) (req : PromiseGetReq)
    (now : Nat) (res : PromiseGetRes) : Prop :=
  (d.readP s s' req.id now none ∧ res = { status := 404 })
  ∨ (∃ p, d.readP s s' req.id now (some p) ∧
      res = { status := 200, promise := some p.toRecord })

def promiseCreate (d : Discipline) (s s' : ServerState) (req : PromiseCreateReq)
    (now : Nat) (res : PromiseCreateRes) : Prop :=
  (req.tags.timerTargeted = true ∧ s' = s ∧ res = { status := 400, promise := none })
  ∨ (req.tags.timerTargeted = false ∧
      ( (∃ p, d.readP s s' req.id now (some p) ∧
          res = { status := 200, promise := some p.toRecord })
      ∨ (∃ s₁ p, d.readP s s₁ req.id now none ∧ createdPromise s₁ s' req now p ∧
          res = { status := 200, promise := some p.toRecord }) ))

def promiseSettle (d : Discipline) (s s' : ServerState) (req : PromiseSettleReq)
    (now : Nat) (res : PromiseSettleRes) : Prop :=
  (req.state.settable = false ∧ s' = s ∧ res = { status := 400 })
  ∨ (req.state.settable = true ∧
      ( (d.readP s s' req.id now none ∧ res = { status := 404 })
      ∨ (∃ p, d.readP s s' req.id now (some p) ∧ p.state ≠ .pending ∧
          res = { status := 200, promise := some p.toRecord })
      ∨ (∃ s₁ p q, d.readP s s₁ req.id now (some p) ∧ p.state = .pending ∧
          q = { p with state := req.state, value := req.value, settledAt := some now } ∧
          wroteSettled s₁ s' q ∧
          res = { status := 200, promise := some q.toRecord }) ))

def promiseRegisterCallback (d : Discipline) (s s' : ServerState)
    (req : PromiseRegisterCallbackReq) (now : Nat)
    (res : PromiseRegisterCallbackRes) : Prop :=
  (req.awaited = req.awaiter ∧ s' = s ∧ res = { status := 400 })
  ∨ (req.awaited ≠ req.awaiter ∧
      ( (d.readP s s' req.awaited now none ∧ res = { status := 404 })
      ∨ (∃ s₁ pa, d.readP s s₁ req.awaited now (some pa) ∧
          d.readP s₁ s' req.awaiter now none ∧ res = { status := 422 })
      ∨ (∃ s₁ pa pw, d.readP s s₁ req.awaited now (some pa) ∧
          d.readP s₁ s' req.awaiter now (some pw) ∧
          (pw.tags.has "resonate:target" = false ∨ pa.external = false) ∧
          res = { status := 422 })
      ∨ (∃ s₁ pa pw, d.readP s s₁ req.awaited now (some pa) ∧
          d.readP s₁ s' req.awaiter now (some pw) ∧
          pw.tags.has "resonate:target" = true ∧ pa.external = true ∧
          (pa.state ≠ .pending ∨ pw.state ≠ .pending) ∧
          res = { status := 200, promise := some pa.toRecord })
      ∨ (∃ s₁ s₂ pa pw, d.readP s s₁ req.awaited now (some pa) ∧
          d.readP s₁ s₂ req.awaiter now (some pw) ∧
          pw.tags.has "resonate:target" = true ∧ pa.external = true ∧
          pa.state = .pending ∧ pw.state = .pending ∧
          wrotePromise s₂ s' (pa.addCallback req.awaiter) ∧
          res = { status := 200, promise := some pa.toRecord }) ))

def promiseRegisterListener (d : Discipline) (s s' : ServerState)
    (req : PromiseRegisterListenerReq) (now : Nat)
    (res : PromiseRegisterListenerRes) : Prop :=
  (addressValid req.address = false ∧ s' = s ∧ res = { status := 400 })
  ∨ (addressValid req.address = true ∧
      ( (d.readP s s' req.awaited now none ∧ res = { status := 404 })
      ∨ (∃ p, d.readP s s' req.awaited now (some p) ∧ p.external = false ∧
          res = { status := 422 })
      ∨ (∃ p, d.readP s s' req.awaited now (some p) ∧ p.external = true ∧
          p.state ≠ .pending ∧ res = { status := 200, promise := some p.toRecord })
      ∨ (∃ s₁ p, d.readP s s₁ req.awaited now (some p) ∧ p.external = true ∧
          p.state = .pending ∧ wrotePromise s₁ s' (p.addListener req.address) ∧
          res = { status := 200, promise := some p.toRecord }) ))

def promiseSearch (_d : Discipline) (s s' : ServerState) (_req : PromiseSearchReq)
    (_now : Nat) (res : PromiseSearchRes) : Prop :=
  s' = s ∧ res = { status := 501 }

/-! ## 8. The task handlers -/

def taskGet (d : Discipline) (s s' : ServerState) (req : TaskGetReq)
    (now : Nat) (res : TaskGetRes) : Prop :=
  (d.readT s s' req.id now none ∧ res = { status := 404 })
  ∨ (∃ t, d.readT s s' req.id now (some (t, none)) ∧ res = { status := 404 })
  ∨ (∃ t p, d.readT s s' req.id now (some (t, some p)) ∧
      res = { status := 200, task := some t.toRecord })

def taskCreate (d : Discipline) (s s' : ServerState) (req : TaskCreateReq)
    (now : Nat) (res : TaskCreateRes) : Prop :=
  let a := req.action
  ((a.tags.has "resonate:target" = false ∨ a.tags.timerTargeted = true) ∧
    s' = s ∧ res = { status := 400 })
  ∨ (a.tags.has "resonate:target" = true ∧ a.tags.timerTargeted = false ∧
    ( (∃ s₁ p t, d.readP s s₁ a.id now none ∧ a.timeoutAt > now ∧
        p = { id := a.id, state := .pending, param := a.param, tags := a.tags,
              timeoutAt := a.timeoutAt, createdAt := now } ∧
        t = { id := p.id, state := .acquired, version := 1,
              ttl := some req.ttl, pid := some req.pid,
              expiresAt := some (now + req.ttl) } ∧
        wrotePromiseTask s₁ s' p t ∧
        res = { status := 200, task := some t.toRecord, promise := some p.toRecord })
    ∨ (∃ s₁ p t, d.readP s s₁ a.id now none ∧ ¬ (a.timeoutAt > now) ∧
        p = { id := a.id, state := .rejectedTimedout, param := a.param, tags := a.tags,
              timeoutAt := a.timeoutAt, createdAt := a.timeoutAt,
              settledAt := some a.timeoutAt } ∧
        t = { id := p.id, state := .fulfilled, version := 0 } ∧
        wrotePromiseTask s₁ s' p t ∧
        res = { status := 200, task := some t.toRecord, promise := some p.toRecord })
    ∨ (∃ p, d.readP s s' a.id now (some p) ∧
        p.tags.has "resonate:target" = false ∧ res = { status := 422 })
    ∨ (∃ s₁ p, d.readP s s₁ a.id now (some p) ∧ p.tags.has "resonate:target" = true ∧
        (d.readT s₁ s' p.id now none ∨ ∃ t, d.readT s₁ s' p.id now (some (t, none))) ∧
        res = { status := 409 })
    ∨ (∃ s₁ p t q, d.readP s s₁ a.id now (some p) ∧ p.tags.has "resonate:target" = true ∧
        d.readT s₁ s' p.id now (some (t, some q)) ∧ t.state = .fulfilled ∧
        res = { status := 200, task := some t.toRecord, promise := some q.toRecord })
    ∨ (∃ s₁ s₂ p t q t', d.readP s s₁ a.id now (some p) ∧
        p.tags.has "resonate:target" = true ∧
        d.readT s₁ s₂ p.id now (some (t, some q)) ∧ t.state = .pending ∧
        t' = { t with state := .acquired, version := t.version + 1,
                      ttl := some req.ttl, pid := some req.pid,
                      expiresAt := some (now + req.ttl),
                      retryAt := none, resumes := [] } ∧
        wroteTask s₂ s' t' ∧
        res = { status := 200, task := some t'.toRecord, promise := some q.toRecord })
    ∨ (∃ s₁ p t q, d.readP s s₁ a.id now (some p) ∧ p.tags.has "resonate:target" = true ∧
        d.readT s₁ s' p.id now (some (t, some q)) ∧
        t.state ≠ .fulfilled ∧ t.state ≠ .pending ∧ res = { status := 409 }) ))

def taskAcquire (d : Discipline) (s s' : ServerState) (req : TaskAcquireReq)
    (now : Nat) (res : TaskAcquireRes) : Prop :=
  (d.readT s s' req.id now none ∧ res = { status := 404 })
  ∨ (∃ t, d.readT s s' req.id now (some (t, none)) ∧ res = { status := 409 })
  ∨ (∃ t p, d.readT s s' req.id now (some (t, some p)) ∧
      (t.state ≠ .pending ∨ p.state ≠ .pending ∨ t.version ≠ req.version) ∧
      res = { status := 409 })
  ∨ (∃ s₁ t p t', d.readT s s₁ req.id now (some (t, some p)) ∧
      t.state = .pending ∧ p.state = .pending ∧ t.version = req.version ∧
      t' = { t with state := .acquired, version := t.version + 1,
                    ttl := some req.ttl, pid := some req.pid,
                    expiresAt := some (now + req.ttl),
                    retryAt := none, resumes := [] } ∧
      wroteTask s₁ s' t' ∧
      res = { status := 200, task := some t'.toRecord, promise := some p.toRecord })

def taskFence (d : Discipline) (s s' : ServerState) (req : TaskFenceReq)
    (now : Nat) (res : TaskFenceRes) : Prop :=
  (req.action.targetId = req.id ∧ s' = s ∧ res = { status := 400 })
  ∨ (req.action.targetId ≠ req.id ∧
      ( (d.readT s s' req.id now none ∧ res = { status := 404 })
      ∨ (∃ t, d.readT s s' req.id now (some (t, none)) ∧ res = { status := 409 })
      ∨ (∃ t p, d.readT s s' req.id now (some (t, some p)) ∧
          (t.state ≠ .acquired ∨ p.state ≠ .pending ∨ t.version ≠ req.version) ∧
          res = { status := 409 })
      ∨ (∃ s₁ t p r inner, d.readT s s₁ req.id now (some (t, some p)) ∧
          t.state = .acquired ∧ p.state = .pending ∧ t.version = req.version ∧
          req.action = .create r ∧ promiseCreate d s₁ s' r now inner ∧
          res = { status := 200, action := some (.create inner) })
      ∨ (∃ s₁ t p r inner, d.readT s s₁ req.id now (some (t, some p)) ∧
          t.state = .acquired ∧ p.state = .pending ∧ t.version = req.version ∧
          req.action = .settle r ∧ promiseSettle d s₁ s' r now inner ∧
          res = { status := 200, action := some (.settle inner) }) ))

def heartbeatOne (d : Discipline) (s s' : ServerState) (pid : String)
    (ref : TaskRef) (now : Nat) : Prop :=
  (d.readT s s' ref.id now none)
  ∨ (∃ t, d.readT s s' ref.id now (some (t, none)))
  ∨ (∃ s₁ t p, d.readT s s₁ ref.id now (some (t, some p)) ∧
      ( (t.state = .acquired ∧ t.version = ref.version ∧ t.pid = some pid ∧
          p.state = .pending ∧
          wroteTask s₁ s' { t with expiresAt := some (now + t.ttl.getD 0) })
      ∨ (¬ (t.state = .acquired ∧ t.version = ref.version ∧ t.pid = some pid ∧
            p.state = .pending) ∧ s' = s₁) ))

def heartbeatAll (d : Discipline) (pid : String) (now : Nat) :
    List TaskRef → ServerState → ServerState → Prop
  | [], s, s' => s' = s
  | ref :: refs, s, s' =>
      ∃ s₁, heartbeatOne d s s₁ pid ref now ∧ heartbeatAll d pid now refs s₁ s'

def taskHeartbeat (d : Discipline) (s s' : ServerState) (req : TaskHeartbeatReq)
    (now : Nat) (res : TaskHeartbeatRes) : Prop :=
  heartbeatAll d req.pid now req.tasks s s' ∧ res = { status := 200 }

/-- The suspension's awaited scan. `none` means "refuse" — an awaited
    promise is missing or internal; `some b` means every awaited promise
    is registrable and `b` says whether any has already settled. -/
def checkedAwaited (d : Discipline) (now : Nat) :
    List PromiseRegisterCallbackReq → ServerState → ServerState → Option Bool → Prop
  | [], s, s', r => s' = s ∧ r = some false
  | a :: rest, s, s', r =>
      (d.readP s s' a.awaited now none ∧ r = none)
      ∨ (∃ pa, d.readP s s' a.awaited now (some pa) ∧ pa.external = false ∧ r = none)
      ∨ (∃ s₁ pa, d.readP s s₁ a.awaited now (some pa) ∧ pa.external = true ∧
          checkedAwaited d now rest s₁ s' none ∧ r = none)
      ∨ (∃ s₁ pa b, d.readP s s₁ a.awaited now (some pa) ∧ pa.external = true ∧
          checkedAwaited d now rest s₁ s' (some b) ∧
          r = some (b || (pa.state != .pending)))

def registeredAwaited (d : Discipline) (awaiter : String) (now : Nat) :
    List PromiseRegisterCallbackReq → ServerState → ServerState → Prop
  | [], s, s' => s' = s
  | a :: rest, s, s' =>
      ∃ s₁, ( d.readP s s₁ a.awaited now none
            ∨ ∃ s₀ pa, d.readP s s₀ a.awaited now (some pa) ∧
                wrotePromise s₀ s₁ (pa.addCallback awaiter) )
          ∧ registeredAwaited d awaiter now rest s₁ s'

/-- The three door checks, together: no actions, self-await, duplicate
    awaited. All three answer 400, so the handler's order among them is
    not observable and the twin does not impose one. -/
def suspendMalformed (req : TaskSuspendReq) : Prop :=
  req.actions = []
  ∨ req.actions.any (·.awaited == req.id) = true
  ∨ (req.actions.map (·.awaited)).eraseDups.length ≠ (req.actions.map (·.awaited)).length

def taskSuspend (d : Discipline) (s s' : ServerState) (req : TaskSuspendReq)
    (now : Nat) (res : TaskSuspendRes) : Prop :=
  (suspendMalformed req ∧ s' = s ∧ res = { status := 400 })
  ∨ (¬ suspendMalformed req ∧
      ( (d.readT s s' req.id now none ∧ res = { status := 404 })
      ∨ (∃ t, d.readT s s' req.id now (some (t, none)) ∧ res = { status := 409 })
      ∨ (∃ t tp, d.readT s s' req.id now (some (t, some tp)) ∧
          (t.state ≠ .acquired ∨ tp.state ≠ .pending ∨ t.version ≠ req.version) ∧
          res = { status := 409 })
      ∨ (∃ s₁ t tp, d.readT s s₁ req.id now (some (t, some tp)) ∧
          t.state = .acquired ∧ tp.state = .pending ∧ t.version = req.version ∧
          checkedAwaited d now req.actions s₁ s' none ∧ res = { status := 422 })
      ∨ (∃ s₁ s₂ t tp, d.readT s s₁ req.id now (some (t, some tp)) ∧
          t.state = .acquired ∧ tp.state = .pending ∧ t.version = req.version ∧
          checkedAwaited d now req.actions s₁ s₂ (some true) ∧
          wroteTask s₂ s' { t with resumes := [] } ∧ res = { status := 300 })
      ∨ (∃ s₁ s₂ s₃ t tp, d.readT s s₁ req.id now (some (t, some tp)) ∧
          t.state = .acquired ∧ tp.state = .pending ∧ t.version = req.version ∧
          checkedAwaited d now req.actions s₁ s₂ (some false) ∧
          registeredAwaited d req.id now req.actions s₂ s₃ ∧
          wroteTask s₃ s' { t with state := .suspended, pid := none, ttl := none,
                                   expiresAt := none, retryAt := none, resumes := [] } ∧
          res = { status := 200 }) ))

def taskFulfill (d : Discipline) (s s' : ServerState) (req : TaskFulfillReq)
    (now : Nat) (res : TaskFulfillRes) : Prop :=
  (req.action.state.settable = false ∧ s' = s ∧ res = { status := 400 })
  ∨ (req.action.state.settable = true ∧
      ( (d.readT s s' req.id now none ∧ res = { status := 404 })
      ∨ (∃ t, d.readT s s' req.id now (some (t, none)) ∧ res = { status := 409 })
      ∨ (∃ t p, d.readT s s' req.id now (some (t, some p)) ∧
          (t.state ≠ .acquired ∨ p.state ≠ .pending ∨ t.version ≠ req.version) ∧
          res = { status := 409 })
      ∨ (∃ s₁ t p q, d.readT s s₁ req.id now (some (t, some p)) ∧
          t.state = .acquired ∧ p.state = .pending ∧ t.version = req.version ∧
          q = { p with state := req.action.state, value := req.action.value,
                       settledAt := some now } ∧
          wroteSettled s₁ s' q ∧
          res = { status := 200, promise := some q.toRecord }) ))

def taskRelease (d : Discipline) (s s' : ServerState) (req : TaskReleaseReq)
    (now : Nat) (res : TaskReleaseRes) : Prop :=
  (d.readT s s' req.id now none ∧ res = { status := 404 })
  ∨ (∃ t, d.readT s s' req.id now (some (t, none)) ∧ res = { status := 409 })
  ∨ (∃ t p, d.readT s s' req.id now (some (t, some p)) ∧
      (t.state ≠ .acquired ∨ p.state ≠ .pending ∨ t.version ≠ req.version) ∧
      res = { status := 409 })
  ∨ (∃ s₁ t p, d.readT s s₁ req.id now (some (t, some p)) ∧
      t.state = .acquired ∧ p.state = .pending ∧ t.version = req.version ∧
      wroteTask s₁ s' { t with state := .pending, pid := none, ttl := none,
                               expiresAt := none, retryAt := some now } ∧
      res = { status := 200 })

def taskHalt (d : Discipline) (s s' : ServerState) (req : TaskHaltReq)
    (now : Nat) (res : TaskHaltRes) : Prop :=
  (d.readT s s' req.id now none ∧ res = { status := 404 })
  ∨ (∃ t, d.readT s s' req.id now (some (t, none)) ∧ res = { status := 404 })
  ∨ (∃ t p, d.readT s s' req.id now (some (t, some p)) ∧ t.state = .fulfilled ∧
      res = { status := 409 })
  ∨ (∃ t p, d.readT s s' req.id now (some (t, some p)) ∧ t.state = .halted ∧
      res = { status := 200 })
  ∨ (∃ s₁ t p, d.readT s s₁ req.id now (some (t, some p)) ∧
      t.state ≠ .fulfilled ∧ t.state ≠ .halted ∧
      wroteTask s₁ s' { t with state := .halted, pid := none, ttl := none,
                               expiresAt := none, retryAt := none } ∧
      res = { status := 200 })

def taskContinue (d : Discipline) (s s' : ServerState) (req : TaskContinueReq)
    (now : Nat) (res : TaskContinueRes) : Prop :=
  (d.readT s s' req.id now none ∧ res = { status := 404 })
  ∨ (∃ t po, d.readT s s' req.id now (some (t, po)) ∧ t.state ≠ .halted ∧
      res = { status := 409 })
  ∨ (∃ t, d.readT s s' req.id now (some (t, none)) ∧ t.state = .halted ∧
      res = { status := 404 })
  ∨ (∃ t p, d.readT s s' req.id now (some (t, some p)) ∧ t.state = .halted ∧
      p.state ≠ .pending ∧ res = { status := 409 })
  ∨ (∃ s₁ t p, d.readT s s₁ req.id now (some (t, some p)) ∧ t.state = .halted ∧
      p.state = .pending ∧
      wroteTask s₁ s' { t with state := .pending, retryAt := some now } ∧
      res = { status := 200 })

def taskSearch (_d : Discipline) (s s' : ServerState) (_req : TaskSearchReq)
    (_now : Nat) (res : TaskSearchRes) : Prop :=
  s' = s ∧ res = { status := 501 }

/-! ## 9. The schedule handlers -/

def scheduleGet (_d : Discipline) (s s' : ServerState) (req : ScheduleGetReq)
    (_now : Nat) (res : ScheduleGetRes) : Prop :=
  s' = s ∧
    ( (scheduleOf s req.id = none ∧ res = { status := 404 })
    ∨ (∃ sch, scheduleOf s req.id = some sch ∧
        res = { status := 200, schedule := some sch }) )

def scheduleCreate (_d : Discipline) (s s' : ServerState) (req : ScheduleCreateReq)
    (now : Nat) (res : ScheduleCreateRes) : Prop :=
  (req.promiseTags.timerTargeted = true ∧ s' = s ∧ res = { status := 400 })
  ∨ (req.promiseTags.timerTargeted = false ∧
      ( (∃ sch, scheduleOf s req.id = some sch ∧ s' = s ∧
          res = { status := 200, schedule := some sch })
      ∨ (∃ sch, scheduleOf s req.id = none ∧
          sch = { id := req.id, cron := req.cron, promiseId := req.promiseId,
                  promiseTimeout := req.promiseTimeout, promiseParam := req.promiseParam,
                  promiseTags := req.promiseTags, createdAt := now,
                  nextRunAt := nextCron req.cron now, lastRunAt := none } ∧
          wroteSchedule s s' sch ∧ res = { status := 200, schedule := some sch }) ))

def scheduleDelete (_d : Discipline) (s s' : ServerState) (req : ScheduleDeleteReq)
    (_now : Nat) (res : ScheduleDeleteRes) : Prop :=
  (scheduleOf s req.id = none ∧ s' = s ∧ res = { status := 404 })
  ∨ (∃ sch, scheduleOf s req.id = some sch ∧ deletedSchedule s s' sch.id ∧
      res = { status := 200 })

def scheduleSearch (_d : Discipline) (s s' : ServerState) (_req : ScheduleSearchReq)
    (_now : Nat) (res : ScheduleSearchRes) : Prop :=
  s' = s ∧ res = { status := 501 }

/-! ## 10. The internal steps

No `Discipline` parameter — but not because they all materialize.
They split:

  R1, R3, R4, and R7 (through `createdIfAbsent`) TOUCH, in both
  disciplines. Materialization is a property of the handler layer, not
  of the machine, so these persist fact P whichever discipline the
  handlers are running.

  R5 and R6 materialize NOTHING. They read the task raw (`getTask`) —
  a choice is never forced by an observation — and the promise through
  `viewPromise`, which serves the verdict without storing it. Their
  refusal paths therefore assert `s' = s` literally, which is why the
  twin can state them without an intermediate state.

Both halves decide through the view; only the first half writes. -/

def r1 (s s' : ServerState) (id : String) (now : Nat) : Prop :=
  ∃ r, touchedPromise s s' id now r

def r3 (s s' : ServerState) (id address : String) (now : Nat) : Prop :=
  (touchedPromise s s' id now none)
  ∨ (∃ p, touchedPromise s s' id now (some p) ∧ p.state = .pending)
  ∨ (∃ p, touchedPromise s s' id now (some p) ∧ p.state ≠ .pending ∧
      p.listeners.contains address = false)
  ∨ (∃ s₁ s₂ p, touchedPromise s s₁ id now (some p) ∧ p.state ≠ .pending ∧
      p.listeners.contains address = true ∧
      wrotePromise s₁ s₂ { p with listeners := p.listeners.filter (· != address) } ∧
      wroteMessage s₂ s' address (.unblock p.toRecord))

def resumedOne (s s' : ServerState) (awaited awaiter : String) (now : Nat) : Prop :=
  (touchedTask s s' awaiter now none)
  ∨ (∃ t, touchedTask s s' awaiter now (some (t, none)))
  ∨ (∃ s₁ t p, touchedTask s s₁ awaiter now (some (t, some p)) ∧
      ( (t.state = .suspended ∧
          wroteTask s₁ s' { t with state := .pending, resumes := [awaited],
                                   retryAt := some now })
      ∨ (t.state ≠ .suspended ∧ t.state ≠ .fulfilled ∧
          t.resumes.contains awaited = false ∧
          wroteTask s₁ s' { t with resumes := t.resumes ++ [awaited] })
      ∨ (t.state ≠ .suspended ∧
          (t.state = .fulfilled ∨ t.resumes.contains awaited = true) ∧ s' = s₁) ))

def r4 (s s' : ServerState) (id awaiter : String) (now : Nat) : Prop :=
  (touchedPromise s s' id now none)
  ∨ (∃ p, touchedPromise s s' id now (some p) ∧ p.state = .pending)
  ∨ (∃ p, touchedPromise s s' id now (some p) ∧ p.state ≠ .pending ∧
      p.callbacks.contains awaiter = false)
  ∨ (∃ s₁ s₂ p, touchedPromise s s₁ id now (some p) ∧ p.state ≠ .pending ∧
      p.callbacks.contains awaiter = true ∧
      wrotePromise s₁ s₂ { p with callbacks := p.callbacks.filter (· != awaiter) } ∧
      resumedOne s₂ s' p.id awaiter now)

def r5 (s s' : ServerState) (id : String) (now : Nat) : Prop :=
  (taskOf s id = none ∧ s' = s)
  ∨ (∃ t, taskOf s id = some t ∧ t.expiresAt = none ∧ s' = s)
  ∨ (∃ t dl, taskOf s id = some t ∧ t.expiresAt = some dl ∧
      ¬ (t.state = .acquired ∧ dl ≤ now) ∧ s' = s)
  ∨ (∃ t dl, taskOf s id = some t ∧ t.expiresAt = some dl ∧
      t.state = .acquired ∧ dl ≤ now ∧ promiseOf s t.id = none ∧ s' = s)
  ∨ (∃ t dl p q, taskOf s id = some t ∧ t.expiresAt = some dl ∧
      t.state = .acquired ∧ dl ≤ now ∧
      promiseOf s t.id = some p ∧ isView now p q ∧ q.state ≠ .pending ∧ s' = s)
  ∨ (∃ t dl p q, taskOf s id = some t ∧ t.expiresAt = some dl ∧
      t.state = .acquired ∧ dl ≤ now ∧
      promiseOf s t.id = some p ∧ isView now p q ∧ q.state = .pending ∧
      wroteTask s s' { t with state := .pending, pid := none, ttl := none,
                              expiresAt := none, retryAt := some now })

def r6 (s s' : ServerState) (id : String) (next now : Nat) : Prop :=
  (taskOf s id = none ∧ s' = s)
  ∨ (∃ t, taskOf s id = some t ∧ t.retryAt = none ∧ s' = s)
  ∨ (∃ t due, taskOf s id = some t ∧ t.retryAt = some due ∧
      ¬ (t.state = .pending ∧ due ≤ now) ∧ s' = s)
  ∨ (∃ t due, taskOf s id = some t ∧ t.retryAt = some due ∧
      t.state = .pending ∧ due ≤ now ∧ promiseOf s t.id = none ∧ s' = s)
  ∨ (∃ t due p q, taskOf s id = some t ∧ t.retryAt = some due ∧
      t.state = .pending ∧ due ≤ now ∧
      promiseOf s t.id = some p ∧ isView now p q ∧ q.state ≠ .pending ∧ s' = s)
  ∨ (∃ s₁ t due p q, taskOf s id = some t ∧ t.retryAt = some due ∧
      t.state = .pending ∧ due ≤ now ∧
      promiseOf s t.id = some p ∧ isView now p q ∧ q.state = .pending ∧
      wroteTask s s₁ { t with retryAt := some next } ∧
      wroteMessage s₁ s' ((q.tags.get? "resonate:target").getD "")
        (.execute t.id t.version))

def createdIfAbsent (s s' : ServerState) (req : PromiseCreateReq) (now : Nat) : Prop :=
  (∃ p, touchedPromise s s' req.id now (some p))
  ∨ (∃ s₁ p, touchedPromise s s₁ req.id now none ∧ createdPromise s₁ s' req now p)

def firedOccurrence (s s' : ServerState) (sch : Schedule) (t : Nat) : Prop :=
  createdIfAbsent s s'
    { id := expand sch.promiseId sch.id t, timeoutAt := t + sch.promiseTimeout,
      param := sch.promiseParam, tags := sch.promiseTags } t

def firedAll (sch : Schedule) : List Nat → ServerState → ServerState → Prop
  | [], s, s' => s' = s
  | t :: ts, s, s' => ∃ s₁, firedOccurrence s s₁ sch t ∧ firedAll sch ts s₁ s'

def r7 (s s' : ServerState) (id : String) (now : Nat) : Prop :=
  (scheduleOf s id = none ∧ s' = s)
  ∨ (∃ sch s₁ ts, scheduleOf s id = some sch ∧
      ts = (occurrences sch.cron sch.nextRunAt now).filter (· ≤ now) ∧
      firedAll sch ts s s₁ ∧
      ( (ts.getLast? = none ∧ s' = s₁)
      ∨ (∃ last, ts.getLast? = some last ∧
          wroteSchedule s₁ s' { sch with lastRunAt := some last,
                                         nextRunAt := nextCron sch.cron last }) ))

/-! ## 11. The step, and the next-state relation

`step` is the propositional twin of `handleA`/`handleAP`: given the
alphabet symbol, the instant and the response, when may `s` go to `s'`?
`next` is Konnov's `tp_next` — the alphabet existentially quantified
away, leaving a bare binary relation on states, which is the object a
temporal property is stated against. -/

open Equivalence (Request Response)
open Abstraction (AStep ATrace stepOfA stepOfAP ValidA ValidAP)

def step (d : Discipline) (s s' : ServerState) (st : AStep) (now : Nat)
    (res : Response) : Prop :=
  match st with
  | .api (.promiseGet req) =>
      ∃ r, res = .promiseGet r ∧ promiseGet d s s' req now r
  | .api (.promiseCreate req) =>
      ∃ r, res = .promiseCreate r ∧ promiseCreate d s s' req now r
  | .api (.promiseSettle req) =>
      ∃ r, res = .promiseSettle r ∧ promiseSettle d s s' req now r
  | .api (.promiseRegisterCallback req) =>
      ∃ r, res = .promiseRegisterCallback r ∧ promiseRegisterCallback d s s' req now r
  | .api (.promiseRegisterListener req) =>
      ∃ r, res = .promiseRegisterListener r ∧ promiseRegisterListener d s s' req now r
  | .api (.promiseSearch req) =>
      ∃ r, res = .promiseSearch r ∧ promiseSearch d s s' req now r
  | .api (.scheduleGet req) =>
      ∃ r, res = .scheduleGet r ∧ scheduleGet d s s' req now r
  | .api (.scheduleCreate req) =>
      ∃ r, res = .scheduleCreate r ∧ scheduleCreate d s s' req now r
  | .api (.scheduleDelete req) =>
      ∃ r, res = .scheduleDelete r ∧ scheduleDelete d s s' req now r
  | .api (.scheduleSearch req) =>
      ∃ r, res = .scheduleSearch r ∧ scheduleSearch d s s' req now r
  | .api (.taskGet req) =>
      ∃ r, res = .taskGet r ∧ taskGet d s s' req now r
  | .api (.taskCreate req) =>
      ∃ r, res = .taskCreate r ∧ taskCreate d s s' req now r
  | .api (.taskAcquire req) =>
      ∃ r, res = .taskAcquire r ∧ taskAcquire d s s' req now r
  | .api (.taskFence req) =>
      ∃ r, res = .taskFence r ∧ taskFence d s s' req now r
  | .api (.taskHeartbeat req) =>
      ∃ r, res = .taskHeartbeat r ∧ taskHeartbeat d s s' req now r
  | .api (.taskSuspend req) =>
      ∃ r, res = .taskSuspend r ∧ taskSuspend d s s' req now r
  | .api (.taskFulfill req) =>
      ∃ r, res = .taskFulfill r ∧ taskFulfill d s s' req now r
  | .api (.taskRelease req) =>
      ∃ r, res = .taskRelease r ∧ taskRelease d s s' req now r
  | .api (.taskHalt req) =>
      ∃ r, res = .taskHalt r ∧ taskHalt d s s' req now r
  | .api (.taskContinue req) =>
      ∃ r, res = .taskContinue r ∧ taskContinue d s s' req now r
  | .api (.taskSearch req) =>
      ∃ r, res = .taskSearch r ∧ taskSearch d s s' req now r
  | .api _      => res = .τ ∧ s' = s
  | .r1 id      => res = .τ ∧ r1 s s' id now
  | .r3 id a    => res = .τ ∧ r3 s s' id a now
  | .r4 id x    => res = .τ ∧ r4 s s' id x now
  | .r5 id      => res = .τ ∧ r5 s s' id now
  | .r6 id next => res = .τ ∧ r6 s s' id next now
  | .r7 id      => res = .τ ∧ r7 s s' id now
  | .idle       => res = .τ ∧ s' = s

/-- The binary relation the machine induces on states — the alphabet,
    the clock and the response quantified away. -/
def next (d : Discipline) (s s' : ServerState) : Prop :=
  ∃ st now res, step d s s' st now res

/-- The initial condition, stated rather than constructed. -/
def init (s : ServerState) : Prop :=
  s.promises = [] ∧ s.tasks = [] ∧ s.schedules = [] ∧ s.outbox = []

/-! ## 12. The bridge

Everything above is joined to the executable machine here. These are
the theorems that make the twin a twin rather than a second, unrelated
specification; until they are discharged the twin is a CLAIM about the
machine, not a view of it. -/

theorem init_correct (s : ServerState) : init s ↔ s = AbstractModel.ServerState.init := by
  cases s; simp [init, AbstractModel.ServerState.init]

theorem promiseGet_correct (s s' : ServerState) (req : PromiseGetReq) (now : Nat)
    (res : PromiseGetRes) :
    promiseGet materialized s s' req now res ↔
      Id.run ((AbstractModel.M.promiseGet req now).run s) = (res, s') := by
  sorry

theorem promiseCreate_correct (s s' : ServerState) (req : PromiseCreateReq) (now : Nat)
    (res : PromiseCreateRes) :
    promiseCreate materialized s s' req now res ↔
      Id.run ((AbstractModel.M.promiseCreate req now).run s) = (res, s') := by
  sorry

theorem promiseSettle_correct (s s' : ServerState) (req : PromiseSettleReq) (now : Nat)
    (res : PromiseSettleRes) :
    promiseSettle materialized s s' req now res ↔
      Id.run ((AbstractModel.M.promiseSettle req now).run s) = (res, s') := by
  sorry

theorem promiseRegisterCallback_correct (s s' : ServerState)
    (req : PromiseRegisterCallbackReq) (now : Nat) (res : PromiseRegisterCallbackRes) :
    promiseRegisterCallback materialized s s' req now res ↔
      Id.run ((AbstractModel.M.promiseRegisterCallback req now).run s) = (res, s') := by
  sorry

theorem promiseRegisterListener_correct (s s' : ServerState)
    (req : PromiseRegisterListenerReq) (now : Nat) (res : PromiseRegisterListenerRes) :
    promiseRegisterListener materialized s s' req now res ↔
      Id.run ((AbstractModel.M.promiseRegisterListener req now).run s) = (res, s') := by
  sorry

theorem promiseSearch_correct (s s' : ServerState) (req : PromiseSearchReq) (now : Nat)
    (res : PromiseSearchRes) :
    promiseSearch materialized s s' req now res ↔
      Id.run ((AbstractModel.M.promiseSearch req now).run s) = (res, s') := by
  constructor
  · rintro ⟨rfl, rfl⟩; rfl
  · intro h
    exact ⟨(congrArg Prod.snd h).symm, (congrArg Prod.fst h).symm⟩

theorem taskGet_correct (s s' : ServerState) (req : TaskGetReq) (now : Nat)
    (res : TaskGetRes) :
    taskGet materialized s s' req now res ↔
      Id.run ((AbstractModel.M.taskGet req now).run s) = (res, s') := by
  sorry

theorem taskCreate_correct (s s' : ServerState) (req : TaskCreateReq) (now : Nat)
    (res : TaskCreateRes) :
    taskCreate materialized s s' req now res ↔
      Id.run ((AbstractModel.M.taskCreate req now).run s) = (res, s') := by
  sorry

theorem taskAcquire_correct (s s' : ServerState) (req : TaskAcquireReq) (now : Nat)
    (res : TaskAcquireRes) :
    taskAcquire materialized s s' req now res ↔
      Id.run ((AbstractModel.M.taskAcquire req now).run s) = (res, s') := by
  sorry

theorem taskFence_correct (s s' : ServerState) (req : TaskFenceReq) (now : Nat)
    (res : TaskFenceRes) :
    taskFence materialized s s' req now res ↔
      Id.run ((AbstractModel.M.taskFence req now).run s) = (res, s') := by
  sorry

theorem taskHeartbeat_correct (s s' : ServerState) (req : TaskHeartbeatReq) (now : Nat)
    (res : TaskHeartbeatRes) :
    taskHeartbeat materialized s s' req now res ↔
      Id.run ((AbstractModel.M.taskHeartbeat req now).run s) = (res, s') := by
  sorry

theorem taskSuspend_correct (s s' : ServerState) (req : TaskSuspendReq) (now : Nat)
    (res : TaskSuspendRes) :
    taskSuspend materialized s s' req now res ↔
      Id.run ((AbstractModel.M.taskSuspend req now).run s) = (res, s') := by
  sorry

theorem taskFulfill_correct (s s' : ServerState) (req : TaskFulfillReq) (now : Nat)
    (res : TaskFulfillRes) :
    taskFulfill materialized s s' req now res ↔
      Id.run ((AbstractModel.M.taskFulfill req now).run s) = (res, s') := by
  sorry

theorem taskRelease_correct (s s' : ServerState) (req : TaskReleaseReq) (now : Nat)
    (res : TaskReleaseRes) :
    taskRelease materialized s s' req now res ↔
      Id.run ((AbstractModel.M.taskRelease req now).run s) = (res, s') := by
  sorry

theorem taskHalt_correct (s s' : ServerState) (req : TaskHaltReq) (now : Nat)
    (res : TaskHaltRes) :
    taskHalt materialized s s' req now res ↔
      Id.run ((AbstractModel.M.taskHalt req now).run s) = (res, s') := by
  sorry

theorem taskContinue_correct (s s' : ServerState) (req : TaskContinueReq) (now : Nat)
    (res : TaskContinueRes) :
    taskContinue materialized s s' req now res ↔
      Id.run ((AbstractModel.M.taskContinue req now).run s) = (res, s') := by
  sorry

theorem taskSearch_correct (s s' : ServerState) (req : TaskSearchReq) (now : Nat)
    (res : TaskSearchRes) :
    taskSearch materialized s s' req now res ↔
      Id.run ((AbstractModel.M.taskSearch req now).run s) = (res, s') := by
  constructor
  · rintro ⟨rfl, rfl⟩; rfl
  · intro h
    exact ⟨(congrArg Prod.snd h).symm, (congrArg Prod.fst h).symm⟩

theorem scheduleGet_correct (s s' : ServerState) (req : ScheduleGetReq) (now : Nat)
    (res : ScheduleGetRes) :
    scheduleGet materialized s s' req now res ↔
      Id.run ((AbstractModel.M.scheduleGet req now).run s) = (res, s') := by
  sorry

theorem scheduleCreate_correct (s s' : ServerState) (req : ScheduleCreateReq) (now : Nat)
    (res : ScheduleCreateRes) :
    scheduleCreate materialized s s' req now res ↔
      Id.run ((AbstractModel.M.scheduleCreate req now).run s) = (res, s') := by
  sorry

theorem scheduleDelete_correct (s s' : ServerState) (req : ScheduleDeleteReq) (now : Nat)
    (res : ScheduleDeleteRes) :
    scheduleDelete materialized s s' req now res ↔
      Id.run ((AbstractModel.M.scheduleDelete req now).run s) = (res, s') := by
  sorry

theorem scheduleSearch_correct (s s' : ServerState) (req : ScheduleSearchReq) (now : Nat)
    (res : ScheduleSearchRes) :
    scheduleSearch materialized s s' req now res ↔
      Id.run ((AbstractModel.M.scheduleSearch req now).run s) = (res, s') := by
  constructor
  · rintro ⟨rfl, rfl⟩; rfl
  · intro h
    exact ⟨(congrArg Prod.snd h).symm, (congrArg Prod.fst h).symm⟩

theorem r1_correct (s s' : ServerState) (id : String) (now : Nat) :
    r1 s s' id now ↔
      Id.run ((AbstractModel.Internal.processPromiseTimeout id now).run s) = ((), s') := by
  sorry

theorem r3_correct (s s' : ServerState) (id address : String) (now : Nat) :
    r3 s s' id address now ↔
      Id.run ((AbstractModel.Internal.processListener id address now).run s) = ((), s') := by
  sorry

theorem r4_correct (s s' : ServerState) (id awaiter : String) (now : Nat) :
    r4 s s' id awaiter now ↔
      Id.run ((AbstractModel.Internal.processCallback id awaiter now).run s) = ((), s') := by
  sorry

theorem r5_correct (s s' : ServerState) (id : String) (now : Nat) :
    r5 s s' id now ↔
      Id.run ((AbstractModel.Internal.processLeaseTimeout id now).run s) = ((), s') := by
  sorry

theorem r6_correct (s s' : ServerState) (id : String) (next now : Nat) :
    r6 s s' id next now ↔
      Id.run ((AbstractModel.Internal.processRetryTimeout id next now).run s) = ((), s') := by
  sorry

theorem r7_correct (s s' : ServerState) (id : String) (now : Nat) :
    r7 s s' id now ↔
      Id.run ((AbstractModel.Internal.processSchedule id now).run s) = ((), s') := by
  sorry

/-- **The bridge, materialized.** Konnov's `tp_next_correct`, at our
    shapes: our step carries a response as well as a successor, so the
    relation is ternary and the equation is a pair. -/
theorem step_correct (s s' : ServerState) (st : AStep) (now : Nat) (res : Response) :
    step materialized s s' st now res ↔ stepOfA st now s = (res, s') := by
  sorry

/-- **The bridge, projected.** The SAME `step`, with the other
    discipline substituted — the twin is written once for both. -/
theorem stepP_correct (s s' : ServerState) (st : AStep) (now : Nat) (res : Response) :
    step projected s s' st now res ↔ stepOfAP st now s = (res, s') := by
  sorry

theorem next_correct (s s' : ServerState) :
    next materialized s s' ↔ ∃ st now res, stepOfA st now s = (res, s') := by
  sorry

/-! ## 13. What the bridge buys

`ValidA` is currently three equations against a run. With the bridge it
is a relation on consecutive trace points and nothing else — the form a
temporal or fairness property can be stated against without ever
mentioning `StateM`. -/

theorem ValidA_correct (tr : ATrace) :
    ValidA tr ↔
      ∀ t : Nat,
        step materialized (tr t).state (tr (t + 1)).state (tr t).req (tr t).now (tr t).res
          ∧ (tr t).now ≤ (tr (t + 1)).now := by
  sorry

theorem ValidAP_correct (tr : ATrace) :
    ValidAP tr ↔
      ∀ t : Nat,
        step projected (tr t).state (tr (t + 1)).state (tr t).req (tr t).now (tr t).res
          ∧ (tr t).now ≤ (tr (t + 1)).now := by
  sorry

/-- A reachability predicate falls out with no machinery: the reflexive
    transitive closure of `next`, from `init`. This is what an INDUCTIVE
    invariant is stated against, and what `invariant-check.lean` can only
    approximate by enumerating scripts. -/
inductive Reachable (d : Discipline) : ServerState → Prop
  | start {s} : init s → Reachable d s
  | step {s s'} : Reachable d s → next d s s' → Reachable d s'

end Propositional
