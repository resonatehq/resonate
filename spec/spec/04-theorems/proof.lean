import «04-theorems».«abstract-twins»

/-!  # The proof of `ResponseLockstepAbstract` — architecture and first cases

The theorem is the one claim in the system whose proof needs no
schedule search: lockstep means φ is the identity, so the whole proof
is a trace induction over one invariant plus a per-step agreement
lemma, discharged handler by handler.

**The invariant** `REq n sP sM`: the two states agree on every
LOOKUP THROUGH THE VIEW — projected promises (`pLook`), viewed tasks
(`tLook`), schedules (`sLook`) — pointwise by id. Not state equality:
the machines genuinely differ in stored bytes (fact-lag) and list
order; but every decision either machine makes goes through exactly
these lookups, which is the design fact the whole theorem rests on.

**The architecture**, each piece fully proven here (no `sorry`
anywhere in this file):

  1. Pure lemmas: `project` preserves identity and absorbs
     (`project_absorb`, n ≤ n'), `view` absorbs over projections
     (`view_absorb`) — the reason the invariant is monotone in time.
  2. Keyed-list lemmas: lookups through the upsert `x :: filter (≠ x.id)`.
  3. `REq.mono`: the invariant survives the clock advancing — the
     formal residue of "facts are stable under monotone time".
  4. **The reduction** (`responseLockstep_of_stepAgreement`): if every
     single step agrees (`StepAgreement`), the unbounded theorem
     follows, by induction over the trace.
  5. Per-step agreement (`Agrees st`) — ALL CASES DISCHARGED: `idle`,
     the searches, the schedule handlers, the five promise handlers,
     the ten task handlers, and the seven rules. The recurring
     arguments: the task read proven once (`bind_taskRead_agrees`);
     touch self-invisibility plus symmetry/transitivity of the
     invariant (R1/R2); the doomed-dispatch fix mechanized — under a
     pending view the raw tasks coincide, under a settled view
     neither side writes (R5/R6); the settled-upsert congruence and
     the value/state split of the touch (R3/R4); the loop handlers by
     list induction over their request-shared lists; `taskFence`
     through the run-level promise-handler agreements; and R7 as a
     list induction over the opaque, shared occurrence term, its
     past-time writes absorbed because facts are stable under
     monotone time (`REq_touchWrite_le`).
  6. **The assembly**: `stepAgreement` by cases on `AStep`, and

       `responseLockstepAbstract : ResponseLockstepAbstract`

     — closed, unconditional, checked by the kernel; `#print axioms`
     reports only `propext`, `Classical.choice`, `Quot.sound`.

What remains for full `LockstepAbstract` is the MESSAGE half:
extend the invariant with outbox equality (every agreeing step writes
the same messages, so it is carried mechanically) and quiescence
congruence for `absQuiesced` — same architecture, on top of these
lemmas.  -/

namespace Abstraction
namespace Proof

open AbstractModel
open ServerModel (PromiseState TaskState)

/-! ### 0. Bool plumbing -/

theorem toFalse {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b
  · rfl
  · exact absurd rfl h

theorem beq_false_of_bne {α} [BEq α] {a b : α} (h : (a != b) = true) : (a == b) = false := by
  cases hab : (a == b) with
  | false => rfl
  | true =>
      rw [show (a != b) = !(a == b) from rfl, hab] at h
      cases h

theorem bne_true_of_beq_false {α} [BEq α] {a b : α} (h : (a == b) = false) : (a != b) = true := by
  rw [show (a != b) = !(a == b) from rfl, h]; rfl

theorem beq_true_of_bne_false {α} [BEq α] {a b : α} (h : (a != b) = false) : (a == b) = true := by
  cases hab : (a == b) with
  | true => rfl
  | false =>
      rw [show (a != b) = !(a == b) from rfl, hab] at h
      cases h

/-! ### 1. Pure lemmas -/

theorem project_id (p : PromiseObject) (n : Nat) : (p.project n).id = p.id := by
  unfold PromiseObject.project
  split
  · split <;> rfl
  · rfl

theorem project_not_pending (p : PromiseObject) (n : Nat)
    (h : (p.state == PromiseState.pending) = false) : p.project n = p := by
  unfold PromiseObject.project
  split
  · next hc => rw [hc.1] at h; cases h
  · rfl

/-- The settled form a due projection produces. -/
theorem project_due (p : PromiseObject) {n : Nat}
    (hc : (p.state == PromiseState.pending) = true) (hd : p.timeoutAt ≤ n) :
    p.project n = if p.isTimer then { p with state := PromiseState.resolved, settledAt := some p.timeoutAt } else { p with state := PromiseState.rejectedTimedout, settledAt := some p.timeoutAt } := by
  unfold PromiseObject.project
  rw [if_pos ⟨hc, hd⟩]

theorem project_undue (p : PromiseObject) {n : Nat} (hd : ¬ p.timeoutAt ≤ n) :
    p.project n = p := by
  unfold PromiseObject.project
  rw [if_neg (fun hx => hd hx.2)]

/-- Fact P is stable under monotone time: projecting later absorbs
    projecting earlier. -/
theorem project_absorb (p : PromiseObject) {n n' : Nat} (h : n ≤ n') :
    (p.project n).project n' = p.project n' := by
  by_cases hc : (p.state == PromiseState.pending) = true
  · by_cases hd : p.timeoutAt ≤ n
    · rw [project_due p hc hd, project_due p hc (Nat.le_trans hd h)]
      by_cases ht : p.isTimer = true
      · rw [if_pos ht]; exact project_not_pending _ n' rfl
      · rw [if_neg ht]; exact project_not_pending _ n' rfl
    · rw [project_undue p hd]
  · rw [project_not_pending p n (toFalse hc)]

/-- A projection that is still pending did nothing. -/
theorem project_of_pending {p : PromiseObject} {n : Nat}
    (h : ((p.project n).state == PromiseState.pending) = true) : p.project n = p := by
  by_cases hc : (p.state == PromiseState.pending) = true
  · by_cases hd : p.timeoutAt ≤ n
    · exfalso
      rw [project_due p hc hd] at h
      by_cases ht : p.isTimer = true
      · rw [if_pos ht] at h; simp at h
      · rw [if_neg ht] at h; simp at h
    · exact project_undue p hd
  · exact project_not_pending p n (toFalse hc)

theorem settable_ne_pending {st : PromiseState} (h : st.settable = true) :
    (st != PromiseState.pending) = true := by
  cases st <;> simp_all [ServerModel.PromiseState.settable]

theorem fulfill_state (t : TaskObject) : (t.fulfill).state = TaskState.fulfilled := rfl

theorem view_of_fulfilled (t : TaskObject) (p : PromiseObject)
    (h : (t.state == TaskState.fulfilled) = true) : t.view p = t := by
  unfold TaskObject.view
  rw [if_neg]
  intro hx
  exact absurd hx.2 (by simp [bne, h])

theorem view_of_pending (t : TaskObject) (p : PromiseObject)
    (h : (p.state == PromiseState.pending) = true) : t.view p = t := by
  unfold TaskObject.view
  rw [if_neg]
  intro hx
  exact absurd hx.1 (by simp [bne, h])

/-- The settling form: promise settled, task not yet fulfilled. -/
theorem view_settles (t : TaskObject) (p : PromiseObject)
    (hq : (p.state == PromiseState.pending) = false)
    (ht : (t.state == TaskState.fulfilled) = false) : t.view p = t.fulfill := by
  unfold TaskObject.view
  rw [if_pos ⟨by simp [bne, hq], by simp [bne, ht]⟩]

theorem view_idem (t : TaskObject) (q : PromiseObject) :
    (t.view q).view q = t.view q := by
  by_cases hq : (q.state == PromiseState.pending) = true
  · rw [view_of_pending t q hq, view_of_pending t q hq]
  · by_cases ht : (t.state == TaskState.fulfilled) = true
    · rw [view_of_fulfilled t q ht, view_of_fulfilled t q ht]
    · rw [view_settles t q (toFalse hq) (toFalse ht)]
      exact view_of_fulfilled _ _ rfl

/-- Fact T is stable under monotone time: viewing against the later
    projection absorbs viewing against the earlier one. Key step: once
    the earlier projection is settled, the LATER projection is the
    SAME object — settling is final — so this is view-idempotence. -/
theorem view_absorb (t : TaskObject) (p : PromiseObject) {n n' : Nat} (h : n ≤ n') :
    (t.view (p.project n)).view (p.project n') = t.view (p.project n') := by
  by_cases hq : ((p.project n).state == PromiseState.pending) = true
  · rw [view_of_pending t _ hq]
  · have heq : p.project n' = p.project n := by
      rw [← project_absorb p h]
      exact project_not_pending _ n' (toFalse hq)
    rw [heq]
    exact view_idem t _

/-! ### 2. Keyed-list lemmas -/

theorem find?_filter_ne {α} (idOf : α → String) (a b : String) (l : List α)
    (h : (a == b) = false) :
    (l.filter (fun y => idOf y != a)).find? (fun y => idOf y == b)
      = l.find? (fun y => idOf y == b) := by
  induction l with
  | nil => rfl
  | cons x l ih =>
      by_cases hx : idOf x = a
      · have h1 : (idOf x != a) = false := by simp [hx]
        have h2 : (idOf x == b) = false := by
          rw [hx]; exact h
        simp [List.filter_cons, h1, List.find?_cons, h2, ih]
      · have h1 : (idOf x != a) = true := by simp [bne, hx]
        simp only [List.filter_cons, h1, if_pos, List.find?_cons]
        cases hxb : (idOf x == b)
        · simp [hxb, ih]
        · simp [hxb]

/-- Lookup through the keyed upsert used by every setter. -/
theorem find?_upsert {α} (idOf : α → String) (x : α) (l : List α) (b : String) :
    ((x :: l.filter (fun y => idOf y != idOf x)).find? (fun y => idOf y == b))
      = if (idOf x == b) = true then some x
        else l.find? (fun y => idOf y == b) := by
  cases hxb : (idOf x == b) with
  | true =>
      rw [if_pos rfl]
      simp [List.find?_cons, hxb]
  | false =>
      rw [if_neg (by simp)]
      have hstep : ((x :: l.filter (fun y => idOf y != idOf x)).find? (fun y => idOf y == b))
          = (l.filter (fun y => idOf y != idOf x)).find? (fun y => idOf y == b) := by
        simp [List.find?_cons, hxb]
      rw [hstep]
      exact find?_filter_ne idOf (idOf x) b l hxb

/-! ### 3. The invariant and its monotonicity -/

/-- Projected-promise lookup. -/
def pLook (n : Nat) (s : ServerState) (id : String) : Option PromiseObject :=
  (s.promises.find? (·.id == id)).map (·.project n)

/-- Fact T as an `Option`-level combinator — named so that statements
    about `tLook` can mention it without anonymous matchers. -/
def applyView (t : TaskObject) : Option PromiseObject → TaskObject
  | some pv => t.view pv
  | none => t

/-- Viewed-task lookup — defined THROUGH `pLook`, so that anything
    preserving `pLook` and the raw task find preserves it. -/
def tLook (n : Nat) (s : ServerState) (id : String) : Option TaskObject :=
  (s.tasks.find? (·.id == id)).map fun t => applyView t (pLook n s id)

def sLook (s : ServerState) (id : String) : Option ServerModel.Schedule :=
  s.schedules.find? (·.id == id)

/-- THE INVARIANT: agreement on every lookup through the view. -/
def REq (n : Nat) (sP sM : ServerState) : Prop :=
  (∀ id, pLook n sP id = pLook n sM id)
    ∧ (∀ id, tLook n sP id = tLook n sM id)
    ∧ (∀ id, sLook sP id = sLook sM id)

theorem REq.refl (n : Nat) (s : ServerState) : REq n s s :=
  ⟨fun _ => rfl, fun _ => rfl, fun _ => rfl⟩

/-- The invariant is pointwise equality of lookups, so it is symmetric
    and transitive — the rules' shared code composes through it. -/
theorem REq.symm {n : Nat} {sP sM : ServerState} (h : REq n sP sM) : REq n sM sP :=
  ⟨fun id => (h.1 id).symm, fun id => (h.2.1 id).symm, fun id => (h.2.2 id).symm⟩

theorem REq.trans {n : Nat} {a b c : ServerState}
    (h1 : REq n a b) (h2 : REq n b c) : REq n a c :=
  ⟨fun id => (h1.1 id).trans (h2.1 id), fun id => (h1.2.1 id).trans (h2.2.1 id),
   fun id => (h1.2.2 id).trans (h2.2.2 id)⟩

theorem pLook_mono {n n' : Nat} (h : n ≤ n') (s : ServerState) (id : String) :
    pLook n' s id = (pLook n s id).map (·.project n') := by
  unfold pLook
  cases s.promises.find? (·.id == id) with
  | none => rfl
  | some p => simp [Option.map, project_absorb p h]

theorem tLook_mono {n n' : Nat} (h : n ≤ n') (s : ServerState) (id : String) :
    tLook n' s id = (tLook n s id).map fun tv => applyView tv (pLook n' s id) := by
  unfold tLook
  cases hf : s.tasks.find? (·.id == id) with
  | none => rfl
  | some t =>
      simp only [Option.map]
      cases hp : s.promises.find? (·.id == id) with
      | none =>
          have h1 : pLook n s id = none := by unfold pLook; rw [hp]; rfl
          have h2 : pLook n' s id = none := by unfold pLook; rw [hp]; rfl
          rw [h1, h2]
          simp only [applyView]
      | some p =>
          have h1 : pLook n s id = some (p.project n) := by
            unfold pLook; rw [hp]; rfl
          have h2 : pLook n' s id = some (p.project n') := by
            unfold pLook; rw [hp]; rfl
          rw [h1, h2]
          simp only [applyView]
          rw [view_absorb t p h]

/-- The invariant survives the clock advancing — the formal residue of
    "facts are stable under monotone time". -/
theorem REq.mono {n n' : Nat} (h : n ≤ n') {sP sM : ServerState}
    (r : REq n sP sM) : REq n' sP sM := by
  obtain ⟨hp, ht, hs⟩ := r
  have hp' : ∀ id, pLook n' sP id = pLook n' sM id := fun id => by
    rw [pLook_mono h sP id, pLook_mono h sM id, hp id]
  refine ⟨hp', fun id => ?_, hs⟩
  rw [tLook_mono h sP id, tLook_mono h sM id, ht id, hp' id]

/-! ### 4. The reduction: per-step agreement gives the theorem -/

/-- One step of the shared schedule agrees on `REq`-related states:
    equal responses, related again. -/
def Agrees (st : AStep) : Prop :=
  ∀ (n : Nat) (sP sM : ServerState), REq n sP sM →
    (stepOfAP st n sP).1 = (stepOfA st n sM).1
      ∧ REq n (stepOfAP st n sP).2 (stepOfA st n sM).2

def StepAgreement : Prop := ∀ st, Agrees st

/-- **THE REDUCTION.** Per-step agreement yields the unbounded
    response-lockstep theorem, by induction over the trace: the
    invariant holds initially (equal states), is carried by
    `Agrees` across each step, and by `REq.mono` across the advancing
    clock; the response conjunct falls out at every index. -/
theorem responseLockstep_of_stepAgreement (h : StepAgreement) :
    ResponseLockstepAbstract := by
  intro trP trM vP vM iP iM same
  have inv : ∀ t, REq ((trP t).now) (trP t).state (trM t).state := by
    intro t
    induction t with
    | zero =>
        rw [iP, iM]; exact REq.refl _ _
    | succ k ih =>
        have hreq := (same k).1
        have hnow := (same k).2
        have hstep := h (trP k).req ((trP k).now) (trP k).state (trM k).state ih
        have hpost : REq ((trP k).now) (trP (k+1)).state (trM (k+1)).state := by
          rw [(vP k).2.1, (vM k).2.1, ← hreq, ← hnow]
          exact hstep.2
        exact REq.mono (vP k).2.2 hpost
  intro t
  rw [(vP t).1, (vM t).1, ← (same t).1, ← (same t).2]
  exact (h (trP t).req ((trP t).now) (trP t).state (trM t).state (inv t)).1

/-! ### 5. Per-step agreement, case by case

Monadic plumbing first: `M` is `StateT ServerState Id`, so runs reduce
definitionally — recorded as `rfl`-lemmas for `simp`. -/

theorem run_bind {α β} (x : M α) (f : α → M β) (s : ServerState) :
    (x >>= f).run s = (f (x.run s).1).run (x.run s).2 := rfl

theorem run_pure {α} (a : α) (s : ServerState) :
    (pure a : M α).run s = (a, s) := rfl

theorem run_map {α β} (f : α → β) (x : M α) (s : ServerState) :
    (f <$> x).run s = (f (x.run s).1, (x.run s).2) := rfl

theorem run_get (s : ServerState) : (get : M ServerState).run s = (s, s) := rfl

theorem run_modify (f : ServerState → ServerState) (s : ServerState) :
    (modify f : M Unit).run s = ((), f s) := rfl

/-- `idle`: both machines stutter. -/
theorem agrees_idle : Agrees .idle := by
  intro n sP sM h
  exact ⟨rfl, h⟩

/-- The searches answer `501` from any state and write nothing. -/
theorem agrees_promiseSearch (req) : Agrees (.api (.promiseSearch req)) := by
  intro n sP sM h
  exact ⟨rfl, h⟩

theorem agrees_taskSearch (req) : Agrees (.api (.taskSearch req)) := by
  intro n sP sM h
  exact ⟨rfl, h⟩

theorem agrees_scheduleSearch (req) : Agrees (.api (.scheduleSearch req)) := by
  intro n sP sM h
  exact ⟨rfl, h⟩

/-! ### 5a. Effect runs — every effect is a pure pair, by `rfl` -/

theorem run_getPromise (id : String) (s : ServerState) :
    (getPromise id).run s = (s.promises.find? (·.id == id), s) := rfl

theorem run_getTask (id : String) (s : ServerState) :
    (getTask id).run s = (s.tasks.find? (·.id == id), s) := rfl

theorem run_getSchedule (id : String) (s : ServerState) :
    (getSchedule id).run s = (s.schedules.find? (·.id == id), s) := rfl

theorem run_setPromise (p : PromiseObject) (s : ServerState) :
    (setPromise p).run s
      = ((), { s with promises := p :: s.promises.filter (·.id != p.id) }) := rfl

theorem run_setTask (t : TaskObject) (s : ServerState) :
    (setTask t).run s
      = ((), { s with tasks := t :: s.tasks.filter (·.id != t.id) }) := rfl

theorem run_setSchedule (sch : ServerModel.Schedule) (s : ServerState) :
    (setSchedule sch).run s
      = ((), { s with schedules := sch :: s.schedules.filter (·.id != sch.id) }) := rfl

theorem run_delSchedule (id : String) (s : ServerState) :
    (delSchedule id).run s
      = ((), { s with schedules := s.schedules.filter (·.id != id) }) := rfl

theorem run_viewPromise (id : String) (n : Nat) (s : ServerState) :
    (viewPromise id n).run s
      = ((s.promises.find? (·.id == id)).map (·.project n), s) := rfl

/-- The state a materializing touch produces: the promise write, plus
    the COUPLED task write — fact P and fact T are stored together, so
    the machine never persists a settled promise over an unfulfilled
    co-keyed task. -/
def touchWrite (n : Nat) (p : PromiseObject) (s : ServerState) : ServerState :=
  let s1 : ServerState :=
    { s with promises := (p.project n) :: s.promises.filter (·.id != (p.project n).id) }
  if ((p.project n).state != PromiseState.pending) = true then
    match s.tasks.find? (·.id == (p.project n).id) with
    | some t =>
        if (t.state != TaskState.fulfilled) = true then
          { s1 with tasks := t.fulfill :: s1.tasks.filter (·.id != t.fulfill.id) }
        else s1
    | none => s1
  else s1

/-- `touchWrite` depends on the promise only through its projection. -/
theorem touchWrite_congr {n : Nat} {p q : PromiseObject}
    (h : p.project n = q.project n) (s : ServerState) :
    touchWrite n p s = touchWrite n q s := by
  unfold touchWrite; rw [h]

/-- `setSettled` on an already-settled record: the coupled write. -/
theorem run_setSettled (q : PromiseObject) (n : Nat) (s : ServerState)
    (hq : (q.state != PromiseState.pending) = true) (hqp : q.project n = q) :
    (setSettled q).run s = ((), touchWrite n q s) := by
  simp only [setSettled, run_bind, run_setPromise, if_pos hq, run_getTask, touchWrite, hqp,
             if_pos hq]
  cases hft : s.tasks.find? (·.id == q.id) with
  | none => simp only [hft, run_pure]
  | some t =>
      by_cases htf : ((t.state != TaskState.fulfilled) = true)
      · simp only [hft, if_pos htf, run_setTask, run_pure]
      · simp only [hft, if_neg htf, run_pure]

/-- The touch, characterized: `viewPromise`'s value, plus the
    conditional coupled write. -/
theorem run_touchPromise (id : String) (n : Nat) (s : ServerState) :
    (touchPromise id n).run s =
      match s.promises.find? (·.id == id) with
      | none => (none, s)
      | some p =>
          (some (p.project n),
            if ((p.project n).state != p.state) = true then touchWrite n p s
            else s) := by
  cases hf : s.promises.find? (·.id == id) with
  | none =>
      simp only [touchPromise, run_bind, run_getPromise, hf, run_pure]
  | some p =>
      by_cases hst : (((p.project n).state != p.state) = true)
      · have hset : ((p.project n).state != PromiseState.pending) = true := by
          cases hpp : ((p.project n).state == PromiseState.pending) with
          | true => rw [project_of_pending hpp] at hst; simp at hst
          | false => exact bne_true_of_beq_false hpp
        have hidem : (p.project n).project n = p.project n := project_absorb p (Nat.le_refl n)
        simp only [touchPromise, run_bind, run_getPromise, hf, if_pos hst,
                   run_setSettled (p.project n) n s hset hidem, run_pure, touchWrite, hidem]
      · simp only [touchPromise, run_bind, run_getPromise, hf, if_neg hst, run_pure]

/-! ### 5b. More keyed-list lemmas: erasure, satisfaction -/

theorem find?_sat {α} (p : α → Bool) (l : List α) (a : α)
    (h : l.find? p = some a) : p a = true := by
  induction l with
  | nil => cases h
  | cons x l ih =>
      cases hx : p x with
      | true =>
          have hc : (x :: l).find? p = some x := by simp [List.find?, hx]
          rw [hc] at h
          cases h
          exact hx
      | false =>
          have hc : (x :: l).find? p = l.find? p := by simp [List.find?, hx]
          rw [hc] at h
          exact ih h

theorem find?_filter_self {α} (idOf : α → String) (a : String) (l : List α) :
    (l.filter (fun y => idOf y != a)).find? (fun y => idOf y == a) = none := by
  induction l with
  | nil => rfl
  | cons x l ih =>
      cases hx : (idOf x == a) with
      | true =>
          have h1 : (idOf x != a) = false := by simp [bne, hx]
          simp [List.filter_cons, h1, ih]
      | false =>
          have h1 : (idOf x != a) = true := by simp [bne, hx]
          simp [List.filter_cons, h1, List.find?, hx, ih]

/-- Lookup through the keyed erasure used by the deletes. -/
theorem find?_erase {α} (idOf : α → String) (a b : String) (l : List α) :
    (l.filter (fun y => idOf y != a)).find? (fun y => idOf y == b)
      = if (a == b) = true then none else l.find? (fun y => idOf y == b) := by
  cases hab : (a == b) with
  | true =>
      rw [if_pos rfl]
      have hba : a = b := eq_of_beq hab
      subst hba
      exact find?_filter_self idOf a l
  | false =>
      rw [if_neg (by simp)]
      exact find?_filter_ne idOf a b l hab

/-! ### 5c. Write congruences: what each write does to the lookups -/

/-- Persisting the projection of a stored promise is invisible to
    `pLook` — materialization is memoization. -/
theorem pLook_materialize (n : Nat) (s : ServerState) (p : PromiseObject)
    (hf : s.promises.find? (·.id == p.id) = some p) (id : String) :
    pLook n { s with promises := (p.project n) :: s.promises.filter (·.id != (p.project n).id) } id
      = pLook n s id := by
  show (((p.project n) :: s.promises.filter (fun y => y.id != (p.project n).id)).find? (fun y => y.id == id)).map (·.project n)
      = (s.promises.find? (fun y => y.id == id)).map (·.project n)
  rw [find?_upsert PromiseObject.id (p.project n) s.promises id]
  cases hxb : ((p.project n).id == id) with
  | true =>
      have hpid : (p.id == id) = true := by rw [← project_id p n]; exact hxb
      have hid : p.id = id := eq_of_beq hpid
      rw [if_pos rfl, ← hid, hf]
      simp [project_absorb p (Nat.le_refl n)]
  | false =>
      rw [if_neg (by simp)]

theorem tLook_ext {n : Nat} {s s' : ServerState}
    (hp : ∀ id, pLook n s' id = pLook n s id)
    (htasks : s'.tasks = s.tasks) (id : String) :
    tLook n s' id = tLook n s id := by
  unfold tLook
  rw [htasks, hp id]

theorem sLook_ext {s s' : ServerState}
    (hsched : s'.schedules = s.schedules) (id : String) :
    sLook s' id = sLook s id := by
  unfold sLook
  rw [hsched]

/-- The touch write is invisible to the WHOLE invariant: an `REq`
    partner absorbs a materialization on the other side. This is the
    formal content of "materialization is memoization of projection". -/
theorem REq_promiseWrite {n : Nat} {sP sM : ServerState} (h : REq n sP sM)
    (p : PromiseObject) (hf : sM.promises.find? (·.id == p.id) = some p) :
    REq n sP { sM with promises := (p.project n) :: sM.promises.filter (·.id != (p.project n).id) } :=
  ⟨fun id => (h.1 id).trans (pLook_materialize n sM p hf id).symm,
   fun id => (h.2.1 id).trans (tLook_ext (pLook_materialize n sM p hf) rfl id).symm,
   fun id => (h.2.2 id).trans (sLook_ext (s := sM) rfl id).symm⟩

/-- The touch's task-side write is invisible to the invariant: where
    the view already fulfills, persisting the fulfilled form changes
    no lookup. -/
theorem REq_touchTaskWrite {n : Nat} {sP sM : ServerState} (h : REq n sP sM)
    (t : TaskObject) (id : String)
    (hf : sM.tasks.find? (·.id == id) = some t)
    (pv : PromiseObject) (hpl : pLook n sM id = some pv)
    (hset : (pv.state == PromiseState.pending) = false)
    (htf : (t.state == TaskState.fulfilled) = false) :
    REq n sP { sM with tasks := t.fulfill :: sM.tasks.filter (·.id != t.fulfill.id) } := by
  have htid : t.id = id := eq_of_beq (find?_sat (fun y => y.id == id) sM.tasks t hf)
  obtain ⟨hp, ht, hs⟩ := h
  have key : ∀ id', tLook n { sM with tasks := t.fulfill :: sM.tasks.filter (·.id != t.fulfill.id) } id'
      = tLook n sM id' := by
    intro id'
    show ((t.fulfill :: sM.tasks.filter (fun y => y.id != t.fulfill.id)).find? (fun y => y.id == id')).map
          (fun tt => applyView tt (pLook n sM id'))
        = tLook n sM id'
    rw [find?_upsert TaskObject.id t.fulfill sM.tasks id']
    by_cases hc : (t.fulfill.id == id') = true
    · have hid' : t.id = id' := eq_of_beq hc
      rw [if_pos hc, ← hid', htid, hpl]
      unfold tLook
      rw [hf, hpl]
      simp only [Option.map, applyView]
      rw [view_of_fulfilled t.fulfill pv rfl, view_settles t pv hset htf]
    · rw [if_neg hc]
      rfl
  exact ⟨hp, fun id' => (ht id').trans (key id').symm, hs⟩

/-- The COUPLED touch write is invisible to the invariant: the promise
    materialization by `REq_promiseWrite`, and the task fulfillment that
    rides with it by `REq_touchTaskWrite`. Materialization is still
    memoization — now of both facts at once. -/
theorem REq_touchWrite {n : Nat} {sP sM : ServerState} (h : REq n sP sM)
    (p : PromiseObject) (hf : sM.promises.find? (·.id == p.id) = some p) :
    REq n sP (touchWrite n p sM) := by
  have h1 := REq_promiseWrite h p hf
  unfold touchWrite
  by_cases hset : (((p.project n).state != PromiseState.pending) = true)
  · simp only [if_pos hset]
    have hpl : pLook n { sM with promises := (p.project n) :: sM.promises.filter (·.id != (p.project n).id) }
        ((p.project n).id) = some (p.project n) := by
      show (((p.project n) :: sM.promises.filter (fun y => y.id != (p.project n).id)).find?
             (fun y => y.id == (p.project n).id)).map (·.project n) = _
      rw [find?_upsert PromiseObject.id (p.project n) sM.promises ((p.project n).id),
          if_pos (by simp)]
      simp [project_absorb p (Nat.le_refl n)]
    cases hft : sM.tasks.find? (·.id == (p.project n).id) with
    | none => simp only [hft]; exact h1
    | some t =>
        by_cases htf : ((t.state != TaskState.fulfilled) = true)
        · simp only [hft, if_pos htf]
          exact REq_touchTaskWrite h1 t ((p.project n).id) hft (p.project n) hpl
            (beq_false_of_bne hset) (beq_false_of_bne htf)
        · simp only [hft, if_neg htf]; exact h1
  · simp only [if_neg hset]; exact h1

/-- The same schedule upsert on both sides preserves the invariant. -/
theorem REq_setSchedule {n : Nat} {sP sM : ServerState} (h : REq n sP sM)
    (sch : ServerModel.Schedule) :
    REq n { sP with schedules := sch :: sP.schedules.filter (·.id != sch.id) }
          { sM with schedules := sch :: sM.schedules.filter (·.id != sch.id) } := by
  obtain ⟨hp, ht, hs⟩ := h
  refine ⟨hp, ht, fun id => ?_⟩
  show ((sch :: sP.schedules.filter (fun y => y.id != sch.id)).find? (fun y => y.id == id))
      = ((sch :: sM.schedules.filter (fun y => y.id != sch.id)).find? (fun y => y.id == id))
  rw [find?_upsert ServerModel.Schedule.id sch sP.schedules id,
      find?_upsert ServerModel.Schedule.id sch sM.schedules id,
      show sP.schedules.find? (fun y => y.id == id) = sM.schedules.find? (fun y => y.id == id) from hs id]

/-- The same schedule erasure on both sides preserves the invariant. -/
theorem REq_delSchedule {n : Nat} {sP sM : ServerState} (h : REq n sP sM)
    (a : String) :
    REq n { sP with schedules := sP.schedules.filter (·.id != a) }
          { sM with schedules := sM.schedules.filter (·.id != a) } := by
  obtain ⟨hp, ht, hs⟩ := h
  refine ⟨hp, ht, fun id => ?_⟩
  show (sP.schedules.filter (fun y => y.id != a)).find? (fun y => y.id == id)
      = (sM.schedules.filter (fun y => y.id != a)).find? (fun y => y.id == id)
  rw [find?_erase ServerModel.Schedule.id a id sP.schedules,
      find?_erase ServerModel.Schedule.id a id sM.schedules,
      show sP.schedules.find? (fun y => y.id == id) = sM.schedules.find? (fun y => y.id == id) from hs id]

/-- After a materializing touch, the co-keyed task IS the fulfilled
    form — the coupled write put it there. -/
theorem touchWrite_task {n : Nat} {p : PromiseObject} {s : ServerState} {t : TaskObject}
    (hset : ((p.project n).state != PromiseState.pending) = true)
    (hf : s.tasks.find? (·.id == (p.project n).id) = some t)
    (htf : (t.state != TaskState.fulfilled) = true) :
    (touchWrite n p s).tasks.find? (·.id == (p.project n).id) = some t.fulfill := by
  have hid : t.id = (p.project n).id :=
    eq_of_beq (find?_sat (fun x => x.id == (p.project n).id) s.tasks t hf)
  unfold touchWrite
  simp only [if_pos hset, hf, if_pos htf]
  show ((t.fulfill :: (s.tasks.filter (fun y => y.id != t.fulfill.id))).find?
          (fun y => y.id == (p.project n).id)) = some t.fulfill
  rw [find?_upsert TaskObject.id t.fulfill s.tasks ((p.project n).id),
      if_pos (show (t.fulfill.id == (p.project n).id) = true by
        show (t.id == (p.project n).id) = true
        rw [hid]; simp)]

/-- Re-upserting the record a lookup already returns is invisible. -/
theorem REq_setTask_idem {n : Nat} {sP sM : ServerState} (h : REq n sP sM) (t : TaskObject)
    (hf : sM.tasks.find? (·.id == t.id) = some t) :
    REq n sP { sM with tasks := t :: sM.tasks.filter (·.id != t.id) } := by
  obtain ⟨hp, ht, hs⟩ := h
  refine ⟨hp, fun id => ?_, hs⟩
  have key : tLook n { sM with tasks := t :: sM.tasks.filter (·.id != t.id) } id = tLook n sM id := by
    show ((t :: sM.tasks.filter (fun y => y.id != t.id)).find? (fun y => y.id == id)).map
          (fun tt => applyView tt (pLook n sM id)) = tLook n sM id
    rw [find?_upsert TaskObject.id t sM.tasks id]
    by_cases hc : (t.id == id) = true
    · rw [if_pos hc, ← eq_of_beq hc]
      unfold tLook; rw [hf]
    · rw [if_neg hc]; rfl
  exact (ht id).trans key.symm

/-! ### 5d. The schedule handlers — no facts, shared code, `sLook` only -/

theorem agrees_scheduleGet (req) : Agrees (.api (.scheduleGet req)) := by
  intro n sP sM h
  have hsid : sP.schedules.find? (·.id == req.id) = sM.schedules.find? (·.id == req.id) :=
    h.2.2 req.id
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.scheduleGet, AbstractModel.scheduleGet,
             run_map, run_bind, run_getSchedule, run_pure, Id.run]
  rw [hsid]
  cases hfM : sM.schedules.find? (·.id == req.id) with
  | none => exact ⟨rfl, h⟩
  | some sch => exact ⟨rfl, h⟩

theorem agrees_scheduleCreate (req) : Agrees (.api (.scheduleCreate req)) := by
  intro n sP sM h
  have hsid : sP.schedules.find? (·.id == req.id) = sM.schedules.find? (·.id == req.id) :=
    h.2.2 req.id
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.scheduleCreate, AbstractModel.scheduleCreate,
             run_map, run_bind, run_getSchedule, run_setSchedule, run_pure, Id.run]
  rw [hsid]
  cases hfM : sM.schedules.find? (·.id == req.id) with
  | some sch => exact ⟨rfl, h⟩
  | none => exact ⟨rfl, REq_setSchedule h _⟩

theorem agrees_scheduleDelete (req) : Agrees (.api (.scheduleDelete req)) := by
  intro n sP sM h
  have hsid : sP.schedules.find? (·.id == req.id) = sM.schedules.find? (·.id == req.id) :=
    h.2.2 req.id
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.scheduleDelete, AbstractModel.scheduleDelete,
             run_map, run_bind, run_getSchedule, run_delSchedule, run_pure, Id.run]
  rw [hsid]
  cases hfM : sM.schedules.find? (·.id == req.id) with
  | none => exact ⟨rfl, h⟩
  | some sch => exact ⟨rfl, REq_delSchedule h _⟩

/-! ### 5e. `promiseGet` — THE TEMPLATE: view served vs view persisted

The P side answers from `pLook` and writes nothing; the M side answers
from the same `pLook` (the touch serves the projection it persists)
and its write is `REq_touchWrite`-invisible. Every remaining promise
and task handler is this argument, composed with more effects. -/

theorem agrees_promiseGet (req) : Agrees (.api (.promiseGet req)) := by
  intro n sP sM h
  have hpi : (sP.promises.find? (·.id == req.id)).map (·.project n)
      = (sM.promises.find? (·.id == req.id)).map (·.project n) := h.1 req.id
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.promiseGet, AbstractModel.promiseGet,
             run_map, run_bind, run_viewPromise, run_touchPromise, run_pure, Id.run]
  cases hfP : sP.promises.find? (·.id == req.id) with
  | none =>
      cases hfM : sM.promises.find? (·.id == req.id) with
      | none => exact ⟨rfl, h⟩
      | some pM => rw [hfP, hfM] at hpi; simp at hpi
  | some pP =>
      cases hfM : sM.promises.find? (·.id == req.id) with
      | none => rw [hfP, hfM] at hpi; simp at hpi
      | some pM =>
          rw [hfP, hfM] at hpi
          simp only [Option.map] at hpi
          have hproj : pP.project n = pM.project n := Option.some.inj hpi
          have hpm : (pM.id == req.id) = true :=
            find?_sat (fun x => x.id == req.id) sM.promises pM hfM
          have hpe : pM.id = req.id := eq_of_beq hpm
          have hf' : sM.promises.find? (·.id == pM.id) = some pM := by
            rw [hpe]; exact hfM
          simp only [Option.map, run_pure]
          refine ⟨?_, ?_⟩
          · rw [hproj]
          · by_cases hc : (((pM.project n).state != pM.state) = true)
            · rw [if_pos hc]
              exact REq_touchWrite h pM hf'
            · rw [if_neg hc]
              exact h

/-! ### 5f. More pure facts: pending views, self-inequality, ids -/

theorem pstate_bne_self (s : PromiseState) : (s != s) = false := by
  cases s <;> rfl

theorem addCallback_id (p : PromiseObject) (a : String) :
    (p.addCallback a).id = p.id := by
  unfold PromiseObject.addCallback; split <;> rfl

theorem addListener_id (p : PromiseObject) (a : String) :
    (p.addListener a).id = p.id := by
  unfold PromiseObject.addListener; split <;> rfl

theorem addCallback_state (p : PromiseObject) (a : String) :
    (p.addCallback a).state = p.state := by
  unfold PromiseObject.addCallback; split <;> rfl

/-- Where `pLook` is empty, `tLook` is the raw find. -/
theorem tLook_raw {n : Nat} {s : ServerState} {id : String}
    (h : pLook n s id = none) : tLook n s id = s.tasks.find? (·.id == id) := by
  unfold tLook
  rw [h]
  cases hf : s.tasks.find? (·.id == id) with
  | none => rfl
  | some t => simp only [Option.map, applyView]

/-- Where `pLook` is pending, the view is the identity and `tLook` is
    the raw find. -/
theorem tLook_pending {n : Nat} {s : ServerState} {id : String} {p : PromiseObject}
    (h : pLook n s id = some p) (hp : (p.state == PromiseState.pending) = true) :
    tLook n s id = s.tasks.find? (·.id == id) := by
  unfold tLook
  rw [h]
  cases hf : s.tasks.find? (·.id == id) with
  | none => rfl
  | some t =>
      simp only [Option.map, applyView]
      rw [view_of_pending t p hp]

/-! ### 5g. Write congruences for the SAME record on both sides -/

/-- The same promise upsert on both sides preserves the invariant,
    PROVIDED the raw task finds at that id agree — which the callers
    obtain from `tLook_raw` (fresh id) or `tLook_pending` (the write
    happens under a pending guard). -/
theorem REq_setPromise {n : Nat} {sP sM : ServerState} (h : REq n sP sM)
    (q : PromiseObject)
    (htq : sP.tasks.find? (·.id == q.id) = sM.tasks.find? (·.id == q.id)) :
    REq n { sP with promises := q :: sP.promises.filter (·.id != q.id) }
          { sM with promises := q :: sM.promises.filter (·.id != q.id) } := by
  obtain ⟨hp, ht, hs⟩ := h
  have hpu : ∀ (s : ServerState) (id : String),
      pLook n { s with promises := q :: s.promises.filter (·.id != q.id) } id
        = if (q.id == id) = true then some (q.project n) else pLook n s id := by
    intro s id
    show ((q :: s.promises.filter (fun y => y.id != q.id)).find? (fun y => y.id == id)).map (·.project n)
        = if (q.id == id) = true then some (q.project n) else pLook n s id
    rw [find?_upsert PromiseObject.id q s.promises id]
    by_cases hc : (q.id == id) = true
    · rw [if_pos hc, if_pos hc]; rfl
    · rw [if_neg hc, if_neg hc]; rfl
  refine ⟨fun id => ?_, fun id => ?_, hs⟩
  · rw [hpu sP id, hpu sM id]
    by_cases hc : (q.id == id) = true
    · rw [if_pos hc, if_pos hc]
    · rw [if_neg hc, if_neg hc]; exact hp id
  · show (sP.tasks.find? (fun y => y.id == id)).map
          (fun t => applyView t (pLook n { sP with promises := q :: sP.promises.filter (·.id != q.id) } id))
        = (sM.tasks.find? (fun y => y.id == id)).map
          (fun t => applyView t (pLook n { sM with promises := q :: sM.promises.filter (·.id != q.id) } id))
    rw [hpu sP id, hpu sM id]
    by_cases hc : (q.id == id) = true
    · rw [if_pos hc, if_pos hc, ← eq_of_beq hc, htq]
    · rw [if_neg hc, if_neg hc, hp id]
      have ht' := ht id
      unfold tLook at ht'
      rw [hp id] at ht'
      exact ht'

/-- The same task upsert on both sides preserves the invariant —
    unconditionally: the written record and the (agreeing) `pLook`
    determine the new `tLook` at that id. -/
theorem REq_setTask {n : Nat} {sP sM : ServerState} (h : REq n sP sM)
    (tk : TaskObject) :
    REq n { sP with tasks := tk :: sP.tasks.filter (·.id != tk.id) }
          { sM with tasks := tk :: sM.tasks.filter (·.id != tk.id) } := by
  obtain ⟨hp, ht, hs⟩ := h
  refine ⟨hp, fun id => ?_, hs⟩
  show ((tk :: sP.tasks.filter (fun y => y.id != tk.id)).find? (fun y => y.id == id)).map
        (fun t => applyView t (pLook n sP id))
      = ((tk :: sM.tasks.filter (fun y => y.id != tk.id)).find? (fun y => y.id == id)).map
        (fun t => applyView t (pLook n sM id))
  rw [find?_upsert TaskObject.id tk sP.tasks id, find?_upsert TaskObject.id tk sM.tasks id, hp id]
  by_cases hc : (tk.id == id) = true
  · rw [if_pos hc, if_pos hc]
  · rw [if_neg hc, if_neg hc]
    have ht' := ht id
    unfold tLook at ht'
    rw [hp id] at ht'
    exact ht'

/-- The same COUPLED settle on both sides preserves the invariant:
    the promise upsert by `REq_setPromise`, and each side's co-keyed
    task fulfillment by `REq_touchTaskWrite`. This is the storage
    invariant's congruence — settling writes both objects, and both
    writes are invisible through the view. -/
theorem REq_setSettled {n : Nat} {sP sM : ServerState} (h : REq n sP sM)
    (q : PromiseObject) (hq : (q.state != PromiseState.pending) = true)
    (hqp : q.project n = q)
    (htq : sP.tasks.find? (·.id == q.id) = sM.tasks.find? (·.id == q.id)) :
    REq n (touchWrite n q sP) (touchWrite n q sM) := by
  have h1 : REq n { sP with promises := q :: sP.promises.filter (·.id != q.id) }
                  { sM with promises := q :: sM.promises.filter (·.id != q.id) } :=
    REq_setPromise h q htq
  have side : ∀ (s : ServerState), REq n { s with promises := q :: s.promises.filter (·.id != q.id) }
      (touchWrite n q s) := by
    intro s
    have hpl : pLook n { s with promises := q :: s.promises.filter (·.id != q.id) } q.id = some q := by
      show ((q :: s.promises.filter (fun y => y.id != q.id)).find? (fun y => y.id == q.id)).map (·.project n) = _
      rw [find?_upsert PromiseObject.id q s.promises q.id, if_pos (by simp)]
      simp [hqp]
    unfold touchWrite
    simp only [hqp, if_pos hq]
    cases hft : s.tasks.find? (·.id == q.id) with
    | none => simp only [hft]; exact REq.refl _ _
    | some t =>
        by_cases htf : ((t.state != TaskState.fulfilled) = true)
        · simp only [hft, if_pos htf]
          exact REq_touchTaskWrite (REq.refl n _) t q.id hft q hpl
            (beq_false_of_bne hq) (beq_false_of_bne htf)
        · simp only [hft, if_neg htf]; exact REq.refl _ _
  exact ((side sP).symm.trans h1).trans (side sM)

/-! ### 5h. The writing promise handlers -/

theorem runAgrees_promiseCreate (req : ServerModel.PromiseCreateReq) (n : Nat)
    {sP sM : ServerState} (h : REq n sP sM) :
    ((Projected.promiseCreate req n).run sP).1 = ((AbstractModel.promiseCreate req n).run sM).1
      ∧ REq n ((Projected.promiseCreate req n).run sP).2 ((AbstractModel.promiseCreate req n).run sM).2 := by
  have hpi : (sP.promises.find? (·.id == req.id)).map (·.project n)
      = (sM.promises.find? (·.id == req.id)).map (·.project n) := h.1 req.id
  simp only [Projected.promiseCreate, AbstractModel.promiseCreate, run_bind,
             run_viewPromise, run_touchPromise, run_pure]
  cases hfP : sP.promises.find? (·.id == req.id) with
  | some pP =>
      cases hfM : sM.promises.find? (·.id == req.id) with
      | none => rw [hfP, hfM] at hpi; simp at hpi
      | some pM =>
          rw [hfP, hfM] at hpi
          simp only [Option.map] at hpi
          have hproj : pP.project n = pM.project n := Option.some.inj hpi
          have hpm : (pM.id == req.id) = true :=
            find?_sat (fun x => x.id == req.id) sM.promises pM hfM
          have hf' : sM.promises.find? (·.id == pM.id) = some pM := by
            rw [eq_of_beq hpm]; exact hfM
          simp only [Option.map, run_pure]
          refine ⟨by rw [hproj], ?_⟩
          by_cases hc : (((pM.project n).state != pM.state) = true)
          · rw [if_pos hc]; exact REq_touchWrite h pM hf'
          · rw [if_neg hc]; exact h
  | none =>
      cases hfM : sM.promises.find? (·.id == req.id) with
      | some pM => rw [hfP, hfM] at hpi; simp at hpi
      | none =>
          have hplP : pLook n sP req.id = none := by unfold pLook; rw [hfP]; rfl
          have hplM : pLook n sM req.id = none := by unfold pLook; rw [hfM]; rfl
          have htq : sP.tasks.find? (·.id == req.id) = sM.tasks.find? (·.id == req.id) := by
            rw [← tLook_raw hplP, ← tLook_raw hplM]; exact h.2.1 req.id
          simp only [Option.map]
          by_cases hto : req.timeoutAt > n
          · simp only [if_pos hto]
            by_cases htag : (req.tags.has "resonate:target") = true
            · simp only [if_pos htag]
              exact ⟨rfl, REq_setTask (REq_setPromise h
                ({ id := req.id, state := PromiseState.pending, param := req.param,
                   tags := req.tags, timeoutAt := req.timeoutAt, createdAt := n } : PromiseObject) htq) _⟩
            · simp only [if_neg htag]
              exact ⟨rfl, REq_setPromise h
                ({ id := req.id, state := PromiseState.pending, param := req.param,
                   tags := req.tags, timeoutAt := req.timeoutAt, createdAt := n } : PromiseObject) htq⟩
          · simp only [if_neg hto]
            by_cases htag : (req.tags.has "resonate:target") = true
            · simp only [if_pos htag]
              exact ⟨rfl, REq_setTask (REq_setPromise h
                ({ id := req.id,
                   state := if req.tags.isTimer then PromiseState.resolved else PromiseState.rejectedTimedout,
                   param := req.param, tags := req.tags, timeoutAt := req.timeoutAt,
                   createdAt := req.timeoutAt, settledAt := some req.timeoutAt } : PromiseObject) htq) _⟩
            · simp only [if_neg htag]
              exact ⟨rfl, REq_setPromise h
                ({ id := req.id,
                   state := if req.tags.isTimer then PromiseState.resolved else PromiseState.rejectedTimedout,
                   param := req.param, tags := req.tags, timeoutAt := req.timeoutAt,
                   createdAt := req.timeoutAt, settledAt := some req.timeoutAt } : PromiseObject) htq⟩

theorem runAgrees_promiseSettle (req : ServerModel.PromiseSettleReq) (n : Nat)
    {sP sM : ServerState} (h : REq n sP sM) :
    ((Projected.promiseSettle req n).run sP).1 = ((AbstractModel.promiseSettle req n).run sM).1
      ∧ REq n ((Projected.promiseSettle req n).run sP).2 ((AbstractModel.promiseSettle req n).run sM).2 := by
  simp only [Projected.promiseSettle, AbstractModel.promiseSettle]
  by_cases hset : (!req.state.settable) = true
  · simp only [if_pos hset]; exact ⟨rfl, h⟩
  · simp only [if_neg hset]
    simp only [run_bind, run_viewPromise, run_touchPromise, run_pure]
    have hpi : (sP.promises.find? (·.id == req.id)).map (·.project n)
        = (sM.promises.find? (·.id == req.id)).map (·.project n) := h.1 req.id
    cases hfP : sP.promises.find? (·.id == req.id) with
    | none =>
        cases hfM : sM.promises.find? (·.id == req.id) with
        | none => exact ⟨rfl, h⟩
        | some pM => rw [hfP, hfM] at hpi; simp at hpi
    | some pP =>
        cases hfM : sM.promises.find? (·.id == req.id) with
        | none => rw [hfP, hfM] at hpi; simp at hpi
        | some pM =>
            rw [hfP, hfM] at hpi
            simp only [Option.map] at hpi
            have hproj : pP.project n = pM.project n := Option.some.inj hpi
            have hpm : (pM.id == req.id) = true :=
              find?_sat (fun x => x.id == req.id) sM.promises pM hfM
            simp only [Option.map]
            rw [hproj]
            by_cases hpend : (((pM.project n).state == PromiseState.pending) = true)
            · have hpp : pM.project n = pM := project_of_pending hpend
              rw [hpp] at hpend ⊢
              simp only [if_pos hpend, if_neg (by simp [pstate_bne_self] : ¬((pM.state != pM.state) = true))]
              have hplP : pLook n sP req.id = some pM := by
                unfold pLook; rw [hfP]; simp only [Option.map]; rw [hproj, hpp]
              have hplM : pLook n sM req.id = some pM := by
                unfold pLook; rw [hfM]; simp only [Option.map]; rw [hpp]
              have htq0 : sP.tasks.find? (·.id == req.id) = sM.tasks.find? (·.id == req.id) := by
                rw [← tLook_pending hplP hpend, ← tLook_pending hplM hpend]; exact h.2.1 req.id
              have htq : sP.tasks.find? (·.id == ({ pM with state := req.state, value := req.value, settledAt := some n } : PromiseObject).id)
                  = sM.tasks.find? (·.id == ({ pM with state := req.state, value := req.value, settledAt := some n } : PromiseObject).id) := by
                show sP.tasks.find? (·.id == pM.id) = sM.tasks.find? (·.id == pM.id)
                rw [eq_of_beq hpm]; exact htq0
              have hqs : (({ pM with state := req.state, value := req.value, settledAt := some n } : PromiseObject).state
                  != PromiseState.pending) = true :=
                settable_ne_pending (by
                  cases hs : req.state.settable with
                  | true => rfl
                  | false => rw [hs] at hset; simp at hset)
              have hqp : ({ pM with state := req.state, value := req.value, settledAt := some n } : PromiseObject).project n
                  = ({ pM with state := req.state, value := req.value, settledAt := some n } : PromiseObject) :=
                project_not_pending _ n (beq_false_of_bne hqs)
              simp only [run_bind, run_setSettled _ n _ hqs hqp, run_pure]
              exact ⟨trivial, REq_setSettled h _ hqs hqp htq⟩
            · simp only [if_neg hpend]
              have hf' : sM.promises.find? (·.id == pM.id) = some pM := by
                rw [eq_of_beq hpm]; exact hfM
              refine ⟨rfl, ?_⟩
              by_cases hc : (((pM.project n).state != pM.state) = true)
              · rw [if_pos hc]; exact REq_touchWrite h pM hf'
              · rw [if_neg hc]; exact h

theorem agrees_promiseCreate (req) : Agrees (.api (.promiseCreate req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, run_map, Id.run]
  exact And.imp (congrArg Equivalence.Response.promiseCreate) (fun x => x)
    (runAgrees_promiseCreate req n h)

theorem agrees_promiseSettle (req) : Agrees (.api (.promiseSettle req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, run_map, Id.run]
  exact And.imp (congrArg Equivalence.Response.promiseSettle) (fun x => x)
    (runAgrees_promiseSettle req n h)

theorem agrees_promiseRegisterListener (req) :
    Agrees (.api (.promiseRegisterListener req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.promiseRegisterListener,
             AbstractModel.promiseRegisterListener, run_map, Id.run]
  by_cases haddr : (!ServerModel.addressValid req.address) = true
  · simp only [if_pos haddr]; exact ⟨rfl, h⟩
  · simp only [if_neg haddr]
    simp only [run_bind, run_viewPromise, run_touchPromise, run_pure]
    have hpi : (sP.promises.find? (·.id == req.awaited)).map (·.project n)
        = (sM.promises.find? (·.id == req.awaited)).map (·.project n) := h.1 req.awaited
    cases hfP : sP.promises.find? (·.id == req.awaited) with
    | none =>
        cases hfM : sM.promises.find? (·.id == req.awaited) with
        | none => exact ⟨rfl, h⟩
        | some pM => rw [hfP, hfM] at hpi; simp at hpi
    | some pP =>
        cases hfM : sM.promises.find? (·.id == req.awaited) with
        | none => rw [hfP, hfM] at hpi; simp at hpi
        | some pM =>
            rw [hfP, hfM] at hpi
            simp only [Option.map] at hpi
            have hproj : pP.project n = pM.project n := Option.some.inj hpi
            have hpm : (pM.id == req.awaited) = true :=
              find?_sat (fun x => x.id == req.awaited) sM.promises pM hfM
            have hf' : sM.promises.find? (·.id == pM.id) = some pM := by
              rw [eq_of_beq hpm]; exact hfM
            simp only [Option.map]
            rw [hproj]
            by_cases hext : (!(pM.project n).external) = true
            · simp only [if_pos hext]
              refine ⟨rfl, ?_⟩
              by_cases hc : (((pM.project n).state != pM.state) = true)
              · rw [if_pos hc]; exact REq_touchWrite h pM hf'
              · rw [if_neg hc]; exact h
            · simp only [if_neg hext]
              by_cases hpend : (((pM.project n).state == PromiseState.pending) = true)
              · have hpp : pM.project n = pM := project_of_pending hpend
                rw [hpp] at hpend ⊢
                simp only [if_pos hpend, if_neg (by simp [pstate_bne_self] : ¬((pM.state != pM.state) = true))]
                have hplP : pLook n sP req.awaited = some pM := by
                  unfold pLook; rw [hfP]; simp only [Option.map]; rw [hproj, hpp]
                have hplM : pLook n sM req.awaited = some pM := by
                  unfold pLook; rw [hfM]; simp only [Option.map]; rw [hpp]
                have htq0 : sP.tasks.find? (·.id == req.awaited)
                    = sM.tasks.find? (·.id == req.awaited) := by
                  rw [← tLook_pending hplP hpend, ← tLook_pending hplM hpend]
                  exact h.2.1 req.awaited
                have htq : sP.tasks.find? (·.id == (pM.addListener req.address).id)
                    = sM.tasks.find? (·.id == (pM.addListener req.address).id) := by
                  rw [addListener_id, eq_of_beq hpm]; exact htq0
                exact ⟨rfl, REq_setPromise h (pM.addListener req.address) htq⟩
              · simp only [if_neg hpend]
                refine ⟨rfl, ?_⟩
                by_cases hc : (((pM.project n).state != pM.state) = true)
                · rw [if_pos hc]; exact REq_touchWrite h pM hf'
                · rw [if_neg hc]; exact h

theorem agrees_promiseRegisterCallback (req) :
    Agrees (.api (.promiseRegisterCallback req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.promiseRegisterCallback,
             AbstractModel.promiseRegisterCallback, run_map, Id.run]
  by_cases heq : (req.awaited == req.awaiter) = true
  · simp only [if_pos heq]; exact ⟨rfl, h⟩
  · simp only [if_neg heq]
    simp only [run_bind, run_viewPromise, run_touchPromise, run_pure]
    have hpi : (sP.promises.find? (·.id == req.awaited)).map (·.project n)
        = (sM.promises.find? (·.id == req.awaited)).map (·.project n) := h.1 req.awaited
    cases hfP : sP.promises.find? (·.id == req.awaited) with
    | none =>
        cases hfM : sM.promises.find? (·.id == req.awaited) with
        | none => exact ⟨rfl, h⟩
        | some pM => rw [hfP, hfM] at hpi; simp at hpi
    | some pP =>
        cases hfM : sM.promises.find? (·.id == req.awaited) with
        | none => rw [hfP, hfM] at hpi; simp at hpi
        | some pM =>
            rw [hfP, hfM] at hpi
            simp only [Option.map] at hpi
            have hproj : pP.project n = pM.project n := Option.some.inj hpi
            have hpm : (pM.id == req.awaited) = true :=
              find?_sat (fun x => x.id == req.awaited) sM.promises pM hfM
            have hf' : sM.promises.find? (·.id == pM.id) = some pM := by
              rw [eq_of_beq hpm]; exact hfM
            simp only [Option.map]
            rw [hproj]
            -- Expose the second read's finds.
            simp only [run_bind, run_viewPromise, run_touchPromise, run_pure]
            -- Abstract the awaited-touch write into one opaque state.
            generalize hsM1 :
              (if (((pM.project n).state != pM.state) = true) then
                touchWrite n pM sM
              else sM) = sM1
            have h1 : REq n sP sM1 := by
              rw [← hsM1]
              by_cases hc : (((pM.project n).state != pM.state) = true)
              · rw [if_pos hc]; exact REq_touchWrite h pM hf'
              · rw [if_neg hc]; exact h
            -- The awaiter read, over (sP, sM1).
            have hpi2 : (sP.promises.find? (·.id == req.awaiter)).map (·.project n)
                = (sM1.promises.find? (·.id == req.awaiter)).map (·.project n) :=
              h1.1 req.awaiter
            cases hfP2 : sP.promises.find? (·.id == req.awaiter) with
            | none =>
                cases hfM2 : sM1.promises.find? (·.id == req.awaiter) with
                | none => exact ⟨rfl, h1⟩
                | some pM2 => rw [hfP2, hfM2] at hpi2; simp at hpi2
            | some pP2 =>
                cases hfM2 : sM1.promises.find? (·.id == req.awaiter) with
                | none => rw [hfP2, hfM2] at hpi2; simp at hpi2
                | some pM2 =>
                    rw [hfP2, hfM2] at hpi2
                    simp only [Option.map] at hpi2
                    have hproj2 : pP2.project n = pM2.project n := Option.some.inj hpi2
                    have hpm2 : (pM2.id == req.awaiter) = true :=
                      find?_sat (fun x => x.id == req.awaiter) sM1.promises pM2 hfM2
                    have hf2' : sM1.promises.find? (·.id == pM2.id) = some pM2 := by
                      rw [eq_of_beq hpm2]; exact hfM2
                    simp only [Option.map]
                    rw [hproj2]
                    -- Abstract the awaiter-touch write too.
                    generalize hsM2 :
                      (if (((pM2.project n).state != pM2.state) = true) then
                        touchWrite n pM2 sM1
                      else sM1) = sM2
                    have h2 : REq n sP sM2 := by
                      rw [← hsM2]
                      by_cases hc : (((pM2.project n).state != pM2.state) = true)
                      · rw [if_pos hc]; exact REq_touchWrite h1 pM2 hf2'
                      · rw [if_neg hc]; exact h1
                    by_cases htag : (!((pM2.project n).tags.has "resonate:target")) = true
                    · simp only [if_pos htag]; exact ⟨rfl, h2⟩
                    · simp only [if_neg htag]
                      by_cases hext : (!(pM.project n).external) = true
                      · simp only [if_pos hext]; exact ⟨rfl, h2⟩
                      · simp only [if_neg hext]
                        by_cases hpend : (((pM.project n).state == PromiseState.pending) = true)
                        · have hpp : pM.project n = pM := project_of_pending hpend
                          rw [hpp] at hpend ⊢
                          simp only [if_pos hpend]
                          by_cases hpend2 : (((pM2.project n).state == PromiseState.pending) = true)
                          · simp only [if_pos hpend2]
                            have hplP : pLook n sP req.awaited = some pM := by
                              unfold pLook; rw [hfP]; simp only [Option.map]; rw [hproj, hpp]
                            have hplM2 : pLook n sM2 req.awaited = some pM := by
                              rw [← h2.1 req.awaited]; exact hplP
                            have htq0 : sP.tasks.find? (·.id == req.awaited)
                                = sM2.tasks.find? (·.id == req.awaited) := by
                              rw [← tLook_pending hplP hpend, ← tLook_pending hplM2 hpend]
                              exact h2.2.1 req.awaited
                            have htq : sP.tasks.find? (·.id == (pM.addCallback req.awaiter).id)
                                = sM2.tasks.find? (·.id == (pM.addCallback req.awaiter).id) := by
                              rw [addCallback_id, eq_of_beq hpm]; exact htq0
                            exact ⟨rfl, REq_setPromise h2 (pM.addCallback req.awaiter) htq⟩
                          · simp only [if_neg hpend2]
                            exact ⟨rfl, h2⟩
                        · simp only [if_neg hpend]
                          exact ⟨rfl, h2⟩

/-! ### 5i. The task read, characterized

`viewTask` serves the pair (fact-T task, fact-P promise); `touchTask`
serves the SAME pair and persists both facts — the promise touch and,
where the view fulfills, the fulfilled task. Note both consult the
promise at `t.id` — the task's own id, which the find guarantees equals
the requested id only propositionally. -/

theorem run_viewTask (id : String) (n : Nat) (s : ServerState) :
    (viewTask id n).run s =
      (match s.tasks.find? (·.id == id) with
       | none => (none, s)
       | some t =>
           match s.promises.find? (·.id == t.id) with
           | none => (some (t, none), s)
           | some p => (some (t.view (p.project n), some (p.project n)), s)) := by
  cases hf : s.tasks.find? (·.id == id) with
  | none => simp only [viewTask, run_bind, run_getTask, hf, run_pure]
  | some t =>
      cases hp : s.promises.find? (·.id == t.id) with
      | none => simp only [viewTask, run_bind, run_getTask, hf, run_getPromise, hp, run_pure]
      | some p => simp only [viewTask, run_bind, run_getTask, hf, run_getPromise, hp, run_pure]

theorem run_touchTask (id : String) (n : Nat) (s : ServerState) :
    (touchTask id n).run s =
      match s.tasks.find? (·.id == id) with
      | none => (none, s)
      | some t =>
        match s.promises.find? (·.id == t.id) with
        | none => (some (t, none), s)
        | some p =>
            if (((p.project n).state != PromiseState.pending) = true
                  ∧ (t.state != TaskState.fulfilled) = true) then
              (some (t.fulfill, some (p.project n)),
               { (if (((p.project n).state != p.state) = true) then
                    touchWrite n p s
                  else s) with
                 tasks := t.fulfill :: (if (((p.project n).state != p.state) = true) then
                    touchWrite n p s
                  else s).tasks.filter (·.id != t.fulfill.id) })
            else
              (some (t, some (p.project n)),
               (if (((p.project n).state != p.state) = true) then
                  touchWrite n p s
                else s)) := by
  cases hf : s.tasks.find? (·.id == id) with
  | none => simp only [touchTask, run_bind, run_getTask, hf, run_pure]
  | some t =>
      cases hp : s.promises.find? (·.id == t.id) with
      | none =>
          simp only [touchTask, run_bind, run_getTask, hf, run_touchPromise, hp, run_pure]
      | some p =>
          by_cases hcond : (((p.project n).state != PromiseState.pending) = true
              ∧ (t.state != TaskState.fulfilled) = true)
          · simp only [touchTask, run_bind, run_getTask, hf, run_touchPromise, hp,
                       if_pos hcond, run_setTask, run_pure]
          · simp only [touchTask, run_bind, run_getTask, hf, run_touchPromise, hp,
                       if_neg hcond, run_pure]

theorem view_id (t : TaskObject) (p : PromiseObject) : (t.view p).id = t.id := by
  unfold TaskObject.view; split <;> rfl

theorem run_pureUnit_bind {α} (f : PUnit → M α) (s : ServerState) :
    ((pure PUnit.unit >>= f) : M α).run s = (f PUnit.unit).run s := rfl

/-- What the read guarantees about the states the CONTINUATIONS run
    on: for each shape of the value, the lookups that produced it —
    stable under the touch writes, so they hold of the M side's
    post-read state as well. -/
def ReadFacts (n : Nat) (id : String) (v : Option (TaskObject × Option PromiseObject))
    (sP' sM' : ServerState) : Prop :=
  match v with
  | none => True
  | some (t, none) =>
      t.id = id ∧ pLook n sP' id = none ∧ pLook n sM' id = none
        ∧ sP'.tasks.find? (·.id == id) = some t ∧ sM'.tasks.find? (·.id == id) = some t
  | some (t, some pv) =>
      t.id = id ∧ pv.id = id
        ∧ pLook n sP' id = some pv ∧ pLook n sM' id = some pv
        ∧ tLook n sP' id = some t ∧ tLook n sM' id = some t

/-- **The task-read agreement, once and for all.** The projected read
    and the touch produce the SAME value on `REq`-related states, the
    touch writes are invariant-invisible, and the continuations
    inherit the lookups behind the value. Every task handler is this
    lemma plus a per-continuation argument. -/
theorem bind_taskRead_agrees {α : Type} (id : String) (n : Nat)
    (kP kM : Option (TaskObject × Option PromiseObject) → M α)
    {sP sM : ServerState} (h : REq n sP sM)
    (hk : ∀ v sP' sM', REq n sP' sM' → ReadFacts n id v sP' sM' →
      ((kP v).run sP').1 = ((kM v).run sM').1
        ∧ REq n ((kP v).run sP').2 ((kM v).run sM').2) :
    ((viewTask id n >>= kP).run sP).1 = ((touchTask id n >>= kM).run sM).1
      ∧ REq n ((viewTask id n >>= kP).run sP).2 ((touchTask id n >>= kM).run sM).2 := by
  rw [run_bind, run_bind, run_viewTask, run_touchTask]
  have hti : (sP.tasks.find? (·.id == id)).map (fun t => applyView t (pLook n sP id))
      = (sM.tasks.find? (·.id == id)).map (fun t => applyView t (pLook n sM id)) :=
    h.2.1 id
  cases hfP : sP.tasks.find? (·.id == id) with
  | none =>
      cases hfM : sM.tasks.find? (·.id == id) with
      | none => exact hk none sP sM h trivial
      | some tM => rw [hfP, hfM] at hti; simp at hti
  | some tP =>
      cases hfM : sM.tasks.find? (·.id == id) with
      | none => rw [hfP, hfM] at hti; simp at hti
      | some tM =>
          rw [hfP, hfM] at hti
          simp only [Option.map] at hti
          have hvv0 : applyView tP (pLook n sP id) = applyView tM (pLook n sM id) :=
            Option.some.inj hti
          have htPid : tP.id = id :=
            eq_of_beq (find?_sat (fun y => y.id == id) sP.tasks tP hfP)
          have htMid : tM.id = id :=
            eq_of_beq (find?_sat (fun y => y.id == id) sM.tasks tM hfM)
          dsimp only
          rw [htPid, htMid]
          have hpp : (sP.promises.find? (·.id == id)).map (·.project n)
              = (sM.promises.find? (·.id == id)).map (·.project n) := h.1 id
          cases hpP : sP.promises.find? (·.id == id) with
          | none =>
              cases hpM : sM.promises.find? (·.id == id) with
              | none =>
                  -- raw values, and they agree
                  have hplP : pLook n sP id = none := by unfold pLook; rw [hpP]; rfl
                  have hplM : pLook n sM id = none := by unfold pLook; rw [hpM]; rfl
                  rw [hplP, hplM] at hvv0
                  simp only [applyView] at hvv0
                  rw [hvv0]
                  exact hk (some (tM, none)) sP sM h
                    ⟨htMid, hplP, hplM, by rw [← hvv0]; exact hfP, hfM⟩
              | some pM0 => rw [hpP, hpM] at hpp; simp at hpp
          | some pP0 =>
              cases hpM : sM.promises.find? (·.id == id) with
              | none => rw [hpP, hpM] at hpp; simp at hpp
              | some pM0 =>
                  rw [hpP, hpM] at hpp
                  simp only [Option.map] at hpp
                  have hpv : pP0.project n = pM0.project n := Option.some.inj hpp
                  have hplP : pLook n sP id = some (pP0.project n) := by
                    unfold pLook; rw [hpP]; rfl
                  have hplM : pLook n sM id = some (pM0.project n) := by
                    unfold pLook; rw [hpM]; rfl
                  have hvv : tP.view (pM0.project n) = tM.view (pM0.project n) := by
                    rw [hplP, hplM, ← hpv] at hvv0
                    simp only [applyView] at hvv0
                    rw [← hpv]; exact hvv0
                  have hpvid : (pM0.project n).id = id := by
                    rw [project_id]
                    exact eq_of_beq (find?_sat (fun x => x.id == id) sM.promises pM0 hpM)
                  have htlP : tLook n sP id = some (tP.view (pM0.project n)) := by
                    unfold tLook; rw [hfP, hplP]
                    simp only [Option.map, applyView]
                    rw [hpv]
                  have hfM' : sM.promises.find? (·.id == pM0.id) = some pM0 := by
                    rw [eq_of_beq (find?_sat (fun x => x.id == id) sM.promises pM0 hpM)]
                    exact hpM
                  dsimp only
                  rw [hpv]
                  by_cases hcond : (((pM0.project n).state != PromiseState.pending) = true
                      ∧ (tM.state != TaskState.fulfilled) = true)
                  · simp only [if_pos hcond]
                    have hset : ((pM0.project n).state == PromiseState.pending) = false :=
                      beq_false_of_bne hcond.1
                    have htf' : (tM.state == TaskState.fulfilled) = false :=
                      beq_false_of_bne hcond.2
                    -- unify the value: P's view IS the fulfilled form
                    rw [hvv, view_settles tM (pM0.project n) hset htf']
                    -- Absorb the writes. When the touch fires, the COUPLED
                    -- write has already fulfilled the task, so touchTask's
                    -- own fulfill is a redundant re-upsert; when it does
                    -- not, the fulfill is the original materialization.
                    have hidf : tM.fulfill.id = (pM0.project n).id := by
                      show tM.id = (pM0.project n).id
                      rw [hpvid]; exact htMid
                    have hkey : REq n sP
                        ({ (if (((pM0.project n).state != pM0.state) = true) then touchWrite n pM0 sM else sM) with
                            tasks := tM.fulfill ::
                              (if (((pM0.project n).state != pM0.state) = true) then touchWrite n pM0 sM else sM).tasks.filter
                                (·.id != tM.fulfill.id) } : ServerState) := by
                      by_cases hc : (((pM0.project n).state != pM0.state) = true)
                      · simp only [if_pos hc]
                        refine REq_setTask_idem (REq_touchWrite h pM0 hfM') tM.fulfill ?_
                        rw [hidf]
                        exact touchWrite_task (bne_true_of_beq_false hset)
                          (by rw [hpvid]; exact hfM) (bne_true_of_beq_false htf')
                      · simp only [if_neg hc]
                        exact REq_touchTaskWrite h tM id hfM (pM0.project n) hplM hset htf'
                    generalize hsM1 : (if (((pM0.project n).state != pM0.state) = true) then
                          touchWrite n pM0 sM
                        else sM) = sM1
                    rw [hsM1] at hkey
                    have h1 : REq n sP sM1 := by
                      rw [← hsM1]
                      by_cases hc : (((pM0.project n).state != pM0.state) = true)
                      · simp only [if_pos hc]; exact REq_touchWrite h pM0 hfM'
                      · simp only [if_neg hc]; exact h
                    generalize hsM2 : ({ sM1 with
                        tasks := tM.fulfill :: sM1.tasks.filter (·.id != tM.fulfill.id) } : ServerState) = sM2
                    rw [hsM2] at hkey
                    have h2 : REq n sP sM2 := hkey
                    refine hk (some (tM.fulfill, some (pM0.project n))) sP sM2 h2
                      ⟨show tM.id = id from htMid, hpvid, ?_, ?_, ?_, ?_⟩
                    · rw [hplP, hpv]
                    · rw [← h2.1 id, hplP, hpv]
                    · rw [htlP, hvv, view_settles tM (pM0.project n) hset htf']
                    · rw [← h2.2.1 id, htlP, hvv, view_settles tM (pM0.project n) hset htf']
                  · simp only [if_neg hcond]
                    have hvm : tM.view (pM0.project n) = tM := by
                      by_cases hA : ((pM0.project n).state == PromiseState.pending) = true
                      · exact view_of_pending tM _ hA
                      · by_cases hB : (tM.state == TaskState.fulfilled) = true
                        · exact view_of_fulfilled tM _ hB
                        · exact absurd ⟨bne_true_of_beq_false (toFalse hA),
                            bne_true_of_beq_false (toFalse hB)⟩ hcond
                    rw [hvv, hvm]
                    generalize hsM1 : (if (((pM0.project n).state != pM0.state) = true) then
                          touchWrite n pM0 sM
                        else sM) = sM1
                    have h1 : REq n sP sM1 := by
                      rw [← hsM1]
                      by_cases hc : (((pM0.project n).state != pM0.state) = true)
                      · simp only [if_pos hc]; exact REq_touchWrite h pM0 hfM'
                      · simp only [if_neg hc]; exact h
                    refine hk (some (tM, some (pM0.project n))) sP sM1 h1
                      ⟨htMid, hpvid, ?_, ?_, ?_, ?_⟩
                    · rw [hplP, hpv]
                    · rw [← h1.1 id, hplP, hpv]
                    · rw [htlP, hvv, hvm]
                    · rw [← h1.2.1 id, htlP, hvv, hvm]

/-! ### 5j. `taskGet` — the task-side template -/

theorem agrees_taskGet (req) : Agrees (.api (.taskGet req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.taskGet,
             AbstractModel.taskGet, run_map, Id.run]
  refine And.imp (congrArg Equivalence.Response.taskGet) (fun x => x)
    (bind_taskRead_agrees req.id n _ _ h ?_)
  intro v sP' sM' h' hf
  cases v with
  | none => exact ⟨rfl, h'⟩
  | some pair =>
      obtain ⟨t, po⟩ := pair
      cases po with
      | none => exact ⟨rfl, h'⟩
      | some pv => exact ⟨rfl, h'⟩

/-! ### 5k. The linear task handlers — read, guard on the view, write
    the view-derived record. All continuation work, over
    `bind_taskRead_agrees`. -/

theorem agrees_taskAcquire (req) : Agrees (.api (.taskAcquire req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.taskAcquire,
             AbstractModel.taskAcquire, run_map, Id.run]
  refine And.imp (congrArg Equivalence.Response.taskAcquire) (fun x => x)
    (bind_taskRead_agrees req.id n _ _ h ?_)
  intro v sP' sM' h' hf
  cases v with
  | none => exact ⟨rfl, h'⟩
  | some pair =>
      obtain ⟨t, po⟩ := pair
      cases po with
      | none => exact ⟨rfl, h'⟩
      | some pv =>
          obtain ⟨hid, hpid, hplP', hplM', htlP', htlM'⟩ := hf
          dsimp only
          by_cases hg1 : (t.state != TaskState.pending) = true
          · simp only [if_pos hg1]; exact ⟨rfl, h'⟩
          · simp only [if_neg hg1, run_pureUnit_bind]
            by_cases hg2 : (pv.state != PromiseState.pending) = true
            · simp only [if_pos hg2]; exact ⟨rfl, h'⟩
            · simp only [if_neg hg2, run_pureUnit_bind]
              by_cases hg3 : (t.version != req.version) = true
              · simp only [if_pos hg3]; exact ⟨rfl, h'⟩
              · simp only [if_neg hg3, run_pureUnit_bind]
                exact ⟨rfl, REq_setTask h' _⟩

theorem agrees_taskRelease (req) : Agrees (.api (.taskRelease req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.taskRelease,
             AbstractModel.taskRelease, run_map, Id.run]
  refine And.imp (congrArg Equivalence.Response.taskRelease) (fun x => x)
    (bind_taskRead_agrees req.id n _ _ h ?_)
  intro v sP' sM' h' hf
  cases v with
  | none => exact ⟨rfl, h'⟩
  | some pair =>
      obtain ⟨t, po⟩ := pair
      cases po with
      | none => exact ⟨rfl, h'⟩
      | some pv =>
          obtain ⟨hid, hpid, hplP', hplM', htlP', htlM'⟩ := hf
          dsimp only
          by_cases hg1 : (t.state != TaskState.acquired) = true
          · simp only [if_pos hg1]; exact ⟨rfl, h'⟩
          · simp only [if_neg hg1, run_pureUnit_bind]
            by_cases hg2 : (pv.state != PromiseState.pending) = true
            · simp only [if_pos hg2]; exact ⟨rfl, h'⟩
            · simp only [if_neg hg2, run_pureUnit_bind]
              by_cases hg3 : (t.version != req.version) = true
              · simp only [if_pos hg3]; exact ⟨rfl, h'⟩
              · simp only [if_neg hg3, run_pureUnit_bind]
                exact ⟨rfl, REq_setTask h' _⟩

theorem agrees_taskHalt (req) : Agrees (.api (.taskHalt req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.taskHalt,
             AbstractModel.taskHalt, run_map, Id.run]
  refine And.imp (congrArg Equivalence.Response.taskHalt) (fun x => x)
    (bind_taskRead_agrees req.id n _ _ h ?_)
  intro v sP' sM' h' hf
  cases v with
  | none => exact ⟨rfl, h'⟩
  | some pair =>
      obtain ⟨t, po⟩ := pair
      cases po with
      | none => exact ⟨rfl, h'⟩
      | some pv =>
          obtain ⟨hid, hpid, hplP', hplM', htlP', htlM'⟩ := hf
          dsimp only
          by_cases hg1 : (t.state == TaskState.fulfilled) = true
          · simp only [if_pos hg1]; exact ⟨rfl, h'⟩
          · simp only [if_neg hg1, run_pureUnit_bind]
            by_cases hg2 : (t.state == TaskState.halted) = true
            · simp only [if_pos hg2]; exact ⟨rfl, h'⟩
            · simp only [if_neg hg2, run_pureUnit_bind]
              exact ⟨rfl, REq_setTask h' _⟩

theorem agrees_taskContinue (req) : Agrees (.api (.taskContinue req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.taskContinue,
             AbstractModel.taskContinue, run_map, Id.run]
  refine And.imp (congrArg Equivalence.Response.taskContinue) (fun x => x)
    (bind_taskRead_agrees req.id n _ _ h ?_)
  intro v sP' sM' h' hf
  cases v with
  | none => exact ⟨rfl, h'⟩
  | some pair =>
      obtain ⟨t, po⟩ := pair
      dsimp only
      by_cases hg1 : (t.state != TaskState.halted) = true
      · simp only [if_pos hg1]; exact ⟨rfl, h'⟩
      · simp only [if_neg hg1, run_pureUnit_bind]
        cases po with
        | none => exact ⟨rfl, h'⟩
        | some pv =>
            obtain ⟨hid, hpid, hplP', hplM', htlP', htlM'⟩ := hf
            by_cases hg2 : (pv.state != PromiseState.pending) = true
            · simp only [if_pos hg2]; exact ⟨rfl, h'⟩
            · simp only [if_neg hg2, run_pureUnit_bind]
              exact ⟨rfl, REq_setTask h' _⟩

theorem agrees_taskFulfill (req) : Agrees (.api (.taskFulfill req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.taskFulfill,
             AbstractModel.taskFulfill, run_map, Id.run]
  by_cases hset0 : (!req.action.state.settable) = true
  · simp only [if_pos hset0]; exact ⟨rfl, h⟩
  · simp only [if_neg hset0, run_pureUnit_bind]
    refine And.imp (congrArg Equivalence.Response.taskFulfill) (fun x => x)
      (bind_taskRead_agrees req.id n _ _ h ?_)
    intro v sP' sM' h' hf
    cases v with
    | none => exact ⟨rfl, h'⟩
    | some pair =>
        obtain ⟨t, po⟩ := pair
        cases po with
        | none => exact ⟨rfl, h'⟩
        | some pv =>
            obtain ⟨hid, hpid, hplP', hplM', htlP', htlM'⟩ := hf
            dsimp only
            by_cases hg1 : (t.state != TaskState.acquired) = true
            · simp only [if_pos hg1]; exact ⟨rfl, h'⟩
            · simp only [if_neg hg1, run_pureUnit_bind]
              by_cases hg2 : (pv.state != PromiseState.pending) = true
              · simp only [if_pos hg2]; exact ⟨rfl, h'⟩
              · simp only [if_neg hg2, run_pureUnit_bind]
                by_cases hg3 : (t.version != req.version) = true
                · simp only [if_pos hg3]; exact ⟨rfl, h'⟩
                · simp only [if_neg hg3, run_pureUnit_bind]
                  -- the write is on the promise: pv is PENDING here, so
                  -- both raw task finds are the viewed task
                  have hpend : (pv.state == PromiseState.pending) = true :=
                    beq_true_of_bne_false (toFalse hg2)
                  have hrP : sP'.tasks.find? (·.id == req.id) = some t := by
                    rw [← tLook_pending hplP' hpend]; exact htlP'
                  have hrM : sM'.tasks.find? (·.id == req.id) = some t := by
                    rw [← tLook_pending hplM' hpend]; exact htlM'
                  have htq : sP'.tasks.find? (·.id == ({ pv with state := req.action.state, value := req.action.value, settledAt := some n } : PromiseObject).id)
                      = sM'.tasks.find? (·.id == ({ pv with state := req.action.state, value := req.action.value, settledAt := some n } : PromiseObject).id) := by
                    show sP'.tasks.find? (·.id == pv.id) = sM'.tasks.find? (·.id == pv.id)
                    rw [hpid, hrP, hrM]
                  have hqs : (({ pv with state := req.action.state, value := req.action.value, settledAt := some n } : PromiseObject).state
                      != PromiseState.pending) = true :=
                    settable_ne_pending (by
                      cases hs : req.action.state.settable with
                      | true => rfl
                      | false => rw [hs] at hset0; simp at hset0)
                  have hqp : ({ pv with state := req.action.state, value := req.action.value, settledAt := some n } : PromiseObject).project n
                      = ({ pv with state := req.action.state, value := req.action.value, settledAt := some n } : PromiseObject) :=
                    project_not_pending _ n (beq_false_of_bne hqs)
                  simp only [run_bind, run_setSettled _ n _ hqs hqp, run_pure]
                  exact ⟨trivial, REq_setSettled h' _ hqs hqp htq⟩

/-! ### 5m. The touches are self-invisible

Instantiating the absorption lemmas at `REq.refl` says each touch is
invisible to the invariant RELATIVE TO ITS OWN STATE. With symmetry
and transitivity, the fact rules R1 and R2 — which BOTH machines run
identically — need no case analysis at all. -/

theorem REq_touchPromise_self (id : String) (n : Nat) (s : ServerState) :
    REq n s ((touchPromise id n).run s).2 := by
  rw [run_touchPromise]
  cases hf : s.promises.find? (·.id == id) with
  | none => exact REq.refl n s
  | some p =>
      have hf' : s.promises.find? (·.id == p.id) = some p := by
        rw [eq_of_beq (find?_sat (fun x => x.id == id) s.promises p hf)]; exact hf
      dsimp only
      by_cases hc : (((p.project n).state != p.state) = true)
      · simp only [if_pos hc]; exact REq_touchWrite (REq.refl n s) p hf'
      · simp only [if_neg hc]; exact REq.refl n s

theorem REq_touchTask_self (id : String) (n : Nat) (s : ServerState) :
    REq n s ((touchTask id n).run s).2 := by
  rw [run_touchTask]
  cases hf : s.tasks.find? (·.id == id) with
  | none => exact REq.refl n s
  | some t =>
      have htid : t.id = id := eq_of_beq (find?_sat (fun y => y.id == id) s.tasks t hf)
      dsimp only
      cases hp : s.promises.find? (·.id == t.id) with
      | none => exact REq.refl n s
      | some p =>
          have hp' : s.promises.find? (·.id == p.id) = some p := by
            rw [eq_of_beq (find?_sat (fun x => x.id == t.id) s.promises p hp)]; exact hp
          have hplid : pLook n s id = some (p.project n) := by
            unfold pLook; rw [← htid, hp]; rfl
          by_cases hcond : (((p.project n).state != PromiseState.pending) = true
              ∧ (t.state != TaskState.fulfilled) = true)
          · simp only [if_pos hcond]
            have hpid : p.id = t.id :=
              eq_of_beq (find?_sat (fun x => x.id == t.id) s.promises p hp)
            have hidf : t.fulfill.id = (p.project n).id := by
              show t.id = (p.project n).id
              rw [project_id, hpid]
            have hkey : REq n s
                ({ (if (((p.project n).state != p.state) = true) then touchWrite n p s else s) with
                    tasks := t.fulfill ::
                      (if (((p.project n).state != p.state) = true) then touchWrite n p s else s).tasks.filter
                        (·.id != t.fulfill.id) } : ServerState) := by
              by_cases hc : (((p.project n).state != p.state) = true)
              · simp only [if_pos hc]
                refine REq_setTask_idem (REq_touchWrite (REq.refl n s) p hp') t.fulfill ?_
                rw [hidf]
                exact touchWrite_task hcond.1
                  (by rw [project_id, hpid, htid]; exact hf) hcond.2
              · simp only [if_neg hc]
                exact REq_touchTaskWrite (REq.refl n s) t id hf (p.project n) hplid
                  (beq_false_of_bne hcond.1) (beq_false_of_bne hcond.2)
            exact hkey
          · simp only [if_neg hcond]
            by_cases hc : (((p.project n).state != p.state) = true)
            · simp only [if_pos hc]; exact REq_touchWrite (REq.refl n s) p hp'
            · simp only [if_neg hc]; exact REq.refl n s

/-! ### 5n. The fact rules R1 and R2 -/

theorem agrees_r1 (id : String) : Agrees (.r1 id) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Rules.promiseTimeout,
             run_bind, run_pure, Id.run]
  exact ⟨trivial, ((REq_touchPromise_self id n sP).symm.trans h).trans
    (REq_touchPromise_self id n sM)⟩

theorem agrees_r2 (id : String) : Agrees (.r2 id) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Rules.taskFulfillment,
             run_bind, run_pure, Id.run]
  exact ⟨trivial, ((REq_touchTask_self id n sP).symm.trans h).trans
    (REq_touchTask_self id n sM)⟩

/-! ### 5o. The choice rules R5 and R6

Both read the TASK raw — but their decisions go through the view, and
that is exactly what makes them agree: under a PENDING view the raw
tasks coincide (`tLook_pending`), so the sides make the same choice
with the same record; under a settled or absent view neither side
writes — TIMEOUT ALWAYS WINS extends to redispatch, mechanized. -/

theorem run_leaseExpiry (id : String) (n : Nat) (s : ServerState) :
    (Rules.leaseExpiry id n).run s = ((),
      match s.tasks.find? (·.id == id) with
      | none => s
      | some t =>
        match t.expiresAt with
        | none => s
        | some d =>
          if ((t.state == TaskState.acquired) = true ∧ d ≤ n) then
            match (s.promises.find? (·.id == t.id)).map (·.project n) with
            | none => s
            | some p =>
                if (p.state == PromiseState.pending) = true then
                  { s with tasks := ({ t with state := TaskState.pending, pid := none, ttl := none, expiresAt := none, retryAt := some n } : TaskObject) :: s.tasks.filter (·.id != ({ t with state := TaskState.pending, pid := none, ttl := none, expiresAt := none, retryAt := some n } : TaskObject).id) }
                else s
          else s) := by
  cases hf : s.tasks.find? (·.id == id) with
  | none => simp only [Rules.leaseExpiry, run_bind, run_getTask, hf, run_pure]
  | some t =>
      cases he : t.expiresAt with
      | none => simp only [Rules.leaseExpiry, run_bind, run_getTask, hf, he, run_pure]
      | some d =>
          by_cases hg : ((t.state == TaskState.acquired) = true ∧ d ≤ n)
          · cases hp : s.promises.find? (·.id == t.id) with
            | none =>
                simp only [Rules.leaseExpiry, run_bind, run_getTask, hf, he, if_pos hg,
                           run_viewPromise, hp, Option.map, run_pure]
            | some p0 =>
                by_cases hpe : (((p0.project n).state == PromiseState.pending) = true)
                · simp only [Rules.leaseExpiry, run_bind, run_getTask, hf, he, if_pos hg,
                             run_viewPromise, hp, Option.map, if_pos hpe, run_setTask, run_pure]
                · simp only [Rules.leaseExpiry, run_bind, run_getTask, hf, he, if_pos hg,
                             run_viewPromise, hp, Option.map, if_neg hpe, run_pure]
          · simp only [Rules.leaseExpiry, run_bind, run_getTask, hf, he, if_neg hg, run_pure]

theorem run_dispatch (id : String) (next : Nat) (n : Nat) (s : ServerState) :
    (Rules.dispatch id next n).run s = ((),
      match s.tasks.find? (·.id == id) with
      | none => s
      | some t =>
        match t.retryAt with
        | none => s
        | some due =>
          if ((t.state == TaskState.pending) = true ∧ due ≤ n) then
            match (s.promises.find? (·.id == t.id)).map (·.project n) with
            | none => s
            | some p =>
                if (p.state == PromiseState.pending) = true then
                  { { s with tasks := ({ t with retryAt := some next } : TaskObject) :: s.tasks.filter (·.id != ({ t with retryAt := some next } : TaskObject).id) } with
                    outbox := (ServerModel.OutboxEntry.mk ((p.tags.get? "resonate:target").getD "") (.execute t.id t.version)) :: ({ s with tasks := ({ t with retryAt := some next } : TaskObject) :: s.tasks.filter (·.id != ({ t with retryAt := some next } : TaskObject).id) } : ServerState).outbox.filter (fun e => e.key != (ServerModel.OutboxEntry.mk ((p.tags.get? "resonate:target").getD "") (.execute t.id t.version)).key) }
                else s
          else s) := by
  cases hf : s.tasks.find? (·.id == id) with
  | none => simp only [Rules.dispatch, run_bind, run_getTask, hf, run_pure]
  | some t =>
      cases he : t.retryAt with
      | none => simp only [Rules.dispatch, run_bind, run_getTask, hf, he, run_pure]
      | some due =>
          by_cases hg : ((t.state == TaskState.pending) = true ∧ due ≤ n)
          · cases hp : s.promises.find? (·.id == t.id) with
            | none =>
                simp only [Rules.dispatch, run_bind, run_getTask, hf, he, if_pos hg,
                           run_viewPromise, hp, Option.map, run_pure]
            | some p0 =>
                by_cases hpe : (((p0.project n).state == PromiseState.pending) = true)
                · simp only [Rules.dispatch, run_bind, run_getTask, hf, he, if_pos hg,
                             run_viewPromise, hp, Option.map, if_pos hpe, run_setTask,
                             setMessage, run_modify, run_pure]
                · simp only [Rules.dispatch, run_bind, run_getTask, hf, he, if_pos hg,
                             run_viewPromise, hp, Option.map, if_neg hpe, run_pure]
          · simp only [Rules.dispatch, run_bind, run_getTask, hf, he, if_neg hg, run_pure]

theorem agrees_r5 (id : String) : Agrees (.r5 id) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, run_bind, run_pure, Id.run,
             run_leaseExpiry]
  refine ⟨trivial, ?_⟩
  have hti : (sP.tasks.find? (·.id == id)).map (fun t => applyView t (pLook n sP id))
      = (sM.tasks.find? (·.id == id)).map (fun t => applyView t (pLook n sM id)) := h.2.1 id
  cases hfP : sP.tasks.find? (·.id == id) with
  | none =>
      cases hfM : sM.tasks.find? (·.id == id) with
      | none => exact h
      | some tM => rw [hfP, hfM] at hti; simp at hti
  | some tP =>
      cases hfM : sM.tasks.find? (·.id == id) with
      | none => rw [hfP, hfM] at hti; simp at hti
      | some tM =>
          have htPid : tP.id = id :=
            eq_of_beq (find?_sat (fun y => y.id == id) sP.tasks tP hfP)
          have htMid : tM.id = id :=
            eq_of_beq (find?_sat (fun y => y.id == id) sM.tasks tM hfM)
          dsimp only
          rw [htPid, htMid]
          have hpp : (sP.promises.find? (·.id == id)).map (·.project n)
              = (sM.promises.find? (·.id == id)).map (·.project n) := h.1 id
          cases hpP : sP.promises.find? (·.id == id) with
          | none =>
              cases hpM : sM.promises.find? (·.id == id) with
              | some pM0 => rw [hpP, hpM] at hpp; simp at hpp
              | none =>
                  simp only [Option.map]
                  cases tP.expiresAt <;> cases tM.expiresAt <;> simp only [ite_self] <;> exact h
          | some pP0 =>
              cases hpM : sM.promises.find? (·.id == id) with
              | none => rw [hpP, hpM] at hpp; simp at hpp
              | some pM0 =>
                  rw [hpP, hpM] at hpp
                  simp only [Option.map] at hpp
                  have hpv : pP0.project n = pM0.project n := Option.some.inj hpp
                  simp only [Option.map]
                  rw [hpv]
                  by_cases hpe : (((pM0.project n).state == PromiseState.pending) = true)
                  · have hplP' : pLook n sP id = some (pM0.project n) := by
                      unfold pLook; rw [hpP]; simp only [Option.map]; rw [hpv]
                    have hplM : pLook n sM id = some (pM0.project n) := by
                      unfold pLook; rw [hpM]; rfl
                    have htt : tP = tM := Option.some.inj (by
                      rw [← hfP, ← hfM, ← tLook_pending hplP' hpe, ← tLook_pending hplM hpe]
                      exact h.2.1 id)
                    subst htt
                    cases he : tP.expiresAt with
                    | none => exact h
                    | some d =>
                        by_cases hg : ((tP.state == TaskState.acquired) = true ∧ d ≤ n)
                        · simp only [if_pos hg, if_pos hpe]
                          exact REq_setTask h _
                        · simp only [if_neg hg]
                          exact h
                  · simp only [if_neg hpe]
                    cases tP.expiresAt <;> cases tM.expiresAt <;> simp only [ite_self] <;> exact h

theorem agrees_r6 (id : String) (next : Nat) : Agrees (.r6 id next) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, run_bind, run_pure, Id.run,
             run_dispatch]
  refine ⟨trivial, ?_⟩
  have hti : (sP.tasks.find? (·.id == id)).map (fun t => applyView t (pLook n sP id))
      = (sM.tasks.find? (·.id == id)).map (fun t => applyView t (pLook n sM id)) := h.2.1 id
  cases hfP : sP.tasks.find? (·.id == id) with
  | none =>
      cases hfM : sM.tasks.find? (·.id == id) with
      | none => exact h
      | some tM => rw [hfP, hfM] at hti; simp at hti
  | some tP =>
      cases hfM : sM.tasks.find? (·.id == id) with
      | none => rw [hfP, hfM] at hti; simp at hti
      | some tM =>
          have htPid : tP.id = id :=
            eq_of_beq (find?_sat (fun y => y.id == id) sP.tasks tP hfP)
          have htMid : tM.id = id :=
            eq_of_beq (find?_sat (fun y => y.id == id) sM.tasks tM hfM)
          dsimp only
          rw [htPid, htMid]
          have hpp : (sP.promises.find? (·.id == id)).map (·.project n)
              = (sM.promises.find? (·.id == id)).map (·.project n) := h.1 id
          cases hpP : sP.promises.find? (·.id == id) with
          | none =>
              cases hpM : sM.promises.find? (·.id == id) with
              | some pM0 => rw [hpP, hpM] at hpp; simp at hpp
              | none =>
                  simp only [Option.map]
                  cases tP.retryAt <;> cases tM.retryAt <;> simp only [ite_self] <;> exact h
          | some pP0 =>
              cases hpM : sM.promises.find? (·.id == id) with
              | none => rw [hpP, hpM] at hpp; simp at hpp
              | some pM0 =>
                  rw [hpP, hpM] at hpp
                  simp only [Option.map] at hpp
                  have hpv : pP0.project n = pM0.project n := Option.some.inj hpp
                  simp only [Option.map]
                  rw [hpv]
                  by_cases hpe : (((pM0.project n).state == PromiseState.pending) = true)
                  · have hplP' : pLook n sP id = some (pM0.project n) := by
                      unfold pLook; rw [hpP]; simp only [Option.map]; rw [hpv]
                    have hplM : pLook n sM id = some (pM0.project n) := by
                      unfold pLook; rw [hpM]; rfl
                    have htt : tP = tM := Option.some.inj (by
                      rw [← hfP, ← hfM, ← tLook_pending hplP' hpe, ← tLook_pending hplM hpe]
                      exact h.2.1 id)
                    subst htt
                    cases he : tP.retryAt with
                    | none => exact h
                    | some due =>
                        by_cases hg : ((tP.state == TaskState.pending) = true ∧ due ≤ n)
                        · simp only [if_pos hg, if_pos hpe]
                          exact REq_setTask h _
                        · simp only [if_neg hg]
                          exact h
                  · simp only [if_neg hpe]
                    cases tP.retryAt <;> cases tM.retryAt <;> simp only [ite_self] <;> exact h

/-! ### 5p. R3 — notify. The write is a SETTLED promise (drop one
    listener): a settled upsert needs no task-agreement side condition,
    because the view against any settled promise is the same
    fulfill-or-identity. -/

theorem view_congr_settled (t : TaskObject) {p1 p2 : PromiseObject}
    (h1 : (p1.state == PromiseState.pending) = false)
    (h2 : (p2.state == PromiseState.pending) = false) :
    t.view p1 = t.view p2 := by
  unfold TaskObject.view
  by_cases ht : (t.state == TaskState.fulfilled) = true
  · rw [if_neg (fun hx => absurd hx.2 (by simp [bne, ht])),
        if_neg (fun hx => absurd hx.2 (by simp [bne, ht]))]
  · rw [if_pos ⟨bne_true_of_beq_false h1, bne_true_of_beq_false (toFalse ht)⟩,
        if_pos ⟨bne_true_of_beq_false h2, bne_true_of_beq_false (toFalse ht)⟩]

/-- The same SETTLED upsert on both sides preserves the invariant —
    no side condition: views against a settled promise cannot tell the
    raw tasks apart any better than views against the old lookup. -/
theorem REq_setPromise_settled {n : Nat} {sP sM : ServerState} (h : REq n sP sM)
    (q : PromiseObject) (hq : (q.state == PromiseState.pending) = false) :
    REq n { sP with promises := q :: sP.promises.filter (·.id != q.id) }
          { sM with promises := q :: sM.promises.filter (·.id != q.id) } := by
  obtain ⟨hp, ht, hs⟩ := h
  have hqp : q.project n = q := project_not_pending q n hq
  have hpu : ∀ (s : ServerState) (id : String),
      pLook n { s with promises := q :: s.promises.filter (·.id != q.id) } id
        = if (q.id == id) = true then some (q.project n) else pLook n s id := by
    intro s id
    show ((q :: s.promises.filter (fun y => y.id != q.id)).find? (fun y => y.id == id)).map (·.project n)
        = if (q.id == id) = true then some (q.project n) else pLook n s id
    rw [find?_upsert PromiseObject.id q s.promises id]
    by_cases hc : (q.id == id) = true
    · rw [if_pos hc, if_pos hc]; rfl
    · rw [if_neg hc, if_neg hc]; rfl
  refine ⟨fun id => ?_, fun id => ?_, hs⟩
  · rw [hpu sP id, hpu sM id]
    by_cases hc : (q.id == id) = true
    · rw [if_pos hc, if_pos hc]
    · rw [if_neg hc, if_neg hc]; exact hp id
  · show (sP.tasks.find? (fun y => y.id == id)).map
          (fun t => applyView t (pLook n { sP with promises := q :: sP.promises.filter (·.id != q.id) } id))
        = (sM.tasks.find? (fun y => y.id == id)).map
          (fun t => applyView t (pLook n { sM with promises := q :: sM.promises.filter (·.id != q.id) } id))
    rw [hpu sP id, hpu sM id]
    by_cases hc : (q.id == id) = true
    · rw [if_pos hc, if_pos hc, hqp]
      have htid := ht id
      unfold tLook at htid
      cases hplo : pLook n sP id with
      | none =>
          have hplo' : pLook n sM id = none := by rw [← hp id]; exact hplo
          rw [hplo] at htid
          rw [hplo'] at htid
          cases hrP : sP.tasks.find? (fun y => y.id == id) with
          | none =>
              cases hrM : sM.tasks.find? (fun y => y.id == id) with
              | none => rfl
              | some b => rw [hrP, hrM] at htid; simp [applyView] at htid
          | some a =>
              cases hrM : sM.tasks.find? (fun y => y.id == id) with
              | none => rw [hrP, hrM] at htid; simp [applyView] at htid
              | some b =>
                  rw [hrP, hrM] at htid
                  simp only [Option.map, applyView] at htid
                  simp only [Option.map, applyView]
                  rw [Option.some.inj htid]
      | some pv =>
          have hplo' : pLook n sM id = some pv := by rw [← hp id]; exact hplo
          rw [hplo] at htid
          rw [hplo'] at htid
          cases hrP : sP.tasks.find? (fun y => y.id == id) with
          | none =>
              cases hrM : sM.tasks.find? (fun y => y.id == id) with
              | none => rfl
              | some b => rw [hrP, hrM] at htid; simp [applyView] at htid
          | some a =>
              cases hrM : sM.tasks.find? (fun y => y.id == id) with
              | none => rw [hrP, hrM] at htid; simp [applyView] at htid
              | some b =>
                  rw [hrP, hrM] at htid
                  simp only [Option.map, applyView] at htid
                  have hab : a.view pv = b.view pv := Option.some.inj htid
                  simp only [Option.map, applyView]
                  by_cases hpvp : (pv.state == PromiseState.pending) = true
                  · have haa : a = b := by
                      rw [← view_of_pending a pv hpvp, ← view_of_pending b pv hpvp]
                      exact hab
                    rw [haa]
                  · rw [view_congr_settled a hq (toFalse hpvp), hab,
                        ← view_congr_settled b hq (toFalse hpvp)]
    · rw [if_neg hc, if_neg hc, hp id]
      have ht' := ht id
      unfold tLook at ht'
      rw [hp id] at ht'
      exact ht'

theorem agrees_r3 (id address : String) : Agrees (.r3 id address) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Rules.notify,
             run_bind, run_touchPromise, run_pure, setMessage, run_modify, Id.run]
  refine ⟨trivial, ?_⟩
  have hpi : (sP.promises.find? (·.id == id)).map (·.project n)
      = (sM.promises.find? (·.id == id)).map (·.project n) := h.1 id
  cases hfP : sP.promises.find? (·.id == id) with
  | none =>
      cases hfM : sM.promises.find? (·.id == id) with
      | none => exact h
      | some pM => rw [hfP, hfM] at hpi; simp at hpi
  | some pP =>
      cases hfM : sM.promises.find? (·.id == id) with
      | none => rw [hfP, hfM] at hpi; simp at hpi
      | some pM =>
          rw [hfP, hfM] at hpi
          simp only [Option.map] at hpi
          have hproj : pP.project n = pM.project n := Option.some.inj hpi
          have hfP' : sP.promises.find? (·.id == pP.id) = some pP := by
            rw [eq_of_beq (find?_sat (fun x => x.id == id) sP.promises pP hfP)]; exact hfP
          have hfM' : sM.promises.find? (·.id == pM.id) = some pM := by
            rw [eq_of_beq (find?_sat (fun x => x.id == id) sM.promises pM hfM)]; exact hfM
          dsimp only
          rw [hproj]
          -- absorb both touch writes
          have hP0 : REq n sP (if (((pM.project n).state != pP.state) = true) then
                touchWrite n pP sP
              else sP) := by
            by_cases hc : (((pM.project n).state != pP.state) = true)
            · simp only [if_pos hc]
              exact REq_touchWrite (REq.refl n sP) pP hfP'
            · simp only [if_neg hc]; exact REq.refl n sP
          generalize hsP1 : (if (((pM.project n).state != pP.state) = true) then
                touchWrite n pP sP
              else sP) = sP1
          rw [hsP1] at hP0
          have hM0 : REq n sM (if (((pM.project n).state != pM.state) = true) then
                touchWrite n pM sM
              else sM) := by
            by_cases hc : (((pM.project n).state != pM.state) = true)
            · simp only [if_pos hc]; exact REq_touchWrite (REq.refl n sM) pM hfM'
            · simp only [if_neg hc]; exact REq.refl n sM
          generalize hsM1 : (if (((pM.project n).state != pM.state) = true) then
                touchWrite n pM sM
              else sM) = sM1
          rw [hsM1] at hM0
          have h1 : REq n sP1 sM1 := (hP0.symm.trans h).trans hM0
          by_cases hpend : (((pM.project n).state == PromiseState.pending) = true)
          · simp only [if_pos hpend]; exact h1
          · simp only [if_neg hpend]
            by_cases hlc : ((pM.project n).listeners.contains address) = true
            · simp only [if_pos hlc]
              exact REq_setPromise_settled h1 _ (toFalse hpend)
            · simp only [if_neg hlc]; exact h1

/-! ### 5q. R4 — resume. The touch's VALUE is the view pair, its STATE
    is self-invisible; splitting the two lets the shared `resumeOne`
    compose through the invariant without expanding the touch. -/

theorem touchTask_val (id : String) (n : Nat) (s : ServerState) :
    ((touchTask id n).run s).1 =
      match s.tasks.find? (·.id == id) with
      | none => none
      | some t =>
        match (s.promises.find? (·.id == t.id)).map (·.project n) with
        | none => some (t, none)
        | some pv => some (t.view pv, some pv) := by
  rw [run_touchTask]
  cases hf : s.tasks.find? (·.id == id) with
  | none => rfl
  | some t =>
      dsimp only
      cases hp : s.promises.find? (·.id == t.id) with
      | none => rfl
      | some p0 =>
          simp only [Option.map]
          by_cases hcond : (((p0.project n).state != PromiseState.pending) = true
              ∧ (t.state != TaskState.fulfilled) = true)
          · simp only [if_pos hcond]
            rw [view_settles t (p0.project n) (beq_false_of_bne hcond.1)
                (beq_false_of_bne hcond.2)]
          · simp only [if_neg hcond]
            have hvm : t.view (p0.project n) = t := by
              by_cases hA : ((p0.project n).state == PromiseState.pending) = true
              · exact view_of_pending t _ hA
              · by_cases hB : (t.state == TaskState.fulfilled) = true
                · exact view_of_fulfilled t _ hB
                · exact absurd ⟨bne_true_of_beq_false (toFalse hA),
                    bne_true_of_beq_false (toFalse hB)⟩ hcond
            rw [hvm]

theorem touchTask_val_agrees (id : String) (n : Nat) {sA sB : ServerState}
    (h : REq n sA sB) :
    ((touchTask id n).run sA).1 = ((touchTask id n).run sB).1 := by
  rw [touchTask_val, touchTask_val]
  have hti : (sA.tasks.find? (·.id == id)).map (fun t => applyView t (pLook n sA id))
      = (sB.tasks.find? (·.id == id)).map (fun t => applyView t (pLook n sB id)) := h.2.1 id
  cases hfA : sA.tasks.find? (·.id == id) with
  | none =>
      cases hfB : sB.tasks.find? (·.id == id) with
      | none => rfl
      | some tB => rw [hfA, hfB] at hti; simp at hti
  | some tA =>
      cases hfB : sB.tasks.find? (·.id == id) with
      | none => rw [hfA, hfB] at hti; simp at hti
      | some tB =>
          rw [hfA, hfB] at hti
          simp only [Option.map] at hti
          have hvv0 : applyView tA (pLook n sA id) = applyView tB (pLook n sB id) :=
            Option.some.inj hti
          have htAid : tA.id = id :=
            eq_of_beq (find?_sat (fun y => y.id == id) sA.tasks tA hfA)
          have htBid : tB.id = id :=
            eq_of_beq (find?_sat (fun y => y.id == id) sB.tasks tB hfB)
          dsimp only
          rw [htAid, htBid]
          have hpp : (sA.promises.find? (·.id == id)).map (·.project n)
              = (sB.promises.find? (·.id == id)).map (·.project n) := h.1 id
          cases hpA : sA.promises.find? (·.id == id) with
          | none =>
              cases hpB : sB.promises.find? (·.id == id) with
              | some pB0 => rw [hpA, hpB] at hpp; simp at hpp
              | none =>
                  have hplA : pLook n sA id = none := by unfold pLook; rw [hpA]; rfl
                  have hplB : pLook n sB id = none := by unfold pLook; rw [hpB]; rfl
                  rw [hplA, hplB] at hvv0
                  simp only [applyView] at hvv0
                  simp only [Option.map]
                  rw [hvv0]
          | some pA0 =>
              cases hpB : sB.promises.find? (·.id == id) with
              | none => rw [hpA, hpB] at hpp; simp at hpp
              | some pB0 =>
                  rw [hpA, hpB] at hpp
                  simp only [Option.map] at hpp
                  have hpv : pA0.project n = pB0.project n := Option.some.inj hpp
                  have hplA : pLook n sA id = some (pA0.project n) := by
                    unfold pLook; rw [hpA]; rfl
                  have hplB : pLook n sB id = some (pB0.project n) := by
                    unfold pLook; rw [hpB]; rfl
                  rw [hplA, hplB, ← hpv] at hvv0
                  simp only [applyView] at hvv0
                  simp only [Option.map]
                  rw [hpv] at hvv0 ⊢
                  rw [hvv0]

/-- The shared wake step: same value on both sides, write derived from
    the view, touch state absorbed. -/
theorem agrees_resumeOne (awaited awaiter : String) (n : Nat) {sA sB : ServerState}
    (h : REq n sA sB) :
    REq n ((Rules.resumeOne awaited awaiter n).run sA).2
          ((Rules.resumeOne awaited awaiter n).run sB).2 := by
  have h1 : REq n ((touchTask awaiter n).run sA).2 ((touchTask awaiter n).run sB).2 :=
    ((REq_touchTask_self awaiter n sA).symm.trans h).trans (REq_touchTask_self awaiter n sB)
  have hv := touchTask_val_agrees awaiter n h
  simp only [Rules.resumeOne, run_bind]
  rw [hv]
  cases hvB : ((touchTask awaiter n).run sB).1 with
  | none => exact h1
  | some pair =>
      obtain ⟨t, po⟩ := pair
      cases po with
      | none => exact h1
      | some pv =>
          dsimp only
          cases t.state with
          | suspended => exact REq_setTask h1 _
          | fulfilled => exact h1
          | pending =>
              by_cases hcont : (!(t.resumes.contains awaited)) = true
              · simp only [if_pos hcont]; exact REq_setTask h1 _
              · simp only [if_neg hcont]; exact h1
          | acquired =>
              by_cases hcont : (!(t.resumes.contains awaited)) = true
              · simp only [if_pos hcont]; exact REq_setTask h1 _
              · simp only [if_neg hcont]; exact h1
          | halted =>
              by_cases hcont : (!(t.resumes.contains awaited)) = true
              · simp only [if_pos hcont]; exact REq_setTask h1 _
              · simp only [if_neg hcont]; exact h1

theorem agrees_r4 (id awaiter : String) : Agrees (.r4 id awaiter) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Rules.resume,
             run_bind, run_touchPromise, run_setPromise, run_pure, Id.run]
  refine ⟨trivial, ?_⟩
  have hpi : (sP.promises.find? (·.id == id)).map (·.project n)
      = (sM.promises.find? (·.id == id)).map (·.project n) := h.1 id
  cases hfP : sP.promises.find? (·.id == id) with
  | none =>
      cases hfM : sM.promises.find? (·.id == id) with
      | none => exact h
      | some pM => rw [hfP, hfM] at hpi; simp at hpi
  | some pP =>
      cases hfM : sM.promises.find? (·.id == id) with
      | none => rw [hfP, hfM] at hpi; simp at hpi
      | some pM =>
          rw [hfP, hfM] at hpi
          simp only [Option.map] at hpi
          have hproj : pP.project n = pM.project n := Option.some.inj hpi
          have hfP' : sP.promises.find? (·.id == pP.id) = some pP := by
            rw [eq_of_beq (find?_sat (fun x => x.id == id) sP.promises pP hfP)]; exact hfP
          have hfM' : sM.promises.find? (·.id == pM.id) = some pM := by
            rw [eq_of_beq (find?_sat (fun x => x.id == id) sM.promises pM hfM)]; exact hfM
          dsimp only
          rw [hproj]
          have hP0 : REq n sP (if (((pM.project n).state != pP.state) = true) then
                touchWrite n pP sP
              else sP) := by
            by_cases hc : (((pM.project n).state != pP.state) = true)
            · simp only [if_pos hc]
              exact REq_touchWrite (REq.refl n sP) pP hfP'
            · simp only [if_neg hc]; exact REq.refl n sP
          generalize hsP1 : (if (((pM.project n).state != pP.state) = true) then
                touchWrite n pP sP
              else sP) = sP1
          rw [hsP1] at hP0
          have hM0 : REq n sM (if (((pM.project n).state != pM.state) = true) then
                touchWrite n pM sM
              else sM) := by
            by_cases hc : (((pM.project n).state != pM.state) = true)
            · simp only [if_pos hc]; exact REq_touchWrite (REq.refl n sM) pM hfM'
            · simp only [if_neg hc]; exact REq.refl n sM
          generalize hsM1 : (if (((pM.project n).state != pM.state) = true) then
                touchWrite n pM sM
              else sM) = sM1
          rw [hsM1] at hM0
          have h1 : REq n sP1 sM1 := (hP0.symm.trans h).trans hM0
          by_cases hpend : (((pM.project n).state == PromiseState.pending) = true)
          · simp only [if_pos hpend]; exact h1
          · simp only [if_neg hpend]
            by_cases hcb : ((pM.project n).callbacks.contains awaiter) = true
            · simp only [if_pos hcb]
              exact agrees_resumeOne (pM.project n).id awaiter n
                (REq_setPromise_settled h1 _ (toFalse hpend))
            · simp only [if_neg hcb]; exact h1

/-! ### 5t. The loop handlers — `taskHeartbeat` and `taskSuspend`,
    by induction over their (request-shared) lists, the invariant
    carried element by element. -/

theorem runAgrees_heartbeatOne (pid : String) (ref : ServerModel.TaskRef) (n : Nat)
    {sP sM : ServerState} (h : REq n sP sM) :
    REq n ((Projected.heartbeatOne pid ref n).run sP).2
          ((AbstractModel.heartbeatOne pid ref n).run sM).2 := by
  simp only [Projected.heartbeatOne, AbstractModel.heartbeatOne]
  refine (bind_taskRead_agrees ref.id n _ _ h ?_).2
  intro v sP' sM' h' hf
  cases v with
  | none => exact ⟨rfl, h'⟩
  | some pair =>
      obtain ⟨t, po⟩ := pair
      cases po with
      | none => exact ⟨rfl, h'⟩
      | some pv =>
          dsimp only
          by_cases hg : ((t.state == TaskState.acquired) = true
              ∧ (t.version == ref.version) = true
              ∧ (t.pid == some pid) = true
              ∧ (pv.state == PromiseState.pending) = true)
          · simp only [if_pos hg]; exact ⟨trivial, REq_setTask h' _⟩
          · simp only [if_neg hg]; exact ⟨trivial, h'⟩

theorem REq_heartbeatAll (pid : String) (n : Nat) (refs : List ServerModel.TaskRef) :
    ∀ {sP sM : ServerState}, REq n sP sM →
      REq n ((Projected.heartbeatAll pid n refs).run sP).2
            ((AbstractModel.heartbeatAll pid n refs).run sM).2 := by
  induction refs with
  | nil =>
      intro sP sM h
      simp only [Projected.heartbeatAll, AbstractModel.heartbeatAll, run_pure]
      exact h
  | cons ref refs ih =>
      intro sP sM h
      simp only [Projected.heartbeatAll, AbstractModel.heartbeatAll, run_bind]
      exact ih (runAgrees_heartbeatOne pid ref n h)

theorem agrees_taskHeartbeat (req) : Agrees (.api (.taskHeartbeat req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.taskHeartbeat,
             AbstractModel.taskHeartbeat, run_map, run_bind, run_pure, Id.run]
  exact ⟨trivial, REq_heartbeatAll req.pid n req.tasks h⟩

theorem runAgrees_checkAwaited (n : Nat)
    (actions : List ServerModel.PromiseRegisterCallbackReq) :
    ∀ {sP sM : ServerState}, REq n sP sM →
      ((Projected.checkAwaited n actions).run sP).1
          = ((AbstractModel.checkAwaited n actions).run sM).1
        ∧ REq n ((Projected.checkAwaited n actions).run sP).2
                ((AbstractModel.checkAwaited n actions).run sM).2 := by
  induction actions with
  | nil =>
      intro sP sM h
      simp only [Projected.checkAwaited, AbstractModel.checkAwaited, run_pure]
      exact ⟨trivial, h⟩
  | cons action rest ih =>
      intro sP sM h
      simp only [Projected.checkAwaited, AbstractModel.checkAwaited, run_bind,
                 run_viewPromise, run_touchPromise, run_pure]
      have hpi : (sP.promises.find? (·.id == action.awaited)).map (·.project n)
          = (sM.promises.find? (·.id == action.awaited)).map (·.project n) :=
        h.1 action.awaited
      cases hfP : sP.promises.find? (·.id == action.awaited) with
      | none =>
          cases hfM : sM.promises.find? (·.id == action.awaited) with
          | none => exact ⟨rfl, h⟩
          | some pM => rw [hfP, hfM] at hpi; simp at hpi
      | some pP =>
          cases hfM : sM.promises.find? (·.id == action.awaited) with
          | none => rw [hfP, hfM] at hpi; simp at hpi
          | some pM =>
              rw [hfP, hfM] at hpi
              simp only [Option.map] at hpi
              have hproj : pP.project n = pM.project n := Option.some.inj hpi
              have hfM' : sM.promises.find? (·.id == pM.id) = some pM := by
                rw [eq_of_beq (find?_sat (fun x => x.id == action.awaited) sM.promises pM hfM)]
                exact hfM
              simp only [Option.map]
              rw [hproj]
              generalize hsM1 :
                (if (((pM.project n).state != pM.state) = true) then
                  touchWrite n pM sM
                else sM) = sM1
              have h1 : REq n sP sM1 := by
                rw [← hsM1]
                by_cases hc : (((pM.project n).state != pM.state) = true)
                · simp only [if_pos hc]; exact REq_touchWrite h pM hfM'
                · simp only [if_neg hc]; exact h
              by_cases hext : (!(pM.project n).external) = true
              · simp only [if_pos hext]; exact ⟨rfl, h1⟩
              · simp only [if_neg hext, run_bind]
                obtain ⟨hval, hst⟩ := ih h1
                rw [hval]
                cases hrec : ((AbstractModel.checkAwaited n rest).run sM1).1 with
                | none => exact ⟨rfl, hst⟩
                | some settled => exact ⟨rfl, hst⟩

theorem runAgrees_registerAwaited (awaiter : String) (n : Nat)
    (actions : List ServerModel.PromiseRegisterCallbackReq) :
    ∀ {sP sM : ServerState}, REq n sP sM →
      REq n ((Projected.registerAwaited awaiter n actions).run sP).2
            ((AbstractModel.registerAwaited awaiter n actions).run sM).2 := by
  induction actions with
  | nil =>
      intro sP sM h
      simp only [Projected.registerAwaited, AbstractModel.registerAwaited, run_pure]
      exact h
  | cons action rest ih =>
      intro sP sM h
      simp only [Projected.registerAwaited, AbstractModel.registerAwaited, run_bind,
                 run_viewPromise, run_touchPromise, run_pure, run_setPromise]
      have hpi : (sP.promises.find? (·.id == action.awaited)).map (·.project n)
          = (sM.promises.find? (·.id == action.awaited)).map (·.project n) :=
        h.1 action.awaited
      cases hfP : sP.promises.find? (·.id == action.awaited) with
      | none =>
          cases hfM : sM.promises.find? (·.id == action.awaited) with
          | none => exact ih h
          | some pM => rw [hfP, hfM] at hpi; simp at hpi
      | some pP =>
          cases hfM : sM.promises.find? (·.id == action.awaited) with
          | none => rw [hfP, hfM] at hpi; simp at hpi
          | some pM =>
              rw [hfP, hfM] at hpi
              simp only [Option.map] at hpi
              have hproj : pP.project n = pM.project n := Option.some.inj hpi
              have hpm : (pM.id == action.awaited) = true :=
                find?_sat (fun x => x.id == action.awaited) sM.promises pM hfM
              have hfM' : sM.promises.find? (·.id == pM.id) = some pM := by
                rw [eq_of_beq hpm]; exact hfM
              simp only [Option.map]
              rw [hproj]
              generalize hsM1 :
                (if (((pM.project n).state != pM.state) = true) then
                  touchWrite n pM sM
                else sM) = sM1
              have h1 : REq n sP sM1 := by
                rw [← hsM1]
                by_cases hc : (((pM.project n).state != pM.state) = true)
                · simp only [if_pos hc]; exact REq_touchWrite h pM hfM'
                · simp only [if_neg hc]; exact h
              by_cases hpend : (((pM.project n).state == PromiseState.pending) = true)
              · -- pending: the view is the raw record on both sides
                have hpp : pM.project n = pM := project_of_pending hpend
                rw [hpp] at hpend ⊢
                have hplP : pLook n sP action.awaited = some pM := by
                  unfold pLook; rw [hfP]; simp only [Option.map]; rw [hproj, hpp]
                have hplM1 : pLook n sM1 action.awaited = some pM := by
                  rw [← h1.1 action.awaited]; exact hplP
                have htq0 : sP.tasks.find? (·.id == action.awaited)
                    = sM1.tasks.find? (·.id == action.awaited) := by
                  rw [← tLook_pending hplP hpend, ← tLook_pending hplM1 hpend]
                  exact h1.2.1 action.awaited
                have htq : sP.tasks.find? (·.id == (pM.addCallback awaiter).id)
                    = sM1.tasks.find? (·.id == (pM.addCallback awaiter).id) := by
                  rw [addCallback_id, eq_of_beq hpm]; exact htq0
                exact ih (REq_setPromise h1 (pM.addCallback awaiter) htq)
              · -- settled: the settled-upsert congruence needs no side condition
                exact ih (REq_setPromise_settled h1 ((pM.project n).addCallback awaiter)
                  (by rw [addCallback_state]; exact toFalse hpend))

theorem agrees_taskSuspend (req) : Agrees (.api (.taskSuspend req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.taskSuspend,
             AbstractModel.taskSuspend, run_map, Id.run]
  by_cases hg0 : (req.actions.isEmpty) = true
  · simp only [if_pos hg0]; exact ⟨rfl, h⟩
  · simp only [if_neg hg0, run_pureUnit_bind]
    by_cases hg1 : (req.actions.any (·.awaited == req.id)) = true
    · simp only [if_pos hg1]; exact ⟨rfl, h⟩
    · simp only [if_neg hg1, run_pureUnit_bind]
      by_cases hg2 : ((req.actions.map (·.awaited)).eraseDups.length
          != (req.actions.map (·.awaited)).length) = true
      · simp only [if_pos hg2]; exact ⟨rfl, h⟩
      · simp only [if_neg hg2, run_pureUnit_bind]
        refine And.imp (congrArg Equivalence.Response.taskSuspend) (fun x => x)
          (bind_taskRead_agrees req.id n _ _ h ?_)
        intro v sP' sM' h' hf
        cases v with
        | none => exact ⟨rfl, h'⟩
        | some pair =>
            obtain ⟨t, po⟩ := pair
            cases po with
            | none => exact ⟨rfl, h'⟩
            | some pv =>
                dsimp only
                by_cases hs1 : (t.state != TaskState.acquired) = true
                · simp only [if_pos hs1]; exact ⟨rfl, h'⟩
                · simp only [if_neg hs1, run_pureUnit_bind]
                  by_cases hs2 : (pv.state != PromiseState.pending) = true
                  · simp only [if_pos hs2]; exact ⟨rfl, h'⟩
                  · simp only [if_neg hs2, run_pureUnit_bind]
                    by_cases hs3 : (t.version != req.version) = true
                    · simp only [if_pos hs3]; exact ⟨rfl, h'⟩
                    · simp only [if_neg hs3, run_pureUnit_bind]
                      simp only [run_bind]
                      obtain ⟨hval, hst⟩ := runAgrees_checkAwaited n req.actions h'
                      rw [hval]
                      cases hchk : ((AbstractModel.checkAwaited n req.actions).run sM').1 with
                      | none => exact ⟨rfl, hst⟩
                      | some settled =>
                          cases settled with
                          | true =>
                              simp only [run_bind, run_setTask, run_pure]
                              exact ⟨trivial, REq_setTask hst _⟩
                          | false =>
                              simp only [run_bind, run_setTask, run_pure]
                              exact ⟨trivial, REq_setTask
                                (runAgrees_registerAwaited req.id n req.actions hst) _⟩

/-! ### 5s. `taskFence` — a task read guarding an inner promise
    handler; the run-level agreements compose directly. -/

theorem agrees_taskFence (req) : Agrees (.api (.taskFence req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.taskFence,
             AbstractModel.taskFence, run_map, Id.run]
  by_cases hg0 : (req.action.targetId == req.id) = true
  · simp only [if_pos hg0]; exact ⟨rfl, h⟩
  · simp only [if_neg hg0, run_pureUnit_bind]
    refine And.imp (congrArg Equivalence.Response.taskFence) (fun x => x)
      (bind_taskRead_agrees req.id n _ _ h ?_)
    intro v sP' sM' h' hf
    cases v with
    | none => exact ⟨rfl, h'⟩
    | some pair =>
        obtain ⟨t, po⟩ := pair
        cases po with
        | none => exact ⟨rfl, h'⟩
        | some pv =>
            dsimp only
            by_cases hg1 : (t.state != TaskState.acquired) = true
            · simp only [if_pos hg1]; exact ⟨rfl, h'⟩
            · simp only [if_neg hg1, run_pureUnit_bind]
              by_cases hg2 : (pv.state != PromiseState.pending) = true
              · simp only [if_pos hg2]; exact ⟨rfl, h'⟩
              · simp only [if_neg hg2, run_pureUnit_bind]
                by_cases hg3 : (t.version != req.version) = true
                · simp only [if_pos hg3]; exact ⟨rfl, h'⟩
                · simp only [if_neg hg3, run_pureUnit_bind]
                  cases hact : req.action with
                  | create r =>
                      simp only [run_bind, run_pure]
                      obtain ⟨hval, hst⟩ := runAgrees_promiseCreate r n h'
                      refine ⟨?_, hst⟩
                      rw [hval]
                  | settle r =>
                      simp only [run_bind, run_pure]
                      obtain ⟨hval, hst⟩ := runAgrees_promiseSettle r n h'
                      refine ⟨?_, hst⟩
                      rw [hval]

/-! ### 5r. R7 — scheduleFire. The occurrence list is one opaque pure
    term shared by both sides (schedules agree, the clock is shared),
    so the rule is a list induction of `promiseCreate` steps. Each
    step runs AS OF a past cron time `t ≤ now` — and a touch write
    made at an earlier instant is still invisible at the current
    clock, because facts are stable under monotone time
    (`project_absorb`). -/

theorem pLook_materialize_le {t n : Nat} (ht : t ≤ n) (s : ServerState) (p : PromiseObject)
    (hf : s.promises.find? (·.id == p.id) = some p) (id : String) :
    pLook n { s with promises := (p.project t) :: s.promises.filter (·.id != (p.project t).id) } id
      = pLook n s id := by
  show (((p.project t) :: s.promises.filter (fun y => y.id != (p.project t).id)).find? (fun y => y.id == id)).map (·.project n)
      = (s.promises.find? (fun y => y.id == id)).map (·.project n)
  rw [find?_upsert PromiseObject.id (p.project t) s.promises id]
  cases hxb : ((p.project t).id == id) with
  | true =>
      have hpid : (p.id == id) = true := by rw [← project_id p t]; exact hxb
      have hid : p.id = id := eq_of_beq hpid
      rw [if_pos rfl, ← hid, hf]
      simp [project_absorb p ht]
  | false =>
      rw [if_neg (by simp)]

theorem REq_touchWrite_le {t n : Nat} (ht : t ≤ n) {sP sM : ServerState} (h : REq n sP sM)
    (p : PromiseObject) (hf : sM.promises.find? (·.id == p.id) = some p) :
    REq n sP (touchWrite t p sM) := by
  have h1 : REq n sP { sM with promises := (p.project t) :: sM.promises.filter (·.id != (p.project t).id) } :=
    ⟨fun id => (h.1 id).trans (pLook_materialize_le ht sM p hf id).symm,
     fun id => (h.2.1 id).trans (tLook_ext (pLook_materialize_le ht sM p hf) rfl id).symm,
     fun id => (h.2.2 id).trans (sLook_ext (s := sM) rfl id).symm⟩
  unfold touchWrite
  by_cases hset : (((p.project t).state != PromiseState.pending) = true)
  · simp only [if_pos hset]
    have hpn : p.project n = p.project t := by
      rw [← project_absorb p ht]
      exact project_not_pending _ n (beq_false_of_bne hset)
    have hpl : pLook n { sM with promises := (p.project t) :: sM.promises.filter (·.id != (p.project t).id) }
        ((p.project t).id) = some (p.project n) := by
      show (((p.project t) :: sM.promises.filter (fun y => y.id != (p.project t).id)).find?
             (fun y => y.id == (p.project t).id)).map (·.project n) = _
      rw [find?_upsert PromiseObject.id (p.project t) sM.promises ((p.project t).id), if_pos (by simp)]
      simp [project_not_pending _ n (beq_false_of_bne hset), hpn]
    have hsetn : ((p.project n).state == PromiseState.pending) = false := by
      rw [hpn]; exact beq_false_of_bne hset
    cases hft : sM.tasks.find? (·.id == (p.project t).id) with
    | none => simp only [hft]; exact h1
    | some tk =>
        by_cases htf : ((tk.state != TaskState.fulfilled) = true)
        · simp only [hft, if_pos htf]
          exact REq_touchTaskWrite h1 tk ((p.project t).id) hft (p.project n) hpl hsetn
            (beq_false_of_bne htf)
        · simp only [hft, if_neg htf]; exact h1
  · simp only [if_neg hset]; exact h1

/-- The SAME `promiseCreate`, run by both machines at a past instant
    `t ≤ n`, preserves the invariant AT the current clock. -/
theorem REq_promiseCreateMM (req : ServerModel.PromiseCreateReq) {t n : Nat} (ht : t ≤ n)
    {sA sB : ServerState} (h : REq n sA sB) :
    REq n ((AbstractModel.promiseCreate req t).run sA).2
          ((AbstractModel.promiseCreate req t).run sB).2 := by
  simp only [AbstractModel.promiseCreate, run_bind, run_touchPromise, run_pure]
  have hpi : (sA.promises.find? (·.id == req.id)).map (·.project n)
      = (sB.promises.find? (·.id == req.id)).map (·.project n) := h.1 req.id
  cases hfA : sA.promises.find? (·.id == req.id) with
  | some pA =>
      cases hfB : sB.promises.find? (·.id == req.id) with
      | none => rw [hfA, hfB] at hpi; simp at hpi
      | some pB =>
          have hfA' : sA.promises.find? (·.id == pA.id) = some pA := by
            rw [eq_of_beq (find?_sat (fun x => x.id == req.id) sA.promises pA hfA)]; exact hfA
          have hfB' : sB.promises.find? (·.id == pB.id) = some pB := by
            rw [eq_of_beq (find?_sat (fun x => x.id == req.id) sB.promises pB hfB)]; exact hfB
          dsimp only
          have hA0 : REq n sA (if (((pA.project t).state != pA.state) = true) then touchWrite t pA sA else sA) := by
            by_cases hc : (((pA.project t).state != pA.state) = true)
            · simp only [if_pos hc]; exact REq_touchWrite_le ht (REq.refl n sA) pA hfA'
            · simp only [if_neg hc]; exact REq.refl n sA
          have hB0 : REq n sB (if (((pB.project t).state != pB.state) = true) then touchWrite t pB sB else sB) := by
            by_cases hc : (((pB.project t).state != pB.state) = true)
            · simp only [if_pos hc]; exact REq_touchWrite_le ht (REq.refl n sB) pB hfB'
            · simp only [if_neg hc]; exact REq.refl n sB
          exact (hA0.symm.trans h).trans hB0
  | none =>
      cases hfB : sB.promises.find? (·.id == req.id) with
      | some pB => rw [hfA, hfB] at hpi; simp at hpi
      | none =>
          have hplA : pLook n sA req.id = none := by unfold pLook; rw [hfA]; rfl
          have hplB : pLook n sB req.id = none := by unfold pLook; rw [hfB]; rfl
          have htq : sA.tasks.find? (·.id == req.id) = sB.tasks.find? (·.id == req.id) := by
            rw [← tLook_raw hplA, ← tLook_raw hplB]; exact h.2.1 req.id
          dsimp only
          by_cases hto : req.timeoutAt > t
          · simp only [if_pos hto]
            by_cases htag : (req.tags.has "resonate:target") = true
            · simp only [if_pos htag]
              exact REq_setTask (REq_setPromise h
                ({ id := req.id, state := PromiseState.pending, param := req.param,
                   tags := req.tags, timeoutAt := req.timeoutAt, createdAt := t } : PromiseObject) htq) _
            · simp only [if_neg htag]
              exact REq_setPromise h
                ({ id := req.id, state := PromiseState.pending, param := req.param,
                   tags := req.tags, timeoutAt := req.timeoutAt, createdAt := t } : PromiseObject) htq
          · simp only [if_neg hto]
            by_cases htag : (req.tags.has "resonate:target") = true
            · simp only [if_pos htag]
              exact REq_setTask (REq_setPromise h
                ({ id := req.id,
                   state := if req.tags.isTimer then PromiseState.resolved else PromiseState.rejectedTimedout,
                   param := req.param, tags := req.tags, timeoutAt := req.timeoutAt,
                   createdAt := req.timeoutAt, settledAt := some req.timeoutAt } : PromiseObject) htq) _
            · simp only [if_neg htag]
              exact REq_setPromise h
                ({ id := req.id,
                   state := if req.tags.isTimer then PromiseState.resolved else PromiseState.rejectedTimedout,
                   param := req.param, tags := req.tags, timeoutAt := req.timeoutAt,
                   createdAt := req.timeoutAt, settledAt := some req.timeoutAt } : PromiseObject) htq

/-- The catch-up loop, by induction over the (shared) occurrence
    list. -/
theorem REq_fireAll (s0 : ServerModel.Schedule) {n : Nat} (ts : List Nat)
    (hle : ∀ t ∈ ts, t ≤ n) :
    ∀ {sA sB : ServerState}, REq n sA sB →
      REq n ((Rules.fireAll s0 ts).run sA).2 ((Rules.fireAll s0 ts).run sB).2 := by
  induction ts with
  | nil =>
      intro sA sB h
      simp only [Rules.fireAll, run_pure]
      exact h
  | cons t ts ih =>
      intro sA sB h
      simp only [Rules.fireAll, Rules.fireOccurrence, run_bind, run_pure]
      exact ih (fun x hx => hle x (List.mem_cons_of_mem _ hx))
        (REq_promiseCreateMM _ (hle t List.mem_cons_self) h)

theorem agrees_r7 (id : String) : Agrees (.r7 id) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Rules.scheduleFire,
             run_bind, run_getSchedule, run_pure, Id.run]
  refine ⟨trivial, ?_⟩
  have hs : sP.schedules.find? (·.id == id) = sM.schedules.find? (·.id == id) := h.2.2 id
  rw [hs]
  cases hsch : sM.schedules.find? (·.id == id) with
  | none => exact h
  | some s0 =>
      dsimp only
      simp only [run_bind, run_pure]
      have hle : ∀ t ∈ (ServerModel.occurrences s0.cron s0.nextRunAt n).filter (· ≤ n), t ≤ n := by
        intro t htm
        exact of_decide_eq_true (List.mem_filter.mp htm).2
      have hloop := REq_fireAll s0 _ hle h
      cases hlast : ((ServerModel.occurrences s0.cron s0.nextRunAt n).filter (· ≤ n)).getLast? with
      | none => exact hloop
      | some last => exact REq_setSchedule hloop _

/-! ### 5l. `taskCreate` — a promise read, creation, or the inner task
    read; every piece already built. -/

theorem agrees_taskCreate (req) : Agrees (.api (.taskCreate req)) := by
  intro n sP sM h
  simp only [stepOfAP, stepOfA, handleAP, handleA, Projected.taskCreate,
             AbstractModel.taskCreate, run_map, Id.run]
  by_cases htag0 : (!req.action.tags.has "resonate:target") = true
  · simp only [if_pos htag0]; exact ⟨rfl, h⟩
  · simp only [if_neg htag0, run_pureUnit_bind]
    simp only [run_bind, run_viewPromise, run_touchPromise, run_pure]
    have hpi : (sP.promises.find? (·.id == req.action.id)).map (·.project n)
        = (sM.promises.find? (·.id == req.action.id)).map (·.project n) := h.1 req.action.id
    cases hfP : sP.promises.find? (·.id == req.action.id) with
    | none =>
        cases hfM : sM.promises.find? (·.id == req.action.id) with
        | some pM => rw [hfP, hfM] at hpi; simp at hpi
        | none =>
            have hplP : pLook n sP req.action.id = none := by unfold pLook; rw [hfP]; rfl
            have hplM : pLook n sM req.action.id = none := by unfold pLook; rw [hfM]; rfl
            have htq : sP.tasks.find? (·.id == req.action.id)
                = sM.tasks.find? (·.id == req.action.id) := by
              rw [← tLook_raw hplP, ← tLook_raw hplM]; exact h.2.1 req.action.id
            simp only [Option.map]
            by_cases hto : req.action.timeoutAt > n
            · simp only [if_pos hto]
              exact ⟨rfl, REq_setTask (REq_setPromise h
                ({ id := req.action.id, state := PromiseState.pending, param := req.action.param,
                   tags := req.action.tags, timeoutAt := req.action.timeoutAt, createdAt := n } : PromiseObject) htq) _⟩
            · simp only [if_neg hto]
              exact ⟨rfl, REq_setTask (REq_setPromise h
                ({ id := req.action.id,
                   state := if req.action.tags.isTimer then PromiseState.resolved else PromiseState.rejectedTimedout,
                   param := req.action.param, tags := req.action.tags, timeoutAt := req.action.timeoutAt,
                   createdAt := req.action.timeoutAt, settledAt := some req.action.timeoutAt } : PromiseObject) htq) _⟩
    | some pP =>
        cases hfM : sM.promises.find? (·.id == req.action.id) with
        | none => rw [hfP, hfM] at hpi; simp at hpi
        | some pM =>
            rw [hfP, hfM] at hpi
            simp only [Option.map] at hpi
            have hproj : pP.project n = pM.project n := Option.some.inj hpi
            have hpm : (pM.id == req.action.id) = true :=
              find?_sat (fun x => x.id == req.action.id) sM.promises pM hfM
            have hf' : sM.promises.find? (·.id == pM.id) = some pM := by
              rw [eq_of_beq hpm]; exact hfM
            simp only [Option.map]
            rw [hproj]
            by_cases htag2 : (!(pM.project n).tags.has "resonate:target") = true
            · simp only [if_pos htag2]
              refine ⟨rfl, ?_⟩
              by_cases hc : (((pM.project n).state != pM.state) = true)
              · simp only [if_pos hc]; exact REq_touchWrite h pM hf'
              · simp only [if_neg hc]; exact h
            · simp only [if_neg htag2, run_pureUnit_bind]
              -- absorb the promise touch, then the inner task read
              generalize hsM1 : (if (((pM.project n).state != pM.state) = true) then
                    touchWrite n pM sM
                  else sM) = sM1
              have h1 : REq n sP sM1 := by
                rw [← hsM1]
                by_cases hc : (((pM.project n).state != pM.state) = true)
                · simp only [if_pos hc]; exact REq_touchWrite h pM hf'
                · simp only [if_neg hc]; exact h
              refine And.imp (congrArg Equivalence.Response.taskCreate) (fun x => x)
                (bind_taskRead_agrees (pM.project n).id n _ _ h1 ?_)
              intro v sP' sM' h' hf
              cases v with
              | none => exact ⟨rfl, h'⟩
              | some pair =>
                  obtain ⟨t, po⟩ := pair
                  cases po with
                  | none => exact ⟨rfl, h'⟩
                  | some pv =>
                      dsimp only
                      by_cases hg1 : (t.state == TaskState.fulfilled) = true
                      · simp only [if_pos hg1]; exact ⟨rfl, h'⟩
                      · simp only [if_neg hg1, run_pureUnit_bind]
                        by_cases hg2 : (t.state == TaskState.pending) = true
                        · simp only [if_pos hg2]
                          exact ⟨rfl, REq_setTask h' _⟩
                        · simp only [if_neg hg2, run_pureUnit_bind]
                          exact ⟨rfl, h'⟩

/-! ### 6. Assembly: every step agrees, and the theorem falls out -/

theorem stepAgreement : StepAgreement := by
  intro st
  cases st with
  | api rq =>
      cases rq with
      | promiseGet req => exact agrees_promiseGet req
      | promiseCreate req => exact agrees_promiseCreate req
      | promiseSettle req => exact agrees_promiseSettle req
      | promiseRegisterCallback req => exact agrees_promiseRegisterCallback req
      | promiseRegisterListener req => exact agrees_promiseRegisterListener req
      | promiseSearch req => exact agrees_promiseSearch req
      | scheduleGet req => exact agrees_scheduleGet req
      | scheduleCreate req => exact agrees_scheduleCreate req
      | scheduleDelete req => exact agrees_scheduleDelete req
      | scheduleSearch req => exact agrees_scheduleSearch req
      | taskGet req => exact agrees_taskGet req
      | taskCreate req => exact agrees_taskCreate req
      | taskAcquire req => exact agrees_taskAcquire req
      | taskFence req => exact agrees_taskFence req
      | taskHeartbeat req => exact agrees_taskHeartbeat req
      | taskSuspend req => exact agrees_taskSuspend req
      | taskFulfill req => exact agrees_taskFulfill req
      | taskRelease req => exact agrees_taskRelease req
      | taskHalt req => exact agrees_taskHalt req
      | taskContinue req => exact agrees_taskContinue req
      | taskSearch req => exact agrees_taskSearch req
      | τPromiseTimeout id => exact fun n sP sM h => ⟨rfl, h⟩
      | τTaskRetryTimeout id => exact fun n sP sM h => ⟨rfl, h⟩
      | τTaskLeaseTimeout id => exact fun n sP sM h => ⟨rfl, h⟩
      | τScheduleTimeout id => exact fun n sP sM h => ⟨rfl, h⟩
      | τResume req => exact fun n sP sM h => ⟨rfl, h⟩
      | idle => exact fun n sP sM h => ⟨rfl, h⟩
  | r1 id => exact agrees_r1 id
  | r2 id => exact agrees_r2 id
  | r3 id address => exact agrees_r3 id address
  | r4 id awaiter => exact agrees_r4 id awaiter
  | r5 id => exact agrees_r5 id
  | r6 id next => exact agrees_r6 id next
  | r7 id => exact agrees_r7 id
  | idle => exact agrees_idle

/-- **THE THEOREM, UNCONDITIONALLY.** Feed the same schedule — every
    step, adversarial rule firings included — to both read disciplines
    from the same initial state, and the response streams are
    pointwise identical. Projection and materialization are the same
    machine, on the synchronous channel, at every step, forever. -/
theorem responseLockstepAbstract : ResponseLockstepAbstract :=
  responseLockstep_of_stepAgreement stepAgreement

end Proof
end Abstraction
