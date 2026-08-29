import «04-theorems».«system»

/-!  # Lookups through the view

The pure lemmas, salvaged. They were the foundation of a lockstep proof
about two machines that no longer exist — but they are not about those
machines. They are about `PromiseObject.project`, `TaskObject.view`, and
the keyed lists `Effect.apply` maintains, all of which are exactly as
they were.

Three of them carry real content:

`project_absorb` — projecting at `n` and again at `n' ≥ n` is
projecting at `n'`. That is what makes Fact P a FACT rather than a
cache: the answer does not depend on when you asked.

`project_of_pending` — a promise that reads pending at `n` is not yet
due at `n`. Fact P contrapositive, and the missing ingredient for the
catalogue entries about settlement stamps: it is what lets a settle
know that `now < timeoutAt`.

`find?_upsert` — a lookup after `Effect.apply`'s `x :: filter (· !=
x.id)`. Every write in the machine has that shape, so every lemma about
what a write does to a lookup goes through this one.

`REq` and its `mono` are the view-lookup invariant: two states agree if
every lookup THROUGH THE VIEW agrees, pointwise by id. Not state
equality — the disciplines genuinely differ in stored bytes — and it is
still the right relation for stating that `run true` and `run false`
are indistinguishable. -/

namespace Abstraction
namespace Lookup

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

/-- Projection moves a promise's state, never the id of the object it
    is part of. `PromiseObject.project` cannot touch an id any more —
    it has none — so this is now a fact about `Object.project`, and it
    is `rfl`. -/
theorem project_id (o : Object) (n : Nat) : (o.project n).id = o.id := rfl

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

/-- A promise that READS pending is not yet due. The contrapositive of
    Fact P, and the form the induction needs: a handler that settles a
    promise it just read as pending is settling it strictly before its
    deadline, so the stamp it writes is below `timeoutAt`. -/
theorem project_pending_not_due {p : PromiseObject} {n : Nat}
    (h : ((p.project n).state == PromiseState.pending) = true) :
    n < (p.project n).timeoutAt := by
  have he : p.project n = p := project_of_pending h
  rw [he] at h ⊢
  by_cases hd : p.timeoutAt ≤ n
  · exfalso
    have hdue := project_due p h hd
    rw [he] at hdue
    by_cases ht : p.isTimer = true
    · rw [if_pos ht] at hdue
      have hst := congrArg PromiseObject.state hdue
      simp at hst
      simp [hst] at h
    · rw [if_neg ht] at hdue
      have hst := congrArg PromiseObject.state hdue
      simp at hst
      simp [hst] at h
  · omega

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

theorem find?_filter_ne {α κ} [BEq κ] [LawfulBEq κ] (idOf : α → κ) (a b : κ) (l : List α)
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
theorem find?_upsert {α κ} [BEq κ] [LawfulBEq κ] (idOf : α → κ) (x : α) (l : List α) (b : κ) :
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
def pLook (n : Nat) (s : ServerState) (id : ServerModel.Ident) : Option PromiseObject :=
  (s.promise? id).map (·.project n)

/-- Fact T as an `Option`-level combinator — named so that statements
    about `tLook` can mention it without anonymous matchers. -/
def applyView (t : TaskObject) : Option PromiseObject → TaskObject
  | some pv => t.view pv
  | none => t

/-- Viewed-task lookup — defined THROUGH `pLook`, so that anything
    preserving `pLook` and the raw task find preserves it. -/
def tLook (n : Nat) (s : ServerState) (id : ServerModel.Ident) : Option TaskObject :=
  (s.task? id).map fun t => applyView t (pLook n s id)

def sLook (s : ServerState) (id : ServerModel.Ident) : Option ServerModel.Schedule :=
  s.schedules.find? (·.id == id)

/-- THE INVARIANT: agreement on every lookup through the view. -/
def REq (n : Nat) (sP sM : ServerState) : Prop :=
  (∀ id, pLook n sP id = pLook n sM id)
    ∧ (∀ id, tLook n sP id = tLook n sM id)
    ∧ (∀ id, sLook sP id = sLook sM id)

theorem REq.refl (n : Nat) (s : ServerState) : REq n s s :=
  ⟨fun _ => rfl, fun _ => rfl, fun _ => rfl⟩

/-- The invariant is pointwise equality of lookups, so it is symmetric
    and transitive — the internal steps' shared code composes through it. -/
theorem REq.symm {n : Nat} {sP sM : ServerState} (h : REq n sP sM) : REq n sM sP :=
  ⟨fun id => (h.1 id).symm, fun id => (h.2.1 id).symm, fun id => (h.2.2 id).symm⟩

theorem REq.trans {n : Nat} {a b c : ServerState}
    (h1 : REq n a b) (h2 : REq n b c) : REq n a c :=
  ⟨fun id => (h1.1 id).trans (h2.1 id), fun id => (h1.2.1 id).trans (h2.2.1 id),
   fun id => (h1.2.2 id).trans (h2.2.2 id)⟩

theorem pLook_mono {n n' : Nat} (h : n ≤ n') (s : ServerState) (id : ServerModel.Ident) :
    pLook n' s id = (pLook n s id).map (·.project n') := by
  unfold pLook
  cases s.promise? id with
  | none => rfl
  | some p => simp [Option.map, project_absorb p h]

theorem tLook_mono {n n' : Nat} (h : n ≤ n') (s : ServerState) (id : ServerModel.Ident) :
    tLook n' s id = (tLook n s id).map fun tv => applyView tv (pLook n' s id) := by
  unfold tLook
  cases hf : s.task? id with
  | none => rfl
  | some t =>
      simp only [Option.map]
      cases hp : s.promise? id with
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

end Lookup
end Abstraction
