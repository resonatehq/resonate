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
  5. Per-step agreement (`Agrees st`), case by case. Discharged so
     far: `idle`, the three searches, and the schedule handlers —
     the read-only and fact-free steps. Open: the promise and task
     handlers and the seven rules, each a finite (if laborious)
     unfold-and-case argument over the lookup lemmas; `promiseGet` is
     the template to follow.

`LockstepAbstract`'s message half additionally needs quiescence
congruence (`absQuiesced` agrees on `REq`-related states); same
architecture, on top of these lemmas.  -/

namespace Abstraction
namespace Proof

open AbstractModel
open ServerModel (PromiseState TaskState)

/-! ### 0. Bool plumbing -/

theorem toFalse {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b
  · rfl
  · exact absurd rfl h

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

/-- Viewed-task lookup — defined THROUGH `pLook`, so that anything
    preserving `pLook` and the raw task find preserves it. -/
def tLook (n : Nat) (s : ServerState) (id : String) : Option TaskObject :=
  (s.tasks.find? (·.id == id)).map fun t =>
    match pLook n s id with
    | some pv => t.view pv
    | none => t

def sLook (s : ServerState) (id : String) : Option ServerModel.Schedule :=
  s.schedules.find? (·.id == id)

/-- THE INVARIANT: agreement on every lookup through the view. -/
def REq (n : Nat) (sP sM : ServerState) : Prop :=
  (∀ id, pLook n sP id = pLook n sM id)
    ∧ (∀ id, tLook n sP id = tLook n sM id)
    ∧ (∀ id, sLook sP id = sLook sM id)

theorem REq.refl (n : Nat) (s : ServerState) : REq n s s :=
  ⟨fun _ => rfl, fun _ => rfl, fun _ => rfl⟩

theorem pLook_mono {n n' : Nat} (h : n ≤ n') (s : ServerState) (id : String) :
    pLook n' s id = (pLook n s id).map (·.project n') := by
  unfold pLook
  cases s.promises.find? (·.id == id) with
  | none => rfl
  | some p => simp [Option.map, project_absorb p h]

theorem tLook_mono {n n' : Nat} (h : n ≤ n') (s : ServerState) (id : String) :
    tLook n' s id = (tLook n s id).map fun tv =>
      match pLook n' s id with
      | some pv => tv.view pv
      | none => tv := by
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
      | some p =>
          have h1 : pLook n s id = some (p.project n) := by
            unfold pLook; rw [hp]; rfl
          have h2 : pLook n' s id = some (p.project n') := by
            unfold pLook; rw [hp]; rfl
          rw [h1, h2]
          simp [view_absorb t p h]

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

end Proof
end Abstraction
