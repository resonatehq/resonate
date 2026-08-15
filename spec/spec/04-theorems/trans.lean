import «04-theorems».«frame»

/-!  # The transition tier

Half the `.trans` catalogue has one shape: take a row of the pre-state,
look it up in the post-state, and relate the two. This file proves those
in one move, and the move is that the SAME handler discharge does the
work.

## The trick

`WritesGood` says every row a step writes satisfies a predicate on ONE
row. A relational property needs a predicate on two. But the pre-state
is fixed for the duration of a step — that is the reader discipline
again — so "relates correctly to the row it replaced" IS a predicate on
one row:

    relPred R a x  :=  match a.promises.find? (·.id == x.id) with
                       | none   => true          -- a birth: nothing to relate to
                       | some p => R p x

So a relational obligation is an ordinary `Q`, `handlers.lean` applies
unchanged, and nothing about the 28 handlers has to be re-proved. The
obligations become "R survives one more transformation on the right",
which is again a statement about `PromiseObject` alone.

## The two facts this needed

Both were already true of the machine and merely unstated.

A birth writes an id the store does not have — `promise.create`,
`task.create` and the schedule fire all reach `createPromise` only after
a read returned `none`. Without that, `relPred` at a birth would have to
relate the newborn to a row it is replacing, and there is no such row.
`writesGood_afterReadPromise` now hands the freshness fact to its `none`
branch.

A settle only ever settles a promise read as PENDING. Without that,
`preserved_settled_promise_record` is false at the settle obligation:
nothing would stop a step from re-settling an already-settled row.

## Where uniqueness enters

`find?_self_of_nodup` is the bridge: a row of the pre-state is the one
its own id looks up. That is `well_formed_store_promise_ids_unique`,
proved in `frame.lean`, and it is a genuine prerequisite rather than a
convenience — with a duplicate id the shadowed copy would relate to
nothing and every entry here would be vacuous for it. -/

namespace Abstraction
namespace Trans

open AbstractModel
open Abstraction.Induction
open Abstraction.Frame

/-! ## A row is what its own id looks up -/

theorem find?_self_of_nodup {α} (key : α → String) :
    ∀ (l : List α), (l.map key).Nodup → ∀ (x : α), x ∈ l →
      l.find? (fun y => key y == key x) = some x
  | [],     _,   x, hx => absurd hx (by simp)
  | c :: l, hnd, x, hx => by
      simp only [List.map_cons] at hnd
      have hnc := List.nodup_cons.mp hnd
      by_cases hk : (key c == key x) = true
      · have hxc : x = c := by
          rcases List.mem_cons.mp hx with rfl | hxl
          · rfl
          · exact absurd (by
              refine List.mem_map.mpr ⟨x, hxl, ?_⟩
              exact (eq_of_beq hk).symm) hnc.1
        simp [List.find?_cons, hk, hxc]
      · have hxl : x ∈ l := by
          rcases List.mem_cons.mp hx with rfl | h
          · exact absurd (by simp) hk
          · exact h
        simp only [List.find?_cons, hk, if_false]
        exact find?_self_of_nodup key l hnc.2 x hxl

/-! ## Ids that transformations preserve -/

theorem addCallback_id (p : PromiseObject) (a : String) : (p.addCallback a).id = p.id := by
  unfold PromiseObject.addCallback; split <;> rfl

theorem addListener_id (p : PromiseObject) (a : String) : (p.addListener a).id = p.id := by
  unfold PromiseObject.addListener; split <;> rfl

/-! ## The relational obligation set

Six fields and reflexivity. Compare `HPromise`: the births are gone,
because a birth has no row to relate to, and that is exactly what the
freshness fact buys. -/

structure HRel (R : PromiseObject → PromiseObject → Bool) : Prop where
  refl         : ∀ p, R p p = true
  project      : ∀ p₀ p (n : Nat), R p₀ p = true → R p₀ (p.project n) = true
  addCallback  : ∀ p₀ p (a : String), R p₀ p = true → R p₀ (p.addCallback a) = true
  addListener  : ∀ p₀ p (a : String), R p₀ p = true → R p₀ (p.addListener a) = true
  settle       : ∀ p₀ p (st : ServerModel.PromiseState) (v : ServerModel.Value) (t : Nat),
                   st.settable = true → p.state = .pending → t < p.timeoutAt →
                   R p₀ p = true →
                   R p₀ { p with state := st, value := v, settledAt := some t } = true
  dropListener : ∀ p₀ p (a : String), R p₀ p = true →
                   R p₀ { p with listeners := p.listeners.filter (· != a) } = true
  dropCallback : ∀ p₀ p (a : String), R p₀ p = true →
                   R p₀ { p with callbacks := p.callbacks.filter (· != a) } = true

def relPred (R : PromiseObject → PromiseObject → Bool) (a : ServerState)
    (x : PromiseObject) : Bool :=
  match a.promises.find? (·.id == x.id) with
  | none   => true
  | some p => R p x

def relQ (R : PromiseObject → PromiseObject → Bool) (a : ServerState) : Q :=
  { promise := relPred R a, task := fun _ => true, schedule := fun _ => true }

theorem hereditary_rel {R : PromiseObject → PromiseObject → Bool} (h : HRel R)
    (a : ServerState) : Hereditary (relQ R a) a where
  project p n hp := by
    show relPred R a (p.project n) = true
    cases hf : a.promises.find? (fun x => x.id == p.id) with
    | none => simp [relPred, Lookup.project_id, hf]
    | some p₀ =>
        have hp' : R p₀ p = true := by simpa [relQ, relPred, hf] using hp
        simp [relPred, Lookup.project_id, hf, h.project p₀ p n hp']
  addCallback p c hp := by
    show relPred R a (p.addCallback c) = true
    cases hf : a.promises.find? (fun x => x.id == p.id) with
    | none => simp [relPred, addCallback_id, hf]
    | some p₀ =>
        have hp' : R p₀ p = true := by simpa [relQ, relPred, hf] using hp
        simp [relPred, addCallback_id, hf, h.addCallback p₀ p c hp']
  addListener p c hp := by
    show relPred R a (p.addListener c) = true
    cases hf : a.promises.find? (fun x => x.id == p.id) with
    | none => simp [relPred, addListener_id, hf]
    | some p₀ =>
        have hp' : R p₀ p = true := by simpa [relQ, relPred, hf] using hp
        simp [relPred, addListener_id, hf, h.addListener p₀ p c hp']
  settle p st v t hs hpend hdue hp := by
    show relPred R a _ = true
    cases hf : a.promises.find? (fun x => x.id == p.id) with
    | none => simp [relPred, hf]
    | some p₀ =>
        have hp' : R p₀ p = true := by simpa [relQ, relPred, hf] using hp
        simp [relPred, hf, h.settle p₀ p st v t hs hpend hdue hp']
  dropListener p c hp := by
    show relPred R a _ = true
    cases hf : a.promises.find? (fun x => x.id == p.id) with
    | none => simp [relPred, hf]
    | some p₀ =>
        have hp' : R p₀ p = true := by simpa [relQ, relPred, hf] using hp
        simp [relPred, hf, h.dropListener p₀ p c hp']
  dropCallback p c hp := by
    show relPred R a _ = true
    cases hf : a.promises.find? (fun x => x.id == p.id) with
    | none => simp [relPred, hf]
    | some p₀ =>
        have hp' : R p₀ p = true := by simpa [relQ, relPred, hf] using hp
        simp [relPred, hf, h.dropCallback p₀ p c hp']
  live id param tags tAt cAt _ hfresh := by simp [relQ, relPred, hfresh]
  dead id st param tags tAt _ hfresh := by simp [relQ, relPred, hfresh]
  tFulfill _ _ := rfl
  tBornPending _ _ := rfl
  tBornDone _ := rfl
  tBornHeld _ _ _ _ := rfl
  tAcquire _ _ _ _ _ := rfl
  tHeartbeat _ _ _ _ := rfl
  tClearResumes _ _ := rfl
  tSuspend _ _ := rfl
  tRepend _ _ _ := rfl
  tHalt _ _ := rfl
  tContinue _ _ _ _ := rfl
  tResume _ _ _ _ _ := rfl
  tAddResume _ _ _ _ _ _ := rfl
  tRearm _ _ _ _ := rfl
  cBorn _ _ _ _ _ _ _ _ := rfl
  cAdvance _ _ _ := rfl

/-! ## The step law

For every row of the pre-state there is a row of the post-state under
the same id, and the two are related. Existence comes from the frame —
rows are never deleted — and the relation from the handler discharge. -/

theorem trans_promise {R : PromiseObject → PromiseObject → Bool} (h : HRel R)
    (mat : Bool) (st : Step) (now : Nat) (s : ServerState)
    (hnd : (s.promises.map (·.id)).Nodup) :
    ∀ p ∈ s.promises,
      ∃ q, (stepOf mat st now s).2.promises.find? (·.id == p.id) = some q ∧ R p q = true := by
  intro p hp
  have hself : s.promises.find? (·.id == p.id) = some p :=
    find?_self_of_nodup (·.id) s.promises hnd p hp
  have hstore : PerStore (relQ R s) s = true := by
    refine perStore_mk ?_ (all_const _) (all_const _)
    refine List.all_eq_true.mpr (fun x hx => ?_)
    have hx' : s.promises.find? (·.id == x.id) = some x :=
      find?_self_of_nodup (·.id) s.promises hnd x hx
    simp [relQ, relPred, hx', h.refl x]
  have hw := writesGood_handle (e := { state := s, mat := mat })
    (hereditary_rel h s) hstore st now
  have hstep : (stepOf mat st now s).2
      = applyAll s ((handle st now) { state := s, mat := mat }).2 := rfl
  rw [hstep]
  rcases find?_applyAll_promise ((handle st now) { state := s, mat := mat }).2 s p.id with
    hfr | ⟨x, hxw, hfr⟩
  · exact ⟨p, by rw [hfr, hself], h.refl p⟩
  · refine ⟨x, hfr, ?_⟩
    have hxid : (x.id == p.id) = true := by
      have := List.find?_some hfr
      simpa using this
    have hxeq : x.id = p.id := eq_of_beq hxid
    have hgood : relPred R s x = true := hw _ hxw
    simp only [relPred, hxeq, hself] at hgood
    exact hgood

/-! ## The catalogue entries

Each is `trans_promise` at its own `R`, plus the shape the entry is
written in. Note that both use the EXISTENCE half as well: the entries
say the row is still there, and `frame.lean` is why. -/

section Entries

open Properties

/-! ### The birth fields never move

The immutability that made `created_at ≤ timeout_at` inductive in the
first place, now stated as the transition property it really is: no
step changes a promise's `param`, `tags`, `timeoutAt` or `createdAt`. -/

def rBirthFields (p q : PromiseObject) : Bool :=
  q.param.data == p.param.data && q.param.headers == p.param.headers
    && q.tags == p.tags && q.timeoutAt == p.timeoutAt && q.createdAt == p.createdAt

theorem hrel_birthFields : HRel rBirthFields where
  refl p := by simp [rBirthFields]
  project p₀ p n h := by
    unfold PromiseObject.project
    split
    · split <;> simpa [rBirthFields] using h
    · simpa [rBirthFields] using h
  addCallback p₀ p c h := by
    unfold PromiseObject.addCallback
    split <;> simpa [rBirthFields] using h
  addListener p₀ p c h := by
    unfold PromiseObject.addListener
    split <;> simpa [rBirthFields] using h
  settle p₀ p st v t _ _ _ h := by simpa [rBirthFields] using h
  dropListener p₀ p c h := by simpa [rBirthFields] using h
  dropCallback p₀ p c h := by simpa [rBirthFields] using h

theorem preserved_promise_birth_fields_immutable_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) (hnd : (s.promises.map (·.id)).Nodup) :
    preserved_promise_birth_fields_immutable n' s (stepOf mat st now s).2 = true := by
  refine List.all_eq_true.mpr (fun p hp => ?_)
  obtain ⟨q, hfind, hR⟩ := trans_promise hrel_birthFields mat st now s hnd p hp
  show (match (stepOf mat st now s).2.promises.find? (·.id == p.id) with
        | none => true | some q => _) = true
  rw [hfind]
  exact hR

/-! ### A settled promise's record is frozen

And note what it does NOT say: the promise itself keeps changing, because
the drains keep removing obligations from it. What is frozen is exactly
`toRecord` — the part a response can carry. The obligation that makes
this work is `settle` needing the row to be PENDING; without it nothing
would stop a step from re-settling a settled row, and this entry would
fail there rather than anywhere in the handlers. -/

def rSettledRecord (p q : PromiseObject) : Bool :=
  p.state == .pending
    || (q.state == p.state && q.settledAt == p.settledAt
        && q.value.data == p.value.data && q.value.headers == p.value.headers)

theorem hrel_settledRecord : HRel rSettledRecord where
  refl p := by simp [rSettledRecord]
  project p₀ p n h := by
    by_cases hp : p₀.state = ServerModel.PromiseState.pending
    · simp [rSettledRecord, hp]
    · have hq : (p.state == p₀.state) = true := by
        simp only [rSettledRecord, Bool.or_eq_true, Bool.and_eq_true] at h
        rcases h with h | ⟨⟨⟨h1, _⟩, _⟩, _⟩
        · exact absurd (by simpa using h) hp
        · exact h1
      have hnp : p.state ≠ ServerModel.PromiseState.pending := by
        intro hc; exact hp (by rw [← eq_of_beq hq, hc])
      rw [Lookup.project_not_pending p n (by simp [hnp])]
      exact h
  addCallback p₀ p c h := by
    unfold PromiseObject.addCallback
    split <;> simpa [rSettledRecord] using h
  addListener p₀ p c h := by
    unfold PromiseObject.addListener
    split <;> simpa [rSettledRecord] using h
  settle p₀ p st v t _ hpend _ h := by
    by_cases hp : p₀.state = ServerModel.PromiseState.pending
    · simp [rSettledRecord, hp]
    · exfalso
      have hq : (p.state == p₀.state) = true := by
        simp only [rSettledRecord, Bool.or_eq_true, Bool.and_eq_true] at h
        rcases h with h | ⟨⟨⟨h1, _⟩, _⟩, _⟩
        · exact absurd (by simpa using h) hp
        · exact h1
      exact hp (by rw [← eq_of_beq hq, hpend])
  dropListener p₀ p c h := by simpa [rSettledRecord] using h
  dropCallback p₀ p c h := by simpa [rSettledRecord] using h

theorem preserved_settled_promise_record_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) (hnd : (s.promises.map (·.id)).Nodup) :
    preserved_settled_promise_record n' s (stepOf mat st now s).2 = true := by
  refine List.all_eq_true.mpr (fun p hp => ?_)
  obtain ⟨q, hfind, hR⟩ := trans_promise hrel_settledRecord mat st now s hnd p hp
  show (p.state == ServerModel.PromiseState.pending ||
        (match (stepOf mat st now s).2.promises.find? (·.id == p.id) with
         | none => false | some q => _)) = true
  rw [hfind]
  simpa [rSettledRecord] using hR

/-! ### A settled promise's STATE is frozen

A strict weakening of the record entry, so it costs a projection rather
than another `HRel`. -/

theorem preserved_promise_state_frozen_once_settled_step (mat : Bool) (st : Step)
    (now n' : Nat) (s : ServerState) (hnd : (s.promises.map (·.id)).Nodup) :
    preserved_promise_state_frozen_once_settled n' s (stepOf mat st now s).2 = true := by
  refine List.all_eq_true.mpr (fun p hp => ?_)
  obtain ⟨q, hfind, hR⟩ := trans_promise hrel_settledRecord mat st now s hnd p hp
  show (p.state == ServerModel.PromiseState.pending ||
        match (stepOf mat st now s).2.promises.find? (·.id == p.id) with
        | none => false | some q => q.state == p.state) = true
  rw [hfind]
  simp only [rSettledRecord, Bool.or_eq_true, Bool.and_eq_true] at hR
  rcases hR with h | ⟨⟨⟨h1, _⟩, _⟩, _⟩
  · simp [h]
  · simp [h1]

/-! ### A pending promise's value never moves

The companion to `pending_has_no_value`: that entry says a pending
promise has no verdict, this one says nothing writes one into it
either. The obligation that carries it is `settle`, from `settable`
implying the new state is not pending — so the only way to acquire a
value is to stop being pending in the same write. -/

def rValueUntilSettled (p q : PromiseObject) : Bool :=
  q.state != .pending
    || (q.value.data == p.value.data && q.value.headers == p.value.headers)

theorem hrel_valueUntilSettled : HRel rValueUntilSettled where
  refl p := by simp [rValueUntilSettled]
  project p₀ p n h := by
    unfold PromiseObject.project
    split
    · split <;> simp [rValueUntilSettled]
    · simpa [rValueUntilSettled] using h
  addCallback p₀ p c h := by
    unfold PromiseObject.addCallback
    split <;> simpa [rValueUntilSettled] using h
  addListener p₀ p c h := by
    unfold PromiseObject.addListener
    split <;> simpa [rValueUntilSettled] using h
  settle p₀ p st v t hst _ _ _ := by
    cases st <;> simp_all [rValueUntilSettled, ServerModel.PromiseState.settable]
  dropListener p₀ p c h := by simpa [rValueUntilSettled] using h
  dropCallback p₀ p c h := by simpa [rValueUntilSettled] using h

theorem preserved_promise_value_until_settlement_step (mat : Bool) (st : Step)
    (now n' : Nat) (s : ServerState) (hnd : (s.promises.map (·.id)).Nodup) :
    preserved_promise_value_until_settlement n' s (stepOf mat st now s).2 = true := by
  refine List.all_eq_true.mpr (fun p hp => ?_)
  obtain ⟨q, hfind, hR⟩ := trans_promise hrel_valueUntilSettled mat st now s hnd p hp
  show (match (stepOf mat st now s).2.promises.find? (·.id == p.id) with
        | none => false | some q => _) = true
  rw [hfind]
  exact hR

/-! ### Ids stay distinct across a step

The `.trans` restatement of the store invariant, and it is the same
theorem: `StoreNodup` is preserved, so the post-state's ids are
distinct. It appears twice in the catalogue because a trace checker sees
states and transitions through different windows. -/

theorem preserved_promise_no_duplicate_ids_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) (h : StoreNodup s) :
    preserved_promise_no_duplicate_ids n' s (stepOf mat st now s).2 = true :=
  promise_ids_unique_of_nodup n' _ (storeNodup_step mat st now s h)

end Entries

end Trans
end Abstraction
