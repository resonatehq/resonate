import «04-theorems».«entries»

/-!  # The frame

The `.trans` half of the catalogue is not about states; it is about
what a step may CHANGE. Half of those entries have the same shape —
take a row of the pre-state, look it up in the post-state, and relate
the two — so they all rest on one fact about `applyAll`:

    a lookup in the post-state either finds exactly what the pre-state
    had, or finds a row the step WROTE.

That is `find?_applyAll_promise` below, and it is the trans-half
counterpart of `perStore_applyAll`. Where the state tier reduces "is
this inductive?" to "what does each handler write?", the frame reduces
"what may this step change?" to the same question — the difference is
that the obligation now relates the written row to the one it replaced
rather than judging it alone.

## What this file proves outright

Rows are never deleted. `Effect.apply` removes a promise or a task only
by writing another with the same id, so an id present before a step is
present after it, whatever the step was. Two catalogue entries follow
with no per-property obligation at all — they are facts about the
effect algebra, not about the protocol.

`delSchedule` is the exception, and that is why there is no
`monotone_schedule_set_grows` in the catalogue: schedules CAN be
deleted, and the catalogue says so by omission.

## Uniqueness, and why it belongs here

The relational entries need one more thing: that a row of the pre-state
is the one `find?` returns for its own id. Without it a duplicate id
could hide a row from its own lookup, and every "preserved" entry would
be vacuous for the shadowed copy. So the four `_ids_unique` /
`_keys_unique` entries are a PREREQUISITE for the trans tier rather
than four more entries, and they are proved here.

They are also the only entries that are facts about the effect algebra
alone. Every write in the machine has the shape `x :: filter (key ≠
key x)`, which is exactly "remove any row with this key, then add
one" — so uniqueness is preserved by construction, whatever the handler
was trying to do. No `Hereditary` obligation appears below.

What core does not ship is the bridge from `Nodup` to
`eraseDups.length`, since `eraseDups` is an accumulator loop with no
verification API. `eraseDupsBy_loop_length` supplies it. -/

namespace Abstraction
namespace Frame

open AbstractModel

/-! ## Nothing is deleted -/

theorem any_id_upsert {α} (idOf : α → String) (x : α) (l : List α) (id : String)
    (h : l.any (fun y => idOf y == id) = true) :
    (x :: l.filter (fun y => idOf y != idOf x)).any (fun y => idOf y == id) = true := by
  obtain ⟨y, hy, hyid⟩ := List.any_eq_true.mp h
  by_cases hx : (idOf x == id) = true
  · simp [List.any_cons, hx]
  · refine List.any_eq_true.mpr ⟨y, ?_, hyid⟩
    simp only [List.mem_cons]
    refine Or.inr (List.mem_filter.mpr ⟨hy, ?_⟩)
    simp only [bne_iff_ne, ne_eq]
    intro hc
    exact absurd (hc ▸ hyid) hx

theorem promise_id_apply (f : Effect) (s : ServerState) (id : String)
    (h : s.promises.any (·.id == id) = true) :
    (f.apply s).promises.any (·.id == id) = true := by
  cases f with
  | setPromise x => exact any_id_upsert (·.id) x s.promises id h
  | setTask _ | setSchedule _ | delSchedule _ | setMessage _ _ =>
      simpa [Effect.apply] using h

theorem task_id_apply (f : Effect) (s : ServerState) (id : String)
    (h : s.tasks.any (·.id == id) = true) :
    (f.apply s).tasks.any (·.id == id) = true := by
  cases f with
  | setTask x => exact any_id_upsert (·.id) x s.tasks id h
  | setPromise _ | setSchedule _ | delSchedule _ | setMessage _ _ =>
      simpa [Effect.apply] using h

theorem promise_id_applyAll :
    ∀ (w : List Effect) (s : ServerState) (id : String),
      s.promises.any (·.id == id) = true → (applyAll s w).promises.any (·.id == id) = true
  | [],      _, _,  h => h
  | f :: fs, s, id, h => promise_id_applyAll fs (f.apply s) id (promise_id_apply f s id h)

theorem task_id_applyAll :
    ∀ (w : List Effect) (s : ServerState) (id : String),
      s.tasks.any (·.id == id) = true → (applyAll s w).tasks.any (·.id == id) = true
  | [],      _, _,  h => h
  | f :: fs, s, id, h => task_id_applyAll fs (f.apply s) id (task_id_apply f s id h)

/-! ## The lookup frame

Either the step left this id alone, or it wrote the row that is there
now. Stated as a disjunction rather than a conditional because the
per-handler obligation is exactly the second disjunct: it is about the
rows in the write list, which is what `WritesGood` already talks about. -/

theorem find?_applyAll_promise :
    ∀ (w : List Effect) (s : ServerState) (id : String),
      (applyAll s w).promises.find? (·.id == id) = s.promises.find? (·.id == id)
      ∨ ∃ x, Effect.setPromise x ∈ w
             ∧ (applyAll s w).promises.find? (·.id == id) = some x
  | [],      _, _  => Or.inl rfl
  | f :: fs, s, id => by
      rw [show applyAll s (f :: fs) = applyAll (f.apply s) fs from rfl]
      rcases find?_applyAll_promise fs (f.apply s) id with h | ⟨x, hx, hfind⟩
      · rw [h]
        cases f with
        | setPromise y =>
            rw [show (Effect.apply s (Effect.setPromise y)).promises
                  = y :: s.promises.filter (fun z => z.id != y.id) from rfl,
                Lookup.find?_upsert (·.id) y s.promises id]
            by_cases hy : (y.id == id) = true
            · exact Or.inr ⟨y, by simp, by rw [if_pos hy]⟩
            · exact Or.inl (by rw [if_neg hy])
        | setTask _ | setSchedule _ | delSchedule _ | setMessage _ _ => exact Or.inl rfl
      · exact Or.inr ⟨x, by simp [hx], hfind⟩

theorem find?_applyAll_task :
    ∀ (w : List Effect) (s : ServerState) (id : String),
      (applyAll s w).tasks.find? (·.id == id) = s.tasks.find? (·.id == id)
      ∨ ∃ x, Effect.setTask x ∈ w ∧ (applyAll s w).tasks.find? (·.id == id) = some x
  | [],      _, _  => Or.inl rfl
  | f :: fs, s, id => by
      rw [show applyAll s (f :: fs) = applyAll (f.apply s) fs from rfl]
      rcases find?_applyAll_task fs (f.apply s) id with h | ⟨x, hx, hfind⟩
      · rw [h]
        cases f with
        | setTask y =>
            rw [show (Effect.apply s (Effect.setTask y)).tasks
                  = y :: s.tasks.filter (fun z => z.id != y.id) from rfl,
                Lookup.find?_upsert (·.id) y s.tasks id]
            by_cases hy : (y.id == id) = true
            · exact Or.inr ⟨y, by simp, by rw [if_pos hy]⟩
            · exact Or.inl (by rw [if_neg hy])
        | setPromise _ | setSchedule _ | delSchedule _ | setMessage _ _ => exact Or.inl rfl
      · exact Or.inr ⟨x, by simp [hx], hfind⟩

/-! ## Uniqueness

`List.eraseDups` is `eraseDupsBy (· == ·)`, an accumulator loop, and
core ships no lemma relating its length to `Nodup`. The invariant the
loop maintains is the obvious one — the accumulator holds exactly the
elements already emitted — and under the disjointness hypothesis no
element is ever dropped. -/

theorem eraseDupsBy_loop_length {α} [BEq α] [LawfulBEq α] :
    ∀ (as bs : List α), as.Nodup → (∀ a ∈ as, a ∉ bs) →
      (List.eraseDupsBy.loop (· == ·) as bs).length = bs.length + as.length
  | [],      bs, _,   _      => by simp [List.eraseDupsBy.loop]
  | a :: as, bs, hnd, hdisj => by
      have hab : bs.any (a == ·) = false := by
        cases hcase : bs.any (fun x => a == x) with
        | false => rfl
        | true =>
            obtain ⟨b, hb, heq⟩ := List.any_eq_true.mp hcase
            exact absurd (by simpa [eq_of_beq heq] using hb) (hdisj a (by simp))
      have hnd' : as.Nodup := (List.nodup_cons.mp hnd).2
      have hna : a ∉ as := (List.nodup_cons.mp hnd).1
      have hdisj' : ∀ x ∈ as, x ∉ (a :: bs) := by
        intro x hx hmem
        rcases List.mem_cons.mp hmem with rfl | hb
        · exact hna hx
        · exact hdisj x (by simp [hx]) hb
      have ih := eraseDupsBy_loop_length as (a :: bs) hnd' hdisj'
      simp only [List.eraseDupsBy.loop, hab]
      simp only [ih, List.length_cons]
      omega

theorem eraseDups_length_of_nodup {α} [BEq α] [LawfulBEq α] (l : List α) (h : l.Nodup) :
    l.eraseDups.length = l.length := by
  have := eraseDupsBy_loop_length l [] h (by simp)
  simpa [List.eraseDups, List.eraseDupsBy] using this

/-- Every write in the machine is `x :: filter (key ≠ key x)`. That
    shape preserves key-uniqueness whatever `x` is, which is why no
    handler appears in this argument. -/
theorem nodup_upsert {α} (key : α → String) (x : α) (l : List α)
    (h : (l.map key).Nodup) :
    (((x :: l.filter (fun y => key y != key x)).map key).Nodup) := by
  simp only [List.map_cons]
  refine List.nodup_cons.mpr ⟨?_, ?_⟩
  · intro hmem
    obtain ⟨y, hy, hyid⟩ := List.mem_map.mp hmem
    have hne := (List.mem_filter.mp hy).2
    simp only [bne_iff_ne, ne_eq] at hne
    exact hne hyid
  · exact List.Nodup.sublist (List.Sublist.map key List.filter_sublist) h

theorem nodup_filter {α} (key : α → String) (p : α → Bool) (l : List α)
    (h : (l.map key).Nodup) : ((l.filter p).map key).Nodup :=
  List.Nodup.sublist (List.Sublist.map key List.filter_sublist) h

/-- Every table's keys are distinct. Carried as one predicate because
    one step can write into several tables. -/
def StoreNodup (s : ServerState) : Prop :=
  (s.promises.map (·.id)).Nodup ∧ (s.tasks.map (·.id)).Nodup
    ∧ (s.schedules.map (·.id)).Nodup ∧ (s.outbox.map (·.key)).Nodup

theorem storeNodup_apply (f : Effect) (s : ServerState) (h : StoreNodup s) :
    StoreNodup (f.apply s) := by
  obtain ⟨h1, h2, h3, h4⟩ := h
  cases f with
  | setPromise x  => exact ⟨nodup_upsert _ x _ h1, h2, h3, h4⟩
  | setTask x     => exact ⟨h1, nodup_upsert _ x _ h2, h3, h4⟩
  | setSchedule x => exact ⟨h1, h2, nodup_upsert _ x _ h3, h4⟩
  | delSchedule i => exact ⟨h1, h2, nodup_filter _ _ _ h3, h4⟩
  | setMessage a m => exact ⟨h1, h2, h3, nodup_upsert _ _ _ h4⟩

theorem storeNodup_applyAll :
    ∀ (w : List Effect) (s : ServerState), StoreNodup s → StoreNodup (applyAll s w)
  | [],      _, h => h
  | f :: fs, s, h => storeNodup_applyAll fs (f.apply s) (storeNodup_apply f s h)

theorem storeNodup_init : StoreNodup ServerState.init :=
  ⟨List.nodup_nil, List.nodup_nil, List.nodup_nil, List.nodup_nil⟩

theorem storeNodup_step (mat : Bool) (st : Step) (now : Nat) (s : ServerState)
    (h : StoreNodup s) : StoreNodup (stepOf mat st now s).2 :=
  storeNodup_applyAll _ s h

/-! ## The two catalogue entries this buys

Both are `.trans` entries, and both need nothing from the handlers: no
step of the machine can delete a promise or a task, because the effect
algebra has no operation that does. -/

section Entries

open Properties

theorem monotone_promise_set_grows_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    monotone_promise_set_grows n' s (stepOf mat st now s).2 = true := by
  refine List.all_eq_true.mpr (fun p hp => ?_)
  show (stepOf mat st now s).2.promises.any (·.id == p.id) = true
  exact promise_id_applyAll _ s p.id
    (List.any_eq_true.mpr ⟨p, hp, by simp⟩)

theorem monotone_task_set_grows_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    monotone_task_set_grows n' s (stepOf mat st now s).2 = true := by
  refine List.all_eq_true.mpr (fun t ht => ?_)
  show (stepOf mat st now s).2.tasks.any (·.id == t.id) = true
  exact task_id_applyAll _ s t.id
    (List.any_eq_true.mpr ⟨t, ht, by simp⟩)

/-! ## And the four uniqueness entries, from `StoreNodup`

Stated as consequences of the strengthening rather than as their own
step laws, because `StoreNodup` is what is actually inductive: the
`eraseDups` phrasing is a decidable encoding of it, not the invariant
itself. -/

theorem promise_ids_unique_of_nodup (now : Nat) (s : ServerState) (h : StoreNodup s) :
    well_formed_store_promise_ids_unique now s = true := by
  simp [well_formed_store_promise_ids_unique, eraseDups_length_of_nodup _ h.1]

theorem task_ids_unique_of_nodup (now : Nat) (s : ServerState) (h : StoreNodup s) :
    well_formed_store_task_ids_unique now s = true := by
  simp [well_formed_store_task_ids_unique, eraseDups_length_of_nodup _ h.2.1]

theorem schedule_ids_unique_of_nodup (now : Nat) (s : ServerState) (h : StoreNodup s) :
    well_formed_store_schedule_ids_unique now s = true := by
  simp [well_formed_store_schedule_ids_unique, eraseDups_length_of_nodup _ h.2.2.1]

theorem outbox_keys_unique_of_nodup (now : Nat) (s : ServerState) (h : StoreNodup s) :
    well_formed_store_outbox_keys_unique now s = true := by
  simp [well_formed_store_outbox_keys_unique, eraseDups_length_of_nodup _ h.2.2.2]

end Entries

end Frame
end Abstraction
