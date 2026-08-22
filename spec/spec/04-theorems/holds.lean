import «04-theorems».«bounded»
import «04-theorems».«trans»

/-!  # The invariants that hold along every run

This is where the tier work cashes out. Each entry below is a statement
about EVERY `Valid` trace starting at `init` — every schedule, every
length, every instant, both read disciplines — and none of it is
`decide`, a bound, or a sample.

## The bridge

Two ingredients, and they are exactly what `entries.lean` and
`frame.lean` produce:

  * the entry holds at `init`
  * every step preserves it

plus `Valid`, which pins each state to the step out of the one before.
`invariant_along_trace` composes them by induction on the index. That
is the whole argument; everything hard already happened in the
obligations.

## Why some go through a strengthening

Several catalogue entries are not inductive alone — the nine task
entries need each other's company, and the two value entries need
`pending_has_no_value`. For those the induction runs on the
CONJUNCTION and the entry is recovered by weakening at each index.
`invariant_along_trace_via` does that in one step, and the shape is
worth noticing: it is the standard inductive-invariant move, and
finding which entries need it was the informative part of the work.

## What this is not

It is not `valid_implies_legal`. That theorem needs ALL 94 entries; the
count below is what is currently proved. The remainder are still backed
only by the 1 464-script sweep, which `bounded.lean` shows is the same
statement with both quantifiers made finite. -/

namespace Abstraction
namespace Holds

open AbstractModel
open Abstraction.Bounded

/-! ## The bridge -/

/-- Holds at `init`, preserved by every step, therefore true at every
    index of every `Valid` run. The clock argument is free because none
    of these entries reads it — `n'` is arbitrary in `hstep`. -/
theorem invariant_along_trace {P : Nat → ServerState → Bool}
    (hinit : ∀ now, P now ServerState.init = true)
    (hstep : ∀ (mat : Bool) (st : Step) (now n' : Nat) (s : ServerState),
               P now s = true → P n' (stepOf mat st now s).2 = true)
    (mat : Bool) (tr : Trace) (hv : Valid mat tr)
    (h0 : (tr 0).state = ServerState.init) :
    ∀ n, P (tr n).now (tr n).state = true
  | 0     => by rw [h0]; exact hinit _
  | n + 1 => by
      rw [(hv n).2.1]
      exact hstep mat (tr n).req (tr n).now (tr (n + 1)).now (tr n).state
        (invariant_along_trace hinit hstep mat tr hv h0 n)

/-- The same, when the entry is inductive only inside a stronger
    predicate: run the induction on `S`, weaken to `P` at each index. -/
theorem invariant_along_trace_via {S : ServerState → Bool} {P : Nat → ServerState → Bool}
    (hinit : S ServerState.init = true)
    (hstep : ∀ (mat : Bool) (st : Step) (now : Nat) (s : ServerState),
               S s = true → S (stepOf mat st now s).2 = true)
    (himp : ∀ (now : Nat) (s : ServerState), S s = true → P now s = true)
    (mat : Bool) (tr : Trace) (hv : Valid mat tr)
    (h0 : (tr 0).state = ServerState.init) :
    ∀ n, P (tr n).now (tr n).state = true := by
  have key : ∀ n, S (tr n).state = true := by
    intro n
    induction n with
    | zero => rw [h0]; exact hinit
    | succ k ih => rw [(hv k).2.1]; exact hstep mat _ _ _ ih
  exact fun n => himp _ _ (key n)

/-- And a `Prop`-valued strengthening, for `StoreNodup`. -/
theorem invariant_along_trace_prop {S : ServerState → Prop} {P : Nat → ServerState → Bool}
    (hinit : S ServerState.init)
    (hstep : ∀ (mat : Bool) (st : Step) (now : Nat) (s : ServerState),
               S s → S (stepOf mat st now s).2)
    (himp : ∀ (now : Nat) (s : ServerState), S s → P now s = true)
    (mat : Bool) (tr : Trace) (hv : Valid mat tr)
    (h0 : (tr 0).state = ServerState.init) :
    ∀ n, P (tr n).now (tr n).state = true := by
  have key : ∀ n, S (tr n).state := by
    intro n
    induction n with
    | zero => rw [h0]; exact hinit
    | succ k ih => rw [(hv k).2.1]; exact hstep mat _ _ _ ih
  exact fun n => himp _ _ (key n)

section Along

variable (mat : Bool) (tr : Trace) (hv : Valid mat tr)
  (h0 : (tr 0).state = ServerState.init)

include mat hv h0

open Properties Abstraction.Induction Abstraction.Frame Abstraction.Trans

/-! ## Promise entries -/

theorem created_at_lte_timeout_at :
    ∀ n, well_formed_promise_created_at_lte_timeout_at (tr n).now (tr n).state = true :=
  invariant_along_trace Induction.created_at_lte_timeout_at_init
    (fun mat st now n' s => Induction.created_at_lte_timeout_at_step mat st now n' s)
    mat tr hv h0

theorem pending_created_before_deadline :
    ∀ n, well_formed_promise_pending_created_before_deadline (tr n).now (tr n).state = true :=
  invariant_along_trace Induction.pending_created_before_deadline_init
    (fun mat st now n' s => Induction.pending_created_before_deadline_step mat st now n' s)
    mat tr hv h0

theorem settled_at_iff_not_pending :
    ∀ n, well_formed_promise_settled_at_iff_not_pending (tr n).now (tr n).state = true :=
  invariant_along_trace Induction.settled_at_iff_not_pending_init
    (fun mat st now n' s => Induction.settled_at_iff_not_pending_step mat st now n' s)
    mat tr hv h0

theorem timedout_is_server_owned :
    ∀ n, well_formed_promise_timedout_is_server_owned (tr n).now (tr n).state = true :=
  invariant_along_trace Induction.timedout_is_server_owned_init
    (fun mat st now n' s => Induction.timedout_is_server_owned_step mat st now n' s)
    mat tr hv h0

theorem settled_at_lte_timeout_at :
    ∀ n, well_formed_promise_settled_at_lte_timeout_at (tr n).now (tr n).state = true :=
  invariant_along_trace Induction.settled_at_lte_timeout_at_init
    (fun mat st now n' s => Induction.settled_at_lte_timeout_at_step mat st now n' s)
    mat tr hv h0

theorem deadline_verdict_matches_timer_tag :
    ∀ n, well_formed_promise_deadline_verdict_matches_timer_tag (tr n).now (tr n).state = true :=
  invariant_along_trace Induction.deadline_verdict_matches_timer_tag_init
    (fun mat st now n' s => Induction.deadline_verdict_matches_timer_tag_step mat st now n' s)
    mat tr hv h0

/-! ### The two value entries, through their conjunction -/

theorem pending_has_no_value :
    ∀ n, well_formed_promise_pending_has_no_value (tr n).now (tr n).state = true :=
  invariant_along_trace_via Induction.no_value_unless_settled_init
    Induction.no_value_unless_settled_step
    Induction.pending_has_no_value_of_strengthening mat tr hv h0

theorem deadline_settlement_has_no_value :
    ∀ n, well_formed_promise_deadline_settlement_has_no_value (tr n).now (tr n).state = true :=
  invariant_along_trace_via Induction.no_value_unless_settled_init
    Induction.no_value_unless_settled_step
    Induction.deadline_settlement_has_no_value_of_strengthening mat tr hv h0

/-! ## Task entries, all nine through `qTaskShape` -/

theorem task_acquired_iff_has_pid :
    ∀ n, well_formed_task_acquired_iff_has_pid (tr n).now (tr n).state = true :=
  invariant_along_trace_via Induction.task_shape_init Induction.task_shape_step
    Induction.task_acquired_iff_has_pid_of_shape mat tr hv h0

theorem task_acquired_iff_has_ttl :
    ∀ n, well_formed_task_acquired_iff_has_ttl (tr n).now (tr n).state = true :=
  invariant_along_trace_via Induction.task_shape_init Induction.task_shape_step
    Induction.task_acquired_iff_has_ttl_of_shape mat tr hv h0

theorem task_acquired_iff_has_expires_at :
    ∀ n, well_formed_task_acquired_iff_has_expires_at (tr n).now (tr n).state = true :=
  invariant_along_trace_via Induction.task_shape_init Induction.task_shape_step
    Induction.task_acquired_iff_has_expires_at_of_shape mat tr hv h0

theorem task_pending_iff_has_retry_at :
    ∀ n, well_formed_task_pending_iff_has_retry_at (tr n).now (tr n).state = true :=
  invariant_along_trace_via Induction.task_shape_init Induction.task_shape_step
    Induction.task_pending_iff_has_retry_at_of_shape mat tr hv h0

theorem task_fulfilled_is_cleared :
    ∀ n, well_formed_task_fulfilled_is_cleared (tr n).now (tr n).state = true :=
  invariant_along_trace_via Induction.task_shape_init Induction.task_shape_step
    Induction.task_fulfilled_is_cleared_of_shape mat tr hv h0

theorem task_suspended_is_cleared :
    ∀ n, well_formed_task_suspended_is_cleared (tr n).now (tr n).state = true :=
  invariant_along_trace_via Induction.task_shape_init Induction.task_shape_step
    Induction.task_suspended_is_cleared_of_shape mat tr hv h0

theorem task_halted_is_cleared :
    ∀ n, well_formed_task_halted_is_cleared (tr n).now (tr n).state = true :=
  invariant_along_trace_via Induction.task_shape_init Induction.task_shape_step
    Induction.task_halted_is_cleared_of_shape mat tr hv h0

theorem task_suspended_has_no_resumes :
    ∀ n, well_formed_task_suspended_has_no_resumes (tr n).now (tr n).state = true :=
  invariant_along_trace_via Induction.task_shape_init Induction.task_shape_step
    Induction.task_suspended_has_no_resumes_of_shape mat tr hv h0

theorem task_acquired_version_positive :
    ∀ n, well_formed_task_acquired_version_positive (tr n).now (tr n).state = true :=
  invariant_along_trace_via Induction.task_shape_init Induction.task_shape_step
    Induction.task_acquired_version_positive_of_shape mat tr hv h0

/-! ## The schedule entry -/

theorem schedule_promise_tags_not_timer_targeted :
    ∀ n, well_formed_schedule_promise_tags_not_timer_targeted (tr n).now (tr n).state = true :=
  invariant_along_trace Induction.schedule_promise_tags_not_timer_targeted_init
    (fun mat st now n' s =>
      Induction.schedule_promise_tags_not_timer_targeted_step mat st now n' s)
    mat tr hv h0

/-! ## The four uniqueness entries, through `StoreNodup` -/

theorem store_nodup : ∀ n, StoreNodup (tr n).state := by
  intro n
  induction n with
  | zero => rw [h0]; exact storeNodup_init
  | succ k ih => rw [(hv k).2.1]; exact storeNodup_step mat _ _ _ ih

theorem object_ids_unique :
    ∀ n, well_formed_store_object_ids_unique (tr n).now (tr n).state = true :=
  fun n => object_ids_unique_of_nodup _ _ (store_nodup mat tr hv h0 n)

theorem schedule_ids_unique :
    ∀ n, well_formed_store_schedule_ids_unique (tr n).now (tr n).state = true :=
  fun n => schedule_ids_unique_of_nodup _ _ (store_nodup mat tr hv h0 n)

theorem outbox_keys_unique :
    ∀ n, well_formed_store_outbox_keys_unique (tr n).now (tr n).state = true :=
  fun n => outbox_keys_unique_of_nodup _ _ (store_nodup mat tr hv h0 n)

/-! ## Transition entries

These need no induction of their own: each is a claim about one step,
and `Valid` says the next state IS that step. What some of them need
is the uniqueness invariant above, which is where the induction shows
up — a row of the pre-state has to be the one its own id looks up. -/

theorem monotone_promise_set_grows :
    ∀ n, Properties.monotone_promise_set_grows (tr n).now (tr n).state (tr (n + 1)).state = true := by
  intro n
  rw [(hv n).2.1]
  exact Frame.monotone_promise_set_grows_step mat _ _ _ _

theorem monotone_task_set_grows :
    ∀ n, Properties.monotone_task_set_grows (tr n).now (tr n).state (tr (n + 1)).state = true := by
  intro n
  rw [(hv n).2.1]
  exact Frame.monotone_task_set_grows_step mat _ _ _ _ (store_nodup mat tr hv h0 n)

theorem preserved_promise_birth_fields_immutable :
    ∀ n, Properties.preserved_promise_birth_fields_immutable
           (tr n).now (tr n).state (tr (n + 1)).state = true := by
  intro n
  rw [(hv n).2.1]
  exact Trans.preserved_promise_birth_fields_immutable_step mat _ _ _ _
    (store_nodup mat tr hv h0 n).1

theorem preserved_settled_promise_record :
    ∀ n, Properties.preserved_settled_promise_record
           (tr n).now (tr n).state (tr (n + 1)).state = true := by
  intro n
  rw [(hv n).2.1]
  exact Trans.preserved_settled_promise_record_step mat _ _ _ _
    (store_nodup mat tr hv h0 n).1

theorem preserved_promise_state_frozen_once_settled :
    ∀ n, Properties.preserved_promise_state_frozen_once_settled
           (tr n).now (tr n).state (tr (n + 1)).state = true := by
  intro n
  rw [(hv n).2.1]
  exact Trans.preserved_promise_state_frozen_once_settled_step mat _ _ _ _
    (store_nodup mat tr hv h0 n).1

theorem preserved_promise_value_until_settlement :
    ∀ n, Properties.preserved_promise_value_until_settlement
           (tr n).now (tr n).state (tr (n + 1)).state = true := by
  intro n
  rw [(hv n).2.1]
  exact Trans.preserved_promise_value_until_settlement_step mat _ _ _ _
    (store_nodup mat tr hv h0 n).1

theorem preserved_promise_no_duplicate_ids :
    ∀ n, Properties.preserved_promise_no_duplicate_ids
           (tr n).now (tr n).state (tr (n + 1)).state = true := by
  intro n
  rw [(hv n).2.1]
  exact Trans.preserved_promise_no_duplicate_ids_step mat _ _ _ _
    (store_nodup mat tr hv h0 n)

theorem preserved_promise_settlement_is_one_way :
    ∀ n, Properties.preserved_promise_settlement_is_one_way
           (tr n).now (tr n).state (tr (n + 1)).state = true := by
  intro n
  rw [(hv n).2.1]
  exact Trans.preserved_promise_settlement_is_one_way_step mat _ _ _ _
    (store_nodup mat tr hv h0 n)

theorem monotone_promise_callbacks_grow_while_pending :
    ∀ n, Properties.monotone_promise_callbacks_grow_while_pending
           (tr n).now (tr n).state (tr (n + 1)).state = true := by
  intro n
  rw [(hv n).2.1]
  exact Trans.monotone_promise_callbacks_grow_while_pending_step mat _ _ _ _
    (store_nodup mat tr hv h0 n)

theorem monotone_promise_listeners_grow_while_pending :
    ∀ n, Properties.monotone_promise_listeners_grow_while_pending
           (tr n).now (tr n).state (tr (n + 1)).state = true := by
  intro n
  rw [(hv n).2.1]
  exact Trans.monotone_promise_listeners_grow_while_pending_step mat _ _ _ _
    (store_nodup mat tr hv h0 n)

end Along

/-! ## Why the schema sometimes needs a strengthening

The induction schema — holds at `init`, preserved by every step,
therefore holds everywhere — is sound, and `invariant_along_trace`
above is its proof. But it asks for something STRONGER than the
property being true of every reachable state. It asks that the property
be preserved from every state satisfying IT, reachable or not. Those
are different sets, and the gap is where the interesting work lives.

Here is the gap, concretely, in one catalogue entry.

`sneaky` is a promise that is pending and carries a value. The entry
`deadline_settlement_has_no_value` HOLDS there — the promise is not
stamped at its deadline, so the entry has nothing to say. One ordinary
step, the promise timeout, projects it: the settlement stamp becomes
its deadline and the value rides along untouched. The entry now FAILS.

So the entry is not preserved from an arbitrary state satisfying it,
and no amount of tactic effort would have proved it. What is true is
that `sneaky` is unreachable, and the reason is a DIFFERENT catalogue
entry: a pending promise has no value. That is why the induction runs
on the conjunction and the entry is recovered by weakening.

Finding which entries need this — and which other entry each one needs
— is the informative output of the exercise, not an obstacle to it. -/

section NotInductiveAlone

open AbstractModel

def sneaky : ServerState :=
  { objects := [{ id := "p",
                  promise := { state := .pending, param := {},
                               value := { data := some "x", headers := [] },
                               tags := [], timeoutAt := 10, createdAt := 0 } }] }

theorem sneaky_satisfies_the_entry :
    Properties.well_formed_promise_deadline_settlement_has_no_value 0 sneaky = true := by
  decide

theorem one_step_breaks_it :
    Properties.well_formed_promise_deadline_settlement_has_no_value 20
      (stepOf true (.r1 "p") 20 sneaky).2 = false := by
  decide

/-- And the reason `sneaky` never arises: it violates a different
    entry. That entry is exactly the one the strengthening adds. -/
theorem sneaky_is_unreachable_because :
    Properties.well_formed_promise_pending_has_no_value 0 sneaky = false := by
  decide

end NotInductiveAlone

end Holds
end Abstraction
