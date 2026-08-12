import «04-theorems».«properties-check»

namespace Abstraction

/-!  # Stage 3, evaluated

The transition catalogue (`02-abstract/properties.lean`) run over the
same corpus as stage 1, consumed as consecutive PAIRS rather than as
states. Same three gates: it holds, every entry rejects a violator, and
the corpus reaches the transition each entry is about. -/

open AbstractModel.Properties
open AbstractModel (ServerState PromiseObject TaskObject)

/-! ### The harness

Consecutive pairs, including the step out of the initial state. -/

def stepsOfA (handle : AStep → Nat → AbstractModel.H Response) :
    List (AStep × Nat) → ServerState → List (Nat × ServerState × ServerState)
  | [], _ => []
  | (st, n) :: w, s =>
      let (_, s') := Id.run ((handle st n).run s)
      (n, s, s') :: stepsOfA handle w s'

def steps (w : List (AStep × Nat)) : List (Nat × ServerState × ServerState) :=
  stepsOfA handleA w AbstractModel.ServerState.init
    ++ stepsOfA handleAP w AbstractModel.ServerState.init

def stepWellFormedRun (w : List (AStep × Nat)) : Bool :=
  (steps w).all (fun (n, a, b) => stepWellFormed n a b)

def stepReport (ws : List (List (AStep × Nat))) : List String :=
  (ws.flatMap fun w => (steps w).flatMap (fun (n, a, b) => stepFailures n a b)).eraseDups

def stepWitnesses (ws : List (List (AStep × Nat)))
    (p : ServerState → ServerState → Bool) : Bool :=
  ws.any fun w => (steps w).any (fun (_, a, b) => p a b)

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

theorem stage3_battery : battery.all stepWellFormedRun = true := by decide

theorem stage3_sweep :
    ((seqsUpToA kernelsResp 3).map instantiateA).all stepWellFormedRun = true := by decide

/-! ### Falsifiability -/

open ServerModel in
def stepMutants : List (String × Bool) :=
  let P : PromiseObject :=
    { id := "a", state := .pending, param := {}, tags := [("resonate:external","true")],
      timeoutAt := 100, createdAt := 10 }
  let S : PromiseObject := { P with state := .resolved, settledAt := some 20, listeners := ["https://l"] }
  let T : TaskObject := { id := "a", state := .pending, version := 3, retryAt := some 0 }
  let A : TaskObject := { id := "a", state := .acquired, version := 4, pid := some "w", ttl := some 5, expiresAt := some 9 }
  let F : TaskObject := { id := "a", state := .fulfilled, version := 3 }
  let S0 : PromiseObject := { P with state := .resolved, settledAt := some 20 }
  let PT : PromiseObject := { P with tags := [("resonate:target","w")] }
  let PTs : PromiseObject := { PT with state := .resolved, settledAt := some 20 }
  let C : Schedule := { id := "c", cron := "* * * * *", promiseId := "p", promiseTimeout := 100,
                        promiseParam := {}, promiseTags := [], nextRunAt := 60, createdAt := 10 }
  let ex : OutboxEntry := { address := "w", message := .execute "a" 3 }
  [ ("preserved_promise_birth_fields_immutable",
       preserved_promise_birth_fields_immutable { promises := [P] } { promises := [{ P with timeoutAt := 9999 }] }),
    ("preserved_settled_promise_record/state_moved",
       preserved_settled_promise_record { promises := [S] } { promises := [{ S with state := .rejected }] }),
    ("preserved_settled_promise_record/settled_at_restamped",
       preserved_settled_promise_record { promises := [S] } { promises := [{ S with settledAt := some 90 }] }),
    ("preserved_settled_promise_record/value_rewritten",
       preserved_settled_promise_record { promises := [S] } { promises := [{ S with value := { data := some "x" } }] }),
    ("monotone_promise_set_grows",
       monotone_promise_set_grows { promises := [P] } { promises := [] }),
    ("monotone_task_set_grows",
       monotone_task_set_grows { tasks := [T] } { tasks := [] }),
    ("preserved_task_version_increments_only_on_acquisition/bumped_without_acquiring",
       preserved_task_version_increments_only_on_acquisition { tasks := [T] } { tasks := [{ T with version := 4 }] }),
    ("preserved_task_version_increments_only_on_acquisition/acquired_without_bumping",
       preserved_task_version_increments_only_on_acquisition { tasks := [T] } { tasks := [{ A with version := 3 }] }),
    ("preserved_task_version_increments_only_on_acquisition/acquired_by_two",
       preserved_task_version_increments_only_on_acquisition { tasks := [T] } { tasks := [{ A with version := 5 }] }),
    ("preserved_task_version_increments_only_on_acquisition/reset_on_release",
       preserved_task_version_increments_only_on_acquisition { tasks := [A] } { tasks := [{ T with version := 0 }] }),
    ("preserved_fulfilled_task/resurrected",
       preserved_fulfilled_task { tasks := [F] } { tasks := [T] }),
    ("preserved_fulfilled_task/regained_a_lease",
       preserved_fulfilled_task { tasks := [F] } { tasks := [{ F with pid := some "w" }] }),
    ("preserved_promise_state_frozen_once_settled",
       preserved_promise_state_frozen_once_settled { promises := [S0] } { promises := [{ S0 with state := .rejected }] }),
    ("preserved_promise_settlement_is_one_way",
       preserved_promise_settlement_is_one_way { promises := [S0] } { promises := [P] }),
    ("consistent_promise_settled_at_moves_with_state",
       consistent_promise_settled_at_moves_with_state { promises := [P] } { promises := [{ P with state := .resolved }] }),
    ("preserved_promise_value_until_settlement",
       preserved_promise_value_until_settlement { promises := [P] } { promises := [{ P with value := { data := some "v" } }] }),
    ("preserved_promise_no_duplicate_ids",
       preserved_promise_no_duplicate_ids { } { promises := [P, P] }),
    ("monotone_promise_callbacks_append_one_while_pending",
       monotone_promise_callbacks_append_one_while_pending { promises := [P] } { promises := [{ P with callbacks := ["x","y"] }] }),
    ("monotone_promise_callbacks_drain_one_once_settled",
       monotone_promise_callbacks_drain_one_once_settled { promises := [{ S0 with callbacks := ["x","y"] }] } { promises := [S0] }),
    ("monotone_promise_listeners_append_one_while_pending",
       monotone_promise_listeners_append_one_while_pending { promises := [P] } { promises := [{ P with listeners := ["https://a","https://b"] }] }),
    ("monotone_promise_listeners_drain_one_once_settled",
       monotone_promise_listeners_drain_one_once_settled { promises := [{ S0 with listeners := ["https://a","https://b"] }] } { promises := [S0] }),
    ("consistent_promise_state_edge_admissible",
       consistent_promise_state_edge_admissible { promises := [S0] } { promises := [{ S0 with state := .rejected }] }),
    ("consistent_task_state_edge_admissible",
       consistent_task_state_edge_admissible { tasks := [T] } { tasks := [{ T with state := .suspended, retryAt := none }] }),
    ("preserved_task_acquisition_only_from_pending",
       preserved_task_acquisition_only_from_pending { tasks := [{ T with state := .suspended, retryAt := none }] } { tasks := [A] }),
    ("preserved_task_suspension_only_from_acquired",
       preserved_task_suspension_only_from_acquired { tasks := [T] } { tasks := [{ T with state := .suspended, retryAt := none }] }),
    ("preserved_task_halted_only_reenters_via_pending",
       preserved_task_halted_only_reenters_via_pending { tasks := [{ T with state := .halted, retryAt := none }] } { tasks := [{ T with state := .suspended, retryAt := none }] }),
    ("consistent_settlement_fulfils_task",
       consistent_settlement_fulfils_task { promises := [PT], tasks := [T] } { promises := [PTs], tasks := [T] }),
    ("consistent_task_fulfilment_needs_settlement",
       consistent_task_fulfilment_needs_settlement { promises := [PT], tasks := [T] } { promises := [PT], tasks := [{ T with state := .fulfilled, retryAt := none }] }),
    ("consistent_obligation_discharge_requires_settled",
       consistent_obligation_discharge_requires_settled { promises := [{ P with callbacks := ["z"] }] } { promises := [P] }),
    ("consistent_callback_consumption_resumes_awaiter",
       consistent_callback_consumption_resumes_awaiter
         { promises := [{ P with id := "c", callbacks := ["a"] }], tasks := [T] }
         { promises := [{ P with id := "c", state := .resolved, settledAt := some 20 }], tasks := [T] }),
    ("consistent_listener_consumption_enqueues_unblock",
       consistent_listener_consumption_enqueues_unblock { promises := [{ S0 with listeners := ["https://l"] }] } { promises := [S0] }),
    ("consistent_wake_follows_callback_consumption",
       consistent_wake_follows_callback_consumption
         { promises := [{ P with id := "c", callbacks := ["a"] }], tasks := [{ T with state := .suspended, retryAt := none }] }
         { promises := [{ P with id := "c", callbacks := ["a"] }], tasks := [{ T with state := .pending, retryAt := some 30 }] }),
    ("consistent_suspension_registers_callback",
       consistent_suspension_registers_callback { promises := [P], tasks := [A] } { promises := [P], tasks := [{ A with state := .suspended, pid := none, ttl := none, expiresAt := none }] }),
    ("consistent_callback_additions_share_one_awaiter",
       consistent_callback_additions_share_one_awaiter { promises := [P] } { promises := [{ P with callbacks := ["u","v"] }] }),
    ("consistent_at_most_one_obligation_discharged",
       consistent_at_most_one_obligation_discharged { promises := [{ S0 with callbacks := ["u","v"] }] } { promises := [S0] }),
    ("consistent_at_most_one_task_acquired",
       consistent_at_most_one_task_acquired { tasks := [T, { T with id := "b" }] } { tasks := [A, { A with id := "b" }] }),
    ("consistent_task_birth_couples_promise_birth",
       consistent_task_birth_couples_promise_birth { } { tasks := [T] }),
    ("monotone_outbox_keys_never_disappear",
       monotone_outbox_keys_never_disappear { outbox := [ex] } { }),
    ("consistent_new_execute_matches_task_and_target",
       consistent_new_execute_matches_task_and_target { promises := [PT], tasks := [T] } { promises := [PT], tasks := [T], outbox := [{ address := "wrong", message := .execute "a" 3 }] }),
    ("consistent_new_unblock_carries_stored_record",
       consistent_new_unblock_carries_stored_record { promises := [P] } { promises := [P], outbox := [{ address := "https://l", message := .unblock P.toRecord }] }),
    ("consistent_new_unblock_discharges_its_listener",
       consistent_new_unblock_discharges_its_listener
         { promises := [{ S0 with listeners := ["https://l"] }] }
         { promises := [{ S0 with listeners := ["https://l"] }], outbox := [{ address := "https://l", message := .unblock S0.toRecord }] }),
    ("preserved_schedule_birth_fields_immutable",
       preserved_schedule_birth_fields_immutable { schedules := [C] } { schedules := [{ C with cron := "@daily" }] }),
    ("consistent_schedule_change_is_single",
       consistent_schedule_change_is_single { schedules := [C, { C with id := "c2" }] } { schedules := [{ C with nextRunAt := 120 }, { C with id := "c2", nextRunAt := 120 }] }),
    ("consistent_schedule_removal_is_isolated",
       consistent_schedule_removal_is_isolated { schedules := [C], promises := [P] } { schedules := [], promises := [] }),
    ("consistent_promise_settlement_stamp/forged_timeout",
       consistent_promise_settlement_stamp 50 { promises := [P] } { promises := [{ P with state := .rejectedTimedout, settledAt := some 50 }] }),
    ("consistent_promise_settlement_stamp/verdict_before_deadline",
       consistent_promise_settlement_stamp 50 { promises := [P] } { promises := [{ P with state := .resolved, settledAt := some 100 }] }),
    ("preserved_timedout_is_server_owned",
       preserved_timedout_is_server_owned 500 { promises := [P] } { promises := [{ P with state := .rejectedTimedout, settledAt := some 500 }] }),
    ("consistent_new_promise_born_clean/born_with_obligations",
       consistent_new_promise_born_clean 50 { } { promises := [{ P with callbacks := ["x"] }] }),
    ("consistent_new_promise_born_clean/born_in_the_future",
       consistent_new_promise_born_clean 5 { } { promises := [P] }),
    ("preserved_no_dead_dispatch",
       preserved_no_dead_dispatch 500
         { promises := [P], tasks := [{ T with state := .acquired, pid := some "w", ttl := some 1, expiresAt := some 1, retryAt := none }] }
         { promises := [P], tasks := [T] }),
    ("preserved_execute_only_for_live_task",
       preserved_execute_only_for_live_task 500
         { promises := [P] }
         { promises := [P], outbox := [{ address := "w", message := .execute "a" 3 }] }) ]

theorem stage3_all_falsifiable : stepMutants.all (fun (_, b) => !b) = true := by decide

/-! ### Reach

A transition property whose transition never occurs is not checked. -/

theorem reaches_settlement_step :
    stepWitnesses battery (fun a b =>
      a.promises.any fun p => p.state == .pending &&
        b.promises.any (fun q => q.id == p.id && q.state != .pending)) = true := by decide

theorem reaches_version_bump :
    stepWitnesses battery (fun a b =>
      a.tasks.any fun t => b.tasks.any (fun u => u.id == t.id && t.version < u.version)) = true := by decide

theorem reaches_obligation_drain :
    stepWitnesses battery (fun a b =>
      a.promises.any fun p => b.promises.any (fun q =>
        q.id == p.id && q.listeners.length < p.listeners.length)) = true := by decide

theorem reaches_settled_promise_step :
    stepWitnesses battery (fun a _ => a.promises.any (·.state != .pending)) = true := by decide

theorem reaches_fulfilled_task_step :
    stepWitnesses battery (fun a _ => a.tasks.any (·.state == .fulfilled)) = true := by decide

/-- The correction, machine-checked: a settled promise's RECORD is
    frozen but the promise is not. The listener drain changes the object
    on a step where `preserved_settled_promise_record` holds. -/
theorem settled_promise_object_is_not_frozen :
    stepWitnesses battery (fun a b =>
      a.promises.any fun p =>
        p.state != .pending &&
          b.promises.any (fun q => q.id == p.id && q.listeners.length < p.listeners.length))
      = true := by decide

end Abstraction
