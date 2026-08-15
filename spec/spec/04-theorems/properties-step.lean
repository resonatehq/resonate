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

def stepsOfA (handle : Step → Nat → AbstractModel.H Response) :
    List (Step × Nat) → ServerState → List (Nat × ServerState × ServerState)
  | [], _ => []
  | (st, n) :: w, s =>
      let (_, s') := Id.run ((handle st n).run s)
      (n, s, s') :: stepsOfA handle w s'

def steps (w : List (Step × Nat)) : List (Nat × ServerState × ServerState) :=
  stepsOfA handleA w AbstractModel.ServerState.init
    ++ stepsOfA handleAP w AbstractModel.ServerState.init

def stepWellFormedRun (w : List (Step × Nat)) : Bool :=
  (steps w).all (fun (n, a, b) => stepWellFormed n a b)

def stepReport (ws : List (List (Step × Nat))) : List String :=
  (ws.flatMap fun w => (steps w).flatMap (fun (n, a, b) => stepFailures n a b)).eraseDups

def stepWitnesses (ws : List (List (Step × Nat)))
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
       preserved_promise_birth_fields_immutable 0 { promises := [P] } { promises := [{ P with timeoutAt := 9999 }] }),
    ("preserved_settled_promise_record/state_moved",
       preserved_settled_promise_record 0 { promises := [S] } { promises := [{ S with state := .rejected }] }),
    ("preserved_settled_promise_record/settled_at_restamped",
       preserved_settled_promise_record 0 { promises := [S] } { promises := [{ S with settledAt := some 90 }] }),
    ("preserved_settled_promise_record/value_rewritten",
       preserved_settled_promise_record 0 { promises := [S] } { promises := [{ S with value := { data := some "x" } }] }),
    ("monotone_promise_set_grows",
       monotone_promise_set_grows 0 { promises := [P] } { promises := [] }),
    ("monotone_task_set_grows",
       monotone_task_set_grows 0 { tasks := [T] } { tasks := [] }),
    ("monotone_task_version_increases_only_on_acquisition/bumped_without_acquiring",
       monotone_task_version_increases_only_on_acquisition 0 { tasks := [T] } { tasks := [{ T with version := 4 }] }),
    ("monotone_task_version_increases_only_on_acquisition/acquired_without_bumping",
       monotone_task_version_increases_only_on_acquisition 0 { tasks := [T] } { tasks := [{ A with version := 3 }] }),
    -- the mutant only `+1` catches: a grant that skips a version. Legal
    -- under a merely-increasing token, and it breaks fencing — a holder
    -- can no longer name the version that supersedes its own.
    ("monotone_task_version_increases_only_on_acquisition/skipped_a_version",
       monotone_task_version_increases_only_on_acquisition 0 { tasks := [T] } { tasks := [{ A with version := 5 }] }),
    ("monotone_task_version_increases_only_on_acquisition/reset_on_release",
       monotone_task_version_increases_only_on_acquisition 0 { tasks := [A] } { tasks := [{ T with version := 0 }] }),
    ("preserved_fulfilled_task/resurrected",
       preserved_fulfilled_task 0 { tasks := [F] } { tasks := [T] }),
    ("preserved_fulfilled_task/regained_a_lease",
       preserved_fulfilled_task 0 { tasks := [F] } { tasks := [{ F with pid := some "w" }] }),
    ("preserved_promise_state_frozen_once_settled",
       preserved_promise_state_frozen_once_settled 0 { promises := [S0] } { promises := [{ S0 with state := .rejected }] }),
    ("preserved_promise_settlement_is_one_way",
       preserved_promise_settlement_is_one_way 0 { promises := [S0] } { promises := [P] }),
    ("consistent_promise_settled_at_moves_with_state",
       consistent_promise_settled_at_moves_with_state 0 { promises := [P] } { promises := [{ P with state := .resolved }] }),
    ("preserved_promise_value_until_settlement",
       preserved_promise_value_until_settlement 0 { promises := [P] } { promises := [{ P with value := { data := some "v" } }] }),
    ("preserved_promise_no_duplicate_ids",
       preserved_promise_no_duplicate_ids 0 { } { promises := [P, P] }),
    ("monotone_promise_callbacks_grow_while_pending",
       monotone_promise_callbacks_grow_while_pending 0 { promises := [{ P with callbacks := ["x"] }] } { promises := [P] }),
    ("monotone_promise_callbacks_shrink_once_settled",
       monotone_promise_callbacks_shrink_once_settled 0 { promises := [S0] } { promises := [{ S0 with callbacks := ["x"] }] }),
    ("monotone_promise_listeners_grow_while_pending",
       monotone_promise_listeners_grow_while_pending 0 { promises := [{ P with listeners := ["https://a"] }] } { promises := [P] }),
    ("monotone_promise_listeners_shrink_once_settled",
       monotone_promise_listeners_shrink_once_settled 0 { promises := [S0] } { promises := [{ S0 with listeners := ["https://a"] }] }),
    ("consistent_promise_state_edge_admissible",
       consistent_promise_state_edge_admissible 0 { promises := [S0] } { promises := [{ S0 with state := .rejected }] }),
    ("consistent_task_state_edge_admissible",
       consistent_task_state_edge_admissible 0 { tasks := [T] } { tasks := [{ T with state := .suspended, retryAt := none }] }),
    ("preserved_task_acquisition_only_from_pending",
       preserved_task_acquisition_only_from_pending 0 { tasks := [{ T with state := .suspended, retryAt := none }] } { tasks := [A] }),
    ("preserved_task_suspension_only_from_acquired",
       preserved_task_suspension_only_from_acquired 0 { tasks := [T] } { tasks := [{ T with state := .suspended, retryAt := none }] }),
    ("preserved_task_halted_only_reenters_via_pending",
       preserved_task_halted_only_reenters_via_pending 0 { tasks := [{ T with state := .halted, retryAt := none }] } { tasks := [{ T with state := .suspended, retryAt := none }] }),
    ("consistent_settlement_fulfils_task",
       consistent_settlement_fulfils_task 0 { promises := [PT], tasks := [T] } { promises := [PTs], tasks := [T] }),
    ("consistent_task_fulfilment_needs_settlement",
       consistent_task_fulfilment_needs_settlement 0 { promises := [PT], tasks := [T] } { promises := [PT], tasks := [{ T with state := .fulfilled, retryAt := none }] }),
    ("consistent_obligation_discharge_requires_settled",
       consistent_obligation_discharge_requires_settled 0 { promises := [{ P with callbacks := ["z"] }] } { promises := [P] }),
    ("consistent_callback_consumption_resumes_awaiter",
       consistent_callback_consumption_resumes_awaiter 0
         { promises := [{ P with id := "c", callbacks := ["a"] }], tasks := [T] }
         { promises := [{ P with id := "c", state := .resolved, settledAt := some 20 }], tasks := [T] }),
    ("consistent_listener_consumption_enqueues_unblock",
       consistent_listener_consumption_enqueues_unblock 0 { promises := [{ S0 with listeners := ["https://l"] }] } { promises := [S0] }),
    ("consistent_wake_follows_callback_consumption",
       consistent_wake_follows_callback_consumption 0
         { promises := [{ P with id := "c", callbacks := ["a"] }], tasks := [{ T with state := .suspended, retryAt := none }] }
         { promises := [{ P with id := "c", callbacks := ["a"] }], tasks := [{ T with state := .pending, retryAt := some 30 }] }),
    ("consistent_suspension_registers_callback",
       consistent_suspension_registers_callback 0 { promises := [P], tasks := [A] } { promises := [P], tasks := [{ A with state := .suspended, pid := none, ttl := none, expiresAt := none }] }),
    ("consistent_task_birth_couples_promise_birth",
       consistent_task_birth_couples_promise_birth 0 { } { tasks := [T] }),
    ("monotone_outbox_keys_never_disappear",
       monotone_outbox_keys_never_disappear 0 { outbox := [ex] } { }),
    ("consistent_new_execute_matches_task_and_target",
       consistent_new_execute_matches_task_and_target 0 { promises := [PT], tasks := [T] } { promises := [PT], tasks := [T], outbox := [{ address := "wrong", message := .execute "a" 3 }] }),
    ("consistent_new_unblock_carries_stored_record",
       consistent_new_unblock_carries_stored_record 0 { promises := [P] } { promises := [P], outbox := [{ address := "https://l", message := .unblock P.toRecord }] }),
    ("consistent_new_unblock_discharges_its_listener",
       consistent_new_unblock_discharges_its_listener 0
         { promises := [{ S0 with listeners := ["https://l"] }] }
         { promises := [{ S0 with listeners := ["https://l"] }], outbox := [{ address := "https://l", message := .unblock S0.toRecord }] }),
    ("preserved_schedule_birth_fields_immutable",
       preserved_schedule_birth_fields_immutable 0 { schedules := [C] } { schedules := [{ C with cron := "@daily" }] }),
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
    ("consistent_task_birth_state",
       consistent_task_birth_state 0 { } { tasks := [{ T with state := .suspended, retryAt := none }] }),
    ("consistent_task_lease_released_atomically",
       consistent_task_lease_released_atomically 0 { tasks := [A] } { tasks := [{ T with version := 4, pid := some "w" }] }),
    ("preserved_task_lease_holder_stable",
       preserved_task_lease_holder_stable 0 { tasks := [A] } { tasks := [{ A with pid := some "w2" }] }),
    ("consistent_task_lease_fields_move_together",
       consistent_task_lease_fields_move_together 0 { tasks := [A] } { tasks := [{ A with ttl := some 50 }] }),
    ("monotone_task_resumes_grow_or_clear",
       monotone_task_resumes_grow_or_clear 0 { tasks := [{ T with resumes := ["b"] }] } { tasks := [{ T with resumes := ["c"] }] }),
    ("consistent_task_resumes_cleared_only_on_dispatch_or_park",
       consistent_task_resumes_cleared_only_on_dispatch_or_park 0 { tasks := [{ A with resumes := ["b"] }] } { tasks := [{ T with version := 4 }] }),
    ("consistent_task_acquisition_is_atomic",
       consistent_task_acquisition_is_atomic 40 { tasks := [T] } { tasks := [A] }),
    ("consistent_task_lease_deadline_is_now_plus_ttl",
       consistent_task_lease_deadline_is_now_plus_ttl 40 { tasks := [A] } { tasks := [{ A with expiresAt := some 999 }] }),
    ("consistent_task_pending_entry_arms_retry",
       consistent_task_pending_entry_arms_retry 7 { tasks := [A] } { tasks := [{ T with version := 4, retryAt := none }] }),
    ("consistent_task_retry_rearm_only_when_due",
       consistent_task_retry_rearm_only_when_due 5 { tasks := [{ T with retryAt := some 50 }] } { tasks := [{ T with retryAt := some 900 }] }),
    ("consistent_task_wake_records_resume",
       consistent_task_wake_records_resume 7 { tasks := [{ T with state := .suspended, retryAt := none }] } { tasks := [{ T with retryAt := some 7 }] }),
    ("consistent_task_state_edge_internal_admissible",
       consistent_task_state_edge_internal_admissible 0 { tasks := [T] } { tasks := [A] }),
    ("consistent_promise_state_edge_internal_admissible",
       consistent_promise_state_edge_internal_admissible 0 { promises := [P] } { promises := [{ P with state := .resolved, settledAt := some 20 }] }),
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

/-! ### The internal-step-only harness

The sweeper properties hold on internal steps and are FALSE on request steps.
That is not a defect — it is what makes them stronger than the general
edge tables, and it means they need their own walk. -/

def stepsWithA (handle : Step → Nat → AbstractModel.H Response) :
    List (Step × Nat) → ServerState → List (Step × Nat × ServerState × ServerState)
  | [], _ => []
  | (st, n) :: w, s =>
      let (_, s') := Id.run ((handle st n).run s)
      (st, n, s, s') :: stepsWithA handle w s'

def isInternalStep : Step → Bool
  | .r1 _ => true | .r3 _ _ => true | .r4 _ _ => true
  | .r5 _ => true | .r6 _ _ => true | .r7 _ => true
  | _ => false

def allSteps (w : List (Step × Nat)) : List (Step × Nat × ServerState × ServerState) :=
  stepsWithA handleA w AbstractModel.ServerState.init
    ++ stepsWithA handleAP w AbstractModel.ServerState.init

def internalWellFormedRun (w : List (Step × Nat)) : Bool :=
  (allSteps w).all (fun (st, n, a, b) => !isInternalStep st || internalWellFormed n a b)

theorem stage3_internal_sweep :
    ((seqsUpToA kernelsResp 3).map instantiateA).all internalWellFormedRun = true := by decide

theorem reaches_internal_steps :
    (battery.any fun w => (allSteps w).any (fun (st, _, _, _) => isInternalStep st)) = true := by decide

/-- Strictly stronger than the general edge tables, machine-checked:
    some REQUEST step in the corpus violates the sweeper properties. If they
    held everywhere they would be a restatement, not a constraint. -/
theorem internal_laws_are_strictly_stronger :
    (((seqsUpToA kernelsResp 3).map instantiateA).any fun w =>
      (allSteps w).any (fun (st, n, a, b) => !isInternalStep st && !internalWellFormed n a b)) = true := by
  decide

/-! ### The `.trans` gap, witnessed

`monotone_task_retry_rearm_advances` is the one known gap whose defect
lives in the MOVE rather than in a row, so its witness belongs here
rather than in `properties-check.lean`. -/

/-- Release a task at 110, then re-arm its retry to instant 0 at instant
    200. The task is pending with `retryAt = some 0` — due at the
    instant it was armed — so the retry step is enabled again
    immediately and dispatches forever. -/
def wGapRetryBackwards : List (Step × Nat) :=
  [ (.api (.taskCreate { pid := "p", ttl := 50, action := { id := "x", timeoutAt := 9000, param := {}, tags := [("resonate:target", "w1")] } }), 100),
    (.api (.taskRelease { id := "x", version := 1 }), 110),
    (.r6 "x" 0, 200) ]

theorem gap_task_retry_rearm_advances_is_violable :
    (steps wGapRetryBackwards).any
      (fun (n, a, b) => !monotone_task_retry_rearm_advances n a b) = true := by decide

/-- And the catalogue does not see it: every state is well formed and
    every step is well formed. That is the whole point of recording the
    gap — the machine admits a livelock the properties call legal. -/
theorem gap_task_retry_rearm_is_invisible_to_the_catalogue :
    stepWellFormedRun wGapRetryBackwards = true := by decide

end Abstraction
