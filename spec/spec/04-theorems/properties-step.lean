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

def stepsOfA (mat : Bool) :
    List (Step × Nat) → ServerState → List (Nat × ServerState × ServerState)
  | [], _ => []
  | (st, n) :: w, s =>
      let (_, s') := stepOf mat st n s
      (n, s, s') :: stepsOfA mat w s'

def steps (w : List (Step × Nat)) : List (Nat × ServerState × ServerState) :=
  stepsOfA true w AbstractModel.ServerState.init
    ++ stepsOfA false w AbstractModel.ServerState.init

/-- Same fold as stage 1 — `legalRun` there is this list under a
    different name. Kept as its own theorem because the mutants and
    reach witnesses below are about the `.trans` half specifically. -/
def transHoldsRun (w : List (Step × Nat)) : Bool :=
  (steps w).all (fun (n, a, b) => legalAt n a b)

def transReport (ws : List (List (Step × Nat))) : List String :=
  (ws.flatMap fun w => (steps w).flatMap (fun (n, a, b) => failingNames n a b)).eraseDups

def stepWitnesses (ws : List (List (Step × Nat)))
    (p : ServerState → ServerState → Bool) : Bool :=
  ws.any fun w => (steps w).any (fun (_, a, b) => p a b)

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

theorem stage3_battery : battery.all transHoldsRun = true := by decide

theorem stage3_sweep :
    ((seqsUpToA kernelsResp 3).map instantiateA).all transHoldsRun = true := by decide

/-! ### Falsifiability -/

/-- A mutant is a pair of states, and a state is now one list. `objWith`
    is where the fusion shows: several mutants used to put the promise at
    one id and the task at another, which the two-store state allowed and
    this one does not. They name the awaiter's own promise now. -/
def objOf (id : String) (p : AbstractModel.PromiseObject) : AbstractModel.Object :=
  { id := id, promise := p }

def objWith (id : String) (p : AbstractModel.PromiseObject)
    (t : AbstractModel.TaskObject) : AbstractModel.Object :=
  { id := id, promise := p, task := some t }

open ServerModel in
def stepMutants : List (String × Bool) :=
  let P : PromiseObject :=
    { state := .pending, param := {}, tags := [("resonate:external","true")],
      timeoutAt := 100, createdAt := 10 }
  let S : PromiseObject := { P with state := .resolved, settledAt := some 20, listeners := ["https://l"] }
  let T : TaskObject := { state := .pending, version := 3, retryAt := some 0 }
  let A : TaskObject := { state := .acquired, version := 4, pid := some "w", ttl := some 5, expiresAt := some 9 }
  let F : TaskObject := { state := .fulfilled, version := 3 }
  let S0 : PromiseObject := { P with state := .resolved, settledAt := some 20 }
  let PT : PromiseObject := { P with tags := [("resonate:target","w")] }
  let PTs : PromiseObject := { PT with state := .resolved, settledAt := some 20 }
  let C : Schedule := { id := "c", cron := "* * * * *", promiseId := "p", promiseTimeout := 100,
                        promiseParam := {}, promiseTags := [], nextRunAt := 60, createdAt := 10 }
  let ex : OutboxEntry := { address := "w", message := .execute "a" 3 }
  [ ("preserved_promise_birth_fields_immutable",
       preserved_promise_birth_fields_immutable 0 { objects := [objOf "a" (P)] } { objects := [objOf "a" ({ P with timeoutAt := 9999 })] }),
    ("preserved_settled_promise_record/state_moved",
       preserved_settled_promise_record 0 { objects := [objOf "a" (S)] } { objects := [objOf "a" ({ S with state := .rejected })] }),
    ("preserved_settled_promise_record/settled_at_restamped",
       preserved_settled_promise_record 0 { objects := [objOf "a" (S)] } { objects := [objOf "a" ({ S with settledAt := some 90 })] }),
    ("preserved_settled_promise_record/value_rewritten",
       preserved_settled_promise_record 0 { objects := [objOf "a" (S)] } { objects := [objOf "a" ({ S with value := { data := some "x" } })] }),
    ("monotone_promise_set_grows",
       monotone_promise_set_grows 0 { objects := [objOf "a" (P)] } { objects := [] }),
    ("monotone_task_set_grows",
       monotone_task_set_grows 0 { objects := [objWith "a" PT (T)] } { objects := [] }),
    ("monotone_task_version_increases_only_on_acquisition/bumped_without_acquiring",
       monotone_task_version_increases_only_on_acquisition 0 { objects := [objWith "a" PT (T)] } { objects := [objWith "a" PT ({ T with version := 4 })] }),
    ("monotone_task_version_increases_only_on_acquisition/acquired_without_bumping",
       monotone_task_version_increases_only_on_acquisition 0 { objects := [objWith "a" PT (T)] } { objects := [objWith "a" PT ({ A with version := 3 })] }),
    -- the mutant only `+1` catches: a grant that skips a version. Legal
    -- under a merely-increasing token, and it breaks fencing — a holder
    -- can no longer name the version that supersedes its own.
    ("monotone_task_version_increases_only_on_acquisition/skipped_a_version",
       monotone_task_version_increases_only_on_acquisition 0 { objects := [objWith "a" PT (T)] } { objects := [objWith "a" PT ({ A with version := 5 })] }),
    ("monotone_task_version_increases_only_on_acquisition/reset_on_release",
       monotone_task_version_increases_only_on_acquisition 0 { objects := [objWith "a" PT (A)] } { objects := [objWith "a" PT ({ T with version := 0 })] }),
    ("preserved_fulfilled_task/resurrected",
       preserved_fulfilled_task 0 { objects := [objWith "a" PT (F)] } { objects := [objWith "a" PT (T)] }),
    ("preserved_fulfilled_task/regained_a_lease",
       preserved_fulfilled_task 0 { objects := [objWith "a" PT (F)] } { objects := [objWith "a" PT ({ F with pid := some "w" })] }),
    ("preserved_promise_state_frozen_once_settled",
       preserved_promise_state_frozen_once_settled 0 { objects := [objOf "a" (S0)] } { objects := [objOf "a" ({ S0 with state := .rejected })] }),
    ("preserved_promise_settlement_is_one_way",
       preserved_promise_settlement_is_one_way 0 { objects := [objOf "a" (S0)] } { objects := [objOf "a" (P)] }),
    ("consistent_promise_settled_at_moves_with_state",
       consistent_promise_settled_at_moves_with_state 0 { objects := [objOf "a" (P)] } { objects := [objOf "a" ({ P with state := .resolved })] }),
    ("preserved_promise_value_until_settlement",
       preserved_promise_value_until_settlement 0 { objects := [objOf "a" (P)] } { objects := [objOf "a" ({ P with value := { data := some "v" } })] }),
    ("preserved_promise_no_duplicate_ids",
       preserved_promise_no_duplicate_ids 0 { } { objects := [objOf "a" (P), objOf "a" (P)] }),
    ("monotone_promise_callbacks_grow_while_pending",
       monotone_promise_callbacks_grow_while_pending 0 { objects := [objOf "a" ({ P with callbacks := ["x"] })] } { objects := [objOf "a" (P)] }),
    ("monotone_promise_callbacks_shrink_once_settled",
       monotone_promise_callbacks_shrink_once_settled 0 { objects := [objOf "a" (S0)] } { objects := [objOf "a" ({ S0 with callbacks := ["x"] })] }),
    ("monotone_promise_listeners_grow_while_pending",
       monotone_promise_listeners_grow_while_pending 0 { objects := [objOf "a" ({ P with listeners := ["https://a"] })] } { objects := [objOf "a" (P)] }),
    ("monotone_promise_listeners_shrink_once_settled",
       monotone_promise_listeners_shrink_once_settled 0 { objects := [objOf "a" (S0)] } { objects := [objOf "a" ({ S0 with listeners := ["https://a"] })] }),
    ("consistent_promise_state_edge_admissible",
       consistent_promise_state_edge_admissible 0 { objects := [objOf "a" (S0)] } { objects := [objOf "a" ({ S0 with state := .rejected })] }),
    ("consistent_task_state_edge_admissible",
       consistent_task_state_edge_admissible 0 { objects := [objWith "a" PT (T)] } { objects := [objWith "a" PT ({ T with state := .suspended, retryAt := none })] }),
    ("preserved_task_acquisition_only_from_pending",
       preserved_task_acquisition_only_from_pending 0 { objects := [objWith "a" PT ({ T with state := .suspended, retryAt := none })] } { objects := [objWith "a" PT (A)] }),
    ("preserved_task_suspension_only_from_acquired",
       preserved_task_suspension_only_from_acquired 0 { objects := [objWith "a" PT (T)] } { objects := [objWith "a" PT ({ T with state := .suspended, retryAt := none })] }),
    ("preserved_task_halted_only_reenters_via_pending",
       preserved_task_halted_only_reenters_via_pending 0 { objects := [objWith "a" PT ({ T with state := .halted, retryAt := none })] } { objects := [objWith "a" PT ({ T with state := .suspended, retryAt := none })] }),
    ("consistent_settlement_fulfils_task",
       consistent_settlement_fulfils_task 0 { objects := [objWith "a" (PT) (T)] } { objects := [objWith "a" (PTs) (T)] }),
    ("consistent_task_fulfilment_needs_settlement",
       consistent_task_fulfilment_needs_settlement 0 { objects := [objWith "a" (PT) (T)] } { objects := [objWith "a" (PT) ({ T with state := .fulfilled, retryAt := none })] }),
    ("consistent_obligation_discharge_requires_settled",
       consistent_obligation_discharge_requires_settled 0 { objects := [objOf "a" ({ P with callbacks := ["z"] })] } { objects := [objOf "a" (P)] }),
    ("consistent_callback_consumption_resumes_awaiter",
       consistent_callback_consumption_resumes_awaiter 0
         { objects := [objOf "c" ({ P with callbacks := ["a"] }), objWith "a" PT (T)] }
         { objects := [objOf "c" ({ P with state := .resolved, settledAt := some 20 }), objWith "a" PT (T)] }),
    ("consistent_listener_consumption_enqueues_unblock",
       consistent_listener_consumption_enqueues_unblock 0 { objects := [objOf "a" ({ S0 with listeners := ["https://l"] })] } { objects := [objOf "a" (S0)] }),
    ("consistent_wake_follows_callback_consumption",
       consistent_wake_follows_callback_consumption 0
         { objects := [objOf "c" ({ P with callbacks := ["a"] }), objWith "a" PT ({ T with state := .suspended, retryAt := none })] }
         { objects := [objOf "c" ({ P with callbacks := ["a"] }), objWith "a" PT ({ T with state := .pending, retryAt := some 30 })] }),
    ("consistent_suspension_registers_callback",
       consistent_suspension_registers_callback 0 { objects := [objWith "a" (P) (A)] } { objects := [objWith "a" (P) ({ A with state := .suspended, pid := none, ttl := none, expiresAt := none })] }),
    ("consistent_task_birth_couples_promise_birth",
       consistent_task_birth_couples_promise_birth 0 { } { objects := [objWith "a" PT (T)] }),
    ("monotone_outbox_keys_never_disappear",
       monotone_outbox_keys_never_disappear 0 { outbox := [ex] } { }),
    ("consistent_new_execute_matches_task_and_target",
       consistent_new_execute_matches_task_and_target 0 { objects := [objWith "a" (PT) (T)] } { objects := [objWith "a" (PT) (T)], outbox := [{ address := "wrong", message := .execute "a" 3 }] }),
    ("consistent_new_unblock_carries_stored_record",
       consistent_new_unblock_carries_stored_record 0 { objects := [objOf "a" (P)] } { objects := [objOf "a" (P)], outbox := [{ address := "https://l", message := .unblock (P.toRecord "a") }] }),
    ("consistent_new_unblock_discharges_its_listener",
       consistent_new_unblock_discharges_its_listener 0
         { objects := [objOf "a" ({ S0 with listeners := ["https://l"] })] }
         { objects := [objOf "a" ({ S0 with listeners := ["https://l"] })], outbox := [{ address := "https://l", message := .unblock (S0.toRecord "a") }] }),
    ("preserved_schedule_birth_fields_immutable",
       preserved_schedule_birth_fields_immutable 0 { schedules := [C] } { schedules := [{ C with cron := "@daily" }] }),
    ("consistent_promise_settlement_stamp/forged_timeout",
       consistent_promise_settlement_stamp 50 { objects := [objOf "a" (P)] } { objects := [objOf "a" ({ P with state := .rejectedTimedout, settledAt := some 50 })] }),
    ("consistent_promise_settlement_stamp/verdict_before_deadline",
       consistent_promise_settlement_stamp 50 { objects := [objOf "a" (P)] } { objects := [objOf "a" ({ P with state := .resolved, settledAt := some 100 })] }),
    ("preserved_timedout_is_server_owned",
       preserved_timedout_is_server_owned 500 { objects := [objOf "a" (P)] } { objects := [objOf "a" ({ P with state := .rejectedTimedout, settledAt := some 500 })] }),
    ("consistent_new_promise_born_clean/born_with_obligations",
       consistent_new_promise_born_clean 50 { } { objects := [objOf "a" ({ P with callbacks := ["x"] })] }),
    ("consistent_new_promise_born_clean/born_in_the_future",
       consistent_new_promise_born_clean 5 { } { objects := [objOf "a" (P)] }),
    ("consistent_task_birth_state",
       consistent_task_birth_state 0 { } { objects := [objWith "a" PT ({ T with state := .suspended, retryAt := none })] }),
    ("consistent_task_lease_released_atomically",
       consistent_task_lease_released_atomically 0 { objects := [objWith "a" PT (A)] } { objects := [objWith "a" PT ({ T with version := 4, pid := some "w" })] }),
    ("preserved_task_lease_holder_stable",
       preserved_task_lease_holder_stable 0 { objects := [objWith "a" PT (A)] } { objects := [objWith "a" PT ({ A with pid := some "w2" })] }),
    ("consistent_task_lease_fields_move_together",
       consistent_task_lease_fields_move_together 0 { objects := [objWith "a" PT (A)] } { objects := [objWith "a" PT ({ A with ttl := some 50 })] }),
    ("monotone_task_resumes_grow_or_clear",
       monotone_task_resumes_grow_or_clear 0 { objects := [objWith "a" PT ({ T with resumes := ["b"] })] } { objects := [objWith "a" PT ({ T with resumes := ["c"] })] }),
    ("consistent_task_resumes_cleared_only_on_dispatch_or_park",
       consistent_task_resumes_cleared_only_on_dispatch_or_park 0 { objects := [objWith "a" PT ({ A with resumes := ["b"] })] } { objects := [objWith "a" PT ({ T with version := 4 })] }),
    ("consistent_task_acquisition_is_atomic",
       consistent_task_acquisition_is_atomic 40 { objects := [objWith "a" PT (T)] } { objects := [objWith "a" PT (A)] }),
    ("consistent_task_lease_deadline_is_now_plus_ttl",
       consistent_task_lease_deadline_is_now_plus_ttl 40 { objects := [objWith "a" PT (A)] } { objects := [objWith "a" PT ({ A with expiresAt := some 999 })] }),
    ("consistent_task_pending_entry_arms_retry",
       consistent_task_pending_entry_arms_retry 7 { objects := [objWith "a" PT (A)] } { objects := [objWith "a" PT ({ T with version := 4, retryAt := none })] }),
    ("consistent_task_retry_rearm_only_when_due",
       consistent_task_retry_rearm_only_when_due 5 { objects := [objWith "a" PT ({ T with retryAt := some 50 })] } { objects := [objWith "a" PT ({ T with retryAt := some 900 })] }),
    ("consistent_task_wake_records_resume",
       consistent_task_wake_records_resume 7 { objects := [objWith "a" PT ({ T with state := .suspended, retryAt := none })] } { objects := [objWith "a" PT ({ T with retryAt := some 7 })] }),
    ("consistent_task_state_edge_internal_admissible",
       consistent_task_state_edge_internal_admissible 0 { objects := [objWith "a" PT (T)] } { objects := [objWith "a" PT (A)] }),
    ("consistent_promise_state_edge_internal_admissible",
       consistent_promise_state_edge_internal_admissible 0 { objects := [objOf "a" (P)] } { objects := [objOf "a" ({ P with state := .resolved, settledAt := some 20 })] }),
    ("preserved_no_dead_dispatch",
       preserved_no_dead_dispatch 500
         { objects := [objWith "a" (P) ({ T with state := .acquired, pid := some "w", ttl := some 1, expiresAt := some 1, retryAt := none })] }
         { objects := [objWith "a" (P) (T)] }),
    ("preserved_execute_only_for_live_task",
       preserved_execute_only_for_live_task 500
         { objects := [objOf "a" (P)] }
         { objects := [objOf "a" (P)], outbox := [{ address := "w", message := .execute "a" 3 }] }) ]

theorem stage3_all_falsifiable : stepMutants.all (fun (_, b) => !b) = true := by decide

/-! ### Reach

A transition property whose transition never occurs is not checked. -/

theorem reaches_settlement_step :
    stepWitnesses battery (fun a b =>
      a.objects.any fun o => o.promise.state == .pending &&
        b.objects.any (fun q => q.id == o.id && q.promise.state != .pending)) = true := by decide

theorem reaches_version_bump :
    stepWitnesses battery (fun a b =>
      a.objects.any fun o => o.task.any fun t =>
        b.objects.any (fun q => q.id == o.id && q.task.any (fun u => t.version < u.version)))
      = true := by decide

theorem reaches_obligation_drain :
    stepWitnesses battery (fun a b =>
      a.objects.any fun o => b.objects.any (fun q =>
        q.id == o.id && q.promise.listeners.length < o.promise.listeners.length))
      = true := by decide

theorem reaches_settled_promise_step :
    stepWitnesses battery (fun a _ => a.promises.any (·.state != .pending)) = true := by decide

theorem reaches_fulfilled_task_step :
    stepWitnesses battery (fun a _ => a.tasks.any (·.state == .fulfilled)) = true := by decide

/-! ### The task read's guard, pinned

A catalogue entry cannot hold this: the difference it protects is not
visible in any state a response can report, which is exactly why it went
unnoticed until the Go fuzzer reached it. So it is pinned directly —
`decide` on one step out of one state. -/

def sTaskless : AbstractModel.ServerState :=
  { objects := [{ id := "a",
                  promise := { state := .pending, param := {},
                               tags := [("resonate:external","true")],
                               timeoutAt := 250, createdAt := 100 } }] }

/-- A task request against an id with no task writes NOTHING — under the
    MATERIALISING discipline, at an instant long past that promise's
    deadline. Delete the `o.task.isSome` test in `readTaskObject` and this
    goes red. -/
theorem taskless_id_task_request_writes_nothing :
    ((stepOf true (.api (.taskGet { id := "a" })) 500 sTaskless).2 == sTaskless
      && (stepOf true (.api (.taskHalt { id := "a" })) 500 sTaskless).2 == sTaskless)
      = true := by decide

/-- And not because nothing ever materialises here: the same state, the
    same instant, a request that IS about that promise, and it settles.
    Without this the theorem above would hold of a machine that had
    stopped materialising altogether. -/
theorem the_same_promise_does_materialise :
    ((stepOf true (.api (.promiseGet { id := "a" })) 500 sTaskless).2 == sTaskless)
      = false := by decide

/-- The correction, machine-checked: a settled promise's RECORD is
    frozen but the promise is not. The listener drain changes the object
    on a step where `preserved_settled_promise_record` holds. -/
theorem settled_promise_object_is_not_frozen :
    stepWitnesses battery (fun a b =>
      a.objects.any fun o =>
        o.promise.state != .pending &&
          b.objects.any (fun q =>
            q.id == o.id && q.promise.listeners.length < o.promise.listeners.length))
      = true := by decide

/-! ### The internal-step-only harness

The sweeper properties hold on internal steps and are FALSE on request steps.
That is not a defect — it is what makes them stronger than the general
edge tables, and it means they need their own walk. -/

def stepsWithA (mat : Bool) :
    List (Step × Nat) → ServerState → List (Step × Nat × ServerState × ServerState)
  | [], _ => []
  | (st, n) :: w, s =>
      let (_, s') := stepOf mat st n s
      (st, n, s, s') :: stepsWithA mat w s'

def allSteps (w : List (Step × Nat)) : List (Step × Nat × ServerState × ServerState) :=
  stepsWithA true w AbstractModel.ServerState.init
    ++ stepsWithA false w AbstractModel.ServerState.init

def internalWellFormedRun (w : List (Step × Nat)) : Bool :=
  (allSteps w).all (fun (st, n, a, b) => !Step.isInternal st || internalWellFormed n a b)

theorem stage3_internal_sweep :
    ((seqsUpToA kernelsResp 3).map instantiateA).all internalWellFormedRun = true := by decide

theorem reaches_internal_steps :
    (battery.any fun w => (allSteps w).any (fun (st, _, _, _) => Step.isInternal st)) = true := by decide

/-- Strictly stronger than the general edge tables, machine-checked:
    some REQUEST step in the corpus violates the sweeper properties. If they
    held everywhere they would be a restatement, not a constraint. -/
theorem internal_laws_are_strictly_stronger :
    (((seqsUpToA kernelsResp 3).map instantiateA).any fun w =>
      (allSteps w).any (fun (st, n, a, b) => !Step.isInternal st && !internalWellFormed n a b)) = true := by
  decide

/-! ### The gap that closed

`monotone_task_retry_rearm_advances` used to live in `gaps`, witnessed
by a script that fired `.r6 "x" 0` — the environment writing a past
instant into the store. There is no such script now: `Step.r6` names
only its task, and the next instant comes from
`Env.config.retryTimeout`, which no step can write. The witness cannot
be expressed, so it is gone, and the property has moved into
`catalogue` where `stage3_sweep` checks it over all 1 464 scripts.

That is the shape a closed gap should leave behind: not a theorem
saying the bug is absent, but the impossibility of writing the bug
down. -/

end Abstraction
