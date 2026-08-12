import «02-abstract».«state»

namespace AbstractModel
namespace Properties

open ServerModel

def well_formed_promise_created_at_lte_timeout_at (p : PromiseObject) : Bool :=
  p.createdAt ≤ p.timeoutAt

def well_formed_promise_pending_created_before_deadline (p : PromiseObject) : Bool :=
  p.state != .pending || p.createdAt < p.timeoutAt

def well_formed_promise_settled_at_lte_timeout_at (p : PromiseObject) : Bool :=
  match p.settledAt with
  | none => true
  | some x => x ≤ p.timeoutAt

def well_formed_promise_created_at_lte_settled_at (p : PromiseObject) : Bool :=
  match p.settledAt with
  | none => true
  | some x => p.createdAt ≤ x

def well_formed_promise_settled_at_iff_not_pending (p : PromiseObject) : Bool :=
  (p.state != .pending) == p.settledAt.isSome

def well_formed_promise_pending_has_no_value (p : PromiseObject) : Bool :=
  p.state != .pending || (p.value.data.isNone && p.value.headers.isEmpty)

def well_formed_promise_deadline_verdict_matches_timer_tag (p : PromiseObject) : Bool :=
  p.settledAt != some p.timeoutAt
    || p.state == (if p.tags.isTimer then .resolved else .rejectedTimedout)

def well_formed_promise_deadline_settlement_has_no_value (p : PromiseObject) : Bool :=
  p.settledAt != some p.timeoutAt
    || (p.value.data.isNone && p.value.headers.isEmpty)

def well_formed_promise_timer_not_targeted (p : PromiseObject) : Bool :=
  !p.tags.timerTargeted

def well_formed_promise_callbacks_unique (p : PromiseObject) : Bool :=
  p.callbacks.eraseDups.length == p.callbacks.length

def well_formed_promise_listeners_unique (p : PromiseObject) : Bool :=
  p.listeners.eraseDups.length == p.listeners.length

def well_formed_promise_obligations_require_external (p : PromiseObject) : Bool :=
  (p.callbacks.isEmpty && p.listeners.isEmpty) || p.external

def well_formed_promise_awaiter_is_not_self (p : PromiseObject) : Bool :=
  !p.callbacks.contains p.id

def well_formed_promise_created_at_lte_now (now : Nat) (p : PromiseObject) : Bool :=
  p.createdAt ≤ now

def well_formed_promise_settled_at_lte_now (now : Nat) (p : PromiseObject) : Bool :=
  match p.settledAt with
  | none => true
  | some x => x ≤ now

def well_formed_task_acquired_iff_has_pid (t : TaskObject) : Bool :=
  (t.state == .acquired) == t.pid.isSome

def well_formed_task_acquired_iff_has_ttl (t : TaskObject) : Bool :=
  (t.state == .acquired) == t.ttl.isSome

def well_formed_task_acquired_iff_has_expires_at (t : TaskObject) : Bool :=
  (t.state == .acquired) == t.expiresAt.isSome

def well_formed_task_pending_iff_has_retry_at (t : TaskObject) : Bool :=
  (t.state == .pending) == t.retryAt.isSome

def well_formed_task_fulfilled_is_cleared (t : TaskObject) : Bool :=
  t.state != .fulfilled
    || (t.pid.isNone && t.ttl.isNone && t.expiresAt.isNone && t.retryAt.isNone
        && t.resumes.isEmpty)

def well_formed_task_suspended_is_cleared (t : TaskObject) : Bool :=
  t.state != .suspended
    || (t.pid.isNone && t.ttl.isNone && t.expiresAt.isNone && t.retryAt.isNone)

def well_formed_task_halted_is_cleared (t : TaskObject) : Bool :=
  t.state != .halted
    || (t.pid.isNone && t.ttl.isNone && t.expiresAt.isNone && t.retryAt.isNone)

def well_formed_task_suspended_has_no_resumes (t : TaskObject) : Bool :=
  t.state != .suspended || t.resumes.isEmpty

def well_formed_task_resumes_unique (t : TaskObject) : Bool :=
  t.resumes.eraseDups.length == t.resumes.length

def well_formed_task_acquired_version_positive (t : TaskObject) : Bool :=
  t.state != .acquired || 1 ≤ t.version

def well_formed_schedule_promise_tags_not_timer_targeted (c : Schedule) : Bool :=
  !c.promiseTags.timerTargeted

def well_formed_schedule_created_at_lte_next_run_at (c : Schedule) : Bool :=
  c.createdAt ≤ c.nextRunAt

def well_formed_schedule_created_at_lte_last_run_at (c : Schedule) : Bool :=
  match c.lastRunAt with
  | none => true
  | some l => c.createdAt ≤ l

def well_formed_schedule_last_run_at_lt_next_run_at (c : Schedule) : Bool :=
  match c.lastRunAt with
  | none => true
  | some l => l < c.nextRunAt

def well_formed_store_promise_ids_unique (s : ServerState) : Bool :=
  (s.promises.map (·.id)).eraseDups.length == s.promises.length

def well_formed_store_task_ids_unique (s : ServerState) : Bool :=
  (s.tasks.map (·.id)).eraseDups.length == s.tasks.length

def well_formed_store_schedule_ids_unique (s : ServerState) : Bool :=
  (s.schedules.map (·.id)).eraseDups.length == s.schedules.length

def well_formed_store_outbox_keys_unique (s : ServerState) : Bool :=
  (s.outbox.map (·.key)).eraseDups.length == s.outbox.length

def consistent_task_iff_targeted_promise (s : ServerState) : Bool :=
  s.tasks.all (fun t =>
      s.promises.any (fun p => p.id == t.id && p.tags.has "resonate:target"))
    && s.promises.all (fun p =>
      !p.tags.has "resonate:target" || s.tasks.any (·.id == p.id))

def consistent_settled_promise_has_fulfilled_task (s : ServerState) : Bool :=
  s.promises.all fun p =>
    p.state == .pending || s.tasks.all (fun t => t.id != p.id || t.state == .fulfilled)

def consistent_callback_awaiter_is_targeted (s : ServerState) : Bool :=
  s.promises.all fun p =>
    p.callbacks.all fun a =>
      s.promises.any (fun q => q.id == a && q.tags.has "resonate:target")

def consistent_listener_addresses_deliverable (s : ServerState) : Bool :=
  s.promises.all fun p => p.listeners.all addressValid

def consistent_outbox_execute_names_existing_task (s : ServerState) : Bool :=
  s.outbox.all fun e =>
    match e.message with
    | .execute id _ => s.tasks.any (·.id == id)
    | .unblock _    => true

def consistent_outbox_never_ahead (s : ServerState) : Bool :=
  s.outbox.all fun e =>
    match e.message with
    | .execute id v =>
        match s.tasks.find? (·.id == id) with
        | some t => v ≤ t.version
        | none   => true
    | .unblock _ => true

def consistent_outbox_execute_address_is_target_tag (s : ServerState) : Bool :=
  s.outbox.all fun e =>
    match e.message with
    | .execute id _ =>
        match s.promises.find? (·.id == id) with
        | some p => e.address == (p.tags.get? "resonate:target").getD ""
        | none   => true
    | .unblock _ => true

def consistent_outbox_unblock_names_settled_promise (s : ServerState) : Bool :=
  s.outbox.all fun e =>
    match e.message with
    | .unblock r =>
        r.state != .pending
          && s.promises.any (fun p => p.id == r.id && p.state != .pending)
    | .execute _ _ => true

def consistent_outbox_unblock_address_deliverable (s : ServerState) : Bool :=
  s.outbox.all fun e =>
    match e.message with
    | .unblock _   => addressValid e.address
    | .execute _ _ => true

def consistent_suspended_task_holds_rung (now : Nat) (s : ServerState) : Bool :=
  s.tasks.all fun t =>
    t.state != .suspended ||
      (match s.promises.find? (·.id == t.id) with
       | none => true
       | some p =>
           (p.project now).state != .pending
             || s.promises.any (·.callbacks.contains t.id))

def consistent_settled_task_promise_settled (s : ServerState) : Bool :=
  s.tasks.all fun t =>
    t.state != .fulfilled ||
      (match s.promises.find? (·.id == t.id) with
       | none => true
       | some p => p.state != .pending)

/-! ### The catalogue as data

Names travel with predicates so a failure reports which property broke
rather than that something did. -/

def promiseChecks : List (String × (PromiseObject → Bool)) :=
  [ ("well_formed_promise_created_at_lte_timeout_at",        well_formed_promise_created_at_lte_timeout_at),
    ("well_formed_promise_pending_created_before_deadline",  well_formed_promise_pending_created_before_deadline),
    ("well_formed_promise_settled_at_lte_timeout_at",        well_formed_promise_settled_at_lte_timeout_at),
    ("well_formed_promise_created_at_lte_settled_at",        well_formed_promise_created_at_lte_settled_at),
    ("well_formed_promise_settled_at_iff_not_pending",       well_formed_promise_settled_at_iff_not_pending),
    ("well_formed_promise_pending_has_no_value",             well_formed_promise_pending_has_no_value),
    ("well_formed_promise_deadline_verdict_matches_timer_tag", well_formed_promise_deadline_verdict_matches_timer_tag),
    ("well_formed_promise_deadline_settlement_has_no_value", well_formed_promise_deadline_settlement_has_no_value),
    ("well_formed_promise_timer_not_targeted",               well_formed_promise_timer_not_targeted),
    ("well_formed_promise_callbacks_unique",                 well_formed_promise_callbacks_unique),
    ("well_formed_promise_listeners_unique",                 well_formed_promise_listeners_unique),
    ("well_formed_promise_obligations_require_external",     well_formed_promise_obligations_require_external),
    ("well_formed_promise_awaiter_is_not_self",              well_formed_promise_awaiter_is_not_self) ]

def promiseClockChecks : List (String × (Nat → PromiseObject → Bool)) :=
  [ ("well_formed_promise_created_at_lte_now",  well_formed_promise_created_at_lte_now),
    ("well_formed_promise_settled_at_lte_now",  well_formed_promise_settled_at_lte_now) ]

def taskChecks : List (String × (TaskObject → Bool)) :=
  [ ("well_formed_task_acquired_iff_has_pid",        well_formed_task_acquired_iff_has_pid),
    ("well_formed_task_acquired_iff_has_ttl",        well_formed_task_acquired_iff_has_ttl),
    ("well_formed_task_acquired_iff_has_expires_at", well_formed_task_acquired_iff_has_expires_at),
    ("well_formed_task_pending_iff_has_retry_at",    well_formed_task_pending_iff_has_retry_at),
    ("well_formed_task_fulfilled_is_cleared",        well_formed_task_fulfilled_is_cleared),
    ("well_formed_task_suspended_is_cleared",        well_formed_task_suspended_is_cleared),
    ("well_formed_task_halted_is_cleared",           well_formed_task_halted_is_cleared),
    ("well_formed_task_suspended_has_no_resumes",    well_formed_task_suspended_has_no_resumes),
    ("well_formed_task_resumes_unique",              well_formed_task_resumes_unique),
    ("well_formed_task_acquired_version_positive",   well_formed_task_acquired_version_positive) ]

def scheduleChecks : List (String × (Schedule → Bool)) :=
  [ ("well_formed_schedule_promise_tags_not_timer_targeted", well_formed_schedule_promise_tags_not_timer_targeted),
    ("well_formed_schedule_created_at_lte_next_run_at",      well_formed_schedule_created_at_lte_next_run_at),
    ("well_formed_schedule_created_at_lte_last_run_at",      well_formed_schedule_created_at_lte_last_run_at),
    ("well_formed_schedule_last_run_at_lt_next_run_at",      well_formed_schedule_last_run_at_lt_next_run_at) ]

def storeChecks : List (String × (ServerState → Bool)) :=
  [ ("well_formed_store_promise_ids_unique",  well_formed_store_promise_ids_unique),
    ("well_formed_store_task_ids_unique",     well_formed_store_task_ids_unique),
    ("well_formed_store_schedule_ids_unique", well_formed_store_schedule_ids_unique),
    ("well_formed_store_outbox_keys_unique",  well_formed_store_outbox_keys_unique),
    ("consistent_task_iff_targeted_promise",              consistent_task_iff_targeted_promise),
    ("consistent_settled_promise_has_fulfilled_task",     consistent_settled_promise_has_fulfilled_task),
    ("consistent_callback_awaiter_is_targeted",           consistent_callback_awaiter_is_targeted),
    ("consistent_listener_addresses_deliverable",         consistent_listener_addresses_deliverable),
    ("consistent_outbox_execute_names_existing_task",     consistent_outbox_execute_names_existing_task),
    ("consistent_outbox_never_ahead",                     consistent_outbox_never_ahead),
    ("consistent_outbox_execute_address_is_target_tag",   consistent_outbox_execute_address_is_target_tag),
    ("consistent_outbox_unblock_names_settled_promise",   consistent_outbox_unblock_names_settled_promise),
    ("consistent_outbox_unblock_address_deliverable",     consistent_outbox_unblock_address_deliverable),
    ("consistent_settled_task_promise_settled",           consistent_settled_task_promise_settled) ]

def storeClockChecks : List (String × (Nat → ServerState → Bool)) :=
  [ ("consistent_suspended_task_holds_rung", consistent_suspended_task_holds_rung) ]

def failures (now : Nat) (s : ServerState) : List String :=
  (promiseChecks.filterMap fun (n, f) => if s.promises.all f then none else some n)
    ++ (promiseClockChecks.filterMap fun (n, f) =>
          if s.promises.all (f now) then none else some n)
    ++ (taskChecks.filterMap fun (n, f) => if s.tasks.all f then none else some n)
    ++ (scheduleChecks.filterMap fun (n, f) => if s.schedules.all f then none else some n)
    ++ (storeChecks.filterMap fun (n, f) => if f s then none else some n)
    ++ (storeClockChecks.filterMap fun (n, f) => if f now s then none else some n)

def well_formed (now : Nat) (s : ServerState) : Bool :=
  (failures now s).isEmpty

/-! ### Record projections

Three catalogue entries are claims about what the WIRE record hides.
They are not state predicates — they are equations on `toRecord`, and
they are the strongest form of "unobservable" the type system can give:
change the hidden field, get the same record. -/

theorem well_formed_promise_record_hides_callbacks (p : PromiseObject) (a : String) :
    (p.addCallback a).toRecord = p.toRecord := by
  unfold PromiseObject.addCallback
  split <;> rfl

theorem well_formed_promise_record_hides_listeners (p : PromiseObject) (a : String) :
    (p.addListener a).toRecord = p.toRecord := by
  unfold PromiseObject.addListener
  split <;> rfl

theorem well_formed_task_record_hides_deadlines (t : TaskObject) (e r : Option Nat) :
    ({ t with expiresAt := e, retryAt := r } : TaskObject).toRecord = t.toRecord := rfl

theorem well_formed_task_record_resumes_is_a_count (t : TaskObject) :
    t.toRecord.resumes = t.resumes.length := rfl

end Properties
end AbstractModel
