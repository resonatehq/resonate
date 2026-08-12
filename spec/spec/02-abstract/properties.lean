import «02-abstract».«state»

namespace AbstractModel
namespace Properties

open ServerModel

def well_formed_promise_created_at_lte_timeout_at (p : PromiseObject) : Bool :=
  p.createdAt ≤ p.timeoutAt

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

def promiseChecks : List (String × (PromiseObject → Bool)) :=
  [ ("well_formed_promise_created_at_lte_timeout_at",   well_formed_promise_created_at_lte_timeout_at),
    ("well_formed_promise_settled_at_lte_timeout_at",   well_formed_promise_settled_at_lte_timeout_at),
    ("well_formed_promise_created_at_lte_settled_at",   well_formed_promise_created_at_lte_settled_at),
    ("well_formed_promise_settled_at_iff_not_pending",  well_formed_promise_settled_at_iff_not_pending),
    ("well_formed_promise_pending_has_no_value",        well_formed_promise_pending_has_no_value),
    ("well_formed_promise_timer_not_targeted",          well_formed_promise_timer_not_targeted),
    ("well_formed_promise_callbacks_unique",            well_formed_promise_callbacks_unique),
    ("well_formed_promise_listeners_unique",            well_formed_promise_listeners_unique),
    ("well_formed_promise_obligations_require_external", well_formed_promise_obligations_require_external),
    ("well_formed_promise_awaiter_is_not_self",         well_formed_promise_awaiter_is_not_self) ]

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
    ("well_formed_task_resumes_unique",              well_formed_task_resumes_unique) ]

def scheduleChecks : List (String × (Schedule → Bool)) :=
  [ ("well_formed_schedule_promise_tags_not_timer_targeted", well_formed_schedule_promise_tags_not_timer_targeted),
    ("well_formed_schedule_created_at_lte_next_run_at",      well_formed_schedule_created_at_lte_next_run_at),
    ("well_formed_schedule_created_at_lte_last_run_at",      well_formed_schedule_created_at_lte_last_run_at),
    ("well_formed_schedule_last_run_at_lt_next_run_at",      well_formed_schedule_last_run_at_lt_next_run_at) ]

def storeChecks : List (String × (ServerState → Bool)) :=
  [ ("well_formed_store_promise_ids_unique",  well_formed_store_promise_ids_unique),
    ("well_formed_store_task_ids_unique",     well_formed_store_task_ids_unique),
    ("well_formed_store_schedule_ids_unique", well_formed_store_schedule_ids_unique),
    ("well_formed_store_outbox_keys_unique",  well_formed_store_outbox_keys_unique) ]

def failures (now : Nat) (s : ServerState) : List String :=
  (promiseChecks.filterMap fun (n, f) => if s.promises.all f then none else some n)
    ++ (promiseClockChecks.filterMap fun (n, f) =>
          if s.promises.all (f now) then none else some n)
    ++ (taskChecks.filterMap fun (n, f) => if s.tasks.all f then none else some n)
    ++ (scheduleChecks.filterMap fun (n, f) => if s.schedules.all f then none else some n)
    ++ (storeChecks.filterMap fun (n, f) => if f s then none else some n)

def well_formed (now : Nat) (s : ServerState) : Bool :=
  (failures now s).isEmpty

end Properties
end AbstractModel
