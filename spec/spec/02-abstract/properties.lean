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

/-! ## Stage 3 — transition properties

Predicates on a PAIR of states linked by one step. Everything in
`02-abstract/properties.lean` is a claim about a state; these are claims
about a change, and no state predicate can express them: "the version
went up by one" is invisible in either endpoint alone.

The corpus is the same as stage 1's — every script, both read
disciplines — but consumed as consecutive pairs rather than as states.

Two of these say something the object-level reading gets wrong.

`preserved_settled_promise_record` freezes `state`, `value` and
`settledAt` once a promise is settled. It does NOT freeze the promise:
the listener and callback rules keep removing obligations from settled
promises, which is how a wake is discharged. What is frozen is exactly
`toRecord` — exactly the part a response can carry — so "a settled
promise never changes again" is true on the wire and false in the store.

`preserved_task_version_increments_only_on_acquisition` is the fencing
law. Version moves by exactly one, only on `pending → acquired`, and by
nothing on any other transition — release, halt, continue, suspend,
fulfilment, lease expiry, retry, wake. `monotone_task_version_never_decreases`
is not carried separately: it follows. -/

def preserved_promise_birth_fields_immutable (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    match b.promises.find? (·.id == p.id) with
    | none => true
    | some q =>
        q.param.data == p.param.data && q.param.headers == p.param.headers
          && q.tags == p.tags && q.timeoutAt == p.timeoutAt && q.createdAt == p.createdAt

def preserved_settled_promise_record (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    p.state == .pending ||
      (match b.promises.find? (·.id == p.id) with
       | none => false
       | some q =>
           q.state == p.state && q.settledAt == p.settledAt
             && q.value.data == p.value.data && q.value.headers == p.value.headers)

def monotone_settled_promise_obligations_shrink (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    p.state == .pending ||
      (match b.promises.find? (·.id == p.id) with
       | none => false
       | some q =>
           q.callbacks.all p.callbacks.contains && q.listeners.all p.listeners.contains)

def monotone_promise_set_grows (a b : ServerState) : Bool :=
  a.promises.all fun p => b.promises.any (·.id == p.id)

def monotone_task_set_grows (a b : ServerState) : Bool :=
  a.tasks.all fun t => b.tasks.any (·.id == t.id)

/-- The fencing law: a task's version rises by exactly one on
    `pending → acquired`, and does not move on any other transition. -/
def preserved_task_version_increments_only_on_acquisition (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none => true
    | some u =>
        if t.state == .pending && u.state == .acquired then
          u.version == t.version + 1
        else
          u.version == t.version

def preserved_fulfilled_task (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    t.state != .fulfilled ||
      (match b.tasks.find? (·.id == t.id) with
       | none => false
       | some u =>
           u.state == .fulfilled && u.version == t.version && u.resumes.isEmpty
             && u.pid.isNone && u.ttl.isNone && u.expiresAt.isNone && u.retryAt.isNone)

/-- `NoDeadDispatch`, state half: no step puts a task into `pending`
    when its promise's deadline has already passed. A task already
    pending before the step is not a re-pend. -/
def preserved_no_dead_dispatch (now : Nat) (a b : ServerState) : Bool :=
  b.tasks.all fun u =>
    u.state != .pending
      || (match a.tasks.find? (·.id == u.id) with
          | some t => t.state == .pending
          | none   => false)
      || (match b.promises.find? (·.id == u.id) with
          | some p => (p.project now).state == .pending
          | none   => true)

/-- `NoDeadDispatch`, message half: an `execute` that was not already
    queued before the step names a task whose promise is still live. -/
def preserved_execute_only_for_live_task (now : Nat) (a b : ServerState) : Bool :=
  b.outbox.all fun e =>
    match e.message with
    | .unblock _ => true
    | .execute id v =>
        a.outbox.any (fun f =>
          match f.message with
          | .execute id' v' => id' == id && v' == v && f.address == e.address
          | .unblock _ => false)
        || (match b.promises.find? (·.id == id) with
            | some p => (p.project now).state == .pending
            | none   => true)

/-! ## Stage 3 — field-level evolution

Derived per field from every write site, then evaluated. The obligation
laws split on the POST-state, not the pre-state: `processCallback`
materializes a deadline AND drains a callback in the same step, so a
promise pending before and settled after loses one. -/

def appendedAtMostOne (pre post : List String) : Bool :=
  post == pre ||
    (post.length == pre.length + 1 && post.take pre.length == pre
      && (post.drop pre.length).all (fun x => !pre.contains x))

def removedAtMostOne (pre post : List String) : Bool :=
  post == pre || pre.any (fun x => post == pre.filter (· != x))

def preserved_promise_state_frozen_once_settled (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    p.state == .pending ||
      (match b.promises.find? (·.id == p.id) with
       | none => false
       | some q => q.state == p.state)

def preserved_promise_settlement_is_one_way (a b : ServerState) : Bool :=
  b.promises.all fun q =>
    q.state != .pending
      || (match a.promises.find? (·.id == q.id) with
          | some p => p.state == .pending
          | none   => true)

def consistent_promise_settled_at_moves_with_state (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    match b.promises.find? (·.id == p.id) with
    | none => false
    | some q => (q.settledAt != p.settledAt) == (q.state != p.state)

def preserved_promise_value_until_settlement (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    match b.promises.find? (·.id == p.id) with
    | none => false
    | some q =>
        q.state != .pending
          || (q.value.data == p.value.data && q.value.headers == p.value.headers)

def preserved_promise_no_duplicate_ids (_a b : ServerState) : Bool :=
  (b.promises.map (·.id)).eraseDups.length == b.promises.length

def monotone_promise_callbacks_append_one_while_pending (a b : ServerState) : Bool :=
  b.promises.all fun q =>
    q.state != .pending ||
      (match a.promises.find? (·.id == q.id) with
       | none => q.callbacks.isEmpty
       | some p => appendedAtMostOne p.callbacks q.callbacks)

def monotone_promise_callbacks_drain_one_once_settled (a b : ServerState) : Bool :=
  b.promises.all fun q =>
    q.state == .pending ||
      (match a.promises.find? (·.id == q.id) with
       | none => q.callbacks.isEmpty
       | some p => removedAtMostOne p.callbacks q.callbacks)

def monotone_promise_listeners_append_one_while_pending (a b : ServerState) : Bool :=
  b.promises.all fun q =>
    q.state != .pending ||
      (match a.promises.find? (·.id == q.id) with
       | none => q.listeners.isEmpty
       | some p => appendedAtMostOne p.listeners q.listeners)

def monotone_promise_listeners_drain_one_once_settled (a b : ServerState) : Bool :=
  b.promises.all fun q =>
    q.state == .pending ||
      (match a.promises.find? (·.id == q.id) with
       | none => q.listeners.isEmpty
       | some p => removedAtMostOne p.listeners q.listeners)

/-! ## Stage 3 — the state machine edges

The admissible pair lists, written out rather than paraphrased from the
handlers, so the check is independent of the code it checks. -/

def consistent_promise_state_edge_admissible (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    match b.promises.find? (·.id == p.id) with
    | none   => true
    | some q =>
        [ (PromiseState.pending,          PromiseState.pending),
          (PromiseState.pending,          PromiseState.resolved),
          (PromiseState.pending,          PromiseState.rejected),
          (PromiseState.pending,          PromiseState.rejectedCanceled),
          (PromiseState.pending,          PromiseState.rejectedTimedout),
          (PromiseState.resolved,         PromiseState.resolved),
          (PromiseState.rejected,         PromiseState.rejected),
          (PromiseState.rejectedCanceled, PromiseState.rejectedCanceled),
          (PromiseState.rejectedTimedout, PromiseState.rejectedTimedout)
        ].contains (p.state, q.state)

def consistent_task_state_edge_admissible (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none   => true
    | some u =>
        [ (TaskState.pending,   TaskState.pending),
          (TaskState.pending,   TaskState.acquired),
          (TaskState.pending,   TaskState.halted),
          (TaskState.pending,   TaskState.fulfilled),
          (TaskState.acquired,  TaskState.pending),
          (TaskState.acquired,  TaskState.acquired),
          (TaskState.acquired,  TaskState.suspended),
          (TaskState.acquired,  TaskState.halted),
          (TaskState.acquired,  TaskState.fulfilled),
          (TaskState.suspended, TaskState.pending),
          (TaskState.suspended, TaskState.suspended),
          (TaskState.suspended, TaskState.halted),
          (TaskState.suspended, TaskState.fulfilled),
          (TaskState.halted,    TaskState.pending),
          (TaskState.halted,    TaskState.halted),
          (TaskState.halted,    TaskState.fulfilled),
          (TaskState.fulfilled, TaskState.fulfilled)
        ].contains (t.state, u.state)

def preserved_task_acquisition_only_from_pending (a b : ServerState) : Bool :=
  b.tasks.all fun u =>
    u.state != .acquired
      || (match a.tasks.find? (·.id == u.id) with
          | some t => t.state == .pending || t.state == .acquired
          | none   => true)

def preserved_task_suspension_only_from_acquired (a b : ServerState) : Bool :=
  b.tasks.all fun u =>
    u.state != .suspended
      || (match a.tasks.find? (·.id == u.id) with
          | some t => t.state == .acquired || t.state == .suspended
          | none   => false)

def preserved_task_halted_only_reenters_via_pending (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    t.state != .halted
      || (match b.tasks.find? (·.id == t.id) with
          | none   => false
          | some u => [TaskState.halted, TaskState.pending, TaskState.fulfilled].contains u.state)

/-! ## Stage 3 — cross-object coupling

What must move together in one step. These are the primitives; several
state invariants above are their inductive consequences. -/

def consistent_settlement_fulfils_task (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    p.state != .pending ||
      (match b.promises.find? (·.id == p.id), b.tasks.find? (·.id == p.id) with
       | some q, some u => q.state == .pending || u.state == .fulfilled
       | _, _ => true)

def consistent_task_fulfilment_needs_settlement (a b : ServerState) : Bool :=
  b.tasks.all fun u =>
    u.state != .fulfilled ||
      (match a.tasks.find? (·.id == u.id) with
       | none => true
       | some t =>
           t.state == .fulfilled
             || (match a.promises.find? (·.id == u.id), b.promises.find? (·.id == u.id) with
                 | some p, some q => p.state == .pending && q.state != .pending
                 | _, _ => false))

def consistent_obligation_discharge_requires_settled (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    match b.promises.find? (·.id == p.id) with
    | none => true
    | some q =>
        (p.callbacks.all q.callbacks.contains && p.listeners.all q.listeners.contains)
          || q.state != .pending

def consistent_callback_consumption_resumes_awaiter (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    p.callbacks.all fun x =>
      (match b.promises.find? (·.id == p.id) with
       | none => true
       | some q => q.callbacks.contains x)
      || (match b.tasks.find? (·.id == x) with
          | none => true
          | some u => u.state == .fulfilled || u.resumes.contains p.id)

def consistent_listener_consumption_enqueues_unblock (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    p.listeners.all fun addr =>
      (match b.promises.find? (·.id == p.id) with
       | none => true
       | some q => q.listeners.contains addr)
      || (b.outbox.filter (fun e =>
            e.address == addr &&
              (match e.message with
               | .unblock r => r.id == p.id && r.state != .pending
               | .execute _ _ => false))).length == 1

def consistent_wake_follows_callback_consumption (a b : ServerState) : Bool :=
  b.tasks.all fun u =>
    match a.tasks.find? (·.id == u.id) with
    | none => true
    | some t =>
        !(t.state == .suspended && u.state == .pending)
          || a.promises.any (fun p =>
               p.callbacks.contains u.id
                 && (match b.promises.find? (·.id == p.id) with
                     | none => false
                     | some q => !q.callbacks.contains u.id)
                 && u.resumes == [p.id])

def consistent_suspension_registers_callback (a b : ServerState) : Bool :=
  b.tasks.all fun u =>
    u.state != .suspended
      || (match a.tasks.find? (·.id == u.id) with
          | none => false
          | some t => t.state == .suspended)
      || b.promises.any (fun q =>
           q.callbacks.contains u.id
             && (match a.promises.find? (·.id == q.id) with
                 | none => true
                 | some p => !p.callbacks.contains u.id)
             && q.state == .pending)

def consistent_callback_additions_share_one_awaiter (a b : ServerState) : Bool :=
  ((b.promises.flatMap fun q =>
      match a.promises.find? (·.id == q.id) with
      | none => q.callbacks
      | some p => q.callbacks.filter (fun x => !p.callbacks.contains x)).eraseDups.length ≤ 1)
    && b.promises.all fun q =>
         (match a.promises.find? (·.id == q.id) with
          | none => q.callbacks.isEmpty
          | some p => q.callbacks.all p.callbacks.contains)
         || q.state == .pending

def consistent_at_most_one_obligation_discharged (a b : ServerState) : Bool :=
  ((a.promises.map fun p =>
      match b.promises.find? (·.id == p.id) with
      | none => 0
      | some q =>
          (p.callbacks.filter (fun x => !q.callbacks.contains x)).length
            + (p.listeners.filter (fun x => !q.listeners.contains x)).length).sum) ≤ 1

def consistent_at_most_one_task_acquired (a b : ServerState) : Bool :=
  (b.tasks.filter fun u =>
     u.state == .acquired &&
       (match a.tasks.find? (·.id == u.id) with
        | none => true
        | some t => t.state != .acquired)).length ≤ 1

def consistent_task_birth_couples_promise_birth (a b : ServerState) : Bool :=
  (b.tasks.all fun u =>
     a.tasks.any (·.id == u.id)
       || ((!a.promises.any (·.id == u.id))
            && (match b.promises.find? (·.id == u.id) with
                | none => false
                | some q =>
                    q.tags.has "resonate:target"
                      && (if u.state == .fulfilled then q.state != .pending
                          else q.state == .pending))
            && ((u.state == .pending && u.version == 0)
                || (u.state == .acquired && u.version == 1)
                || (u.state == .fulfilled && u.version == 0))))
  && (b.promises.all fun q =>
        a.promises.any (·.id == q.id)
          || !q.tags.has "resonate:target"
          || b.tasks.any (·.id == q.id))

/-! ## Stage 3 — the outbox -/

def monotone_outbox_keys_never_disappear (a b : ServerState) : Bool :=
  a.outbox.all fun e => b.outbox.any (fun f => f.key == e.key)

def consistent_new_execute_matches_task_and_target (a b : ServerState) : Bool :=
  b.outbox.all fun f =>
    match f.message with
    | .unblock _ => true
    | .execute id v =>
        a.outbox.any (fun e =>
          match e.message with
          | .execute id' v' => id' == id && v' == v && e.address == f.address
          | .unblock _ => false)
        || ((match b.tasks.find? (·.id == id) with
             | some t => t.version == v
             | none   => false)
            && (match b.promises.find? (·.id == id) with
                | some p => f.address == (p.tags.get? "resonate:target").getD ""
                | none   => false))

def consistent_new_unblock_carries_stored_record (a b : ServerState) : Bool :=
  b.outbox.all fun f =>
    match f.message with
    | .execute _ _ => true
    | .unblock r =>
        a.outbox.any (fun e =>
          match e.message with
          | .unblock r' => e.address == f.address && r'.id == r.id
          | .execute _ _ => false)
        || (r.state != .pending
            && (match b.promises.find? (·.id == r.id) with
                | some p =>
                    p.state == r.state && p.settledAt == r.settledAt
                      && p.value.data == r.value.data && p.timeoutAt == r.timeoutAt
                      && p.createdAt == r.createdAt
                | none => false))

def consistent_new_unblock_discharges_its_listener (a b : ServerState) : Bool :=
  b.outbox.all fun f =>
    match f.message with
    | .execute _ _ => true
    | .unblock r =>
        a.outbox.any (fun e =>
          match e.message with
          | .unblock r' => e.address == f.address && r'.id == r.id
          | .execute _ _ => false)
        || ((a.promises.any fun p => p.id == r.id && p.listeners.contains f.address)
            && (b.promises.all fun p => p.id != r.id || !p.listeners.contains f.address))

/-! ## Stage 3 — schedules

Only the laws that hold unconditionally. The ordering laws
(`nextRunAt` never regresses, the run marks advance together) are true
of the protocol but not of the model: they need axioms on `nextCron`
and `occurrences`, which are `opaque` with no value. They are not
carried here, because a law that passes only because nothing reaches it
is not being checked. -/

def preserved_schedule_birth_fields_immutable (a b : ServerState) : Bool :=
  a.schedules.all fun c =>
    match b.schedules.find? (·.id == c.id) with
    | none => true
    | some d =>
        d.cron == c.cron && d.promiseId == c.promiseId
          && d.promiseTimeout == c.promiseTimeout
          && d.promiseParam.data == c.promiseParam.data
          && d.promiseParam.headers == c.promiseParam.headers
          && d.promiseTags == c.promiseTags && d.createdAt == c.createdAt

def consistent_schedule_change_is_single (a b : ServerState) : Bool :=
  let gone  := a.schedules.filter fun c => !b.schedules.any (·.id == c.id)
  let born  := b.schedules.filter fun d => !a.schedules.any (·.id == d.id)
  let moved := a.schedules.filter fun c =>
                 match b.schedules.find? (·.id == c.id) with
                 | none => false
                 | some d => !(d.nextRunAt == c.nextRunAt && d.lastRunAt == c.lastRunAt)
  gone.length + born.length + moved.length ≤ 1

def consistent_schedule_removal_is_isolated (a b : ServerState) : Bool :=
  a.schedules.all (fun c => b.schedules.any (·.id == c.id))
    || (b.promises.length == a.promises.length
        && b.tasks.length == a.tasks.length
        && b.outbox.length == a.outbox.length
        && b.schedules.length + 1 == a.schedules.length)

/-! ## Stage 3 — task field evolution

The lease is three fields that move as one unit, the retry alarm is
armed on every entry into `pending`, and the resume buffer is cleared
only where work is handed over or parked. -/

def consistent_task_birth_state (a b : ServerState) : Bool :=
  b.tasks.all fun u =>
    (a.tasks.any (·.id == u.id))
    || (u.state == .pending && u.version == 0 && u.retryAt.isSome
          && u.pid.isNone && u.ttl.isNone && u.expiresAt.isNone && u.resumes.isEmpty)
    || (u.state == .fulfilled && u.version == 0 && u.retryAt.isNone
          && u.pid.isNone && u.ttl.isNone && u.expiresAt.isNone && u.resumes.isEmpty)
    || (u.state == .acquired && u.version == 1 && u.retryAt.isNone
          && u.pid.isSome && u.ttl.isSome && u.expiresAt.isSome && u.resumes.isEmpty)

def consistent_task_lease_released_atomically (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none => true
    | some u =>
        !(t.state == .acquired && u.state != .acquired)
        || (u.pid.isNone && u.ttl.isNone && u.expiresAt.isNone && u.version == t.version)

def preserved_task_lease_holder_stable (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none => true
    | some u =>
        !(t.state == .acquired && u.state == .acquired && u.version == t.version)
        || (u.pid == t.pid && u.ttl == t.ttl)

def consistent_task_lease_fields_move_together (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none => true
    | some u =>
        (u.pid == t.pid && u.ttl == t.ttl && u.expiresAt == t.expiresAt)
        || (t.state != .acquired && u.state == .acquired
              && u.pid.isSome && u.ttl.isSome && u.expiresAt.isSome)
        || (t.state == .acquired && u.state != .acquired
              && u.pid.isNone && u.ttl.isNone && u.expiresAt.isNone)
        || (t.state == .acquired && u.state == .acquired
              && u.pid == t.pid && u.ttl == t.ttl)

def consistent_task_resumes_buffer_or_clear (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none => true
    | some u =>
        u.resumes == t.resumes
        || u.resumes.isEmpty
        || (u.resumes.length == t.resumes.length + 1
              && u.resumes.take t.resumes.length == t.resumes
              && u.resumes.eraseDups.length == u.resumes.length)

def consistent_task_resumes_cleared_only_on_dispatch_or_park (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none => true
    | some u =>
        !(!t.resumes.isEmpty && u.resumes.isEmpty)
        || u.state == .acquired || u.state == .suspended || u.state == .fulfilled

def consistent_task_acquisition_is_atomic (now : Nat) (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none => true
    | some u =>
        !(t.state != .acquired && u.state == .acquired)
        || (t.state == .pending && u.version == t.version + 1
              && u.pid.isSome && u.ttl.isSome
              && u.expiresAt == some (now + u.ttl.getD 0)
              && u.retryAt.isNone && u.resumes.isEmpty)

def consistent_task_lease_deadline_is_now_plus_ttl (now : Nat) (a b : ServerState) : Bool :=
  b.tasks.all fun u =>
    match u.expiresAt with
    | none => true
    | some d =>
        d == now + u.ttl.getD 0
        || (match a.tasks.find? (·.id == u.id) with
            | some t => t.expiresAt == some d && t.ttl == u.ttl && t.state == u.state
            | none   => false)

def consistent_task_pending_entry_arms_retry (now : Nat) (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none => true
    | some u =>
        !(t.state != .pending && u.state == .pending) || u.retryAt == some now

def consistent_task_retry_rearm_only_when_due (now : Nat) (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none => true
    | some u =>
        !(t.state == .pending && u.state == .pending && u.retryAt != t.retryAt)
        || (match t.retryAt with | some due => decide (due ≤ now) | none => false)

def consistent_task_wake_replaces_resumes (now : Nat) (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none => true
    | some u =>
        !(t.state == .suspended && u.state == .pending)
        || (u.resumes.length == 1 && u.retryAt == some now && u.version == t.version)

/-! ## The sweeper laws

Applied to INTERNAL steps only, and false on request steps — which is
what makes them strictly stronger than the general edge tables. A
background sweeper may re-pend, fulfil, resume-from-suspended or
refresh; it may never acquire, suspend, halt or continue a task, and it
may settle a promise only by its deadline. A server whose reaper does
any of the rest steals a lease or invents a verdict nobody asked for. -/

def consistent_task_state_edge_rule_admissible (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none   => true
    | some u =>
        [ (TaskState.pending,   TaskState.pending),
          (TaskState.pending,   TaskState.fulfilled),
          (TaskState.acquired,  TaskState.pending),
          (TaskState.acquired,  TaskState.acquired),
          (TaskState.acquired,  TaskState.fulfilled),
          (TaskState.suspended, TaskState.pending),
          (TaskState.suspended, TaskState.suspended),
          (TaskState.suspended, TaskState.fulfilled),
          (TaskState.halted,    TaskState.halted),
          (TaskState.halted,    TaskState.fulfilled),
          (TaskState.fulfilled, TaskState.fulfilled)
        ].contains (t.state, u.state)

def consistent_promise_state_edge_rule_admissible (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    match b.promises.find? (·.id == p.id) with
    | none   => true
    | some q =>
        (p.state == q.state)
          || (p.state == .pending
                && (q.state == .rejectedTimedout || (q.state == .resolved && p.isTimer)))

def ruleChecks : List (String × (ServerState → ServerState → Bool)) :=
  [ ("consistent_task_state_edge_rule_admissible",    consistent_task_state_edge_rule_admissible),
    ("consistent_promise_state_edge_rule_admissible", consistent_promise_state_edge_rule_admissible) ]

def ruleFailures (a b : ServerState) : List String :=
  ruleChecks.filterMap fun (n, f) => if f a b then none else some n

def ruleWellFormed (a b : ServerState) : Bool := (ruleFailures a b).isEmpty

def stepChecks : List (String × (ServerState → ServerState → Bool)) :=
  [ ("preserved_promise_birth_fields_immutable",  preserved_promise_birth_fields_immutable),
    ("preserved_settled_promise_record",          preserved_settled_promise_record),
    ("monotone_promise_set_grows",                monotone_promise_set_grows),
    ("monotone_task_set_grows",                   monotone_task_set_grows),
    ("preserved_task_version_increments_only_on_acquisition",
       preserved_task_version_increments_only_on_acquisition),
    ("preserved_fulfilled_task",                  preserved_fulfilled_task),
    ("preserved_promise_state_frozen_once_settled", preserved_promise_state_frozen_once_settled),
    ("preserved_promise_settlement_is_one_way",   preserved_promise_settlement_is_one_way),
    ("consistent_promise_settled_at_moves_with_state", consistent_promise_settled_at_moves_with_state),
    ("preserved_promise_value_until_settlement",  preserved_promise_value_until_settlement),
    ("preserved_promise_no_duplicate_ids",        preserved_promise_no_duplicate_ids),
    ("monotone_promise_callbacks_append_one_while_pending", monotone_promise_callbacks_append_one_while_pending),
    ("monotone_promise_callbacks_drain_one_once_settled",   monotone_promise_callbacks_drain_one_once_settled),
    ("monotone_promise_listeners_append_one_while_pending", monotone_promise_listeners_append_one_while_pending),
    ("monotone_promise_listeners_drain_one_once_settled",   monotone_promise_listeners_drain_one_once_settled),
    ("consistent_promise_state_edge_admissible",  consistent_promise_state_edge_admissible),
    ("consistent_task_state_edge_admissible",     consistent_task_state_edge_admissible),
    ("preserved_task_acquisition_only_from_pending", preserved_task_acquisition_only_from_pending),
    ("preserved_task_suspension_only_from_acquired", preserved_task_suspension_only_from_acquired),
    ("preserved_task_halted_only_reenters_via_pending", preserved_task_halted_only_reenters_via_pending),
    ("consistent_settlement_fulfils_task",        consistent_settlement_fulfils_task),
    ("consistent_task_fulfilment_needs_settlement", consistent_task_fulfilment_needs_settlement),
    ("consistent_obligation_discharge_requires_settled", consistent_obligation_discharge_requires_settled),
    ("consistent_callback_consumption_resumes_awaiter", consistent_callback_consumption_resumes_awaiter),
    ("consistent_listener_consumption_enqueues_unblock", consistent_listener_consumption_enqueues_unblock),
    ("consistent_wake_follows_callback_consumption", consistent_wake_follows_callback_consumption),
    ("consistent_suspension_registers_callback",  consistent_suspension_registers_callback),
    ("consistent_callback_additions_share_one_awaiter", consistent_callback_additions_share_one_awaiter),
    ("consistent_at_most_one_obligation_discharged", consistent_at_most_one_obligation_discharged),
    ("consistent_at_most_one_task_acquired",      consistent_at_most_one_task_acquired),
    ("consistent_task_birth_couples_promise_birth", consistent_task_birth_couples_promise_birth),
    ("monotone_outbox_keys_never_disappear",      monotone_outbox_keys_never_disappear),
    ("consistent_new_execute_matches_task_and_target", consistent_new_execute_matches_task_and_target),
    ("consistent_new_unblock_carries_stored_record", consistent_new_unblock_carries_stored_record),
    ("consistent_new_unblock_discharges_its_listener", consistent_new_unblock_discharges_its_listener),
    ("preserved_schedule_birth_fields_immutable", preserved_schedule_birth_fields_immutable),
    ("consistent_schedule_change_is_single",      consistent_schedule_change_is_single),
    ("consistent_schedule_removal_is_isolated",   consistent_schedule_removal_is_isolated),
    ("consistent_task_birth_state",               consistent_task_birth_state),
    ("consistent_task_lease_released_atomically", consistent_task_lease_released_atomically),
    ("preserved_task_lease_holder_stable",        preserved_task_lease_holder_stable),
    ("consistent_task_lease_fields_move_together", consistent_task_lease_fields_move_together),
    ("consistent_task_resumes_buffer_or_clear",   consistent_task_resumes_buffer_or_clear),
    ("consistent_task_resumes_cleared_only_on_dispatch_or_park",
       consistent_task_resumes_cleared_only_on_dispatch_or_park) ]

/-- The settlement dichotomy: a promise leaving `pending` did so either
    by a client verdict stamped at `now`, strictly before the deadline
    and never `rejectedTimedout`; or by its deadline, stamped AT the
    deadline, verdict fixed by the timer tag, value untouched. -/
def consistent_promise_settlement_stamp (now : Nat) (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    p.state != .pending ||
      (match b.promises.find? (·.id == p.id) with
       | none => false
       | some q =>
           q.state == .pending
             || (q.settledAt == some now && now < q.timeoutAt
                   && q.state != .rejectedTimedout)
             || (q.settledAt == some q.timeoutAt && q.timeoutAt ≤ now
                   && (if q.isTimer then q.state == .resolved
                       else q.state == .rejectedTimedout)
                   && q.value.data == p.value.data
                   && q.value.headers == p.value.headers))

/-- `rejectedTimedout` is server-owned: a client can never forge it,
    and it is stamped at the deadline, never at the wall clock. -/
def preserved_timedout_is_server_owned (now : Nat) (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    p.state != .pending
      || (match b.promises.find? (·.id == p.id) with
          | none   => true
          | some q => q.state != .rejectedTimedout
                        || (p.timeoutAt ≤ now && q.settledAt == some p.timeoutAt))

/-- A promise that appears in a step is born clean: no obligations, no
    value, in the past, and in exactly one of the two birth shapes. -/
def consistent_new_promise_born_clean (now : Nat) (a b : ServerState) : Bool :=
  b.promises.all fun q =>
    a.promises.any (·.id == q.id)
      || (q.callbacks.isEmpty && q.listeners.isEmpty
          && q.value.data.isNone && q.value.headers.isEmpty
          && q.createdAt ≤ now
          && ((q.state == .pending && q.settledAt.isNone && q.createdAt < q.timeoutAt)
              || (q.settledAt == some q.timeoutAt && q.createdAt == q.timeoutAt
                  && q.timeoutAt ≤ now
                  && (if q.isTimer then q.state == .resolved
                      else q.state == .rejectedTimedout))))

def stepClockChecks : List (String × (Nat → ServerState → ServerState → Bool)) :=
  [ ("preserved_no_dead_dispatch",            preserved_no_dead_dispatch),
    ("preserved_execute_only_for_live_task",  preserved_execute_only_for_live_task),
    ("consistent_promise_settlement_stamp",   consistent_promise_settlement_stamp),
    ("preserved_timedout_is_server_owned",    preserved_timedout_is_server_owned),
    ("consistent_new_promise_born_clean",     consistent_new_promise_born_clean),
    ("consistent_task_acquisition_is_atomic", consistent_task_acquisition_is_atomic),
    ("consistent_task_lease_deadline_is_now_plus_ttl", consistent_task_lease_deadline_is_now_plus_ttl),
    ("consistent_task_pending_entry_arms_retry", consistent_task_pending_entry_arms_retry),
    ("consistent_task_retry_rearm_only_when_due", consistent_task_retry_rearm_only_when_due),
    ("consistent_task_wake_replaces_resumes",  consistent_task_wake_replaces_resumes) ]

def stepFailures (now : Nat) (a b : ServerState) : List String :=
  (stepChecks.filterMap fun (n, f) => if f a b then none else some n)
    ++ (stepClockChecks.filterMap fun (n, f) => if f now a b then none else some n)

def stepWellFormed (now : Nat) (a b : ServerState) : Bool :=
  (stepFailures now a b).isEmpty

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
