import «04-theorems».«properties-check»

namespace Abstraction

/-!  # Stage 3 — transition properties

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

open AbstractModel (ServerState PromiseObject TaskObject)

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

def stepChecks : List (String × (ServerState → ServerState → Bool)) :=
  [ ("preserved_promise_birth_fields_immutable",  preserved_promise_birth_fields_immutable),
    ("preserved_settled_promise_record",          preserved_settled_promise_record),
    ("monotone_settled_promise_obligations_shrink", monotone_settled_promise_obligations_shrink),
    ("monotone_promise_set_grows",                monotone_promise_set_grows),
    ("monotone_task_set_grows",                   monotone_task_set_grows),
    ("preserved_task_version_increments_only_on_acquisition",
       preserved_task_version_increments_only_on_acquisition),
    ("preserved_fulfilled_task",                  preserved_fulfilled_task) ]

def stepClockChecks : List (String × (Nat → ServerState → ServerState → Bool)) :=
  [ ("preserved_no_dead_dispatch",            preserved_no_dead_dispatch),
    ("preserved_execute_only_for_live_task",  preserved_execute_only_for_live_task) ]

def stepFailures (now : Nat) (a b : ServerState) : List String :=
  (stepChecks.filterMap fun (n, f) => if f a b then none else some n)
    ++ (stepClockChecks.filterMap fun (n, f) => if f now a b then none else some n)

def stepWellFormed (now : Nat) (a b : ServerState) : Bool :=
  (stepFailures now a b).isEmpty

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
  [ ("preserved_promise_birth_fields_immutable",
       preserved_promise_birth_fields_immutable { promises := [P] } { promises := [{ P with timeoutAt := 9999 }] }),
    ("preserved_settled_promise_record/state_moved",
       preserved_settled_promise_record { promises := [S] } { promises := [{ S with state := .rejected }] }),
    ("preserved_settled_promise_record/settled_at_restamped",
       preserved_settled_promise_record { promises := [S] } { promises := [{ S with settledAt := some 90 }] }),
    ("preserved_settled_promise_record/value_rewritten",
       preserved_settled_promise_record { promises := [S] } { promises := [{ S with value := { data := some "x" } }] }),
    ("monotone_settled_promise_obligations_shrink",
       monotone_settled_promise_obligations_shrink { promises := [S] } { promises := [{ S with listeners := ["https://l", "https://m"] }] }),
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
