import «02-abstract».«state»

/-!  # The conformance catalogue

Every property an implementation of the Resonate protocol must satisfy,
stated once, named, and evaluable. This file is meant to be readable on
its own: if you are porting the protocol, you should be able to work
from here without reading the machine it was derived from.

## How to use it

There are exactly two ways to check a property, and `Property` says which one a
property needs:

  `.state f`   check `f now s` at every state your server passes
               through. A violation is a bad row or a bad join — the
               store is in a shape the protocol does not admit.

  `.trans f`   check `f now a b` at every pair of consecutive states,
               where `b` is the state your server reached from `a` in
               ONE step. A violation is a bad transaction — each state
               is fine on its own and the move between them is not.

`failures now s` and `stepFailures now a b` run the whole catalogue and
return the NAMES of the properties that broke. Report the name: it is the same
string in Lean, Go, TypeScript and Verus, so a violation means the same
thing everywhere.

Both walks take `now`. Most properties ignore it. The ones that do not are the
ones that are only true relative to a clock reading — those are worth
knowing about, because two of them were originally written without it
and were wrong. `created_at ≤ settled_at`, for instance, is NOT a
property of a single state: nothing stops a server creating a promise at
instant 500 and settling it at instant 100. What is step-local is
`created_at ≤ now` and `settled_at ≤ now`, from which the chain follows
as long as your clock does not run backwards.

## Reading a name

    well_formed_<subject>_<claim>   one object, one state
    consistent_<subject>_<claim>    several objects, one state
    preserved_<subject>_<claim>     a field or record that must not move
    monotone_<subject>_<claim>      something that may move one way only

The predicate above each catalogue entry is the portable statement. The
`Property` wrapper is dispatch, nothing more — in Go every `.state` property is a
`func(s *ServerState) bool` and every `.trans` property is a
`func(a, b *ServerState) bool`, and the lifting (`s.promises.all …`)
becomes a loop.

## What is NOT here

  * **Provenance properties.** Two properties restrict what a BACKGROUND job may do
    on its own initiative — it may not acquire, suspend, halt or
    continue a task, and it may settle a promise only by its deadline.
    They cannot be `.state` or `.trans`, because the two states do not
    say what caused the move. They are at the bottom of this file, with
    their own walk. Your implementation knows the cause and can check
    them directly; we can only check them against the model.
  * **Response properties.** Status codes and response bodies need the
    request, which neither walk carries.
  * **Liveness.** `04-theorems/liveness.lean`. Those quantify over
    infinite traces and are not decidable, so they are `Prop`s with no
    executable form — plus a bounded shadow that can refute them.
  * **Algebraic properties** — create is idempotent, settle absorbs, halt then
    continue equals release. Those are equations between programs, not
    predicates over states.

## The door checks

A request that fails validation gets 400 and writes nothing, so you might
conclude the malformed shapes are unreachable and need no property. That
is exactly backwards: the 400 is the *reason* the shape is impossible, and
a property that names the shape is what catches an implementation that
forgot the check. The door check and the property are the same claim
stated at two places — one refuses the write, one refuses the row — and
the second is the one a conformance suite can run against a server whose
door you did not write.

Every 400 in the machine, and the property that shadows it:

    promise.create      timer + target        well_formed_promise_timer_not_targeted
    promise.settle      state = pending       well_formed_promise_settled_at_iff_not_pending
    promise.settle      state = timedout      well_formed_promise_timedout_is_server_owned
    promise.callback    awaited = awaiter     well_formed_promise_awaiter_is_not_self
    promise.listener    undeliverable addr    consistent_listener_addresses_deliverable
    task.create         untargeted / timer    consistent_task_iff_targeted_promise
    task.suspend        no actions            consistent_suspended_task_holds_rung
    task.suspend        self-await            well_formed_promise_awaiter_is_not_self
    task.suspend        duplicate awaited     well_formed_promise_callbacks_unique
    task.fulfill        state not settable    well_formed_promise_timedout_is_server_owned
                                              well_formed_promise_settled_at_iff_not_pending
    schedule.create     timer + target        well_formed_schedule_promise_tags_not_timer_targeted

    task.fence          action targets self   — no shadow, see below

Eleven of the twelve leave a row a property can name. `task.fence`'s
self-target check does not, and the reason is worth stating rather than
hiding: a fence bypass writes a state that is *legal in every respect*.
The task is acquired, its promise is pending, the action settles that
promise, and the coupled write fulfils the task — the identical state a
plain `task.fulfill` would have produced. What the check protects is
authority, not shape: a task may not use its own fence to act on the
promise it is executing. Authority is provenance, and provenance is not
visible in a state or in a pair of states, so this one belongs to the
`internalChecks` family at the bottom of the file in spirit, and to your
own implementation's tests in practice.

## Plausible and false

Nine claims that read like properties of this protocol and are not. Each
was proposed, checked, and refuted; they are recorded because the
refutation is the useful part.

  * *a settled promise is immutable* — the internal steps keep removing
    callbacks and listeners from settled promises. What is frozen is
    `toRecord`, which is exactly what a response can carry.
  * *a pending promise has `timeoutAt > now`* — false by design. Reads
    project, so a past-deadline promise sits stored-pending until
    something touches it.
  * *settling twice is an error* — it absorbs, with 200.
  * *a late settle fails* — it returns **200** carrying
    `rejectedTimedout`. A client that reads only the status concludes
    its value was stored.
  * *firing any internal step is unobservable* — true of the promise
    timeout, the listener drain and the retry dispatch; false of the
    callback drain and the lease timeout, both visible through
    `task.get`.
  * *`task.acquire` is idempotent* — it is exclusive. The second gets
    409 and the version has moved.
  * *advancing the clock is neutral* — only for already-settled objects.
  * *`promise.create` commutes with a clock advance* — `createdAt` is
    `now` on the live path and `timeoutAt` on the born-dead path.
  * *exclusivity holds per task* — it holds per **version**. With
    `ttl = 0` a second acquisition legally succeeds, at the next
    version.


## Scope

Stated against the COALESCED machine (`02-abstract`), where deadlines
live on the objects and obligations are drained by internal steps. A
server built like `03-concrete` — separate timer tables, a deferred
queue — must satisfy these plus the representation properties in that
directory. Where the two machines disagree, the coalesced statement is
the protocol and the concrete one is an implementation choice.

## Evidence

The properties hold over every script of length ≤ 3 over an adversarial
alphabet, under both read disciplines, at every state and every
consecutive pair — `04-theorems/properties-check.lean` and
`properties-step.lean`. Those files also carry the two checks that
matter more than passing: every property rejects a hand-built violator, and
the corpus actually reaches the states that make each guard bite. A property
that cannot fail, or whose guard nothing satisfies, is not being
checked. -/

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

/-- `rejectedTimedout` is server-owned. `PromiseState.settable` refuses
    it at every door a client can settle through, so a stored promise in
    that state was written by its own deadline and carries that deadline
    as its settlement instant. Forget the `settable` check in
    `promise.settle` or `task.fulfill` and a client can name the state
    directly; the row it leaves behind is settled at `now`, not at
    `timeoutAt`, and this rejects it.

    The converse — deadline instant implies the deadline verdict — is
    `well_formed_promise_deadline_verdict_matches_timer_tag`. Neither
    implies the other: that one reads a promise settled AT its deadline
    and pins the verdict; this one reads the timeout verdict and pins the
    instant. -/
def well_formed_promise_timedout_is_server_owned (p : PromiseObject) : Bool :=
  p.state != .rejectedTimedout || p.settledAt == some p.timeoutAt

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
the listener and callback internal steps keep removing obligations from settled
promises, which is how a wake is discharged. What is frozen is exactly
`toRecord` — exactly the part a response can carry — so "a settled
promise never changes again" is true on the wire and false in the store.

`monotone_task_version_increases_only_on_acquisition` is the fencing
property. Version moves by exactly one, only on `pending → acquired`, and by
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

def monotone_promise_set_grows (a b : ServerState) : Bool :=
  a.promises.all fun p => b.promises.any (·.id == p.id)

def monotone_task_set_grows (a b : ServerState) : Bool :=
  a.tasks.all fun t => b.tasks.any (·.id == t.id)

/-- The fencing property: a task's version rises by exactly one on
    `pending → acquired`, and does not move on any other transition. -/
def monotone_task_version_increases_only_on_acquisition (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none => true
    | some u =>
        if t.state == .pending && u.state == .acquired then
          t.version < u.version
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
properties split on the POST-state, not the pre-state: `processCallback`
materializes a deadline AND drains a callback in the same step, so a
promise pending before and settled after loses one. -/

/-- `xs ⊆ ys`. The obligation properties are stated as containment, not
    as a step count: an implementation may register or drain a whole
    ledger in one transaction, and doing so is not a violation. What is
    forbidden is movement in the wrong DIRECTION. -/
def subsetOf (xs ys : List String) : Bool := xs.all ys.contains

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

def monotone_promise_callbacks_grow_while_pending (a b : ServerState) : Bool :=
  b.promises.all fun q =>
    q.state != .pending ||
      (match a.promises.find? (·.id == q.id) with
       | none => q.callbacks.isEmpty
       | some p => subsetOf p.callbacks q.callbacks)

def monotone_promise_callbacks_shrink_once_settled (a b : ServerState) : Bool :=
  b.promises.all fun q =>
    q.state == .pending ||
      (match a.promises.find? (·.id == q.id) with
       | none => q.callbacks.isEmpty
       | some p => subsetOf q.callbacks p.callbacks)

def monotone_promise_listeners_grow_while_pending (a b : ServerState) : Bool :=
  b.promises.all fun q =>
    q.state != .pending ||
      (match a.promises.find? (·.id == q.id) with
       | none => q.listeners.isEmpty
       | some p => subsetOf p.listeners q.listeners)

def monotone_promise_listeners_shrink_once_settled (a b : ServerState) : Bool :=
  b.promises.all fun q =>
    q.state == .pending ||
      (match a.promises.find? (·.id == q.id) with
       | none => q.listeners.isEmpty
       | some p => subsetOf q.listeners p.listeners)

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
                 && u.resumes.contains p.id)

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
                || (u.state == .acquired && 1 ≤ u.version)
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

Only the properties that hold unconditionally. The ordering properties
(`nextRunAt` never regresses, the run marks advance together) are true
of the protocol but not of the model: they need axioms on `nextCron`
and `occurrences`, which are `opaque` with no value. They are not
carried here, because a property that passes only because nothing reaches it
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

def consistent_task_birth_state (a b : ServerState) : Bool :=
  b.tasks.all fun u =>
    (a.tasks.any (·.id == u.id))
    || (u.state == .pending && u.retryAt.isSome
          && u.pid.isNone && u.ttl.isNone && u.expiresAt.isNone && u.resumes.isEmpty)
    || (u.state == .fulfilled && u.retryAt.isNone
          && u.pid.isNone && u.ttl.isNone && u.expiresAt.isNone && u.resumes.isEmpty)
    || (u.state == .acquired && 1 ≤ u.version && u.retryAt.isNone
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

def monotone_task_resumes_grow_or_clear (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none => true
    | some u => u.resumes.isEmpty || subsetOf t.resumes u.resumes

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
        || (t.state == .pending && t.version < u.version
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

def consistent_task_wake_records_resume (now : Nat) (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    match b.tasks.find? (·.id == t.id) with
    | none => true
    | some u =>
        !(t.state == .suspended && u.state == .pending)
        || (!u.resumes.isEmpty && u.retryAt == some now && u.version == t.version)

/-! ## The sweeper properties

Applied to INTERNAL steps only, and false on request steps — which is
what makes them strictly stronger than the general edge tables. A
background sweeper may re-pend, fulfil, resume-from-suspended or
refresh; it may never acquire, suspend, halt or continue a task, and it
may settle a promise only by its deadline. A server whose reaper does
any of the rest steals a lease or invents a verdict nobody asked for. -/

def consistent_task_state_edge_internal_admissible (a b : ServerState) : Bool :=
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

def consistent_promise_state_edge_internal_admissible (a b : ServerState) : Bool :=
  a.promises.all fun p =>
    match b.promises.find? (·.id == p.id) with
    | none   => true
    | some q =>
        (p.state == q.state)
          || (p.state == .pending
                && (q.state == .rejectedTimedout || (q.state == .resolved && p.isTimer)))

def internalChecks : List (String × (ServerState → ServerState → Bool)) :=
  [ ("consistent_task_state_edge_internal_admissible",    consistent_task_state_edge_internal_admissible),
    ("consistent_promise_state_edge_internal_admissible", consistent_promise_state_edge_internal_admissible) ]

def internalFailures (a b : ServerState) : List String :=
  internalChecks.filterMap fun (n, f) => if f a b then none else some n

def internalWellFormed (a b : ServerState) : Bool := (internalFailures a b).isEmpty

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

/-! # The catalogue

Every property, in one list, each with the name an implementation should use
when it reports a violation. `Property` says which walk the property needs and
nothing else; the predicates above say what it means. -/

inductive Property where
  /-- Check at every state the implementation passes through. -/
  | state (f : Nat → ServerState → Bool)
  /-- Check at every pair of consecutive states — one step. -/
  | trans (f : Nat → ServerState → ServerState → Bool)

structure Named where
  name : String
  property : Property

def catalogue : List Named :=
  [ { name := "well_formed_promise_created_at_lte_timeout_at"
      , property := .state (fun _ s => s.promises.all well_formed_promise_created_at_lte_timeout_at) },
    { name := "well_formed_promise_pending_created_before_deadline"
      , property := .state (fun _ s => s.promises.all well_formed_promise_pending_created_before_deadline) },
    { name := "well_formed_promise_settled_at_lte_timeout_at"
      , property := .state (fun _ s => s.promises.all well_formed_promise_settled_at_lte_timeout_at) },
    { name := "well_formed_promise_created_at_lte_settled_at"
      , property := .state (fun _ s => s.promises.all well_formed_promise_created_at_lte_settled_at) },
    { name := "well_formed_promise_settled_at_iff_not_pending"
      , property := .state (fun _ s => s.promises.all well_formed_promise_settled_at_iff_not_pending) },
    { name := "well_formed_promise_pending_has_no_value"
      , property := .state (fun _ s => s.promises.all well_formed_promise_pending_has_no_value) },
    { name := "well_formed_promise_deadline_verdict_matches_timer_tag"
      , property := .state (fun _ s => s.promises.all well_formed_promise_deadline_verdict_matches_timer_tag) },
    { name := "well_formed_promise_deadline_settlement_has_no_value"
      , property := .state (fun _ s => s.promises.all well_formed_promise_deadline_settlement_has_no_value) },
    { name := "well_formed_promise_timer_not_targeted"
      , property := .state (fun _ s => s.promises.all well_formed_promise_timer_not_targeted) },
    { name := "well_formed_promise_timedout_is_server_owned"
      , property := .state (fun _ s => s.promises.all well_formed_promise_timedout_is_server_owned) },
    { name := "well_formed_promise_callbacks_unique"
      , property := .state (fun _ s => s.promises.all well_formed_promise_callbacks_unique) },
    { name := "well_formed_promise_listeners_unique"
      , property := .state (fun _ s => s.promises.all well_formed_promise_listeners_unique) },
    { name := "well_formed_promise_obligations_require_external"
      , property := .state (fun _ s => s.promises.all well_formed_promise_obligations_require_external) },
    { name := "well_formed_promise_awaiter_is_not_self"
      , property := .state (fun _ s => s.promises.all well_formed_promise_awaiter_is_not_self) },
    { name := "well_formed_promise_created_at_lte_now"
      , property := .state (fun now s => s.promises.all (well_formed_promise_created_at_lte_now now)) },
    { name := "well_formed_promise_settled_at_lte_now"
      , property := .state (fun now s => s.promises.all (well_formed_promise_settled_at_lte_now now)) },
    { name := "well_formed_task_acquired_iff_has_pid"
      , property := .state (fun _ s => s.tasks.all well_formed_task_acquired_iff_has_pid) },
    { name := "well_formed_task_acquired_iff_has_ttl"
      , property := .state (fun _ s => s.tasks.all well_formed_task_acquired_iff_has_ttl) },
    { name := "well_formed_task_acquired_iff_has_expires_at"
      , property := .state (fun _ s => s.tasks.all well_formed_task_acquired_iff_has_expires_at) },
    { name := "well_formed_task_pending_iff_has_retry_at"
      , property := .state (fun _ s => s.tasks.all well_formed_task_pending_iff_has_retry_at) },
    { name := "well_formed_task_fulfilled_is_cleared"
      , property := .state (fun _ s => s.tasks.all well_formed_task_fulfilled_is_cleared) },
    { name := "well_formed_task_suspended_is_cleared"
      , property := .state (fun _ s => s.tasks.all well_formed_task_suspended_is_cleared) },
    { name := "well_formed_task_halted_is_cleared"
      , property := .state (fun _ s => s.tasks.all well_formed_task_halted_is_cleared) },
    { name := "well_formed_task_suspended_has_no_resumes"
      , property := .state (fun _ s => s.tasks.all well_formed_task_suspended_has_no_resumes) },
    { name := "well_formed_task_resumes_unique"
      , property := .state (fun _ s => s.tasks.all well_formed_task_resumes_unique) },
    { name := "well_formed_task_acquired_version_positive"
      , property := .state (fun _ s => s.tasks.all well_formed_task_acquired_version_positive) },
    { name := "well_formed_schedule_promise_tags_not_timer_targeted"
      , property := .state (fun _ s => s.schedules.all well_formed_schedule_promise_tags_not_timer_targeted) },
    { name := "well_formed_schedule_created_at_lte_next_run_at"
      , property := .state (fun _ s => s.schedules.all well_formed_schedule_created_at_lte_next_run_at) },
    { name := "well_formed_schedule_created_at_lte_last_run_at"
      , property := .state (fun _ s => s.schedules.all well_formed_schedule_created_at_lte_last_run_at) },
    { name := "well_formed_schedule_last_run_at_lt_next_run_at"
      , property := .state (fun _ s => s.schedules.all well_formed_schedule_last_run_at_lt_next_run_at) },
    { name := "well_formed_store_promise_ids_unique"
      , property := .state (fun _ s => well_formed_store_promise_ids_unique s) },
    { name := "well_formed_store_task_ids_unique"
      , property := .state (fun _ s => well_formed_store_task_ids_unique s) },
    { name := "well_formed_store_schedule_ids_unique"
      , property := .state (fun _ s => well_formed_store_schedule_ids_unique s) },
    { name := "well_formed_store_outbox_keys_unique"
      , property := .state (fun _ s => well_formed_store_outbox_keys_unique s) },
    { name := "consistent_task_iff_targeted_promise"
      , property := .state (fun _ s => consistent_task_iff_targeted_promise s) },
    { name := "consistent_settled_promise_has_fulfilled_task"
      , property := .state (fun _ s => consistent_settled_promise_has_fulfilled_task s) },
    { name := "consistent_callback_awaiter_is_targeted"
      , property := .state (fun _ s => consistent_callback_awaiter_is_targeted s) },
    { name := "consistent_listener_addresses_deliverable"
      , property := .state (fun _ s => consistent_listener_addresses_deliverable s) },
    { name := "consistent_outbox_execute_names_existing_task"
      , property := .state (fun _ s => consistent_outbox_execute_names_existing_task s) },
    { name := "consistent_outbox_never_ahead"
      , property := .state (fun _ s => consistent_outbox_never_ahead s) },
    { name := "consistent_outbox_execute_address_is_target_tag"
      , property := .state (fun _ s => consistent_outbox_execute_address_is_target_tag s) },
    { name := "consistent_outbox_unblock_names_settled_promise"
      , property := .state (fun _ s => consistent_outbox_unblock_names_settled_promise s) },
    { name := "consistent_outbox_unblock_address_deliverable"
      , property := .state (fun _ s => consistent_outbox_unblock_address_deliverable s) },
    { name := "consistent_settled_task_promise_settled"
      , property := .state (fun _ s => consistent_settled_task_promise_settled s) },
    { name := "consistent_suspended_task_holds_rung"
      , property := .state (consistent_suspended_task_holds_rung) },
    { name := "preserved_promise_birth_fields_immutable"
      , property := .trans (fun _ a b => preserved_promise_birth_fields_immutable a b) },
    { name := "preserved_settled_promise_record"
      , property := .trans (fun _ a b => preserved_settled_promise_record a b) },
    { name := "monotone_promise_set_grows"
      , property := .trans (fun _ a b => monotone_promise_set_grows a b) },
    { name := "monotone_task_set_grows"
      , property := .trans (fun _ a b => monotone_task_set_grows a b) },
    { name := "monotone_task_version_increases_only_on_acquisition"
      , property := .trans (fun _ a b => monotone_task_version_increases_only_on_acquisition a b) },
    { name := "preserved_fulfilled_task"
      , property := .trans (fun _ a b => preserved_fulfilled_task a b) },
    { name := "preserved_promise_state_frozen_once_settled"
      , property := .trans (fun _ a b => preserved_promise_state_frozen_once_settled a b) },
    { name := "preserved_promise_settlement_is_one_way"
      , property := .trans (fun _ a b => preserved_promise_settlement_is_one_way a b) },
    { name := "consistent_promise_settled_at_moves_with_state"
      , property := .trans (fun _ a b => consistent_promise_settled_at_moves_with_state a b) },
    { name := "preserved_promise_value_until_settlement"
      , property := .trans (fun _ a b => preserved_promise_value_until_settlement a b) },
    { name := "preserved_promise_no_duplicate_ids"
      , property := .trans (fun _ a b => preserved_promise_no_duplicate_ids a b) },
    { name := "monotone_promise_callbacks_grow_while_pending"
      , property := .trans (fun _ a b => monotone_promise_callbacks_grow_while_pending a b) },
    { name := "monotone_promise_callbacks_shrink_once_settled"
      , property := .trans (fun _ a b => monotone_promise_callbacks_shrink_once_settled a b) },
    { name := "monotone_promise_listeners_grow_while_pending"
      , property := .trans (fun _ a b => monotone_promise_listeners_grow_while_pending a b) },
    { name := "monotone_promise_listeners_shrink_once_settled"
      , property := .trans (fun _ a b => monotone_promise_listeners_shrink_once_settled a b) },
    { name := "consistent_promise_state_edge_admissible"
      , property := .trans (fun _ a b => consistent_promise_state_edge_admissible a b) },
    { name := "consistent_task_state_edge_admissible"
      , property := .trans (fun _ a b => consistent_task_state_edge_admissible a b) },
    { name := "preserved_task_acquisition_only_from_pending"
      , property := .trans (fun _ a b => preserved_task_acquisition_only_from_pending a b) },
    { name := "preserved_task_suspension_only_from_acquired"
      , property := .trans (fun _ a b => preserved_task_suspension_only_from_acquired a b) },
    { name := "preserved_task_halted_only_reenters_via_pending"
      , property := .trans (fun _ a b => preserved_task_halted_only_reenters_via_pending a b) },
    { name := "consistent_settlement_fulfils_task"
      , property := .trans (fun _ a b => consistent_settlement_fulfils_task a b) },
    { name := "consistent_task_fulfilment_needs_settlement"
      , property := .trans (fun _ a b => consistent_task_fulfilment_needs_settlement a b) },
    { name := "consistent_obligation_discharge_requires_settled"
      , property := .trans (fun _ a b => consistent_obligation_discharge_requires_settled a b) },
    { name := "consistent_callback_consumption_resumes_awaiter"
      , property := .trans (fun _ a b => consistent_callback_consumption_resumes_awaiter a b) },
    { name := "consistent_listener_consumption_enqueues_unblock"
      , property := .trans (fun _ a b => consistent_listener_consumption_enqueues_unblock a b) },
    { name := "consistent_wake_follows_callback_consumption"
      , property := .trans (fun _ a b => consistent_wake_follows_callback_consumption a b) },
    { name := "consistent_suspension_registers_callback"
      , property := .trans (fun _ a b => consistent_suspension_registers_callback a b) },
    { name := "consistent_task_birth_couples_promise_birth"
      , property := .trans (fun _ a b => consistent_task_birth_couples_promise_birth a b) },
    { name := "monotone_outbox_keys_never_disappear"
      , property := .trans (fun _ a b => monotone_outbox_keys_never_disappear a b) },
    { name := "consistent_new_execute_matches_task_and_target"
      , property := .trans (fun _ a b => consistent_new_execute_matches_task_and_target a b) },
    { name := "consistent_new_unblock_carries_stored_record"
      , property := .trans (fun _ a b => consistent_new_unblock_carries_stored_record a b) },
    { name := "consistent_new_unblock_discharges_its_listener"
      , property := .trans (fun _ a b => consistent_new_unblock_discharges_its_listener a b) },
    { name := "preserved_schedule_birth_fields_immutable"
      , property := .trans (fun _ a b => preserved_schedule_birth_fields_immutable a b) },
    { name := "consistent_task_birth_state"
      , property := .trans (fun _ a b => consistent_task_birth_state a b) },
    { name := "consistent_task_lease_released_atomically"
      , property := .trans (fun _ a b => consistent_task_lease_released_atomically a b) },
    { name := "preserved_task_lease_holder_stable"
      , property := .trans (fun _ a b => preserved_task_lease_holder_stable a b) },
    { name := "consistent_task_lease_fields_move_together"
      , property := .trans (fun _ a b => consistent_task_lease_fields_move_together a b) },
    { name := "monotone_task_resumes_grow_or_clear"
      , property := .trans (fun _ a b => monotone_task_resumes_grow_or_clear a b) },
    { name := "consistent_task_resumes_cleared_only_on_dispatch_or_park"
      , property := .trans (fun _ a b => consistent_task_resumes_cleared_only_on_dispatch_or_park a b) },
    { name := "preserved_no_dead_dispatch"
      , property := .trans (preserved_no_dead_dispatch) },
    { name := "preserved_execute_only_for_live_task"
      , property := .trans (preserved_execute_only_for_live_task) },
    { name := "consistent_promise_settlement_stamp"
      , property := .trans (consistent_promise_settlement_stamp) },
    { name := "preserved_timedout_is_server_owned"
      , property := .trans (preserved_timedout_is_server_owned) },
    { name := "consistent_new_promise_born_clean"
      , property := .trans (consistent_new_promise_born_clean) },
    { name := "consistent_task_acquisition_is_atomic"
      , property := .trans (consistent_task_acquisition_is_atomic) },
    { name := "consistent_task_lease_deadline_is_now_plus_ttl"
      , property := .trans (consistent_task_lease_deadline_is_now_plus_ttl) },
    { name := "consistent_task_pending_entry_arms_retry"
      , property := .trans (consistent_task_pending_entry_arms_retry) },
    { name := "consistent_task_retry_rearm_only_when_due"
      , property := .trans (consistent_task_retry_rearm_only_when_due) },
    { name := "consistent_task_wake_records_resume"
      , property := .trans (consistent_task_wake_records_resume) } ]

/-! ### The two walks

`failures` for an implementation that can inspect a state; `stepFailures`
for one that can inspect a state transition. Both report by NAME, so a
violation names the property it broke. -/

def failures (now : Nat) (s : ServerState) : List String :=
  catalogue.filterMap fun l =>
    match l.property with
    | .state f => if f now s then none else some l.name
    | .trans _ => none

def well_formed (now : Nat) (s : ServerState) : Bool :=
  (failures now s).isEmpty

def stepFailures (now : Nat) (a b : ServerState) : List String :=
  catalogue.filterMap fun l =>
    match l.property with
    | .state _ => none
    | .trans f => if f now a b then none else some l.name

def stepWellFormed (now : Nat) (a b : ServerState) : Bool :=
  (stepFailures now a b).isEmpty

def stateCount : Nat := (catalogue.filter (fun l => match l.property with | .state _ => true | _ => false)).length
def transCount : Nat := (catalogue.filter (fun l => match l.property with | .trans _ => true | _ => false)).length

/-! ## Known gaps

Four constraints that are true of the PROTOCOL and false of this
machine. They are not in `catalogue`, because a reachable state
violates each — putting them there would turn the sweep red. They are
here because an implementation should enforce them at its doors even
though the specification does not, and because a gap recorded in Lean
with a witness is worth more than a gap recorded in prose.

They carry the same `Property` as the catalogue, so an implementation
that closes a gap moves the entry up into `catalogue` unchanged and the
walks pick it up with no other edit.

A gap needs a witness for the opposite reason a property does. A
property must be shown FALSIFIABLE — that a violator exists and is
rejected. A gap must be shown REACHED — that this machine actually
produces a state the constraint forbids. A gap with no witness is
either already enforced, or a predicate reporting on itself: the
target gap below was first written with `addressValid`, which every
ordinary `resonate:target = "w1"` fails, and it "witnessed" a defect
that was entirely in the predicate.

`04-theorems/properties-check.lean` and `properties-step.lean` carry
the witnesses; each is `= true` on the NEGATED constraint, so a gap
that gets closed turns red and says so. -/

/-- A lease of length zero expires at the instant it is granted.
    `task.create`/`task.acquire` accept `ttl = 0` and there is no lower
    bound anywhere. -/
def well_formed_task_ttl_positive (s : ServerState) : Bool :=
  s.tasks.all fun t => t.state != .acquired || 0 < t.ttl.getD 0

/-- `Tags.has` is `isSome`, so `("resonate:target", "")` carries a
    target that is the empty string. The dispatch sites fall back to
    `getD ""`, so the task is created and its `execute` is enqueued to
    an address nothing can receive. `addressValid` guards listener
    addresses only.

    Non-emptiness is the whole claim, and deliberately so: a target
    names a WORKER GROUP, not a URL — `resonate:target = "w1"` is the
    ordinary case throughout the corpus — so `addressValid`, which
    demands an `http`/`https`/`poll` scheme, is the wrong predicate here
    and would reject every well-formed targeted promise. -/
def well_formed_promise_target_is_nonempty (s : ServerState) : Bool :=
  s.promises.all fun p =>
    match p.tags.get? "resonate:target" with
    | none      => true
    | some addr => !addr.isEmpty

/-- A first dispatch scheduled at or after the promise's own deadline
    can never fire: by the time it comes due the promise has timed out
    and the dispatch guard refuses. The promise is born pending, is
    never executed, and dies `rejectedTimedout`. Nothing relates
    `resonate:delay` to `timeoutAt`, and `parseNat` is total, so a
    malformed delay becomes a garbage instant with the same effect. -/
def well_formed_promise_delay_before_deadline (s : ServerState) : Bool :=
  s.promises.all fun p =>
    match p.tags.get? "resonate:delay" with
    | none   => true
    | some d => parseNat d < p.timeoutAt

/-- A re-arm must move the deadline FORWARD. `processRetryTimeout` takes
    the next instant as a parameter and imposes no lower bound, so
    re-arming a due task to an instant already past leaves the retry
    step enabled at the same instant it just fired — the dispatcher
    re-dispatches forever and the outbox grows without bound. Note the
    shape: the defect is in the MOVE, so no state predicate can see it.
    `consistent_task_retry_rearm_only_when_due` constrains WHEN you may
    re-arm; nothing constrains what you may re-arm TO. -/
def monotone_task_retry_rearm_advances (now : Nat) (a b : ServerState) : Bool :=
  a.tasks.all fun t =>
    t.state != .pending ||
      (match b.tasks.find? (·.id == t.id) with
       | none   => true
       | some u => u.state != .pending || u.retryAt == t.retryAt
                     || (match u.retryAt with
                         | some d => now < d
                         | none   => false))

def gaps : List Named :=
  [ { name := "well_formed_task_ttl_positive"
      , property := .state (fun _ s => well_formed_task_ttl_positive s) },
    { name := "well_formed_promise_target_is_nonempty"
      , property := .state (fun _ s => well_formed_promise_target_is_nonempty s) },
    { name := "well_formed_promise_delay_before_deadline"
      , property := .state (fun _ s => well_formed_promise_delay_before_deadline s) },
    { name := "monotone_task_retry_rearm_advances"
      , property := .trans (monotone_task_retry_rearm_advances) } ]

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
