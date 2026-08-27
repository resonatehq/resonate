import «04-theorems».«corpus»
import «02-abstract».«properties»

namespace Abstraction

open AbstractModel.Properties


/-!  # Stage 1, evaluated

The single-object catalogue (`02-abstract/properties.lean`) run over
every state every script passes through, under both read disciplines.
Each state is checked at the instant of the step that produced it — the
two `..._lte_now` entries are about stored timestamps being in the past,
so they need one.

Three checks, not one, because passing is the weakest of the three:

  1. **`stage1_sweep` / `stage1_battery`** — nothing in the corpus
     violates the catalogue.
  2. **`stage1_all_falsifiable`** — every entry REJECTS a hand-built
     violator. A predicate typo'd into a tautology passes (1) forever.
  3. **the reach theorems** — the corpus actually reaches the states
     that make each guarded entry bite. A property whose guard is never
     satisfied is not being checked; it is being ignored.

(3) is what caught the alphabet's blind spots: `kernelsResp` registers
no listener, creates no internal promise, never holds two tasks and
never queues two messages, so five guards were dead. Four are closed by
`coverage` below. The fifth — schedules — is not reachable at all,
because `nextCron` and `occurrences` are `opaque` with no value, so all
four `well_formed_schedule_*` entries are carried unexercised. Same
reason `scheduleTimeoutsMirror` is unexercised in
`invariant-check.lean`, and the same reason the Go checker rejects
traces mentioning schedules. -/

def statesOfA (mat : Bool) :
    List (Step × Nat) → AbstractModel.ServerState → List (Nat × AbstractModel.ServerState)
  | [], _ => []
  | (st, n) :: w, s =>
      let (_, s') := stepOf mat st n s
      (n, s') :: statesOfA mat w s'

/-- Both readings of the one machine. This used to concatenate two
    DRIVERS, `handleA` and `handleAP`; it now concatenates two values of
    `mat` through the same one. -/
def trace (w : List (Step × Nat)) : List (Nat × AbstractModel.ServerState) :=
  statesOfA true w AbstractModel.ServerState.init
    ++ statesOfA false w AbstractModel.ServerState.init

/-- Consecutive pairs, which is what the fold takes. -/
def pairs (mat : Bool) :
    List (Step × Nat) → AbstractModel.ServerState →
    List (Nat × AbstractModel.ServerState × AbstractModel.ServerState)
  | [], _ => []
  | (st, n) :: w, s =>
      let (_, s') := stepOf mat st n s
      (n, s, s') :: pairs mat w s'

def allPairs (w : List (Step × Nat)) :=
  pairs true w AbstractModel.ServerState.init
    ++ pairs false w AbstractModel.ServerState.init

/-- `Legal`, evaluated on a finite run: its body at every index, which
    is what `legalAt` is — plus the terminal state's `.state` half,
    which an infinite trace would have checked at the next index and a
    finite one has to close by hand. The second conjunct is the price of
    the first being `Legal`'s body and not something stronger. -/
def legalRun (w : List (Step × Nat)) : Bool :=
  (allPairs w).all (fun (n, a, b) => legalAt n a b)
    && (allPairs w).all (fun (n, _, b) => stateHolds n b)

/-- Which entries fail, by name. A HARNESS concern: the specification
    says what `legalAt` is, and collecting the names of what broke is
    for whoever is debugging a red sweep. The catalogue carries the
    names as data, which is what makes this three lines here rather
    than a facility the specification has to provide. -/
def failingNames (now : Nat) (a b : AbstractModel.ServerState) : List String :=
  AbstractModel.Properties.catalogue.filterMap fun l =>
    match l.property with
    | .state f => if f now a && f now b then none else some l.name  -- both, for the report
    | .trans f => if f now a b          then none else some l.name

def report (ws : List (List (Step × Nat))) : List String :=
  (ws.flatMap fun w => (allPairs w).flatMap (fun (n, a, b) => failingNames n a b)).eraseDups

def witnesses (ws : List (List (Step × Nat))) (p : AbstractModel.ServerState → Bool) : Bool :=
  ws.any fun w => (trace w).any (fun (_, s) => p s)

/-! ### Closing the alphabet's blind spots -/

def covInternal : List (Step × Nat) :=
  [ (.api (.promiseCreate { id := oid "i", timeoutAt := 1000, param := {}, tags := [] }), 100),
    (.api (.promiseRegisterListener { awaited := oid "i", address := "https://l" }), 110),
    (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := oid "x", timeoutAt := 2000, param := {}, tags := tgtTags } }), 120),
    (.api (.promiseRegisterCallback { awaited := oid "i", awaiter := oid "x" }), 130) ]

def covListeners : List (Step × Nat) :=
  [ (.api (.promiseCreate { id := oid "a", timeoutAt := 1000, param := {}, tags := extTags }), 100),
    (.api (.promiseRegisterListener { awaited := oid "a", address := "https://l1" }), 110),
    (.api (.promiseRegisterListener { awaited := oid "a", address := "https://l2" }), 120),
    (.api (.promiseSettle { id := oid "a", state := .resolved, value := {} }), 200),
    (.listener (oid "a") "https://l1", 210),
    (.listener (oid "a") "https://l2", 220) ]

def covTwoTasks : List (Step × Nat) :=
  [ (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := oid "x", timeoutAt := 5000, param := {}, tags := tgtTags } }), 100),
    (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := oid "y", timeoutAt := 5000, param := {}, tags := tgtTags } }), 110),
    (.api (.taskRelease { id := oid "x", version := 1 }), 120),
    (.api (.taskRelease { id := oid "y", version := 1 }), 130),
    (.taskRetryTimeout (oid "x"), 140),
    (.taskRetryTimeout (oid "y"), 150) ]

/-- `b2` halts a task whose own promise has already timed out, so the
    halt 409s and no `.halted` state is ever reached. Halting a LIVE
    task is what exercises `well_formed_task_halted_is_cleared`. -/
def covHalt : List (Step × Nat) :=
  [ (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := oid "h", timeoutAt := 5000, param := {}, tags := tgtTags } }), 100),
    (.api (.taskHalt { id := oid "h" }), 110),
    (.api (.taskContinue { id := oid "h" }), 120) ]

def battery : List (List (Step × Nat)) :=
  [wLag, b1, b2, b3, b4, b5, b6, covInternal, covListeners, covTwoTasks, covHalt]

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

theorem stage1_battery : battery.all legalRun = true := by decide

/-- Every script up to length 3 over the adversarial alphabet — 1 464
    scripts, both disciplines, every intermediate state. -/
theorem stage1_sweep :
    ((seqsUpToA kernelsResp 3).map instantiateA).all legalRun = true := by decide

/-! ### Falsifiability

One hand-built violator per entry. `mutants` pairs the entry's name
with the verdict its violator receives; every verdict must be `false`.

A property is a function of the whole state, so a violator is a state —
even when the defect is one bad row. These three build the smallest
state that holds one object, which is what most of the violators below
want. -/

/-- A task no longer stands alone, so `oneTask` supplies the promise it
    is part of — a targeted one, since that is what the catalogue says
    carries a task. Which promise does not matter to the task entries
    below; that it must be SOME promise is the fusion showing up here. -/
def carrier : AbstractModel.PromiseObject :=
  { state := .pending, param := {}, tags := [("resonate:target","w")],
    timeoutAt := 100, createdAt := 10 }

def onePromise (p : AbstractModel.PromiseObject) : AbstractModel.ServerState :=
  { objects := [{ id := oid "a", promise := p }] }

def oneTask (t : AbstractModel.TaskObject) : AbstractModel.ServerState :=
  { objects := [{ id := oid "a", promise := carrier, task := some t }] }

def oneSchedule (c : ServerModel.Schedule) : AbstractModel.ServerState :=
  { schedules := [c] }

open ServerModel AbstractModel.Properties in
def mutants : List (String × Bool) :=
  let P : AbstractModel.PromiseObject :=
    { state := .pending, param := {}, tags := [("resonate:external","true")],
      timeoutAt := 100, createdAt := 10 }
  let T : AbstractModel.TaskObject := { state := .pending, version := 1, retryAt := some 0 }
  let obj : AbstractModel.PromiseObject → Option AbstractModel.TaskObject →
              AbstractModel.ServerState :=
    fun p t => { objects := [{ id := oid "a", promise := p, task := t }] }
  let C : Schedule := { id := oid "c", cron := "*", promiseId := oid "p", promiseTimeout := 1,
                        promiseParam := {}, promiseTags := [], nextRunAt := 50, createdAt := 10 }
  [ ("well_formed_promise_created_at_lte_timeout_at",
       well_formed_promise_created_at_lte_timeout_at 0 (onePromise { P with createdAt := 500 })),
    ("well_formed_promise_settled_at_lte_timeout_at",
       well_formed_promise_settled_at_lte_timeout_at 0 (onePromise { P with settledAt := some 500 })),
    ("well_formed_promise_created_at_lte_settled_at",
       well_formed_promise_created_at_lte_settled_at 0 (onePromise { P with settledAt := some 5 })),
    ("well_formed_promise_settled_at_iff_not_pending/settled_at_while_pending",
       well_formed_promise_settled_at_iff_not_pending 0 (onePromise { P with settledAt := some 20 })),
    ("well_formed_promise_settled_at_iff_not_pending/no_settled_at_when_settled",
       well_formed_promise_settled_at_iff_not_pending 0 (onePromise { P with state := .resolved })),
    ("well_formed_promise_pending_has_no_value",
       well_formed_promise_pending_has_no_value 0 (onePromise { P with value := { data := some "x" } })),
    ("well_formed_promise_timer_not_targeted",
       well_formed_promise_timer_not_targeted 0 (onePromise { P with tags := [("resonate:timer","true"), ("resonate:target","w")] })),
    -- the forged verdict: a client names `rejectedTimedout` and the row
    -- is stamped at the wall clock instead of at the deadline.
    ("well_formed_promise_timedout_is_server_owned",
       well_formed_promise_timedout_is_server_owned 0 (onePromise { P with state := .rejectedTimedout, settledAt := some 50 })),
    ("well_formed_promise_callbacks_unique",
       well_formed_promise_callbacks_unique 0 (onePromise { P with callbacks := [oid "x", oid "x"] })),
    ("well_formed_promise_listeners_unique",
       well_formed_promise_listeners_unique 0 (onePromise { P with listeners := ["u","u"] })),
    ("well_formed_promise_obligations_require_external",
       well_formed_promise_obligations_require_external 0 (onePromise { P with tags := [], callbacks := [oid "x"] })),
    ("well_formed_promise_awaiter_is_not_self",
       well_formed_promise_awaiter_is_not_self 0 (onePromise { P with callbacks := [oid "a"] })),
    ("well_formed_promise_created_at_lte_now",
       well_formed_promise_created_at_lte_now 5 (onePromise P)),
    ("well_formed_promise_settled_at_lte_now",
       well_formed_promise_settled_at_lte_now 5 (onePromise { P with settledAt := some 20 })),
    ("well_formed_task_acquired_iff_has_pid",
       well_formed_task_acquired_iff_has_pid 0 (oneTask { T with state := .acquired, ttl := some 1, expiresAt := some 1, retryAt := none })),
    ("well_formed_task_acquired_iff_has_ttl",
       well_formed_task_acquired_iff_has_ttl 0 (oneTask { T with state := .acquired, pid := some "w", expiresAt := some 1, retryAt := none })),
    ("well_formed_task_acquired_iff_has_expires_at",
       well_formed_task_acquired_iff_has_expires_at 0 (oneTask { T with state := .acquired, pid := some "w", ttl := some 1, retryAt := none })),
    ("well_formed_task_pending_iff_has_retry_at",
       well_formed_task_pending_iff_has_retry_at 0 (oneTask { T with retryAt := none })),
    ("well_formed_task_fulfilled_is_cleared",
       well_formed_task_fulfilled_is_cleared 0 (oneTask { T with state := .fulfilled, pid := some "w" })),
    ("well_formed_task_suspended_is_cleared",
       well_formed_task_suspended_is_cleared 0 (oneTask { T with state := .suspended, pid := some "w" })),
    ("well_formed_task_halted_is_cleared",
       well_formed_task_halted_is_cleared 0 (oneTask { T with state := .halted, pid := some "w" })),
    ("well_formed_task_suspended_has_no_resumes",
       well_formed_task_suspended_has_no_resumes 0 (oneTask { T with state := .suspended, resumes := [oid "b"] })),
    ("well_formed_task_resumes_unique",
       well_formed_task_resumes_unique 0 (oneTask { T with resumes := [oid "b", oid "b"] })),
    ("well_formed_schedule_promise_tags_not_timer_targeted",
       well_formed_schedule_promise_tags_not_timer_targeted 0 (oneSchedule { C with promiseTags := [("resonate:timer","true"), ("resonate:target","w")] })),
    ("well_formed_schedule_created_at_lte_next_run_at",
       well_formed_schedule_created_at_lte_next_run_at 0 (oneSchedule { C with nextRunAt := 1 })),
    ("well_formed_schedule_created_at_lte_last_run_at",
       well_formed_schedule_created_at_lte_last_run_at 0 (oneSchedule { C with lastRunAt := some 1 })),
    ("well_formed_schedule_last_run_at_lt_next_run_at",
       well_formed_schedule_last_run_at_lt_next_run_at 0 (oneSchedule { C with lastRunAt := some 90 })),
    ("well_formed_promise_pending_created_before_deadline",
       well_formed_promise_pending_created_before_deadline 0 (onePromise { P with createdAt := 100 })),
    ("well_formed_promise_deadline_verdict_matches_timer_tag",
       well_formed_promise_deadline_verdict_matches_timer_tag 0 (onePromise { P with tags := [("resonate:timer","true")], state := .rejectedTimedout, settledAt := some 100 })),
    ("well_formed_promise_deadline_settlement_has_no_value",
       well_formed_promise_deadline_settlement_has_no_value 0 (onePromise { P with state := .rejectedTimedout, settledAt := some 100, value := { data := some "boom" } })),
    ("well_formed_task_acquired_version_positive",
       well_formed_task_acquired_version_positive 0 (oneTask { T with state := .acquired, version := 0, pid := some "w", ttl := some 1, expiresAt := some 1, retryAt := none })),
    -- Both halves of the entry still have a violator to name. Fusing
    -- the row stopped the MACHINE from writing them; it did not stop the
    -- catalogue from saying they are wrong, which is the whole reason
    -- the entry survived the fusion.
    ("consistent_task_iff_targeted_promise/task_without_target",
       consistent_task_iff_targeted_promise 0 (obj P (some T))),
    ("consistent_task_iff_targeted_promise/target_without_task",
       consistent_task_iff_targeted_promise 0
         (obj { P with tags := [("resonate:target","w")] } none)),
    ("consistent_settled_promise_has_fulfilled_task",
       consistent_settled_promise_has_fulfilled_task 0
         (obj { P with state := .resolved, settledAt := some 20 } (some T))),
    ("consistent_callback_awaiter_is_targeted",
       consistent_callback_awaiter_is_targeted 0 (obj { P with callbacks := [oid "z"] } none)),
    ("consistent_listener_addresses_deliverable",
       consistent_listener_addresses_deliverable 0
         (obj { P with listeners := ["not-an-address"] } none)),
    ("consistent_outbox_execute_names_existing_task",
       consistent_outbox_execute_names_existing_task 0 { outbox := [{ address := "w", message := .execute (oid "ghost") 0 }] }),
    ("consistent_outbox_never_ahead",
       consistent_outbox_never_ahead 0
         { objects := [{ id := oid "a", promise := carrier, task := some T }],
           outbox := [{ address := "w", message := .execute (oid "a") 9 }] }),
    ("consistent_outbox_execute_address_is_target_tag",
       consistent_outbox_execute_address_is_target_tag 0
         { objects := [{ id := oid "a", promise := { P with tags := [("resonate:target","w")] } }],
           outbox := [{ address := "wrong", message := .execute (oid "a") 0 }] }),
    ("consistent_outbox_unblock_names_settled_promise",
       consistent_outbox_unblock_names_settled_promise 0
         { objects := [{ id := oid "a", promise := P }],
           outbox := [{ address := "https://l", message := .unblock (P.toRecord (oid "a")) }] }),
    ("consistent_outbox_unblock_address_deliverable",
       consistent_outbox_unblock_address_deliverable 0
         { objects := [{ id := oid "a", promise := { P with state := .resolved, settledAt := some 20 } }],
           outbox := [{ address := "nope",
                        message := .unblock
                          (({ P with state := .resolved, settledAt := some 20 }
                              : AbstractModel.PromiseObject).toRecord (oid "a")) }] }),
    ("consistent_settled_task_promise_settled",
       consistent_settled_task_promise_settled 0
         (obj P (some { T with state := .fulfilled, retryAt := none }))),
    ("consistent_suspended_task_holds_rung",
       consistent_suspended_task_holds_rung 50
         (obj P (some { T with state := .suspended, retryAt := none }))),
    ("well_formed_store_object_ids_unique",
       well_formed_store_object_ids_unique 0
         { objects := [{ id := oid "a", promise := P }, { id := oid "a", promise := P }] }),
    ("well_formed_store_schedule_ids_unique",
       well_formed_store_schedule_ids_unique 0 { schedules := [C, C] }),
    ("well_formed_store_outbox_keys_unique",
       well_formed_store_outbox_keys_unique 0 { outbox := [{ address := "w", message := .execute (oid "a") 1 }, { address := "w", message := .execute (oid "a") 2 }] }) ]

/-- Every catalogue entry rejects its violator. A property that cannot
    fail is not a property. -/
theorem stage1_all_falsifiable : mutants.all (fun (_, b) => !b) = true := by decide

/-! ### Reach

The guard of each guarded entry, witnessed. Pinned as theorems rather
than claimed in a comment, and one of them cannot be pinned at all. -/

theorem reaches_settled_promise :
    witnesses battery (fun s => s.promises.any (·.settledAt.isSome)) = true := by decide

theorem reaches_timer_promise :
    witnesses battery (fun s => s.promises.any (·.isTimer)) = true := by decide

theorem reaches_internal_promise :
    witnesses battery (fun s => s.promises.any (fun p => !p.external)) = true := by decide

theorem reaches_callbacks :
    witnesses battery (fun s => s.promises.any (fun p => !p.callbacks.isEmpty)) = true := by decide

theorem reaches_listeners :
    witnesses battery (fun s => s.promises.any (fun p => !p.listeners.isEmpty)) = true := by decide

theorem reaches_two_promises :
    witnesses battery (fun s => s.promises.length ≥ 2) = true := by decide

theorem reaches_task_pending :
    witnesses battery (fun s => s.tasks.any (·.state == .pending)) = true := by decide

theorem reaches_task_acquired :
    witnesses battery (fun s => s.tasks.any (·.state == .acquired)) = true := by decide

theorem reaches_task_suspended :
    witnesses battery (fun s => s.tasks.any (·.state == .suspended)) = true := by decide

theorem reaches_task_halted :
    witnesses battery (fun s => s.tasks.any (·.state == .halted)) = true := by decide

theorem reaches_task_fulfilled :
    witnesses battery (fun s => s.tasks.any (·.state == .fulfilled)) = true := by decide

theorem reaches_task_resumes :
    witnesses battery (fun s => s.tasks.any (fun t => !t.resumes.isEmpty)) = true := by decide

theorem reaches_two_tasks :
    witnesses battery (fun s => s.tasks.length ≥ 2) = true := by decide

theorem reaches_two_outbox_entries :
    witnesses battery (fun s => s.outbox.length ≥ 2) = true := by decide


/-- The guard of `well_formed_promise_timedout_is_server_owned`. Without
    this the property is vacuous: it says nothing about a corpus that
    never produces the state it constrains. -/
theorem reaches_timedout_promise :
    witnesses battery (fun s => s.promises.any (·.state == .rejectedTimedout)) = true := by decide

theorem reaches_deadline_settlement :
    witnesses battery (fun s => s.promises.any (fun p => p.settledAt == some p.timeoutAt)) = true := by decide

theorem reaches_outbox_execute :
    witnesses battery (fun s => s.outbox.any (fun e =>
      match e.message with | .execute _ _ => true | .unblock _ => false)) = true := by decide

theorem reaches_outbox_unblock :
    witnesses battery (fun s => s.outbox.any (fun e =>
      match e.message with | .unblock _ => true | .execute _ _ => false)) = true := by decide

/-- The four `well_formed_schedule_*` entries are exercised by NOTHING:
    no script can create a schedule that runs, because `nextCron` and
    `occurrences` are `opaque` with no value. They are in the catalogue
    because they are true of the protocol, not because anything here
    checks them. -/
theorem schedules_unreached :
    witnesses battery (fun s => !s.schedules.isEmpty) = false := by decide

/-! ### The known gaps, witnessed

Each of these is true of the protocol and false of the machine. The
theorems are `= false`: they assert that a reachable state VIOLATES the
constraint, so if the machine is ever fixed these go red and say so. -/

open AbstractModel.Properties (well_formed_task_ttl_positive
  well_formed_promise_target_is_nonempty
  well_formed_promise_delay_before_deadline)

/-- `ttl = 0` is accepted: the task is acquired with `expiresAt = now`,
    a lease already expired at the instant it was granted. -/
def wGapTtlZero : List (Step × Nat) :=
  [ (.api (.taskCreate { pid := "p", ttl := 0, action := { id := oid "x", timeoutAt := 9000, param := {}, tags := tgtTags } }), 100) ]

theorem gap_task_ttl_positive_is_violable :
    (trace wGapTtlZero).any (fun (n, s) => !well_formed_task_ttl_positive n s) = true := by decide

/-- `("resonate:target","")` passes `Tags.has`, so a task is created and
    its dispatch is enqueued to the empty address. -/
def emptyTargetTags : ServerModel.Tags := [("resonate:target", "")]

def wGapEmptyTarget : List (Step × Nat) :=
  [ (.api (.promiseCreate { id := oid "y", timeoutAt := 9000, param := {}, tags := emptyTargetTags }), 100),
    (.taskRetryTimeout (oid "y"), 110) ]

theorem gap_promise_target_is_nonempty_is_violable :
    (trace wGapEmptyTarget).any (fun (n, s) => !well_formed_promise_target_is_nonempty n s) = true := by decide

/-- And the gap is a gap, not an artefact of the predicate: an ORDINARY
    targeted promise satisfies it. `resonate:target = "w1"` names a
    worker group; a predicate that rejected it would be reporting itself,
    not the machine. -/
theorem ordinary_target_is_nonempty :
    (trace covInternal).all (fun (n, s) => well_formed_promise_target_is_nonempty n s) = true := by decide

theorem gap_empty_target_reaches_the_outbox :
    (trace wGapEmptyTarget).any (fun (_, s) => s.outbox.any (·.address == "")) = true := by decide

/-- A delay past the promise's own deadline: the task is born pending
    with `retryAt = 5000` on a promise that dies at 200. It is never
    dispatched, and it is not an error. -/
def lateDelayTags : ServerModel.Tags := [("resonate:target", "w"), ("resonate:delay", "5000")]

def wGapLateDelay : List (Step × Nat) :=
  [ (.api (.promiseCreate { id := oid "z", timeoutAt := 200, param := {}, tags := lateDelayTags }), 100) ]

theorem gap_promise_delay_before_deadline_is_violable :
    (trace wGapLateDelay).any (fun (n, s) => !well_formed_promise_delay_before_deadline n s) = true := by decide

end Abstraction
