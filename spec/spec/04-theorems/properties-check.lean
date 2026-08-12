import «04-theorems».«abstract-twins»
import «02-abstract».«properties»

namespace Abstraction

open AbstractModel.Properties
open Equivalence (extTags tgtTags)

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

def statesOfA (handle : AStep → Nat → AbstractModel.H Response) :
    List (AStep × Nat) → AbstractModel.ServerState → List (Nat × AbstractModel.ServerState)
  | [], _ => []
  | (st, n) :: w, s =>
      let (_, s') := Id.run ((handle st n).run s)
      (n, s') :: statesOfA handle w s'

def trace (w : List (AStep × Nat)) : List (Nat × AbstractModel.ServerState) :=
  statesOfA handleA w AbstractModel.ServerState.init
    ++ statesOfA handleAP w AbstractModel.ServerState.init

def wellFormedRun (w : List (AStep × Nat)) : Bool :=
  (trace w).all (fun (n, s) => well_formed n s)

/-- Which entries fail, by name — the shape a counterexample report
    needs, and the reason the catalogue carries its names as data. -/
def report (ws : List (List (AStep × Nat))) : List String :=
  (ws.flatMap fun w => (trace w).flatMap (fun (n, s) => failures n s)).eraseDups

def witnesses (ws : List (List (AStep × Nat))) (p : AbstractModel.ServerState → Bool) : Bool :=
  ws.any fun w => (trace w).any (fun (_, s) => p s)

/-! ### Closing the alphabet's blind spots -/

def covInternal : List (AStep × Nat) :=
  [ (.api (.promiseCreate { id := "i", timeoutAt := 1000, param := {}, tags := [] }), 100),
    (.api (.promiseRegisterListener { awaited := "i", address := "https://l" }), 110),
    (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } }), 120),
    (.api (.promiseRegisterCallback { awaited := "i", awaiter := "x" }), 130) ]

def covListeners : List (AStep × Nat) :=
  [ (.api (.promiseCreate { id := "a", timeoutAt := 1000, param := {}, tags := extTags }), 100),
    (.api (.promiseRegisterListener { awaited := "a", address := "https://l1" }), 110),
    (.api (.promiseRegisterListener { awaited := "a", address := "https://l2" }), 120),
    (.api (.promiseSettle { id := "a", state := .resolved, value := {} }), 200),
    (.r3 "a" "https://l1", 210),
    (.r3 "a" "https://l2", 220) ]

def covTwoTasks : List (AStep × Nat) :=
  [ (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := "x", timeoutAt := 5000, param := {}, tags := tgtTags } }), 100),
    (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := "y", timeoutAt := 5000, param := {}, tags := tgtTags } }), 110),
    (.api (.taskRelease { id := "x", version := 1 }), 120),
    (.api (.taskRelease { id := "y", version := 1 }), 130),
    (.r6 "x" 9000, 140),
    (.r6 "y" 9000, 150) ]

/-- `b2` halts a task whose own promise has already timed out, so the
    halt 409s and no `.halted` state is ever reached. Halting a LIVE
    task is what exercises `well_formed_task_halted_is_cleared`. -/
def covHalt : List (AStep × Nat) :=
  [ (.api (.taskCreate { pid := "p0", ttl := 100, action := { id := "h", timeoutAt := 5000, param := {}, tags := tgtTags } }), 100),
    (.api (.taskHalt { id := "h" }), 110),
    (.api (.taskContinue { id := "h" }), 120) ]

def battery : List (List (AStep × Nat)) :=
  [wLag, b1, b2, b3, b4, b5, covInternal, covListeners, covTwoTasks, covHalt]

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

theorem stage1_battery : battery.all wellFormedRun = true := by decide

/-- Every script up to length 3 over the adversarial alphabet — 1 464
    scripts, both disciplines, every intermediate state. -/
theorem stage1_sweep :
    ((seqsUpToA kernelsResp 3).map instantiateA).all wellFormedRun = true := by decide

/-! ### Falsifiability

One hand-built violator per entry. `mutants` pairs the entry's name
with the verdict its violator receives; every verdict must be `false`. -/

open ServerModel AbstractModel.Properties in
def mutants : List (String × Bool) :=
  let P : AbstractModel.PromiseObject :=
    { id := "a", state := .pending, param := {}, tags := [("resonate:external","true")],
      timeoutAt := 100, createdAt := 10 }
  let T : AbstractModel.TaskObject := { id := "a", state := .pending, version := 1, retryAt := some 0 }
  let C : Schedule := { id := "c", cron := "*", promiseId := "p", promiseTimeout := 1,
                        promiseParam := {}, promiseTags := [], nextRunAt := 50, createdAt := 10 }
  [ ("well_formed_promise_created_at_lte_timeout_at",
       well_formed_promise_created_at_lte_timeout_at { P with createdAt := 500 }),
    ("well_formed_promise_settled_at_lte_timeout_at",
       well_formed_promise_settled_at_lte_timeout_at { P with settledAt := some 500 }),
    ("well_formed_promise_created_at_lte_settled_at",
       well_formed_promise_created_at_lte_settled_at { P with settledAt := some 5 }),
    ("well_formed_promise_settled_at_iff_not_pending/settled_at_while_pending",
       well_formed_promise_settled_at_iff_not_pending { P with settledAt := some 20 }),
    ("well_formed_promise_settled_at_iff_not_pending/no_settled_at_when_settled",
       well_formed_promise_settled_at_iff_not_pending { P with state := .resolved }),
    ("well_formed_promise_pending_has_no_value",
       well_formed_promise_pending_has_no_value { P with value := { data := some "x" } }),
    ("well_formed_promise_timer_not_targeted",
       well_formed_promise_timer_not_targeted { P with tags := [("resonate:timer","true"), ("resonate:target","w")] }),
    ("well_formed_promise_callbacks_unique",
       well_formed_promise_callbacks_unique { P with callbacks := ["x","x"] }),
    ("well_formed_promise_listeners_unique",
       well_formed_promise_listeners_unique { P with listeners := ["u","u"] }),
    ("well_formed_promise_obligations_require_external",
       well_formed_promise_obligations_require_external { P with tags := [], callbacks := ["x"] }),
    ("well_formed_promise_awaiter_is_not_self",
       well_formed_promise_awaiter_is_not_self { P with callbacks := ["a"] }),
    ("well_formed_promise_created_at_lte_now",
       well_formed_promise_created_at_lte_now 5 P),
    ("well_formed_promise_settled_at_lte_now",
       well_formed_promise_settled_at_lte_now 5 { P with settledAt := some 20 }),
    ("well_formed_task_acquired_iff_has_pid",
       well_formed_task_acquired_iff_has_pid { T with state := .acquired, ttl := some 1, expiresAt := some 1, retryAt := none }),
    ("well_formed_task_acquired_iff_has_ttl",
       well_formed_task_acquired_iff_has_ttl { T with state := .acquired, pid := some "w", expiresAt := some 1, retryAt := none }),
    ("well_formed_task_acquired_iff_has_expires_at",
       well_formed_task_acquired_iff_has_expires_at { T with state := .acquired, pid := some "w", ttl := some 1, retryAt := none }),
    ("well_formed_task_pending_iff_has_retry_at",
       well_formed_task_pending_iff_has_retry_at { T with retryAt := none }),
    ("well_formed_task_fulfilled_is_cleared",
       well_formed_task_fulfilled_is_cleared { T with state := .fulfilled, pid := some "w" }),
    ("well_formed_task_suspended_is_cleared",
       well_formed_task_suspended_is_cleared { T with state := .suspended, pid := some "w" }),
    ("well_formed_task_halted_is_cleared",
       well_formed_task_halted_is_cleared { T with state := .halted, pid := some "w" }),
    ("well_formed_task_suspended_has_no_resumes",
       well_formed_task_suspended_has_no_resumes { T with state := .suspended, resumes := ["b"] }),
    ("well_formed_task_resumes_unique",
       well_formed_task_resumes_unique { T with resumes := ["b","b"] }),
    ("well_formed_schedule_promise_tags_not_timer_targeted",
       well_formed_schedule_promise_tags_not_timer_targeted { C with promiseTags := [("resonate:timer","true"), ("resonate:target","w")] }),
    ("well_formed_schedule_created_at_lte_next_run_at",
       well_formed_schedule_created_at_lte_next_run_at { C with nextRunAt := 1 }),
    ("well_formed_schedule_created_at_lte_last_run_at",
       well_formed_schedule_created_at_lte_last_run_at { C with lastRunAt := some 1 }),
    ("well_formed_schedule_last_run_at_lt_next_run_at",
       well_formed_schedule_last_run_at_lt_next_run_at { C with lastRunAt := some 90 }),
    ("well_formed_store_promise_ids_unique",
       well_formed_store_promise_ids_unique { promises := [P, P] }),
    ("well_formed_store_task_ids_unique",
       well_formed_store_task_ids_unique { tasks := [T, T] }),
    ("well_formed_store_schedule_ids_unique",
       well_formed_store_schedule_ids_unique { schedules := [C, C] }),
    ("well_formed_store_outbox_keys_unique",
       well_formed_store_outbox_keys_unique { outbox := [{ address := "w", message := .execute "a" 1 }, { address := "w", message := .execute "a" 2 }] }) ]

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

/-- The four `well_formed_schedule_*` entries are exercised by NOTHING:
    no script can create a schedule that runs, because `nextCron` and
    `occurrences` are `opaque` with no value. They are in the catalogue
    because they are true of the protocol, not because anything here
    checks them. -/
theorem schedules_unreached :
    witnesses battery (fun s => !s.schedules.isEmpty) = false := by decide

end Abstraction
