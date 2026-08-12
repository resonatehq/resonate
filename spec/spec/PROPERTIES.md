# The property catalogue

What an implementation must satisfy. One name per property, no numbers, so
the same name means the same thing in Lean, Go, TypeScript and Verus.

## Naming

    <stage>_<subject>_<claim>

| prefix | shape | harness |
|---|---|---|
| `well_formed_` | one object, one state | evaluate at every state |
| `consistent_` | many objects, one state | evaluate at every state |
| `preserved_` / `monotone_` | two states linked by one step | evaluate on consecutive pairs |
| `idempotent_` `absorbing_` `exclusive_` `neutral_` `commutes_` | two steps from the SAME state | branching harness — **the repo has none** |
| `responds_` / `frames_` | the response, and what a step must not touch | per-operation postcondition |
| `eventually_` | traces under fairness | **no fairness predicate exists** |

Where `implementation-questions.md` already names a property, the claim half
of the name is that name, so the two vocabularies stay one. The mapping is at
the end.

## The three checks

Passing is the weakest evidence a property can have. Every entry needs:

1. **holds** — no state in the corpus violates it
2. **falsifiable** — a hand-built violator is rejected (a predicate typo'd
   into a tautology passes 1 forever)
3. **reached** — the corpus reaches states where the guard actually bites

`spec/04-theorems/properties-check.lean` does all three for stage 1. Check 3
is what found five dead guards in the sweep alphabet on the first run.

## Scope

Stated against the ABSTRACT machine unless marked `[concrete]`. The abstract
machine has no representation noise, so a property stated there ports to any
storage shape; the `[concrete]` entries are what a server that keeps separate
timer tables must additionally maintain.

⚠ marks a property the machine does **not** currently guarantee.

---

# Stage 1 — one object, one state

## Promise

| name | statement |
|---|---|
| `well_formed_promise_created_at_lte_timeout_at` | `createdAt ≤ timeoutAt` |
| `well_formed_promise_pending_created_before_deadline` | a **pending** promise has `createdAt < timeoutAt`; equality is reserved for promises born dead |
| `well_formed_promise_settled_at_lte_timeout_at` | `settledAt ≤ timeoutAt` when settled |
| `well_formed_promise_created_at_lte_settled_at` | `createdAt ≤ settledAt` when settled — **trace-relative**, not step-local |
| `well_formed_promise_created_at_lte_now` | stored creation instant is in the past |
| `well_formed_promise_settled_at_lte_now` | stored settlement instant is in the past |
| `well_formed_promise_settled_at_iff_not_pending` | `settledAt` is present exactly when the state is terminal |
| `well_formed_promise_pending_has_no_value` | a pending promise carries the empty value |
| `well_formed_promise_deadline_verdict_matches_timer_tag` | `settledAt = timeoutAt` ⟹ `resolved` for a timer, `rejectedTimedout` otherwise |
| `well_formed_promise_deadline_settlement_has_no_value` | a promise settled by its deadline carries the empty value — the value slot belongs to whoever settles, and nobody settled |
| `well_formed_promise_timer_not_targeted` | never both `resonate:timer` and `resonate:target` |
| `well_formed_promise_callbacks_unique` | no duplicate awaiter |
| `well_formed_promise_listeners_unique` | no duplicate address |
| `well_formed_promise_awaiter_is_not_self` | a promise never awaits itself |
| `well_formed_promise_param_and_value_are_opaque` | no control-flow decision reads `param` or `value`; only `tags` is machine-legible |
| `well_formed_promise_record_hides_obligations` | the wire record carries no callback or listener information |
| ⚠ `well_formed_promise_target_address_is_deliverable` | the `resonate:target` value should be routable — **unvalidated**; `("resonate:target","")` passes `Tags.has` and yields `execute` to the empty address |
| ⚠ `well_formed_promise_delay_before_deadline` | `resonate:delay` should precede `timeoutAt` — **unchecked**; a later delay yields a promise born pending that is never dispatched and dies timed out |

## Task

| name | statement |
|---|---|
| `well_formed_task_acquired_iff_has_pid` | acquired ⟺ `pid` present |
| `well_formed_task_acquired_iff_has_ttl` | acquired ⟺ `ttl` present |
| `well_formed_task_acquired_iff_has_expires_at` | acquired ⟺ `expiresAt` present |
| `well_formed_task_pending_iff_has_retry_at` | pending ⟺ `retryAt` present |
| `well_formed_task_fulfilled_is_cleared` | fulfilled ⟹ pid, ttl, expiresAt, retryAt cleared and resumes empty |
| `well_formed_task_suspended_is_cleared` | suspended ⟹ pid, ttl, expiresAt, retryAt cleared |
| `well_formed_task_halted_is_cleared` | halted ⟹ pid, ttl, expiresAt, retryAt cleared |
| `well_formed_task_suspended_has_no_resumes` | suspended ⟹ resumes empty — the fact that made the previous guarantee vacuous |
| `well_formed_task_resumes_unique` | no duplicate resume trigger |
| `well_formed_task_acquired_version_positive` | an acquired task has `version ≥ 1`; version 0 is the never-acquired advertisement |
| `well_formed_task_expires_at_is_acquire_time_plus_ttl` | `expiresAt = now + ttl` on every write; ttl is fixed at acquisition and never renegotiated |
| `well_formed_task_record_hides_deadlines` | the wire record exposes `ttl` and `pid`, never `expiresAt` or `retryAt` |
| `well_formed_task_record_resumes_is_a_count` | the resume identities are never externalized |
| ⚠ `well_formed_task_ttl_positive` | **unchecked**: `ttl = 0` gives `expiresAt = now`, a lease expired at the instant it is granted |

## Schedule and store

| name | statement |
|---|---|
| `well_formed_schedule_promise_tags_not_timer_targeted` | occurrence tags are well formed at the schedule door |
| `well_formed_schedule_created_at_lte_next_run_at` | needs `nextCron_strictly_after` |
| `well_formed_schedule_created_at_lte_last_run_at` | needs `occurrences_in_window` |
| `well_formed_schedule_last_run_at_lt_next_run_at` | needs `nextCron_strictly_after` |
| `well_formed_occurrence_deadline_relative_to_its_occurrence` | an occurrence's deadline is *its own* instant plus the template timeout, not the firing instant — otherwise a backlog replayed after a restart produces different promises |
| `well_formed_store_promise_ids_unique` | |
| `well_formed_store_task_ids_unique` | |
| `well_formed_store_schedule_ids_unique` | |
| `well_formed_store_outbox_keys_unique` | |
| ⚠ `well_formed_task_timeout_kind_valid` `[concrete]` | **unchecked**: `kind` is a bare `Nat`; a `kind := 2` row satisfies every existing conjunct, is dropped by `alpha`, and is never fired or collected |
| ⚠ `well_formed_config_retry_positive` `[concrete]` | **unchecked**: at `retryTimeout = 0` the retry rule re-arms at `now`, is permanently enabled, and starves every other rule. Safety survives; all liveness dies |

---

# Stage 2 — many objects, one state

| name | statement |
|---|---|
| `consistent_task_iff_targeted_promise` | a task exists **iff** a promise with that id exists and carries `resonate:target`. Replaces `coKeyed`, which states only the forward half and drops the tag |
| `consistent_settled_promise_has_fulfilled_task` | a settled promise's co-keyed task is fulfilled — the coupled write |
| `consistent_timer_promise_has_no_task` | *derivable*, see deletions |
| `consistent_callback_awaiter_is_targeted` | every awaiter id names a promise carrying `resonate:target`, hence a task a resume can reach |
| `consistent_obligations_require_external_awaited` | any promise carrying a callback or listener is external. **This is what pays for the deliberately weak timeout mirror**: an obligation can never attach where no discharge path exists. Enforced at three doors, in no invariant |
| `consistent_listener_addresses_deliverable` | every listener address is `http://`, `https://` or `poll://…@group` |
| `consistent_outbox_execute_names_existing_task` | no dangling dispatch. The outbox is the one component with no reference property today |
| `consistent_outbox_execute_address_is_target_tag` | dispatch follows the promise's declared worker group, never the last claimant's pid |
| `consistent_outbox_never_ahead` | every queued `execute` carries a version ≤ the task's current version — the server never dispatches a token it has not issued |
| `consistent_outbox_execute_unique_per_task` | at most one `execute` per task id; the key is the task id alone, so repeated dispatch coalesces to the latest instead of accumulating |
| `consistent_outbox_unblock_carries_settled_record` | the record inside an `unblock` is never pending and its `settledAt` is identical to what `promise.get` answers at that instant and every later one |
| `consistent_wake_rung_at_most_one_stage` | a wake obligation is a callback **or** a deferred entry, never both — settlement moves it, it does not copy it |
| `consistent_suspended_task_holds_rung` | every suspended task with a live own promise holds at least one rung (`noOrphanedSuspension`) |
| `consistent_pending_task_has_armed_dispatch` | every pending task has an armed retry deadline. **This, not the outbox, is the at-least-once mechanism** |
| `consistent_task_version_identifies_lease_epoch` | for one task id, each version value is granted to at most one acquisition |
| `consistent_quiesced_no_pending_promise_past_deadline` | after quiescence no stored promise is pending with a passed deadline |
| `consistent_quiesced_settled_promise_retains_no_obligations` | after quiescence every settled promise's obligations have become messages or wakes |
| ⚠ `consistent_await_graph_is_acyclic` | **not enforced, by design**: only the self-edge is refused. Mutual suspension deadlocks silently until both deadlines pass, and nothing reports it |
| ⚠ `consistent_callback_reflects_a_declared_await` | **not enforced**: no authorization on registration. Any caller can force a wake of any live suspended task — repeatable, and a cost defect rather than a safety one |
| ⚠ `consistent_unblock_has_redelivery_backstop` | **structurally absent**: the listener is removed in the step that enqueues, a settled promise accepts no new listener, and no rule inspects the outbox. A dropped `unblock` is gone permanently. `execute` has the retry timer; `unblock` has nothing |
| `consistent_task_timeout_unique_per_id_kind` `[concrete]` | ← `uniqueTimeouts` |
| `consistent_task_timeout_names_task` `[concrete]` | ← `timeoutsNameTasks`. **Not** implied by the lease/retry disciplines: those quantify over tasks, so an orphan row escapes them |
| `consistent_task_lease_armed_iff_acquired` `[concrete]` | ← `leaseDiscipline` |
| `consistent_task_retry_armed_iff_pending` `[concrete]` | ← `retryDiscipline` |
| `consistent_deferred_moved_not_copied` `[concrete]` | ← `deferredMoved`, first clause |
| `consistent_deferred_names_task` `[concrete]` | ← `deferredMoved`, second clause |
| `consistent_deferred_unique` `[concrete]` | no `(awaited, awaiter)` pair twice — `undefer` removes **all** copies on the first visit, so a duplicate is dequeued once and processed twice |
| `consistent_promise_timeout_mirrors_promise` `[concrete]` | ← `promiseTimeoutsMirror`, armed ⟹ pending |
| `consistent_external_pending_promise_armed` `[concrete]` | ← `promiseTimeoutsMirror`, pending ⟹ armed, external only. The asymmetry is the fact lag |
| `consistent_schedule_timeout_mirrors_schedule` `[concrete]` | ← `scheduleTimeoutsMirror`. **Reached by nothing** — the one conjunct with zero evidence |
| `consistent_timeout_fire_guarded_by_object` `[concrete]` | no timeout transition acts unless the object's own stored deadline is due, independently of the row that enabled it. This is what makes the timer tables a hint rather than an authority, and hence what makes a stale row harmless |
| `consistent_promise_timeout_table_derivable` `[concrete]` | the promise-timeout table is a function of the promises — droppable, rebuildable at startup |
| `consistent_schedule_timeout_table_derivable` `[concrete]` | likewise for schedules |
| `consistent_task_timeout_table_not_derivable` `[concrete]` | the task-timeout table is the **only** record of a lease deadline; `TaskObject` stores `ttl` but no acquisition instant. Rebuilding leases as `now + ttl` at startup silently extends every in-flight lease by the downtime |

---

# Stage 3 — two states, one step

## Immutability and finality

| name | statement |
|---|---|
| `preserved_promise_birth_fields_immutable` | `id`, `param`, `tags`, `timeoutAt`, `createdAt` freeze at birth |
| `preserved_promise_settlement_is_final` | once non-pending, `state`, `value` and `settledAt` never change again. **`callbacks` and `listeners` are NOT frozen** — the rules still drain them. What freezes is exactly `toRecord`, which is exactly what a response can carry |
| `monotone_promise_obligations_shrink_after_settlement` | a callback or listener may be added only while the promise is pending |
| `monotone_promise_set_grows` | no step deletes a promise or a task. A server that evicts settled promises breaks create idempotence and re-runs finished work |
| `preserved_task_id` | |
| `monotone_task_version_never_decreases` | |
| `preserved_task_version_increments_only_on_acquisition` | and then by exactly one |
| `preserved_fulfilled_task` | no step moves a task out of fulfilled |
| `preserved_settlement_instant_independent_of_observation` | a deadline settlement is stamped at the deadline, never at the instant that discovered it. Two replicas sweeping at different times must agree |
| `monotone_clock_never_runs_backwards` | |

## Guards and gating

| name | statement |
|---|---|
| `preserved_no_dead_dispatch` | no rule and no handler re-pends a task or emits work for it once its promise's deadline has passed — every decision routes through the projected promise. *The one line missing from all three real implementations* |
| `preserved_task_read_materializes_before_guarding` | every task handler resolves the promise deadline before evaluating any task guard |
| `preserved_responses_are_projected` | every promise or task record a 200 carries is the **stored** object projected to `now` — never the requested one, never the raw bytes |
| `preserved_stale_version_never_writes` | a version mismatch performs no task write and no fenced side effect |
| `preserved_task_suspend_registers_all_or_none` | the whole awaited list is validated before any registration; a 422 leaves no dangling callback |
| `preserved_task_claim_across_settled_suspend` | the 300 answer leaves the task acquired at the same version with its lease intact, clearing only the resume buffer. It is the one refusal that writes |
| `preserved_execute_emitted_only_for_live_task` | at the instant of emission the named task's promise is projected-pending |
| `preserved_execute_carries_current_version` | the dispatch carries the version at emission time, not an anticipated one |
| `preserved_task_version_across_wake` | waking a suspended task does not bump its version, so the execute it emits carries a usable token |
| `preserved_wake_obligation_across_step` | across any step, a rung persists or the task has left suspension (`wakeConserved`) |
| `preserved_wake_obligation_void_when_awaiter_expired` | TIMEOUT ALWAYS WINS: the drain discards a resume whose awaiter's own promise is dead |
| `preserved_deferred_dequeue_atomic` `[concrete]` | removing a resume from the queue and applying it is one step. A crash between them loses the wake permanently, because the callback rung has already been consumed |
| `preserved_config_immutable` `[concrete]` | nothing writes the config; the refinement reads the retry constant from the initial state, so a runtime-reconfigurable cadence breaks it |
| `preserved_occurrence_promises_under_schedule_delete` | deleting a schedule removes only the schedule; occurrences already created survive |
| `consistent_repending_arms_immediate_retry` | every transition into pending arms the retry deadline at the current instant, so dispatch is enabled at that same instant |

---

# Branching — two steps from one state

**No test in the repo has this shape.** Everything in `04-theorems` compares
two machines along one shared script; everything in `valid/porc` replays one
recorded history. `exclusive_task_acquire_at_version` — the single most
important law here — has no test at either level.

| name | statement |
|---|---|
| `exclusive_at_most_one_valid_claim` | from one pre-state, two acquisitions at the same version cannot both succeed. **Per version, not per task**: with `ttl = 0` a second acquisition legally succeeds — at version 2, never at version 1 |
| `exclusive_task_acquire_across_doors` | `task.create` and `task.acquire` racing on a pending task cannot both acquire it. The exclusion is the `pending` guard, not the version guard — `task.create` carries no version |
| `exclusive_task_holder_ops_serialize` | from one acquired pre-state, at most one of suspend(200) / fulfill / release succeeds. `task.fence` is the exception and stays repeatable |
| `exclusive_promise_settlement_single_writer` | settle, fulfill and fenced settle decide the value at most once; the loser observes the winner's |
| `exclusive_lease_and_retry_rules_per_task` | at most one of the lease and retry rules is enabled for a task, since one needs acquired and the other pending |
| `exclusive_lease_timeout_races_heartbeat` | both are enabled on an expired-but-unreaped lease, both interleavings are admitted, and neither produces two holders |
| `idempotent_promise_create_returns_stored_record` | a repeat create returns the **stored** record and ignores the request body entirely |
| `absorbing_promise_settle_stored_settlement` | a second settle returns 200 with the **stored** settlement. A 200 does not mean "my value was stored" — pair this with a body assertion, never a status assertion |
| `absorbing_promise_timeout_beats_settle` | past the deadline no settlement can change state or value; the boundary is `≤`, so the timeout wins *at* the deadline |
| `absorbing_task_create_fulfilled_task` | create against a fulfilled task serves the answer, it does not re-execute |
| `idempotent_promise_register_callback` / `idempotent_promise_register_listener` | at most one wake, at most one notification, per pair |
| `neutral_registration_on_settled_awaited` | registering on a settled promise returns 200 and records nothing — no obligation can attach to a settlement already past |
| `neutral_registration_with_dead_awaiter` | likewise when the *awaiter's* own promise is dead. Note the response is 200 either way, so the caller cannot tell registered from not-registered |
| `idempotent_task_heartbeat_at_fixed_clock` | the effect is a function of `now` and `ttl`, never accumulating |
| `neutral_task_heartbeat_foreign_or_stale` | a heartbeat writes nothing to a task it does not own, at a stale version, or with a settled promise |
| `neutral_task_heartbeat_unnamed_task` | it writes only to the tasks its ref list names |
| `idempotent_task_halt` | halting an already-halted task returns 200 and writes nothing |
| `idempotent_task_fence_action` | a fence does not consume its version, so the same fenced request may be replayed |
| `neutral_task_fence_stale_version` | a refused fence does not evaluate its carried action |
| `exclusive_task_fence_target_is_not_self` | a holder settles its own promise through `task.fulfill`, not through a fence |
| `exclusive_task_continue_single_success` | two continues cannot both succeed |
| `exclusive_schedule_delete_single_success` | |
| `idempotent_schedule_create` | |
| `neutral_validation_rejection_writes_nothing` | every 400 returns before any read or write, in either discipline |
| `neutral_read_observationally_pure` | interposing any read produces no observation the trace without it could not produce — whether the server materializes on read or projects. **Not** "reads don't write": under materialization they do |
| `absorbing_promise_projection_under_monotone_clock` | projecting at an earlier instant then a later one equals projecting only at the later one |
| `absorbing_task_view_under_monotone_clock` | likewise for the task view |
| `neutral_clock_advance_settled_object` | advancing the clock changes no response for an already-settled promise or its task |
| `idempotent_promise_timeout_rule` / `idempotent_lease_timeout_rule` / `idempotent_retry_timeout_rule` | firing twice equals firing once; firing an unenabled rule is the identity |
| `idempotent_quiescence` | quiescing an already-quiesced state changes nothing |
| `idempotent_schedule_occurrence_creation` | re-firing creates no duplicate promise for an occurrence already fired |
| ⚠ `idempotent_schedule_rule` | **not enforced**: nothing says the occurrence window is empty once consumed. Needs `occurrences_empty_when_unreached` |
| `commutes_internal_rule_disjoint_affects` | two enabled firings with disjoint affected sets commute. **Excludes arming pairs** — the promise timeout is what enables the callback drain, so those two do not commute, and that is the bug the cone shipped twice |
| `commutes_internal_callback_internal_callback` | two drains waking the same awaiter commute *on the response channel*, because `resumes` is a count and not a list |
| `commutes_request_request_disjoint_origin` | requests over disjoint object sets with no awaits-edge commute — this is what makes per-origin partitioned replay sound |

## Which rules are invisible

Merged from two agents that disagreed; the resolution is worth stating.

- **Response-neutral:** the promise timeout (it writes back exactly what
  every read already reports, *because* reads project), the listener drain
  (listener sets are in no record), the retry dispatch (`retryAt` is in no
  record).
- **Observable:** the callback drain (suspended → pending, resume count
  moves) and the lease timeout (acquired → pending, pid cleared). Both are
  visible through `task.get`.

The blanket claim "internal rules are unobservable" is false, and it is
exactly why the linearizability checker must be a nondeterministic subset
construction rather than a replay.

---

# Response and framing

| name | statement |
|---|---|
| `responds_status_in_closed_set` | every status is one of 200, 300, 400, 404, 409, 422, 501 |
| `responds_deterministic` | response and successor are a total function of (state, request, instant) |
| `responds_discipline_independent` | the response stream does not depend on the read discipline |
| `responds_bad_request_state_independent` | every 400 is decided by the request alone — the eight 400-capable operations admit a stateless validator |
| `responds_bad_request_dominates` | 400 outranks 404, 409 and 422 |
| `responds_not_found_names_the_primary_key` | a missing *secondary* object is 422, never 404 |
| `responds_conflict_is_task_scoped` | no promise or schedule operation ever returns 409 |
| `responds_conflict_means_state_or_version` | the three sub-causes are deliberately indistinguishable |
| `responds_unprocessable_means_coreferenced_object` | 422 is always about an object other than the one being modified |
| `responds_guard_order_is_total` | when two guards both fail the answer is determined; where competing guards share a status no order is imposed |
| `responds_task_heartbeat_total` | the only always-200 operation. A worker cannot learn from the protocol that its lease is gone |
| `responds_search_not_implemented` | 501, unconditionally; 200 is unreachable |
| `responds_payload_absent_unless_ok` | a non-200 carries only `status`. A 409 must not helpfully return the current version — the client calls `task.get` |
| `responds_promise_record_omits_obligations` | the await graph is unobservable |
| `responds_task_create_success_is_not_ownership` | one status covers created-and-acquired, born-dead, and already-fulfilled. Ownership is readable only from `state`, `pid` and `version` |
| `responds_task_acquire_returns_incremented_version` | the returned version is the only token later fenced calls accept |
| `responds_task_suspend_redirect_is_not_an_error` | 300 means "keep running" |
| `responds_task_fence_outer_status_independent_of_inner` | a fenced 400 travels as `200 { action: { status: 400 } }` |
| `responds_task_fulfill_expired_promise_conflicts` | past the deadline `task.fulfill` is **409** while `promise.settle` is **200** — the two settlement doors diverge at the same instant, because the task read applies fact T before the `acquired` guard runs |
| `frames_api_outbox_untouched` | no API operation appends to the outbox; every message comes from a rule |
| `frames_api_promises_and_tasks_never_deleted` | `promise.get` never regresses from 200 to 404 |
| `frames_schedule_operations_touch_only_schedules` | and no other operation writes a schedule |
| `frames_lease_operations_write_only_the_task` | acquire, release, halt, continue write exactly one object |
| `frames_refusal_no_write_under_projection` | under projection every 4xx leaves the store bit-identical |
| `preserved_refusal_view_unchanged` | under materialization a 4xx **may** write — the read persists a passed deadline before the guard fails — but only facts the pre-state already implied. What is forbidden is stamping `settledAt := now` instead of the deadline. *A conformance test asserting "a refused request doesn't dirty the store" is wrong for half of all conforming servers* |
| ⚠ `responds_preload_specified` | **dead protocol surface**: four response types declare `preload` and no handler ever populates it |

---

# Stage 4 — liveness

No fairness predicate exists anywhere in the specification. `Valid` is pure
safety: a machine that never fires a rule satisfies every theorem in
`04-theorems`. All of these are therefore **unstated**, and the first work is
a fairness condition to state them against.

| name | statement |
|---|---|
| ⚠ `eventually_promise_leaves_pending` | requires clock progress. `Valid` allows a constant clock, so the trace where `now = 0` forever is valid and a promise with `timeoutAt = 100` stays pending |
| ⚠ `eventually_determined_wake_materializes` | the liveness half of the durable-execution guarantee |
| ⚠ `eventually_settled_promise_notifies_listeners` | strictly weaker than the dispatch case — delivery is unmodelled and has no retry |
| ⚠ `eventually_pending_task_receives_execute` | |
| ⚠ `eventually_expired_lease_returns_task_to_pending` | |
| ⚠ `eventually_promise_timeout_disarmed` `[concrete]` | |
| ⚠ `eventually_task_timeout_disarmed` `[concrete]` | the retry timer never disarms itself — it re-arms unconditionally, and its only collection path is the promise-timeout coupled write |
| ⚠ `eventually_deferred_drained` `[concrete]` | and the materialized drain is **not a fixpoint**: its `processResume` touches, which can enqueue entries the snapshot loop never visits |

---

# Deleted as derivable

Kept here so nothing is silently lost.

| deleted | follows from |
|---|---|
| `well_formed_promise_timer_never_rejected_timedout` | `well_formed_promise_deadline_verdict_matches_timer_tag` + `..._timed_out_stamped_at_deadline` |
| `consistent_timer_promise_has_no_task` | `well_formed_promise_timer_not_targeted` + `consistent_task_iff_targeted_promise` |
| `consistent_task_promise_co_keyed` (`coKeyed`) | `consistent_task_iff_targeted_promise` — strictly weaker; replace it |
| `consistent_task_timeout_at_most_one` | lease/retry disciplines + exclusivity of task states |
| `consistent_settled_promise_no_armed_timeouts` | the two mirror directions + `consistent_settled_promise_has_fulfilled_task` |
| `consistent_promise_timeout_unique_per_promise` (differing deadlines) | `consistent_promise_timeout_mirrors_promise`; exact duplicates are inert by `consistent_timeout_fire_guarded_by_object` |
| `consistent_live_task_promise_armed` | `consistent_task_iff_targeted_promise` + `consistent_external_pending_promise_armed` |
| `well_formed_outbox_unblock_address_deliverable` | `consistent_listener_addresses_deliverable` |
| `preserved_task_version_stable_across_release_halt_continue_suspend` | `preserved_task_version_increments_only_on_acquisition` |
| `preserved_task_lease_expiry_does_not_fence_by_version` | same, + `exclusive_task_holder_ops_serialize` |
| `well_formed_task_resumes_singleton_after_wake` | `well_formed_task_suspended_has_no_resumes` + the wake write |
| `responds_promise_create_idempotent_on_id_alone` | `idempotent_promise_create_returns_stored_record` + `preserved_responses_are_projected` |
| `responds_promise_settle_deadline_beats_request` | `absorbing_promise_timeout_beats_settle` + `preserved_responses_are_projected` |
| `responds_task_continue_conflict_precedes_not_found` | `responds_guard_order_is_total` |
| `responds_any_bad_request_precedes_all_other_codes` | `responds_bad_request_state_independent` |
| `exclusive_timeout_and_dispatch_rules_per_id` | `preserved_no_dead_dispatch` |
| `preserved_response_stream_across_read_disciplines` | `preserved_view_agreement_under_step` + `monotone_clock_never_runs_backwards` |
| `monotone_task_retry_timeout_advances` `[concrete]` | `well_formed_config_retry_positive` |
| `idempotent_task_fence_repeated_action` | create-idempotence + settle-absorption + `neutral_task_fence_stale_version` |

---

# Not properties — plausible and false

| claim | why it is false |
|---|---|
| a settled promise is immutable | the listener and callback drains rewrite settled promises |
| a pending promise has `timeoutAt > now` | false by design; the machine is lazy and a past-deadline promise sits stored-pending |
| settling twice is an error | it absorbs with 200 |
| a late settle fails | it returns **200** carrying `rejectedTimedout`. A client reading only the status concludes its value was stored |
| firing any internal rule is unobservable | true of three rules, false of the callback drain and the lease timeout |
| `task.acquire` is idempotent | it is exclusive; the second gets 409 and the version has moved |
| the clock advance is neutral | only for already-settled objects |
| `promise.create` then a clock advance commutes with the reverse order | `createdAt` is `now` on the live path and `timeoutAt` on the born-dead path |
| exclusivity holds per task | it holds per **version**; `ttl = 0` admits a legal second acquisition at version 2 |

---

# Axioms needed

The four `well_formed_schedule_*` entries and `idempotent_schedule_rule` are
not merely unexercised — they are **unprovable**, because `nextCron`,
`occurrences` and `expand` are `opaque` with no value. Adding these makes
them provable:

- `nextCron_strictly_after : ∀ c n, n < nextCron c n`
- `occurrences_in_window : ∀ c s n t, t ∈ occurrences c s n → s ≤ t ∧ t ≤ n`
- `occurrences_empty_when_unreached : ∀ c s n, n < s → occurrences c s n = []`
- `occurrences_sorted` — the rule takes the last element as the new `lastRunAt`; on an unsorted list `nextRunAt` regresses
- `occurrences_is_the_nextCron_chain` — needed for any no-lost-occurrence claim
- `expand_injective_in_timestamp`, plus disjointness of expanded ids from client-chosen ids — without it a client can pre-create a promise at an occurrence's id and silently suppress that occurrence

---

# Open design questions

Surfaced by the extraction; none of these is a bug I should pick a side on.

1. **Is the task version a capability or a public tag?** `task.get` returns
   it and the holding pid to any caller, and fence/suspend/fulfill/release
   authorize on version alone — none of those requests carries a pid. But
   `heartbeat` *does* check pid. The machine is inconsistent with itself.
2. **`task.halt` + `task.continue` is an unfenced `task.release`.** Verified
   byte-identical resulting state. Release requires the holder's version;
   the pair requires nothing.
3. **May a client settle a live timer?** `settable` excludes `pending` and
   `rejectedTimedout` and says nothing about timers, so a client can reject
   a timer long before it would fire.
4. **Is `promise.create` id-idempotent or request-idempotent?** A repeat
   create with a contradicting body silently returns the incumbent. There is
   no mismatch signal.
5. **Does `rejectedCanceled` mean anything?** Nothing distinguishes it from
   `rejected`; an implementation could collapse them and pass everything here.
6. **Should the acquirer learn why it was woken?** The resume buffer is
   cleared before the acquire response is built, so the count is always 0.
   The window to observe it is a `task.get` between wake and claim.

---

# Mapping to `implementation-questions.md`

| this catalogue | there |
|---|---|
| `preserved_no_dead_dispatch` | `NoDeadDispatch` (C4, C5, C6, C7) |
| `consistent_obligations_require_external_awaited` | `ObligationsAreDischargeable` (C2), `NoStrandedListener` (C3) |
| `exclusive_at_most_one_valid_claim` | `AtMostOneValidClaim` (C10) |
| `consistent_outbox_never_ahead` | `OutboxNeverAhead` (C9) |
| `neutral_registration_on_settled_awaited` | `SettledPromiseHasNoSubscriptions` (C11) |
| `preserved_responses_are_projected` | `ResponsesAreProjected`, `ResponsesNeverRegress`, `Stickiness` (C8) |
| `consistent_external_pending_promise_armed` | `ArmingIsExternalOnly` |
| `preserved_task_read_materializes_before_guarding` | `NoHaltOnDead` (C4) |
| `consistent_suspended_task_holds_rung` | `SuspendedTaskHasCallback` |
| `consistent_task_timeout_unique_per_id_kind` | `TaskHasAtMostOneTimer` |
| `eventually_promise_leaves_pending` | `PromiseLivenessGuard`, `TimeoutLivenessGuard` |

---

# Counts

| stage | entries | of which not enforced |
|---|---|---|
| 1 — one object, one state | 35 | 5 |
| 2 — many objects, one state | 36 | 3 |
| 3 — two states, one step | 25 | 0 |
| branching | 37 | 1 |
| response and framing | 22 | 1 |
| 4 — liveness | 8 | 8 |
| **total** | **163** | **18** |

Plus 19 deleted as derivable, 9 refuted as false, and 6 axioms outstanding.

**30 of the 163 are implemented and checked** — all of stage 1's
single-object entries, in `spec/02-abstract/properties.lean`, swept over
1 464 scripts under both read disciplines with a falsifiability battery and
14 reach witnesses.
