import «04-theorems».«induction»

/-!  # What each handler writes

The 28-handler discharge, generic in the property. `induction.lean`
reduces "is this property inductive?" to eleven `PromiseObject`
obligations plus this file, and this file is paid ONCE: nothing below
mentions a particular property.

Every proof is `split` down to the writes and then one `Hereditary`
field at each promise write. Nothing here reasons about the protocol. A
handler that grew a new promise write would fail to compile rather than
pass silently — which is the whole point of making the enumeration a
theorem instead of a docstring.

## Reading these proofs

Two shapes recur, both artefacts of how `do` compiles rather than of
the protocol.

`if guard then return 400` becomes a JOIN POINT: the rest of the
handler is bound to `__do_jp` and the fall-through branch is `pure () >>=
__do_jp`. `wg_guard` steps over one of those, `writesGood_pure` on the
rejection and `writesGood_pureBind` on the continuation.

And the read combinators hand back their branches as `f none` / `f
(some p)` — correct but not yet reduced, since `f` is a `match` and
nothing has fired the iota rule. `dsimp only` fires it. Without that
step a subsequent `split` splits the match again, and both branches
come back. -/

set_option maxHeartbeats 400000

namespace Abstraction
namespace Induction

open AbstractModel

/-- Discharges everything that writes no promise: tasks, schedules,
    messages, pures, and the control structure between them. -/
macro "wg_trivial" : tactic => `(tactic| repeat (first
  | exact writesGood_pure _ _ _
  | exact writesGood_ask _ _
  | exact writesGood_getPromise _ _ _
  | exact writesGood_getTask _ _ _
  | exact writesGood_getSchedule _ _ _
  | exact writesGood_setTask _ _ _
  | exact writesGood_setSchedule _ _ _
  | exact writesGood_delSchedule _ _ _
  | exact writesGood_setMessage _ _ _ _
  | apply writesGood_bind'
  | refine writesGood_ite _ _ _ _ _ ?_ ?_
  | dsimp only
  | split))

/-- One `if guard then return <error>` and the join point it leaves. -/
macro "wg_guard" : tactic => `(tactic|
  (refine writesGood_ite _ _ _ _ _ (writesGood_pure _ _ _) ?_
   refine writesGood_pureBind _ _ _ _ ?_))

section Handlers

variable {q : PromiseObject → Bool} {e : Env} (hq : Hereditary q)
  (hs : PerPromise q e.state = true)

include hq hs

set_option linter.unusedSectionVars false

/-! ### Birth, and the read that may materialise -/

theorem writesGood_createPromise (req : ServerModel.PromiseCreateReq) (now : Nat) :
    WritesGood q e (createPromise req now) := by
  unfold createPromise
  refine writesGood_iteH _ _ _ _ _ (fun h => ?_) (fun _ => ?_)
  · refine writesGood_bind' _ _ _ _
      (writesGood_setPromise _ _ _ (hq.live _ _ _ _ _ (by omega))) ?_
    wg_trivial
  · refine writesGood_bind' _ _ _ _
      (writesGood_setPromise _ _ _ (hq.dead _ _ _ _ _ (by split <;> simp))) ?_
    wg_trivial

theorem writesGood_createIfAbsent (req : ServerModel.PromiseCreateReq) (now : Nat) :
    WritesGood q e (createIfAbsent req now) := by
  unfold createIfAbsent
  refine writesGood_afterReadPromise hq hs _ _ _ ?_ ?_
  · exact writesGood_bind' _ _ _ _ (writesGood_createPromise hq hs _ _) (writesGood_pure _ _ _)
  · intro p hp; exact writesGood_pure _ _ _

/-! ### The promise API -/

theorem writesGood_promiseGet (req : ServerModel.PromiseGetReq) (now : Nat) :
    WritesGood q e (promiseGet req now) := by
  unfold promiseGet
  refine writesGood_afterReadPromise hq hs _ _ _ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro p hp; exact writesGood_pure _ _ _

theorem writesGood_promiseCreate (req : ServerModel.PromiseCreateReq) (now : Nat) :
    WritesGood q e (promiseCreate req now) := by
  unfold promiseCreate
  wg_guard
  refine writesGood_afterReadPromise hq hs _ _ _ ?_ ?_
  · exact writesGood_bind' _ _ _ _ (writesGood_createPromise hq hs _ _) (writesGood_pure _ _ _)
  · intro p hp; exact writesGood_pure _ _ _

theorem writesGood_promiseSettle (req : ServerModel.PromiseSettleReq) (now : Nat) :
    WritesGood q e (promiseSettle req now) := by
  unfold promiseSettle
  wg_guard
  refine writesGood_afterReadPromise hq hs _ _ _ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro p hp
    dsimp only
    refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
    exact writesGood_bind' _ _ _ _
      (writesGood_setSettled _ _ (hq.settle p _ _ _ hp)) (writesGood_pure _ _ _)

theorem writesGood_promiseRegisterCallback
    (req : ServerModel.PromiseRegisterCallbackReq) (now : Nat) :
    WritesGood q e (promiseRegisterCallback req now) := by
  unfold promiseRegisterCallback
  wg_guard
  refine writesGood_afterReadPromise hq hs _ _ _ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro pa hpa
    dsimp only
    refine writesGood_afterReadPromise hq hs _ _ _ ?_ ?_
    · exact writesGood_pure _ _ _
    · intro pw hpw
      dsimp only
      wg_guard
      wg_guard
      refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
      refine writesGood_ite _ _ _ _ _ ?_ ?_
      · exact writesGood_bind' _ _ _ _
          (writesGood_setPromise _ _ _ (hq.addCallback pa _ hpa)) (writesGood_pure _ _ _)
      · exact writesGood_bind' _ _ _ _ (writesGood_pure _ _ _) (writesGood_pure _ _ _)

theorem writesGood_promiseRegisterListener
    (req : ServerModel.PromiseRegisterListenerReq) (now : Nat) :
    WritesGood q e (promiseRegisterListener req now) := by
  unfold promiseRegisterListener
  wg_guard
  refine writesGood_afterReadPromise hq hs _ _ _ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro pa hpa
    dsimp only
    wg_guard
    refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
    exact writesGood_bind' _ _ _ _
      (writesGood_setPromise _ _ _ (hq.addListener pa _ hpa)) (writesGood_pure _ _ _)

theorem writesGood_promiseSearch (req : ServerModel.PromiseSearchReq) (now : Nat) :
    WritesGood q e (promiseSearch req now) := writesGood_pure _ _ _

/-! ### The task API -/

theorem writesGood_taskGet (req : ServerModel.TaskGetReq) (now : Nat) :
    WritesGood q e (taskGet req now) := by
  unfold taskGet
  refine writesGood_afterReadTask hq hs _ _ _ ?_ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro t; exact writesGood_pure _ _ _
  · intro t p hp; exact writesGood_pure _ _ _

theorem writesGood_taskCreate (req : ServerModel.TaskCreateReq) (now : Nat) :
    WritesGood q e (taskCreate req now) := by
  simp only [taskCreate]
  wg_guard
  refine writesGood_afterReadPromise hq hs _ _ _ ?_ ?_
  · dsimp only
    refine writesGood_iteH _ _ _ _ _ (fun h => ?_) (fun _ => ?_)
    · refine writesGood_bind' _ _ _ _
        (writesGood_setPromise _ _ _ (hq.live _ _ _ _ _ (by omega))) ?_
      wg_trivial
    · refine writesGood_bind' _ _ _ _
        (writesGood_setPromise _ _ _ (hq.dead _ _ _ _ _ (by simp))) ?_
      wg_trivial
  · intro p hp
    dsimp only
    wg_guard
    refine writesGood_afterReadTask hq hs _ _ _ ?_ ?_ ?_
    · exact writesGood_pure _ _ _
    · intro t; exact writesGood_pure _ _ _
    · intro t p' hp'; dsimp only; wg_trivial

theorem writesGood_taskAcquire (req : ServerModel.TaskAcquireReq) (now : Nat) :
    WritesGood q e (taskAcquire req now) := by
  unfold taskAcquire
  refine writesGood_afterReadTask hq hs _ _ _ ?_ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro t; exact writesGood_pure _ _ _
  · intro t p hp; dsimp only; wg_trivial

theorem writesGood_taskFence (req : ServerModel.TaskFenceReq) (now : Nat) :
    WritesGood q e (taskFence req now) := by
  unfold taskFence
  wg_guard
  refine writesGood_afterReadTask hq hs _ _ _ ?_ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro t; exact writesGood_pure _ _ _
  · intro t p hp
    dsimp only
    wg_guard
    wg_guard
    wg_guard
    split
    · exact writesGood_bind' _ _ _ _ (writesGood_promiseCreate hq hs _ _)
        (writesGood_pure _ _ _)
    · exact writesGood_bind' _ _ _ _ (writesGood_promiseSettle hq hs _ _)
        (writesGood_pure _ _ _)

theorem writesGood_heartbeatOne (pid : String) (ref : ServerModel.TaskRef) (now : Nat) :
    WritesGood q e (heartbeatOne pid ref now) := by
  unfold heartbeatOne
  refine writesGood_afterReadTask hq hs _ _ _ ?_ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro t; exact writesGood_pure _ _ _
  · intro t p hp; dsimp only; wg_trivial

theorem writesGood_heartbeatAll (pid : String) (now : Nat) :
    ∀ refs, WritesGood q e (heartbeatAll pid now refs)
  | [] => by rw [heartbeatAll]; exact writesGood_pure _ _ _
  | r :: rs => by
      rw [heartbeatAll]
      exact writesGood_bind' _ _ _ _ (writesGood_heartbeatOne hq hs pid r now)
        (writesGood_heartbeatAll pid now rs)

theorem writesGood_taskHeartbeat (req : ServerModel.TaskHeartbeatReq) (now : Nat) :
    WritesGood q e (taskHeartbeat req now) := by
  unfold taskHeartbeat
  exact writesGood_bind' _ _ _ _ (writesGood_heartbeatAll hq hs _ _ _) (writesGood_pure _ _ _)

theorem writesGood_checkAwaited (now : Nat) :
    ∀ acts, WritesGood q e (checkAwaited now acts)
  | [] => by rw [checkAwaited]; exact writesGood_pure _ _ _
  | a :: rest => by
      rw [checkAwaited]
      refine writesGood_afterReadPromise hq hs _ _ _ ?_ ?_
      · exact writesGood_pure _ _ _
      · intro p hp
        dsimp only
        refine writesGood_ite _ _ _ _ _ (writesGood_pure _ _ _) ?_
        refine writesGood_bind' _ _ _ _ (writesGood_checkAwaited now rest) ?_
        wg_trivial

/-- The one list recursion that writes a promise per element. -/
theorem writesGood_registerAwaited (awaiter : String) (now : Nat) :
    ∀ acts, WritesGood q e (registerAwaited awaiter now acts)
  | [] => by rw [registerAwaited]; exact writesGood_pure _ _ _
  | a :: rest => by
      rw [registerAwaited]
      refine writesGood_afterReadPromise hq hs _ _ _ ?_ ?_
      · dsimp only
        exact writesGood_bind' _ _ _ _ (writesGood_pure _ _ _)
          (writesGood_registerAwaited awaiter now rest)
      · intro p hp
        dsimp only
        exact writesGood_bind' _ _ _ _
          (writesGood_setPromise _ _ _ (hq.addCallback p _ hp))
          (writesGood_registerAwaited awaiter now rest)

theorem writesGood_taskSuspend (req : ServerModel.TaskSuspendReq) (now : Nat) :
    WritesGood q e (taskSuspend req now) := by
  unfold taskSuspend
  wg_guard
  wg_guard
  wg_guard
  refine writesGood_afterReadTask hq hs _ _ _ ?_ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro t; exact writesGood_pure _ _ _
  · intro t p hp
    dsimp only
    wg_guard
    wg_guard
    wg_guard
    refine writesGood_bind' _ _ _ _ (writesGood_checkAwaited hq hs _ _) ?_
    split
    · exact writesGood_pure _ _ _
    · exact writesGood_bind' _ _ _ _ (writesGood_setTask _ _ _) (writesGood_pure _ _ _)
    · exact writesGood_bind' _ _ _ _ (writesGood_registerAwaited hq hs _ _ _)
        (writesGood_bind' _ _ _ _ (writesGood_setTask _ _ _) (writesGood_pure _ _ _))

theorem writesGood_taskFulfill (req : ServerModel.TaskFulfillReq) (now : Nat) :
    WritesGood q e (taskFulfill req now) := by
  unfold taskFulfill
  wg_guard
  refine writesGood_afterReadTask hq hs _ _ _ ?_ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro t; exact writesGood_pure _ _ _
  · intro t p hp
    dsimp only
    wg_guard
    wg_guard
    wg_guard
    exact writesGood_bind' _ _ _ _
      (writesGood_setSettled _ _ (hq.settle p _ _ _ hp)) (writesGood_pure _ _ _)

theorem writesGood_taskRelease (req : ServerModel.TaskReleaseReq) (now : Nat) :
    WritesGood q e (taskRelease req now) := by
  unfold taskRelease
  refine writesGood_afterReadTask hq hs _ _ _ ?_ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro t; exact writesGood_pure _ _ _
  · intro t p hp; dsimp only; wg_trivial

theorem writesGood_taskHalt (req : ServerModel.TaskHaltReq) (now : Nat) :
    WritesGood q e (taskHalt req now) := by
  unfold taskHalt
  refine writesGood_afterReadTask hq hs _ _ _ ?_ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro t; exact writesGood_pure _ _ _
  · intro t p hp; dsimp only; wg_trivial

theorem writesGood_taskContinue (req : ServerModel.TaskContinueReq) (now : Nat) :
    WritesGood q e (taskContinue req now) := by
  unfold taskContinue
  refine writesGood_afterReadTask hq hs _ _ _ ?_ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro t; dsimp only; wg_trivial
  · intro t p hp; dsimp only; wg_trivial

theorem writesGood_taskSearch (req : ServerModel.TaskSearchReq) (now : Nat) :
    WritesGood q e (taskSearch req now) := writesGood_pure _ _ _

/-! ### The schedule API — no promise ever crosses it -/

theorem writesGood_scheduleGet (req : ServerModel.ScheduleGetReq) (now : Nat) :
    WritesGood q e (scheduleGet req now) := by
  unfold scheduleGet; wg_trivial

theorem writesGood_scheduleCreate (req : ServerModel.ScheduleCreateReq) (now : Nat) :
    WritesGood q e (scheduleCreate req now) := by
  unfold scheduleCreate; wg_trivial

theorem writesGood_scheduleDelete (req : ServerModel.ScheduleDeleteReq) (now : Nat) :
    WritesGood q e (scheduleDelete req now) := by
  unfold scheduleDelete; wg_trivial

theorem writesGood_scheduleSearch (req : ServerModel.ScheduleSearchReq) (now : Nat) :
    WritesGood q e (scheduleSearch req now) := writesGood_pure _ _ _

/-! ### The six internal steps

These read through `withMat`, so they take the `withMat` combinators.
`touchPromise` is `withMat true`, `viewPromise` is `withMat false`, and
the difference is invisible to this argument — which is the formal
content of "the read discipline is not a protocol decision". -/

theorem writesGood_processPromiseTimeout (id : String) (now : Nat) :
    WritesGood q e (Internal.processPromiseTimeout id now) := by
  unfold Internal.processPromiseTimeout touchPromise
  refine writesGood_afterMatReadPromise hq true hs _ _ _ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro p hp; exact writesGood_pure _ _ _

theorem writesGood_processListener (id : String) (address : String) (now : Nat) :
    WritesGood q e (Internal.processListener id address now) := by
  unfold Internal.processListener touchPromise
  refine writesGood_afterMatReadPromise hq true hs _ _ _ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro p hp
    dsimp only
    refine writesGood_ite _ _ _ _ _ (writesGood_pure _ _ _) ?_
    refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
    exact writesGood_bind' _ _ _ _
      (writesGood_setPromise _ _ _ (hq.dropListener p _ hp)) (writesGood_setMessage _ _ _ _)

theorem writesGood_resumeOne (awaited awaiter : String) (now : Nat) :
    WritesGood q e (Internal.resumeOne awaited awaiter now) := by
  unfold Internal.resumeOne touchTask
  refine writesGood_afterTouchTask hq hs _ _ _ ?_ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro t; exact writesGood_pure _ _ _
  · intro t p hp; dsimp only; wg_trivial

theorem writesGood_processCallback (id : String) (awaiter : String) (now : Nat) :
    WritesGood q e (Internal.processCallback id awaiter now) := by
  unfold Internal.processCallback touchPromise
  refine writesGood_afterMatReadPromise hq true hs _ _ _ ?_ ?_
  · exact writesGood_pure _ _ _
  · intro p hp
    dsimp only
    refine writesGood_ite _ _ _ _ _ (writesGood_pure _ _ _) ?_
    refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
    exact writesGood_bind' _ _ _ _
      (writesGood_setPromise _ _ _ (hq.dropCallback p _ hp))
      (writesGood_resumeOne hq hs _ _ _)

theorem writesGood_processLeaseTimeout (id : String) (now : Nat) :
    WritesGood q e (Internal.processLeaseTimeout id now) := by
  unfold Internal.processLeaseTimeout viewPromise
  refine writesGood_bind' _ _ _ _ (writesGood_getTask _ _ _) ?_
  split
  · exact writesGood_pure _ _ _
  · split
    · exact writesGood_pure _ _ _
    · refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
      refine writesGood_afterMatReadPromise hq false hs _ _ _ ?_ ?_
      · exact writesGood_pure _ _ _
      · intro p hp; dsimp only; wg_trivial

theorem writesGood_processRetryTimeout (id : String) (now : Nat) :
    WritesGood q e (Internal.processRetryTimeout id now) := by
  unfold Internal.processRetryTimeout viewPromise
  refine writesGood_bind' _ _ _ _ (writesGood_getTask _ _ _) ?_
  split
  · exact writesGood_pure _ _ _
  · split
    · exact writesGood_pure _ _ _
    · refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
      refine writesGood_afterMatReadPromise hq false hs _ _ _ ?_ ?_
      · exact writesGood_pure _ _ _
      · intro p hp; dsimp only; wg_trivial

theorem writesGood_fireOccurrence (c : ServerModel.Schedule) (t : Nat) :
    WritesGood q e (Internal.fireOccurrence c t) := by
  unfold Internal.fireOccurrence
  exact writesGood_createIfAbsent hq hs _ _

theorem writesGood_fireAll (c : ServerModel.Schedule) :
    ∀ ts, WritesGood q e (Internal.fireAll c ts)
  | [] => by rw [Internal.fireAll]; exact writesGood_pure _ _ _
  | t :: ts => by
      rw [Internal.fireAll]
      exact writesGood_bind' _ _ _ _ (writesGood_fireOccurrence hq hs c t)
        (writesGood_fireAll c ts)

theorem writesGood_processSchedule (id : String) (now : Nat) :
    WritesGood q e (Internal.processSchedule id now) := by
  unfold Internal.processSchedule
  refine writesGood_bind' _ _ _ _ (writesGood_getSchedule _ _ _) ?_
  split
  · exact writesGood_pure _ _ _
  · refine writesGood_bind' _ _ _ _ (writesGood_fireAll hq hs _ _) ?_
    wg_trivial

/-! ### And the dispatcher

The theorem the enumeration was for. Every step of the machine — 21
requests, 6 internal jobs, `idle` — writes only promises satisfying
`q`, given the eleven obligations and a state that already satisfies
`q` everywhere. A step added without a case here does not compile. -/

theorem writesGood_handle (st : Step) (now : Nat) : WritesGood q e (handle st now) := by
  cases st with
  | api r =>
      cases r with
      | promiseGet r => exact writesGood_map _ _ _ _ (writesGood_promiseGet hq hs r now)
      | promiseCreate r => exact writesGood_map _ _ _ _ (writesGood_promiseCreate hq hs r now)
      | promiseSettle r => exact writesGood_map _ _ _ _ (writesGood_promiseSettle hq hs r now)
      | promiseRegisterCallback r =>
          exact writesGood_map _ _ _ _ (writesGood_promiseRegisterCallback hq hs r now)
      | promiseRegisterListener r =>
          exact writesGood_map _ _ _ _ (writesGood_promiseRegisterListener hq hs r now)
      | promiseSearch r => exact writesGood_map _ _ _ _ (writesGood_promiseSearch hq hs r now)
      | scheduleGet r => exact writesGood_map _ _ _ _ (writesGood_scheduleGet hq hs r now)
      | scheduleCreate r => exact writesGood_map _ _ _ _ (writesGood_scheduleCreate hq hs r now)
      | scheduleDelete r => exact writesGood_map _ _ _ _ (writesGood_scheduleDelete hq hs r now)
      | scheduleSearch r => exact writesGood_map _ _ _ _ (writesGood_scheduleSearch hq hs r now)
      | taskGet r => exact writesGood_map _ _ _ _ (writesGood_taskGet hq hs r now)
      | taskCreate r => exact writesGood_map _ _ _ _ (writesGood_taskCreate hq hs r now)
      | taskAcquire r => exact writesGood_map _ _ _ _ (writesGood_taskAcquire hq hs r now)
      | taskFence r => exact writesGood_map _ _ _ _ (writesGood_taskFence hq hs r now)
      | taskHeartbeat r => exact writesGood_map _ _ _ _ (writesGood_taskHeartbeat hq hs r now)
      | taskSuspend r => exact writesGood_map _ _ _ _ (writesGood_taskSuspend hq hs r now)
      | taskFulfill r => exact writesGood_map _ _ _ _ (writesGood_taskFulfill hq hs r now)
      | taskRelease r => exact writesGood_map _ _ _ _ (writesGood_taskRelease hq hs r now)
      | taskHalt r => exact writesGood_map _ _ _ _ (writesGood_taskHalt hq hs r now)
      | taskContinue r => exact writesGood_map _ _ _ _ (writesGood_taskContinue hq hs r now)
      | taskSearch r => exact writesGood_map _ _ _ _ (writesGood_taskSearch hq hs r now)
  | r1 id =>
      exact writesGood_bind' _ _ _ _ (writesGood_processPromiseTimeout hq hs id now)
        (writesGood_pure _ _ _)
  | r3 id a =>
      exact writesGood_bind' _ _ _ _ (writesGood_processListener hq hs id a now)
        (writesGood_pure _ _ _)
  | r4 id x =>
      exact writesGood_bind' _ _ _ _ (writesGood_processCallback hq hs id x now)
        (writesGood_pure _ _ _)
  | r5 id =>
      exact writesGood_bind' _ _ _ _ (writesGood_processLeaseTimeout hq hs id now)
        (writesGood_pure _ _ _)
  | r6 id =>
      exact writesGood_bind' _ _ _ _ (writesGood_processRetryTimeout hq hs id now)
        (writesGood_pure _ _ _)
  | r7 id =>
      exact writesGood_bind' _ _ _ _ (writesGood_processSchedule hq hs id now)
        (writesGood_pure _ _ _)
  | idle => exact writesGood_pure _ _ _

end Handlers

/-! ## The tier theorem

Eleven `PromiseObject` obligations buy inductiveness over the whole
machine. This is the reusable result: 36 of the 45 `.state` properties
are `.all` over one table, and for each of them the work is now the
eleven lemmas and nothing else. -/

theorem perPromise_step {q : PromiseObject → Bool} (hq : Hereditary q)
    (mat : Bool) (st : Step) (now : Nat) (s : ServerState) :
    PerPromise q s = true → PerPromise q (stepOf mat st now s).2 = true := by
  intro hsq
  refine perPromise_applyAll q _ s hsq ?_
  exact writesGood_handle (e := { state := s, mat := mat }) hq hsq st now

/-! ## The first catalogue entry, proved

`well_formed_promise_created_at_lte_timeout_at` is `PerPromise
qCreatedLeTimeout` by definition, so the two results above compose into
the statement the sweep could only sample: it holds at `init`, and
every step of the machine preserves it — at any length, under either
read discipline, at any instant.

The clock argument is free here because the property does not mention
`now`. That is not true of the whole catalogue, which is why
`stateHolds_clock` is a separate obligation. -/

theorem created_le_timeout_init (now : Nat) :
    Properties.well_formed_promise_created_at_lte_timeout_at now ServerState.init = true := rfl

theorem created_le_timeout_step (mat : Bool) (st : Step) (now n' : Nat) (s : ServerState) :
    Properties.well_formed_promise_created_at_lte_timeout_at now s = true →
    Properties.well_formed_promise_created_at_lte_timeout_at n' (stepOf mat st now s).2 = true :=
  perPromise_step hereditary_createdLeTimeout mat st now s

end Induction
end Abstraction
