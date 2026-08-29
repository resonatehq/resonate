import «04-theorems».«induction»

/-!  # What each handler writes

The 28-handler discharge, generic in the property. `induction.lean`
reduces "is this property inductive?" to a fixed list of obligations
about a single row, and this file is paid ONCE: nothing below mentions
a particular property.

Every proof is `split` down to the writes and then one `Hereditary`
field at each write. Nothing here reasons about the protocol. A handler
that grew a new write would fail to compile rather than pass silently —
which is the whole point of making the enumeration a theorem instead of
a docstring.

## Where the prechecks enter

Five task obligations carry a guard, and each guard is a precheck the
handler performs:

  `tHeartbeat`  needs the task ACQUIRED   — `task.heartbeat`'s test
  `tContinue`   needs the task HALTED     — `task.continue`'s 409
  `tResume`     needs the task SUSPENDED  — the resume step's match arm
  `tAddResume`  needs it NOT suspended and the rung absent — the drain's test
  `tRearm`      needs the task PENDING    — the retry step's guard

plus `settle` needing `settable`, `dead` needing the timer split, and
`cBorn` needing `!timerTargeted`. Delete any one of those checks
upstream and the corresponding obligation is what stops compiling. That
is the mechanism by which a 400 becomes a state invariant.

## Reading these proofs

Two shapes recur, both artefacts of how `do` compiles rather than of
the protocol.

`if guard then return 400` becomes a JOIN POINT: the rest of the
handler is bound to `__do_jp` and the fall-through branch is `pure () >>=
__do_jp`. `wg_guard` steps over one of those.

And the read combinators hand back their branches as `f none` / `f
(some p)` — correct but not yet reduced, since `f` is a `match` and
nothing has fired the iota rule. `dsimp only` fires it. Without that
step a subsequent `split` splits the match again and both branches come
back. -/

set_option maxHeartbeats 1000000

namespace Abstraction
namespace Induction

open AbstractModel

/-- Discharges everything that writes no row into a table this tier is
    about: messages, schedule deletions, pures, and the control
    structure between them. Task and schedule WRITES are deliberately
    absent — each one owes a named obligation. -/
macro "wg_trivial" : tactic => `(tactic| repeat (first
  | exact writesGood_pure _ _ _
  | exact writesGood_ask _ _
  | exact writesGood_getObject _ _ _
  | exact writesGood_getSchedule _ _ _
  | exact writesGood_setMessage _ _ _ _
  | exact writesGood_delSchedule _ _ _
  | apply writesGood_bind'
  | refine writesGood_ite _ _ _ _ _ ?_ ?_
  | dsimp only
  | split))

/-- One `if guard then return <error>` and the join point it leaves. -/
macro "wg_guard" : tactic => `(tactic|
  (refine writesGood_ite _ _ _ _ _ (writesGood_pure _ _ _) ?_
   refine writesGood_pureBind _ _ _ _ ?_))

section Handlers

variable {g : Q} {e : Env} (hq : Hereditary g e.state)
  (hs : PerStore g e.state = true)

include hq hs

set_option linter.unusedSectionVars false

/-! ### Birth -/

theorem writesGood_createPromise (req : ServerModel.PromiseCreateReq) (now : Nat)
    (hfresh : e.state.objects.find? (·.id == req.id) = none) :
    WritesGood g e (createPromise req now) := by
  unfold createPromise
  refine writesGood_iteH _ _ _ _ _ (fun h => ?_) (fun _ => ?_)
  · refine writesGood_bind' _ _ _ _
      (writesGood_setPromise _ _ _ _ (hq.live _ _ _ _ _ (by omega) hfresh)) ?_
    repeat' first
      | exact writesGood_pure _ _ _
      | exact writesGood_setTask _ _ _ _ (hq.tBornPending _)
      | apply writesGood_bind'
      | dsimp only
      | split
  · refine writesGood_bind' _ _ _ _
      (writesGood_setPromise _ _ _ _ (hq.dead _ _ _ _ _ (by split <;> simp) hfresh)) ?_
    repeat' first
      | exact writesGood_pure _ _ _
      | exact writesGood_setTask _ _ _ _ (hq.tBornDone)
      | apply writesGood_bind'
      | dsimp only
      | split

theorem writesGood_createIfAbsent (req : ServerModel.PromiseCreateReq) (now : Nat) :
    WritesGood g e (createIfAbsent req now) := by
  unfold createIfAbsent
  refine writesGood_afterReadObjectP hq hs _ _ _ (fun hfresh => ?_) ?_
  · exact writesGood_bind' _ _ _ _ (writesGood_createPromise hq hs _ _ hfresh)
      (writesGood_pure _ _ _)
  · intro p hp _ _; exact writesGood_pure _ _ _

/-! ### The promise API -/

theorem writesGood_promiseGet (req : ServerModel.PromiseGetReq) (now : Nat) :
    WritesGood g e (promiseGet req now) := by
  unfold promiseGet
  refine writesGood_afterReadObjectP hq hs _ _ _ (fun _ => ?_) ?_
  · exact writesGood_pure _ _ _
  · intro p hp _ _; exact writesGood_pure _ _ _

theorem writesGood_promiseCreate (req : ServerModel.PromiseCreateReq) (now : Nat) :
    WritesGood g e (promiseCreate req now) := by
  unfold promiseCreate
  wg_guard
  refine writesGood_afterReadObjectP hq hs _ _ _ (fun hfresh => ?_) ?_
  · exact writesGood_bind' _ _ _ _ (writesGood_createPromise hq hs _ _ hfresh)
      (writesGood_pure _ _ _)
  · intro p hp _ _; exact writesGood_pure _ _ _

theorem writesGood_promiseSettle (req : ServerModel.PromiseSettleReq) (now : Nat) :
    WritesGood g e (promiseSettle req now) := by
  unfold promiseSettle
  refine writesGood_iteH _ _ _ _ _ (fun _ => writesGood_pure _ _ _) (fun hset => ?_)
  refine writesGood_pureBind _ _ _ _ ?_
  refine writesGood_afterReadObject hq hs _ _ _ (fun _ => ?_) ?_
  · exact writesGood_pure _ _ _
  · intro p ho hdue hsto
    have hp : g.promise p.id p.promise = true := QObj_promise ho
    refine writesGood_iteH _ _ _ _ _ (fun hpend => ?_) (fun _ => writesGood_pure _ _ _)
    exact writesGood_bind' _ _ _ _
      (writesGood_setSettled hq _ ho _
        (hq.settle _ p.promise _ _ _ hsto (by simpa using hset) (by simpa using hpend) (hdue hpend) hp))
      (writesGood_pure _ _ _)

theorem writesGood_promiseRegisterCallback
    (req : ServerModel.PromiseRegisterCallbackReq) (now : Nat) :
    WritesGood g e (promiseRegisterCallback req now) := by
  unfold promiseRegisterCallback
  wg_guard
  wg_guard
  refine writesGood_afterReadObjectP hq hs _ _ _ (fun _ => ?_) ?_
  · exact writesGood_pure _ _ _
  · intro pa hpa _ hsto
    dsimp only
    refine writesGood_afterReadObjectP hq hs _ _ _ (fun _ => ?_) ?_
    · exact writesGood_pure _ _ _
    · intro pw hpw _ hstow
      dsimp only
      wg_guard
      wg_guard
      refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
      refine writesGood_ite _ _ _ _ _ ?_ ?_
      · exact writesGood_bind' _ _ _ _
          (writesGood_setPromise _ _ _ _ (hq.addCallback _ pa.promise _ hsto hpa)) (writesGood_pure _ _ _)
      · exact writesGood_bind' _ _ _ _ (writesGood_pure _ _ _) (writesGood_pure _ _ _)

theorem writesGood_promiseRegisterListener
    (req : ServerModel.PromiseRegisterListenerReq) (now : Nat) :
    WritesGood g e (promiseRegisterListener req now) := by
  unfold promiseRegisterListener
  refine writesGood_afterReadObjectP hq hs _ _ _ (fun _ => ?_) ?_
  · exact writesGood_pure _ _ _
  · intro pa hpa _ hsto
    dsimp only
    wg_guard
    refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
    exact writesGood_bind' _ _ _ _
      (writesGood_setPromise _ _ _ _ (hq.addListener _ pa.promise _ hsto hpa)) (writesGood_pure _ _ _)

theorem writesGood_promiseSearch (req : ServerModel.PromiseSearchReq) (now : Nat) :
    WritesGood g e (promiseSearch req now) := writesGood_pure _ _ _

/-! ### The task API -/

theorem writesGood_taskGet (req : ServerModel.TaskGetReq) (now : Nat) :
    WritesGood g e (taskGet req now) := by
  unfold taskGet
  refine writesGood_afterReadTaskObject hq hs _ _ _ (writesGood_pure _ _ _) ?_
  intro o ho _ _
  dsimp only
  split <;> exact writesGood_pure _ _ _

theorem writesGood_taskCreate (req : ServerModel.TaskCreateReq) (now : Nat) :
    WritesGood g e (taskCreate req now) := by
  simp only [taskCreate]
  refine writesGood_iteH _ _ _ _ _ (fun _ => writesGood_pure _ _ _) (fun hgd => ?_)
  refine writesGood_pureBind _ _ _ _ ?_
  -- `task.create` faces no timers, and here is why: it REQUIRES
  -- `resonate:target` and REFUSES `timerTargeted`, and the two together
  -- leave `isTimer` false. The born-dead branch below needs it.
  have hnotimer : req.action.tags.isTimer = false := by
    have h1 : ¬ ((req.action.tags.otype != .runnable) = true) := fun h => hgd (Or.inl h)
    have h2 : ¬ (req.action.tags.timerTargeted = true) := fun h => hgd (Or.inr h)
    have htgt : req.action.tags.has "resonate:target" = true := by
      simp at h1
      exact (ServerModel.otype_runnable_iff_targeted _).mp h1
    simp [ServerModel.Tags.timerTargeted, htgt] at h2
    exact h2
  refine writesGood_afterReadObject hq hs _ _ _ (fun hfresh => ?_) ?_
  · dsimp only
    refine writesGood_iteH _ _ _ _ _ (fun h => ?_) (fun _ => ?_)
    · refine writesGood_bind' _ _ _ _
        (writesGood_setPromise _ _ _ _ (hq.live _ _ _ _ _ (by omega) hfresh)) ?_
      exact writesGood_bind' _ _ _ _
        (writesGood_setTask _ _ _ _ (hq.tBornHeld _ _ _)) (writesGood_pure _ _ _)
    · refine writesGood_bind' _ _ _ _
        (writesGood_setPromise _ _ _ _
          (hq.dead _ _ _ _ _ (by simp [hnotimer]) hfresh)) ?_
      exact writesGood_bind' _ _ _ _
        (writesGood_setTask _ _ _ _ (hq.tBornDone)) (writesGood_pure _ _ _)
  · intro p hp _ hsto
    dsimp only
    wg_guard
    split
    · exact writesGood_pure _ _ _
    · rename_i t hteq
      have ht : g.task t = true := QObj_task hp hteq
      refine writesGood_ite _ _ _ _ _ (writesGood_pure _ _ _) ?_
      refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
      exact writesGood_bind' _ _ _ _
        (writesGood_setTask _ _ _ _ (hq.tAcquire t _ _ _ ht)) (writesGood_pure _ _ _)

theorem writesGood_taskAcquire (req : ServerModel.TaskAcquireReq) (now : Nat) :
    WritesGood g e (taskAcquire req now) := by
  unfold taskAcquire
  refine writesGood_afterReadTaskObject hq hs _ _ _ (writesGood_pure _ _ _) ?_
  intro p hp _ _
  dsimp only
  split
  · exact writesGood_pure _ _ _
  · rename_i t hteq
    have ht : g.task t = true := QObj_task hp hteq
    have hp' : g.promise p.id p.promise = true := QObj_promise hp
    wg_guard
    wg_guard
    wg_guard
    exact writesGood_bind' _ _ _ _
      (writesGood_setTask _ _ _ _ (hq.tAcquire t _ _ _ ht)) (writesGood_pure _ _ _)

theorem writesGood_taskFence (req : ServerModel.TaskFenceReq) (now : Nat) :
    WritesGood g e (taskFence req now) := by
  unfold taskFence
  wg_guard
  wg_guard
  refine writesGood_afterReadTaskObject hq hs _ _ _ (writesGood_pure _ _ _) ?_
  intro p hp _ _
  dsimp only
  split
  · exact writesGood_pure _ _ _
  · rename_i t hteq
    have ht : g.task t = true := QObj_task hp hteq
    have hp' : g.promise p.id p.promise = true := QObj_promise hp
    wg_guard
    wg_guard
    wg_guard
    split
    · exact writesGood_bind' _ _ _ _ (writesGood_promiseCreate hq hs _ _)
        (writesGood_pure _ _ _)
    · exact writesGood_bind' _ _ _ _ (writesGood_promiseSettle hq hs _ _)
        (writesGood_pure _ _ _)

theorem writesGood_heartbeatOne (pid : String) (ref : ServerModel.TaskRef) (now : Nat) :
    WritesGood g e (heartbeatOne pid ref now) := by
  unfold heartbeatOne
  refine writesGood_afterReadTaskObject hq hs _ _ _ (writesGood_pure _ _ _) ?_
  intro p hp _ _
  dsimp only
  split
  · exact writesGood_pure _ _ _
  · rename_i t hteq
    have ht : g.task t = true := QObj_task hp hteq
    have hp' : g.promise p.id p.promise = true := QObj_promise hp
    refine writesGood_iteH _ _ _ _ _ (fun hgd => ?_) (fun _ => writesGood_pure _ _ _)
    exact writesGood_setTask _ _ _ _ (hq.tHeartbeat t _ hgd.1 ht)

theorem writesGood_heartbeatAll (pid : String) (now : Nat) :
    ∀ refs, WritesGood g e (heartbeatAll pid now refs)
  | [] => by rw [heartbeatAll]; exact writesGood_pure _ _ _
  | r :: rs => by
      rw [heartbeatAll]
      exact writesGood_bind' _ _ _ _ (writesGood_heartbeatOne hq hs pid r now)
        (writesGood_heartbeatAll pid now rs)

theorem writesGood_taskHeartbeat (req : ServerModel.TaskHeartbeatReq) (now : Nat) :
    WritesGood g e (taskHeartbeat req now) := by
  unfold taskHeartbeat
  exact writesGood_bind' _ _ _ _ (writesGood_heartbeatAll hq hs _ _ _) (writesGood_pure _ _ _)

theorem writesGood_checkAwaited (now : Nat) :
    ∀ acts, WritesGood g e (checkAwaited now acts)
  | [] => by rw [checkAwaited]; exact writesGood_pure _ _ _
  | a :: rest => by
      rw [checkAwaited]
      refine writesGood_afterReadObjectP hq hs _ _ _ (fun _ => ?_) ?_
      · exact writesGood_pure _ _ _
      · intro p hp _ hsto
        dsimp only
        refine writesGood_ite _ _ _ _ _ (writesGood_pure _ _ _) ?_
        refine writesGood_bind' _ _ _ _ (writesGood_checkAwaited now rest) ?_
        wg_trivial

/-- The one list recursion that writes a promise per element. -/
theorem writesGood_registerAwaited (awaiter : ServerModel.Ident) (now : Nat) :
    ∀ acts, WritesGood g e (registerAwaited awaiter now acts)
  | [] => by rw [registerAwaited]; exact writesGood_pure _ _ _
  | a :: rest => by
      rw [registerAwaited]
      refine writesGood_afterReadObjectP hq hs _ _ _ (fun _ => ?_) ?_
      · dsimp only
        exact writesGood_bind' _ _ _ _ (writesGood_pure _ _ _)
          (writesGood_registerAwaited awaiter now rest)
      · intro p hp _ hsto
        dsimp only
        exact writesGood_bind' _ _ _ _
          (writesGood_setPromise _ _ _ _ (hq.addCallback _ p.promise _ hsto hp))
          (writesGood_registerAwaited awaiter now rest)

theorem writesGood_taskSuspend (req : ServerModel.TaskSuspendReq) (now : Nat) :
    WritesGood g e (taskSuspend req now) := by
  unfold taskSuspend
  wg_guard
  wg_guard
  wg_guard
  wg_guard
  refine writesGood_afterReadTaskObject hq hs _ _ _ (writesGood_pure _ _ _) ?_
  intro p hp _ _
  dsimp only
  split
  · exact writesGood_pure _ _ _
  · rename_i t hteq
    have ht : g.task t = true := QObj_task hp hteq
    have hp' : g.promise p.id p.promise = true := QObj_promise hp
    wg_guard
    wg_guard
    wg_guard
    refine writesGood_bind' _ _ _ _ (writesGood_checkAwaited hq hs _ _) ?_
    split
    · exact writesGood_pure _ _ _
    · exact writesGood_bind' _ _ _ _
        (writesGood_setTask _ _ _ _ (hq.tClearResumes t ht)) (writesGood_pure _ _ _)
    · exact writesGood_bind' _ _ _ _ (writesGood_registerAwaited hq hs _ _ _)
        (writesGood_bind' _ _ _ _
          (writesGood_setTask _ _ _ _ (hq.tSuspend t ht)) (writesGood_pure _ _ _))

theorem writesGood_taskFulfill (req : ServerModel.TaskFulfillReq) (now : Nat) :
    WritesGood g e (taskFulfill req now) := by
  unfold taskFulfill
  refine writesGood_iteH _ _ _ _ _ (fun _ => writesGood_pure _ _ _) (fun hset => ?_)
  refine writesGood_pureBind _ _ _ _ ?_
  refine writesGood_afterReadTaskObject hq hs _ _ _ (writesGood_pure _ _ _) ?_
  intro p hp hdue hsto
  dsimp only
  split
  · exact writesGood_pure _ _ _
  · rename_i t hteq
    have ht : g.task t = true := QObj_task hp hteq
    have hp' : g.promise p.id p.promise = true := QObj_promise hp
    wg_guard
    refine writesGood_iteH _ _ _ _ _ (fun _ => writesGood_pure _ _ _) (fun hpend => ?_)
    refine writesGood_pureBind _ _ _ _ ?_
    wg_guard
    exact writesGood_bind' _ _ _ _
      (writesGood_setSettled hq _ hp _
        (hq.settle _ p.promise _ _ _ hsto (by simpa using hset) (by simpa using hpend)
          (hdue (by simpa using hpend)) hp'))
      (writesGood_pure _ _ _)

theorem writesGood_taskRelease (req : ServerModel.TaskReleaseReq) (now : Nat) :
    WritesGood g e (taskRelease req now) := by
  unfold taskRelease
  refine writesGood_afterReadTaskObject hq hs _ _ _ (writesGood_pure _ _ _) ?_
  intro p hp _ _
  dsimp only
  split
  · exact writesGood_pure _ _ _
  · rename_i t hteq
    have ht : g.task t = true := QObj_task hp hteq
    have hp' : g.promise p.id p.promise = true := QObj_promise hp
    wg_guard
    wg_guard
    wg_guard
    exact writesGood_bind' _ _ _ _
      (writesGood_setTask _ _ _ _ (hq.tRepend t _ ht)) (writesGood_pure _ _ _)

theorem writesGood_taskHalt (req : ServerModel.TaskHaltReq) (now : Nat) :
    WritesGood g e (taskHalt req now) := by
  unfold taskHalt
  refine writesGood_afterReadTaskObject hq hs _ _ _ (writesGood_pure _ _ _) ?_
  intro p hp _ _
  dsimp only
  split
  · exact writesGood_pure _ _ _
  · rename_i t hteq
    have ht : g.task t = true := QObj_task hp hteq
    have hp' : g.promise p.id p.promise = true := QObj_promise hp
    wg_guard
    wg_guard
    exact writesGood_bind' _ _ _ _
      (writesGood_setTask _ _ _ _ (hq.tHalt t ht)) (writesGood_pure _ _ _)

theorem writesGood_taskContinue (req : ServerModel.TaskContinueReq) (now : Nat) :
    WritesGood g e (taskContinue req now) := by
  unfold taskContinue
  refine writesGood_afterReadTaskObject hq hs _ _ _ (writesGood_pure _ _ _) ?_
  intro p ho _ _
  dsimp only
  split
  · exact writesGood_pure _ _ _
  · rename_i t hteq
    have ht : g.task t = true := QObj_task ho hteq
    have hp : g.promise p.id p.promise = true := QObj_promise ho
    refine writesGood_iteH _ _ _ _ _ (fun _ => writesGood_pure _ _ _) (fun hhalt => ?_)
    refine writesGood_pureBind _ _ _ _ ?_
    wg_guard
    exact writesGood_bind' _ _ _ _
      (writesGood_setTask _ _ _ _ (hq.tContinue t _ (by simpa using hhalt) ht))
      (writesGood_pure _ _ _)

theorem writesGood_taskSearch (req : ServerModel.TaskSearchReq) (now : Nat) :
    WritesGood g e (taskSearch req now) := writesGood_pure _ _ _

/-! ### The schedule API -/

theorem writesGood_scheduleGet (req : ServerModel.ScheduleGetReq) (now : Nat) :
    WritesGood g e (scheduleGet req now) := by
  unfold scheduleGet; wg_trivial

theorem writesGood_scheduleCreate (req : ServerModel.ScheduleCreateReq) (now : Nat) :
    WritesGood g e (scheduleCreate req now) := by
  unfold scheduleCreate
  refine writesGood_iteH _ _ _ _ _ (fun _ => writesGood_pure _ _ _) (fun htt => ?_)
  refine writesGood_pureBind _ _ _ _ ?_
  refine writesGood_bind' _ _ _ _ (writesGood_getSchedule _ _ _) ?_
  split
  · exact writesGood_pure _ _ _
  · exact writesGood_bind' _ _ _ _
      (writesGood_setSchedule _ _ _ (hq.cBorn _ _ _ _ _ _ _ (by simpa using htt)))
      (writesGood_pure _ _ _)

theorem writesGood_scheduleDelete (req : ServerModel.ScheduleDeleteReq) (now : Nat) :
    WritesGood g e (scheduleDelete req now) := by
  unfold scheduleDelete; wg_trivial

theorem writesGood_scheduleSearch (req : ServerModel.ScheduleSearchReq) (now : Nat) :
    WritesGood g e (scheduleSearch req now) := writesGood_pure _ _ _

/-! ### The six internal steps

These read through `withMat`, so they take the `withMat` combinators.
`touchPromise` is `withMat true`, `viewPromise` is `withMat false`, and
the difference is invisible to this argument — which is the formal
content of "the read discipline is not a protocol decision". -/

theorem writesGood_processPromiseTimeout (req : ServerModel.PromiseTimeoutReq) (now : Nat) :
    WritesGood g e (Internal.processPromiseTimeout req now) := by
  unfold Internal.processPromiseTimeout touchObject
  refine writesGood_afterMatReadObjectP hq true hs _ _ _ (fun _ => ?_) ?_
  · exact writesGood_pure _ _ _
  · intro p hp _ _; exact writesGood_pure _ _ _

theorem writesGood_resumeOne (awaited awaiter : ServerModel.Ident) (now : Nat) :
    WritesGood g e (Internal.resumeOne awaited awaiter now) := by
  unfold Internal.resumeOne touchTaskObject
  refine writesGood_afterMatReadTaskObject hq true hs _ _ _
    (writesGood_pure _ _ _) ?_
  intro o ho _ _
  dsimp only
  split
  · exact writesGood_pure _ _ _
  · rename_i t hteq
    have ht : g.task t = true := QObj_task ho hteq
    split
    · rename_i hsusp
      exact writesGood_setTask _ _ _ _ (hq.tResume t _ _ hsusp ht)
    all_goals
      first
        | exact writesGood_pure _ _ _
        | (rename_i hst
           refine writesGood_iteH _ _ _ _ _ (fun hc => ?_) (fun _ => writesGood_pure _ _ _)
           exact writesGood_setTask _ _ _ _
             (hq.tAddResume t _ (by simp [hst]) (by simp [hst]) (by simpa using hc) ht))

theorem writesGood_processCallback (req : ServerModel.PromiseRegisterCallbackReq) (now : Nat) :
    WritesGood g e (Internal.processCallback req now) := by
  unfold Internal.processCallback touchObject
  refine writesGood_afterMatReadObjectP hq true hs _ _ _ (fun _ => ?_) ?_
  · exact writesGood_pure _ _ _
  · intro p hp _ hsto
    dsimp only
    refine writesGood_iteH _ _ _ _ _ (fun _ => writesGood_pure _ _ _) (fun hns => ?_)
    refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
    exact writesGood_bind' _ _ _ _
      (writesGood_setPromise _ _ _ _ (hq.dropCallback _ p.promise _ hsto (by simpa using hns) hp))
      (writesGood_resumeOne hq hs _ _ _)

theorem writesGood_processListener (req : ServerModel.PromiseRegisterListenerReq) (now : Nat) :
    WritesGood g e (Internal.processListener req now) := by
  unfold Internal.processListener touchObject
  refine writesGood_afterMatReadObjectP hq true hs _ _ _ (fun _ => ?_) ?_
  · exact writesGood_pure _ _ _
  · intro p hp _ hsto
    dsimp only
    refine writesGood_iteH _ _ _ _ _ (fun _ => writesGood_pure _ _ _) (fun hns => ?_)
    refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
    exact writesGood_bind' _ _ _ _
      (writesGood_setPromise _ _ _ _ (hq.dropListener _ p.promise _ hsto (by simpa using hns) hp))
      (writesGood_setMessage _ _ _ _)

theorem writesGood_processLeaseTimeout (req : ServerModel.TaskLeaseTimeoutReq) (now : Nat) :
    WritesGood g e (Internal.processLeaseTimeout req now) := by
  unfold Internal.processLeaseTimeout viewTaskObject
  refine writesGood_afterMatReadTaskObject hq false hs _ _ _
    (writesGood_pure _ _ _) ?_
  intro o ho _ _
  dsimp only
  split
  · exact writesGood_pure _ _ _
  · rename_i t hteq
    have hgt : g.task t = true := QObj_task ho hteq
    split
    · exact writesGood_pure _ _ _
    · refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
      refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
      exact writesGood_setTask _ _ _ _ (hq.tRepend t _ hgt)

theorem writesGood_processRetryTimeout (req : ServerModel.TaskRetryTimeoutReq) (now : Nat) :
    WritesGood g e (Internal.processRetryTimeout req now) := by
  unfold Internal.processRetryTimeout viewTaskObject
  refine writesGood_afterMatReadTaskObject hq false hs _ _ _
    (writesGood_pure _ _ _) ?_
  intro o ho _ _
  dsimp only
  split
  · exact writesGood_pure _ _ _
  · rename_i t hteq
    have hgt : g.task t = true := QObj_task ho hteq
    split
    · exact writesGood_pure _ _ _
    · refine writesGood_iteH _ _ _ _ _ (fun hgd => ?_) (fun _ => writesGood_pure _ _ _)
      refine writesGood_ite _ _ _ _ _ ?_ (writesGood_pure _ _ _)
      refine writesGood_bind' _ _ _ _ (writesGood_ask _ _) ?_
      exact writesGood_bind' _ _ _ _
        (writesGood_setTask _ _ _ _ (hq.tRearm t _ hgd.1 hgt))
        (writesGood_setMessage _ _ _ _)

theorem writesGood_fireOccurrence (c : ServerModel.Schedule) (t : Nat) :
    WritesGood g e (Internal.fireOccurrence c t) := by
  unfold Internal.fireOccurrence
  exact writesGood_createIfAbsent hq hs _ _

theorem writesGood_fireAll (c : ServerModel.Schedule) :
    ∀ ts, WritesGood g e (Internal.fireAll c ts)
  | [] => by rw [Internal.fireAll]; exact writesGood_pure _ _ _
  | t :: ts => by
      rw [Internal.fireAll]
      exact writesGood_bind' _ _ _ _ (writesGood_fireOccurrence hq hs c t)
        (writesGood_fireAll c ts)

theorem writesGood_processSchedule (req : ServerModel.ScheduleTimeoutReq) (now : Nat) :
    WritesGood g e (Internal.processSchedule req now) := by
  unfold Internal.processSchedule
  refine writesGood_bind' _ _ _ _ (writesGood_getSchedule _ _ _) ?_
  rw [getSchedule_fst]
  split
  · exact writesGood_pure _ _ _
  · rename_i c hc
    refine writesGood_bind' _ _ _ _ (writesGood_fireAll hq hs _ _) ?_
    split
    · exact writesGood_setSchedule _ _ _ (hq.cAdvance c _ (getSchedule_sound hs hc))
    · exact writesGood_pure _ _ _

/-! ### And the dispatcher

The theorem the enumeration was for. Every step of the machine — 21
requests, 6 internal jobs, `idle` — writes only rows satisfying `g`,
given the obligations and a store that already satisfies `g`
everywhere. A step added without a case here does not compile. -/

theorem writesGood_handleExternal (rq : Equivalence.Request) (now : Nat) :
    WritesGood g e (handleExternal rq now) := by
  cases rq with
  | promiseGet              req => exact writesGood_map _ _ _ _ (writesGood_promiseGet hq hs req now)
  | promiseCreate           req => exact writesGood_map _ _ _ _ (writesGood_promiseCreate hq hs req now)
  | promiseSettle           req => exact writesGood_map _ _ _ _ (writesGood_promiseSettle hq hs req now)
  | promiseRegisterCallback req => exact writesGood_map _ _ _ _ (writesGood_promiseRegisterCallback hq hs req now)
  | promiseRegisterListener req => exact writesGood_map _ _ _ _ (writesGood_promiseRegisterListener hq hs req now)
  | promiseSearch           req => exact writesGood_map _ _ _ _ (writesGood_promiseSearch hq hs req now)
  | scheduleGet             req => exact writesGood_map _ _ _ _ (writesGood_scheduleGet hq hs req now)
  | scheduleCreate          req => exact writesGood_map _ _ _ _ (writesGood_scheduleCreate hq hs req now)
  | scheduleDelete          req => exact writesGood_map _ _ _ _ (writesGood_scheduleDelete hq hs req now)
  | scheduleSearch          req => exact writesGood_map _ _ _ _ (writesGood_scheduleSearch hq hs req now)
  | taskGet                 req => exact writesGood_map _ _ _ _ (writesGood_taskGet hq hs req now)
  | taskCreate              req => exact writesGood_map _ _ _ _ (writesGood_taskCreate hq hs req now)
  | taskAcquire             req => exact writesGood_map _ _ _ _ (writesGood_taskAcquire hq hs req now)
  | taskFence               req => exact writesGood_map _ _ _ _ (writesGood_taskFence hq hs req now)
  | taskHeartbeat           req => exact writesGood_map _ _ _ _ (writesGood_taskHeartbeat hq hs req now)
  | taskSuspend             req => exact writesGood_map _ _ _ _ (writesGood_taskSuspend hq hs req now)
  | taskFulfill             req => exact writesGood_map _ _ _ _ (writesGood_taskFulfill hq hs req now)
  | taskRelease             req => exact writesGood_map _ _ _ _ (writesGood_taskRelease hq hs req now)
  | taskHalt                req => exact writesGood_map _ _ _ _ (writesGood_taskHalt hq hs req now)
  | taskContinue            req => exact writesGood_map _ _ _ _ (writesGood_taskContinue hq hs req now)
  | taskSearch              req => exact writesGood_map _ _ _ _ (writesGood_taskSearch hq hs req now)

theorem writesGood_handleInternal (rq : InternalStep) (now : Nat) :
    WritesGood g e (handleInternal rq now) := by
  cases rq with
  | promiseTimeout   req => exact writesGood_processPromiseTimeout hq hs req now
  | callback         req => exact writesGood_processCallback hq hs req now
  | listener         req => exact writesGood_processListener hq hs req now
  | taskLeaseTimeout req => exact writesGood_processLeaseTimeout hq hs req now
  | taskRetryTimeout req => exact writesGood_processRetryTimeout hq hs req now
  | scheduleTimeout  req => exact writesGood_processSchedule hq hs req now

theorem writesGood_handle (st : Step) (now : Nat) : WritesGood g e (handle st now) := by
  cases st with
  | external rq => exact writesGood_handleExternal hq hs rq now
  | internal rq =>
      exact writesGood_bind' _ _ _ _ (writesGood_handleInternal hq hs rq now)
        (writesGood_pure _ _ _)
  | idle => exact writesGood_pure _ _ _

end Handlers

/-! ## The tier theorem

The obligations buy inductiveness over the whole machine. This is the
reusable result: every `.state` entry that is an `.all` over one table
now costs its obligations and nothing else. -/

theorem perStore_step {g : Q} (mat : Bool) (st : Step) (now : Nat) (s : ServerState)
    (hq : Hereditary g s) :
    PerStore g s = true → PerStore g (stepOf mat st now s).2 = true := by
  intro hsq
  refine perStore_applyAll g _ s hsq ?_
  exact writesGood_handle (e := { state := s, mat := mat }) hq hsq st now

end Induction
end Abstraction
