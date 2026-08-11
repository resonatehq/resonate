import «03-concrete».«p».«P-01-promise.get»
import «03-concrete».«p».«P-04-promise.register_callback»
import «03-concrete».«p».«P-05-promise.register_listener»
import «03-concrete».«p».«P-06-promise.search»
import «03-concrete».«p».«S-01-schedule.get»
import «03-concrete».«p».«S-02-schedule.create»
import «03-concrete».«p».«S-03-schedule.delete»
import «03-concrete».«p».«S-04-schedule.search»
import «03-concrete».«p».«02-timeouts»
import «03-concrete».«p».«03-resume»
import «03-concrete».«p».«T-01-task.get»
import «03-concrete».«p».«T-02-task.create»
import «03-concrete».«p».«T-03-task.acquire»
import «03-concrete».«p».«T-04-task.fence»
import «03-concrete».«p».«T-05-task.heartbeat»
import «03-concrete».«p».«T-06-task.suspend»
import «03-concrete».«p».«T-07-task.fulfill»
import «03-concrete».«p».«T-08-task.release»
import «03-concrete».«p».«T-09-task.halt»
import «03-concrete».«p».«T-10-task.continue»
import «03-concrete».«p».«T-11-task.search»
import «03-concrete».«m».«P-01-promise.get»
import «03-concrete».«m».«P-04-promise.register_callback»
import «03-concrete».«m».«P-05-promise.register_listener»
import «03-concrete».«m».«P-06-promise.search»
import «03-concrete».«m».«S-01-schedule.get»
import «03-concrete».«m».«S-02-schedule.create»
import «03-concrete».«m».«S-03-schedule.delete»
import «03-concrete».«m».«S-04-schedule.search»
import «03-concrete».«m».«02-timeouts»
import «03-concrete».«m».«03-resume»
import «03-concrete».«m».«T-01-task.get»
import «03-concrete».«m».«T-02-task.create»
import «03-concrete».«m».«T-03-task.acquire»
import «03-concrete».«m».«T-04-task.fence»
import «03-concrete».«m».«T-05-task.heartbeat»
import «03-concrete».«m».«T-06-task.suspend»
import «03-concrete».«m».«T-07-task.fulfill»
import «03-concrete».«m».«T-08-task.release»
import «03-concrete».«m».«T-09-task.halt»
import «03-concrete».«m».«T-10-task.continue»
import «03-concrete».«m».«T-11-task.search»

/-!  # Equivalence — the trace framework

The system as a trace, following `03stable/04-trace.lean`: a trace is
an infinite sequence of state-action pairs — the state the machine is
in, the request that hits it there, the response it externalizes, and
the instant on the wall clock. A trace is VALID iff every two
subsequent pairs are connected by a step and the clock never runs
backwards. Both machines share one alphabet (`Request`/`Response` —
the wire types are common), one state type, and one validity
definition, instantiated with each machine's step function: `ValidP`
and `ValidM`.

The alphabet's internal (τ) requests are OBLIGATION-GUARDED: a timeout
may fire only if a due obligation record is on the books, a resume
only for a pair in the deferred set. This matters more here than it
did for one machine: the obligation records are the τs' enabledness
state, and the two machines move them at different instants — -m's
touch discharges a promise timeout (and defers its resumes) at the
moment of observation, -p's τ does so later. A τ whose obligation has
already been discharged is a silent no-op, which is exactly what lets
one machine stutter where the other has already acted.  -/

namespace Equivalence

open ServerModel

deriving instance BEq for ServerModel.Value
deriving instance BEq for ServerModel.PromiseRecord
deriving instance BEq for ServerModel.TaskRecord
deriving instance BEq for ServerModel.PromiseObject
deriving instance BEq for ServerModel.TaskObject
deriving instance BEq for ServerModel.Schedule
deriving instance BEq for ServerModel.ResumeReq
deriving instance BEq for ServerModel.PromiseTimeout
deriving instance BEq for ServerModel.TaskTimeout
deriving instance BEq for ServerModel.ScheduleTimeout
deriving instance BEq for ServerModel.Message
deriving instance BEq for ServerModel.OutboxEntry
deriving instance BEq for ServerModel.ServerConfig
deriving instance BEq for ServerModel.ServerState
deriving instance BEq for ServerModel.ResumeRes
deriving instance BEq for ServerModel.PromiseGetRes
deriving instance BEq for ServerModel.PromiseCreateRes
deriving instance BEq for ServerModel.PromiseSettleRes
deriving instance BEq for ServerModel.PromiseRegisterCallbackRes
deriving instance BEq for ServerModel.PromiseRegisterListenerRes
deriving instance BEq for ServerModel.PromiseSearchRes
deriving instance BEq for ServerModel.ScheduleGetRes
deriving instance BEq for ServerModel.ScheduleCreateRes
deriving instance BEq for ServerModel.ScheduleDeleteRes
deriving instance BEq for ServerModel.ScheduleSearchRes
deriving instance BEq for ServerModel.TaskGetRes
deriving instance BEq for ServerModel.TaskCreateRes
deriving instance BEq for ServerModel.TaskAcquireRes
deriving instance BEq for ServerModel.TaskFenceInnerRes
deriving instance BEq for ServerModel.TaskFenceRes
deriving instance BEq for ServerModel.TaskHeartbeatRes
deriving instance BEq for ServerModel.TaskSuspendRes
deriving instance BEq for ServerModel.TaskFulfillRes
deriving instance BEq for ServerModel.TaskReleaseRes
deriving instance BEq for ServerModel.TaskHaltRes
deriving instance BEq for ServerModel.TaskContinueRes
deriving instance BEq for ServerModel.TaskSearchRes

/-- The protocol alphabet: every transition either machine can take. -/
inductive Request
  | promiseGet              (req : PromiseGetReq)
  | promiseCreate           (req : PromiseCreateReq)
  | promiseSettle           (req : PromiseSettleReq)
  | promiseRegisterCallback (req : PromiseRegisterCallbackReq)
  | promiseRegisterListener (req : PromiseRegisterListenerReq)
  | promiseSearch           (req : PromiseSearchReq)
  | scheduleGet             (req : ScheduleGetReq)
  | scheduleCreate          (req : ScheduleCreateReq)
  | scheduleDelete          (req : ScheduleDeleteReq)
  | scheduleSearch          (req : ScheduleSearchReq)
  | taskGet                 (req : TaskGetReq)
  | taskCreate              (req : TaskCreateReq)
  | taskAcquire             (req : TaskAcquireReq)
  | taskFence               (req : TaskFenceReq)
  | taskHeartbeat           (req : TaskHeartbeatReq)
  | taskSuspend             (req : TaskSuspendReq)
  | taskFulfill             (req : TaskFulfillReq)
  | taskRelease             (req : TaskReleaseReq)
  | taskHalt                (req : TaskHaltReq)
  | taskContinue            (req : TaskContinueReq)
  | taskSearch              (req : TaskSearchReq)
  | τPromiseTimeout         (id : String)
  | τTaskRetryTimeout       (id : String)
  | τTaskLeaseTimeout       (id : String)
  | τScheduleTimeout        (id : String)
  | τResume                 (req : ResumeReq)
  | idle
  deriving Repr

/-- τ-steps and `idle` are silent: nobody outside is listening. The
    drain's oracle-facing `ResumeOutcome` rides along as `.resume`; it
    is internal all the same. -/
inductive Response
  | promiseGet              (res : PromiseGetRes)
  | promiseCreate           (res : PromiseCreateRes)
  | promiseSettle           (res : PromiseSettleRes)
  | promiseRegisterCallback (res : PromiseRegisterCallbackRes)
  | promiseRegisterListener (res : PromiseRegisterListenerRes)
  | promiseSearch           (res : PromiseSearchRes)
  | scheduleGet             (res : ScheduleGetRes)
  | scheduleCreate          (res : ScheduleCreateRes)
  | scheduleDelete          (res : ScheduleDeleteRes)
  | scheduleSearch          (res : ScheduleSearchRes)
  | taskGet                 (res : TaskGetRes)
  | taskCreate              (res : TaskCreateRes)
  | taskAcquire             (res : TaskAcquireRes)
  | taskFence               (res : TaskFenceRes)
  | taskHeartbeat           (res : TaskHeartbeatRes)
  | taskSuspend             (res : TaskSuspendRes)
  | taskFulfill             (res : TaskFulfillRes)
  | taskRelease             (res : TaskReleaseRes)
  | taskHalt                (res : TaskHaltRes)
  | taskContinue            (res : TaskContinueRes)
  | taskSearch              (res : TaskSearchRes)
  | resume                  (res : ResumeRes)
  | τ
  deriving Repr, BEq

/-- External requests are the client-visible ones; τs and `idle` are
    the machine's own business. Observation quantifies over the former
    and is silent on the latter. -/
def Request.isExternal : Request → Bool
  | .τPromiseTimeout _   => false
  | .τTaskRetryTimeout _ => false
  | .τTaskLeaseTimeout _ => false
  | .τScheduleTimeout _  => false
  | .τResume _           => false
  | .idle                => false
  | _                    => true

/-- The projected machine's step: dispatch to the -p handlers. -/
def handleP (rq : Request) (now : Nat) : M Response :=
  match rq with
  | .promiseGet req              => Response.promiseGet <$> promiseGet req now
  | .promiseCreate req           => Response.promiseCreate <$> promiseCreate req now
  | .promiseSettle req           => Response.promiseSettle <$> promiseSettle req now
  | .promiseRegisterCallback req => Response.promiseRegisterCallback <$> promiseRegisterCallback req now
  | .promiseRegisterListener req => Response.promiseRegisterListener <$> promiseRegisterListener req now
  | .promiseSearch req           => Response.promiseSearch <$> promiseSearch req now
  | .scheduleGet req             => Response.scheduleGet <$> scheduleGet req now
  | .scheduleCreate req          => Response.scheduleCreate <$> scheduleCreate req now
  | .scheduleDelete req          => Response.scheduleDelete <$> scheduleDelete req now
  | .scheduleSearch req          => Response.scheduleSearch <$> scheduleSearch req now
  | .taskGet req                 => Response.taskGet <$> taskGet req now
  | .taskCreate req              => Response.taskCreate <$> taskCreate req now
  | .taskAcquire req             => Response.taskAcquire <$> taskAcquire req now
  | .taskFence req               => Response.taskFence <$> taskFence req now
  | .taskHeartbeat req           => Response.taskHeartbeat <$> taskHeartbeat req now
  | .taskSuspend req             => Response.taskSuspend <$> taskSuspend req now
  | .taskFulfill req             => Response.taskFulfill <$> taskFulfill req now
  | .taskRelease req             => Response.taskRelease <$> taskRelease req now
  | .taskHalt req                => Response.taskHalt <$> taskHalt req now
  | .taskContinue req            => Response.taskContinue <$> taskContinue req now
  | .taskSearch req              => Response.taskSearch <$> taskSearch req now
  | .τPromiseTimeout id => do
      if (← get).promiseTimeouts.any (fun e => e.id == id && decide (e.timeout ≤ now)) then
        Timeouts.onPromiseTimeout id now
      return .τ
  | .τTaskRetryTimeout id => do
      Timeouts.onTaskRetryTimeout id now   -- self-guarded on its due record
      return .τ
  | .τTaskLeaseTimeout id => do
      Timeouts.onTaskLeaseTimeout id now   -- self-guarded on its due record
      return .τ
  | .τScheduleTimeout id => do
      if (← get).scheduleTimeouts.any (fun e => e.id == id && decide (e.timeout ≤ now)) then
        Timeouts.onScheduleTimeout id now
      return .τ
  | .τResume req => do
      if (← get).deferred.any (fun d => d.awaited == req.awaited && d.awaiter == req.awaiter) then
        undefer req
        Response.resume <$> onResume req now
      else
        return .τ
  | .idle => return .τ

/-- The materialized machine's step: dispatch to the -m handlers. -/
def handleM (rq : Request) (now : Nat) : M Response :=
  match rq with
  | .promiseGet req              => Response.promiseGet <$> Materialized.promiseGet req now
  | .promiseCreate req           => Response.promiseCreate <$> Materialized.promiseCreate req now
  | .promiseSettle req           => Response.promiseSettle <$> Materialized.promiseSettle req now
  | .promiseRegisterCallback req => Response.promiseRegisterCallback <$> Materialized.promiseRegisterCallback req now
  | .promiseRegisterListener req => Response.promiseRegisterListener <$> Materialized.promiseRegisterListener req now
  | .promiseSearch req           => Response.promiseSearch <$> Materialized.promiseSearch req now
  | .scheduleGet req             => Response.scheduleGet <$> Materialized.scheduleGet req now
  | .scheduleCreate req          => Response.scheduleCreate <$> Materialized.scheduleCreate req now
  | .scheduleDelete req          => Response.scheduleDelete <$> Materialized.scheduleDelete req now
  | .scheduleSearch req          => Response.scheduleSearch <$> Materialized.scheduleSearch req now
  | .taskGet req                 => Response.taskGet <$> Materialized.taskGet req now
  | .taskCreate req              => Response.taskCreate <$> Materialized.taskCreate req now
  | .taskAcquire req             => Response.taskAcquire <$> Materialized.taskAcquire req now
  | .taskFence req               => Response.taskFence <$> Materialized.taskFence req now
  | .taskHeartbeat req           => Response.taskHeartbeat <$> Materialized.taskHeartbeat req now
  | .taskSuspend req             => Response.taskSuspend <$> Materialized.taskSuspend req now
  | .taskFulfill req             => Response.taskFulfill <$> Materialized.taskFulfill req now
  | .taskRelease req             => Response.taskRelease <$> Materialized.taskRelease req now
  | .taskHalt req                => Response.taskHalt <$> Materialized.taskHalt req now
  | .taskContinue req            => Response.taskContinue <$> Materialized.taskContinue req now
  | .taskSearch req              => Response.taskSearch <$> Materialized.taskSearch req now
  | .τPromiseTimeout id => do
      if (← get).promiseTimeouts.any (fun e => e.id == id && decide (e.timeout ≤ now)) then
        Materialized.Timeouts.onPromiseTimeout id now
      return .τ
  | .τTaskRetryTimeout id => do
      Materialized.Timeouts.onTaskRetryTimeout id now
      return .τ
  | .τTaskLeaseTimeout id => do
      Materialized.Timeouts.onTaskLeaseTimeout id now
      return .τ
  | .τScheduleTimeout id => do
      if (← get).scheduleTimeouts.any (fun e => e.id == id && decide (e.timeout ≤ now)) then
        Materialized.Timeouts.onScheduleTimeout id now
      return .τ
  | .τResume req => do
      if (← get).deferred.any (fun d => d.awaited == req.awaited && d.awaiter == req.awaiter) then
        undefer req
        Response.resume <$> Materialized.onResume req now
      else
        return .τ
  | .idle => return .τ

/-- One protocol step as a total function. -/
def stepOf (handle : Request → Nat → M Response)
    (rq : Request) (now : Nat) (s : ServerState) : Response × ServerState :=
  Id.run ((handle rq now).run s)

/-- A state, the request that hits it there, the response the machine
    externalizes for it, and the instant on the wall clock. -/
structure StateAction where
  state : ServerState
  req   : Request
  res   : Response
  now   : Nat

/-- A trace is an infinite sequence of state-action pairs. -/
abbrev Trace := Nat → StateAction

/-- **Validity**, parameterized by the machine: two subsequent pairs
    are connected by a step — the recorded response and the successor's
    state are exactly the handler's output — and the clock never runs
    backwards. -/
def Valid (handle : Request → Nat → M Response) (tr : Trace) : Prop :=
  ∀ t : Nat,
    (tr t).res = (stepOf handle (tr t).req (tr t).now (tr t).state).1 ∧
    (tr (t + 1)).state = (stepOf handle (tr t).req (tr t).now (tr t).state).2 ∧
    (tr t).now ≤ (tr (t + 1)).now

def ValidP : Trace → Prop := Valid handleP
def ValidM : Trace → Prop := Valid handleM

/-! ### Finite instruments

Executable counterparts for `decide`: run a finite request script,
collect responses and the final state.  -/

/-- Run a finite script of (request, instant) steps. -/
def runFin (handle : Request → Nat → M Response)
    (w : List (Request × Nat)) : List Response × ServerState :=
  Id.run ((w.mapM (fun (rq, n) => handle rq n)).run ServerState.init)

def eqSet [BEq α] (a b : List α) : Bool :=
  a.all b.contains && b.all a.contains

/-- State equality up to list order (the components are keyed). -/
def stateEq (a b : ServerState) : Bool :=
  a.config == b.config
    && eqSet a.promises b.promises
    && eqSet a.tasks b.tasks
    && eqSet a.schedules b.schedules
    && eqSet a.deferred b.deferred
    && eqSet a.promiseTimeouts b.promiseTimeouts
    && eqSet a.taskTimeouts b.taskTimeouts
    && eqSet a.scheduleTimeouts b.scheduleTimeouts
    && eqSet a.outbox b.outbox

/-- Quiescence: discharge the settlement pipeline — every armed, due
    promise timeout, then every deferred resume. A wake arms no new due
    promise timeout, so one round is the fixpoint. Dispatch obligations
    (retry, lease) are pure delivery policy and stay put. -/
def quiesce (now : Nat) : M Unit := do
  let ids := ((← get).promiseTimeouts.filter (fun e => decide (e.timeout ≤ now))).map (·.id)
  for id in ids do
    Timeouts.onPromiseTimeout id now
  drain now

def quiesced (now : Nat) (s : ServerState) : ServerState :=
  (Id.run ((quiesce now).run s)).2

end Equivalence
