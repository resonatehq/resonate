import «02-actions-p».«P-01-promise.get»
import «02-actions-p».«P-04-promise.register_callback»
import «02-actions-p».«P-05-promise.register_listener»
import «02-actions-p».«02-timeouts»
import «02-actions-p».«03-resume»
import «02-actions-p».«T-01-task.get»
import «02-actions-p».«T-02-task.create»
import «02-actions-p».«T-03-task.acquire»
import «02-actions-p».«T-04-task.fence»
import «02-actions-p».«T-05-task.heartbeat»
import «02-actions-p».«T-06-task.suspend»
import «02-actions-p».«T-07-task.fulfill»
import «02-actions-p».«T-08-task.release»
import «02-actions-p».«T-09-task.halt»
import «02-actions-p».«T-10-task.continue»
import «02-actions-m».«P-01-promise.get»
import «02-actions-m».«P-04-promise.register_callback»
import «02-actions-m».«P-05-promise.register_listener»
import «02-actions-m».«02-timeouts»
import «02-actions-m».«03-resume»
import «02-actions-m».«T-01-task.get»
import «02-actions-m».«T-02-task.create»
import «02-actions-m».«T-03-task.acquire»
import «02-actions-m».«T-04-task.fence»
import «02-actions-m».«T-05-task.heartbeat»
import «02-actions-m».«T-06-task.suspend»
import «02-actions-m».«T-07-task.fulfill»
import «02-actions-m».«T-08-task.release»
import «02-actions-m».«T-09-task.halt»
import «02-actions-m».«T-10-task.continue»

/-!  # Equivalence — the driver

The twin machines share one state type and one wire surface; they
differ only in read discipline: -p serves the projected view and writes
nothing, -m persists the projection at the moment of observation. The
equivalence claim:

> For every trace of requests and internal transitions (with a monotone
> clock), the two machines return IDENTICAL responses, and their final
> states are equal after τ-CLOSURE — firing the promise-timeout
> transition on every promise.

Responses need no closure: observations agree step by step. States
differ mid-trace by exactly the set of due timeouts -p has observed but
not yet persisted — the closure discharges precisely that lag, on both
sides (it is idempotent on -m's already-materialized objects).

`equivalent` runs a trace through both machines from the empty state
and checks both halves executably. `02-traces.lean` is the battery;
a full proof would be an induction showing each handler pair commutes
with the closure — the shape `close ∘ handlerP = close ∘ handlerM` on
projection-closed states.

Schedules are omitted from traces only because `nextCron` is opaque
(nothing to evaluate under `decide`); the schedule handlers are
byte-identical between the machines.  -/

namespace Equivalence

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
deriving instance BEq for ServerModel.PromiseGetRes
deriving instance BEq for ServerModel.PromiseCreateRes
deriving instance BEq for ServerModel.PromiseSettleRes
deriving instance BEq for ServerModel.PromiseRegisterCallbackRes
deriving instance BEq for ServerModel.PromiseRegisterListenerRes
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

/-- One step of a trace: a request, or an internal transition the
    environment chooses to fire. -/
inductive Op
  | promiseGet       (req : ServerModel.PromiseGetReq) (now : Nat)
  | promiseCreate    (req : ServerModel.PromiseCreateReq) (now : Nat)
  | promiseSettle    (req : ServerModel.PromiseSettleReq) (now : Nat)
  | registerCallback (req : ServerModel.PromiseRegisterCallbackReq) (now : Nat)
  | registerListener (req : ServerModel.PromiseRegisterListenerReq) (now : Nat)
  | taskGet          (req : ServerModel.TaskGetReq) (now : Nat)
  | taskCreate       (req : ServerModel.TaskCreateReq) (now : Nat)
  | taskAcquire      (req : ServerModel.TaskAcquireReq) (now : Nat)
  | taskFence        (req : ServerModel.TaskFenceReq) (now : Nat)
  | taskHeartbeat    (req : ServerModel.TaskHeartbeatReq) (now : Nat)
  | taskSuspend      (req : ServerModel.TaskSuspendReq) (now : Nat)
  | taskFulfill      (req : ServerModel.TaskFulfillReq) (now : Nat)
  | taskRelease      (req : ServerModel.TaskReleaseReq) (now : Nat)
  | taskHalt         (req : ServerModel.TaskHaltReq) (now : Nat)
  | taskContinue     (req : ServerModel.TaskContinueReq) (now : Nat)
  | tauPromise       (id : String) (now : Nat)
  | tauRetry         (id : String) (now : Nat)
  | tauLease         (id : String) (now : Nat)
  | tauDrain         (now : Nat)

/-- A trace observation: the response, uniformly typed. τ-steps are
    unobserved (`.tau`). -/
inductive OpRes
  | pGet       (r : ServerModel.PromiseGetRes)
  | pCreate    (r : ServerModel.PromiseCreateRes)
  | pSettle    (r : ServerModel.PromiseSettleRes)
  | pCallback  (r : ServerModel.PromiseRegisterCallbackRes)
  | pListener  (r : ServerModel.PromiseRegisterListenerRes)
  | tGet       (r : ServerModel.TaskGetRes)
  | tCreate    (r : ServerModel.TaskCreateRes)
  | tAcquire   (r : ServerModel.TaskAcquireRes)
  | tFence     (r : ServerModel.TaskFenceRes)
  | tHeartbeat (r : ServerModel.TaskHeartbeatRes)
  | tSuspend   (r : ServerModel.TaskSuspendRes)
  | tFulfill   (r : ServerModel.TaskFulfillRes)
  | tRelease   (r : ServerModel.TaskReleaseRes)
  | tHalt      (r : ServerModel.TaskHaltRes)
  | tContinue  (r : ServerModel.TaskContinueRes)
  | tau
  deriving BEq

open ServerModel (M) in
def runOpP : Op → M OpRes
  | .promiseGet r n       => return .pGet (← promiseGet r n)
  | .promiseCreate r n    => return .pCreate (← promiseCreate r n)
  | .promiseSettle r n    => return .pSettle (← promiseSettle r n)
  | .registerCallback r n => return .pCallback (← promiseRegisterCallback r n)
  | .registerListener r n => return .pListener (← promiseRegisterListener r n)
  | .taskGet r n          => return .tGet (← taskGet r n)
  | .taskCreate r n       => return .tCreate (← taskCreate r n)
  | .taskAcquire r n      => return .tAcquire (← taskAcquire r n)
  | .taskFence r n        => return .tFence (← taskFence r n)
  | .taskHeartbeat r n    => return .tHeartbeat (← taskHeartbeat r n)
  | .taskSuspend r n      => return .tSuspend (← taskSuspend r n)
  | .taskFulfill r n      => return .tFulfill (← taskFulfill r n)
  | .taskRelease r n      => return .tRelease (← taskRelease r n)
  | .taskHalt r n         => return .tHalt (← taskHalt r n)
  | .taskContinue r n     => return .tContinue (← taskContinue r n)
  | .tauPromise id n      => do Timeouts.onPromiseTimeout id n; return .tau
  | .tauRetry id n        => do Timeouts.onTaskRetryTimeout id n; return .tau
  | .tauLease id n        => do Timeouts.onTaskLeaseTimeout id n; return .tau
  | .tauDrain n           => do drain n; return .tau

open ServerModel (M) in
def runOpM : Op → M OpRes
  | .promiseGet r n       => return .pGet (← Materialized.promiseGet r n)
  | .promiseCreate r n    => return .pCreate (← Materialized.promiseCreate r n)
  | .promiseSettle r n    => return .pSettle (← Materialized.promiseSettle r n)
  | .registerCallback r n => return .pCallback (← Materialized.promiseRegisterCallback r n)
  | .registerListener r n => return .pListener (← Materialized.promiseRegisterListener r n)
  | .taskGet r n          => return .tGet (← Materialized.taskGet r n)
  | .taskCreate r n       => return .tCreate (← Materialized.taskCreate r n)
  | .taskAcquire r n      => return .tAcquire (← Materialized.taskAcquire r n)
  | .taskFence r n        => return .tFence (← Materialized.taskFence r n)
  | .taskHeartbeat r n    => return .tHeartbeat (← Materialized.taskHeartbeat r n)
  | .taskSuspend r n      => return .tSuspend (← Materialized.taskSuspend r n)
  | .taskFulfill r n      => return .tFulfill (← Materialized.taskFulfill r n)
  | .taskRelease r n      => return .tRelease (← Materialized.taskRelease r n)
  | .taskHalt r n         => return .tHalt (← Materialized.taskHalt r n)
  | .taskContinue r n     => return .tContinue (← Materialized.taskContinue r n)
  | .tauPromise id n      => do Materialized.Timeouts.onPromiseTimeout id n; return .tau
  | .tauRetry id n        => do Materialized.Timeouts.onTaskRetryTimeout id n; return .tau
  | .tauLease id n        => do Materialized.Timeouts.onTaskLeaseTimeout id n; return .tau
  | .tauDrain n           => do Materialized.drain n; return .tau

/-- τ-CLOSURE: fire the promise-timeout transition on every promise.
    On -p this persists every due-but-unmaterialized fact; on -m it is
    (almost everywhere) a no-op. Both machines use -p's τ here — they
    are the same transition. -/
def close (now : Nat) : ServerModel.M Unit := do
  let ids := (← get).promises.map (·.id)
  for id in ids do
    Timeouts.onPromiseTimeout id now

def eqSet [BEq α] (a b : List α) : Bool :=
  a.all b.contains && b.all a.contains

/-- State equality up to list order (the components are keyed). -/
def stateEq (a b : ServerModel.ServerState) : Bool :=
  a.config == b.config
    && eqSet a.promises b.promises
    && eqSet a.tasks b.tasks
    && eqSet a.schedules b.schedules
    && eqSet a.deferred b.deferred
    && eqSet a.promiseTimeouts b.promiseTimeouts
    && eqSet a.taskTimeouts b.taskTimeouts
    && eqSet a.scheduleTimeouts b.scheduleTimeouts
    && eqSet a.outbox b.outbox

/-- THE EQUIVALENCE CHECK: identical responses, closure-equal states. -/
def equivalent (ops : List Op) (nowFinal : Nat) : Bool :=
  let (rsP, sP) := Id.run ((ops.mapM runOpP).run ServerModel.ServerState.init)
  let (rsM, sM) := Id.run ((ops.mapM runOpM).run ServerModel.ServerState.init)
  rsP == rsM
    && stateEq (Id.run ((close nowFinal).run sP)).2
               (Id.run ((close nowFinal).run sM)).2

end Equivalence
