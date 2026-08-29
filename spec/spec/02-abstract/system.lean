import «02-abstract».«internal»

namespace Equivalence

open ServerModel

deriving instance BEq for ServerModel.Value
deriving instance BEq for ServerModel.PromiseRecord
deriving instance BEq for ServerModel.TaskRecord
deriving instance BEq for ServerModel.Schedule
deriving instance BEq for ServerModel.ResumeReq
deriving instance BEq for ServerModel.Message
deriving instance BEq for ServerModel.OutboxEntry
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
  deriving Repr

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
  | silent
  deriving Repr, BEq

end Equivalence

namespace Abstraction

open Equivalence
open ServerModel (Ident)

inductive Step
  | api              (rq : Request)
  | promiseTimeout   (id : Ident)
  | listener         (id : Ident) (address : String)
  | callback         (id awaiter : Ident)
  | taskLeaseTimeout (id : Ident)
  | taskRetryTimeout (id : Ident)
  | scheduleTimeout  (id : Ident)
  | idle
  deriving Repr

def Step.isExternal : Step → Bool
  | .api _ => true
  | _      => false

def Step.isInternal : Step → Bool
  | .promiseTimeout _   => true | .listener _ _       => true
  | .callback _ _       => true | .taskLeaseTimeout _ => true
  | .taskRetryTimeout _ => true | .scheduleTimeout _  => true
  | _                   => false

deriving instance BEq for AbstractModel.TaskObject

deriving instance BEq for AbstractModel.PromiseObject
deriving instance BEq for AbstractModel.Object
deriving instance BEq for AbstractModel.ServerState

def handle (st : Step) (now : Nat) : AbstractModel.H Response :=
  match st with
  | .api (.promiseGet req)              => Response.promiseGet <$> AbstractModel.promiseGet req now
  | .api (.promiseCreate req)           => Response.promiseCreate <$> AbstractModel.promiseCreate req now
  | .api (.promiseSettle req)           => Response.promiseSettle <$> AbstractModel.promiseSettle req now
  | .api (.promiseRegisterCallback req) => Response.promiseRegisterCallback <$> AbstractModel.promiseRegisterCallback req now
  | .api (.promiseRegisterListener req) => Response.promiseRegisterListener <$> AbstractModel.promiseRegisterListener req now
  | .api (.promiseSearch req)           => Response.promiseSearch <$> AbstractModel.promiseSearch req now
  | .api (.scheduleGet req)             => Response.scheduleGet <$> AbstractModel.scheduleGet req now
  | .api (.scheduleCreate req)          => Response.scheduleCreate <$> AbstractModel.scheduleCreate req now
  | .api (.scheduleDelete req)          => Response.scheduleDelete <$> AbstractModel.scheduleDelete req now
  | .api (.scheduleSearch req)          => Response.scheduleSearch <$> AbstractModel.scheduleSearch req now
  | .api (.taskGet req)                 => Response.taskGet <$> AbstractModel.taskGet req now
  | .api (.taskCreate req)              => Response.taskCreate <$> AbstractModel.taskCreate req now
  | .api (.taskAcquire req)             => Response.taskAcquire <$> AbstractModel.taskAcquire req now
  | .api (.taskFence req)               => Response.taskFence <$> AbstractModel.taskFence req now
  | .api (.taskHeartbeat req)           => Response.taskHeartbeat <$> AbstractModel.taskHeartbeat req now
  | .api (.taskSuspend req)             => Response.taskSuspend <$> AbstractModel.taskSuspend req now
  | .api (.taskFulfill req)             => Response.taskFulfill <$> AbstractModel.taskFulfill req now
  | .api (.taskRelease req)             => Response.taskRelease <$> AbstractModel.taskRelease req now
  | .api (.taskHalt req)                => Response.taskHalt <$> AbstractModel.taskHalt req now
  | .api (.taskContinue req)            => Response.taskContinue <$> AbstractModel.taskContinue req now
  | .api (.taskSearch req)              => Response.taskSearch <$> AbstractModel.taskSearch req now
  | .promiseTimeout id      => do AbstractModel.Internal.processPromiseTimeout id now; return .silent
  | .listener id a    => do AbstractModel.Internal.processListener id a now; return .silent
  | .callback id x    => do AbstractModel.Internal.processCallback id x now; return .silent
  | .taskLeaseTimeout id      => do AbstractModel.Internal.processLeaseTimeout id now; return .silent
  | .taskRetryTimeout id      => do AbstractModel.Internal.processRetryTimeout id now; return .silent
  | .scheduleTimeout id      => do AbstractModel.Internal.processSchedule id now; return .silent
  | .idle       => return .silent

def stepOf (mat : Bool) (st : Step) (now : Nat) (s : AbstractModel.ServerState) :
    Response × AbstractModel.ServerState :=
  AbstractModel.run mat (handle st now) s

def runFin (mat : Bool) :
    List (Step × Nat) → AbstractModel.ServerState →
    List Response × AbstractModel.ServerState
  | [],           s => ([], s)
  | (st, n) :: w, s =>
      let (r, s')   := stepOf mat st n s
      let (rs, s'') := runFin mat w s'
      (r :: rs, s'')

structure StateAction where
  state : AbstractModel.ServerState
  req   : Step
  res   : Response
  now   : Nat

abbrev Trace := Nat → StateAction

def Valid (mat : Bool) (tr : Trace) : Prop :=
  ∀ t : Nat,
    (tr t).res = (stepOf mat (tr t).req (tr t).now (tr t).state).1 ∧
    (tr (t + 1)).state = (stepOf mat (tr t).req (tr t).now (tr t).state).2 ∧
    (tr t).now ≤ (tr (t + 1)).now

end Abstraction
