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

inductive InternalStep
  | promiseTimeout   (id : Ident)
  | listener         (id : Ident) (address : String)
  | callback         (id awaiter : Ident)
  | taskLeaseTimeout (id : Ident)
  | taskRetryTimeout (id : Ident)
  | scheduleTimeout  (id : Ident)
  deriving Repr, DecidableEq

inductive Step
  | external (rq : Request)
  | internal (st : InternalStep)
  | idle
  deriving Repr

def Step.isExternal : Step → Bool
  | .external _ => true
  | _           => false

def Step.isInternal : Step → Bool
  | .internal _ => true
  | _           => false

deriving instance BEq for AbstractModel.TaskObject

deriving instance BEq for AbstractModel.PromiseObject
deriving instance BEq for AbstractModel.Object
deriving instance BEq for AbstractModel.ServerState

def handleInternal (st : InternalStep) (now : Nat) : AbstractModel.H Unit :=
  match st with
  | .promiseTimeout id   => AbstractModel.Internal.processPromiseTimeout id now
  | .listener id a       => AbstractModel.Internal.processListener id a now
  | .callback id x       => AbstractModel.Internal.processCallback id x now
  | .taskLeaseTimeout id => AbstractModel.Internal.processLeaseTimeout id now
  | .taskRetryTimeout id => AbstractModel.Internal.processRetryTimeout id now
  | .scheduleTimeout id  => AbstractModel.Internal.processSchedule id now

def handle (st : Step) (now : Nat) : AbstractModel.H Response :=
  match st with
  | .external (.promiseGet req)              => Response.promiseGet <$> AbstractModel.promiseGet req now
  | .external (.promiseCreate req)           => Response.promiseCreate <$> AbstractModel.promiseCreate req now
  | .external (.promiseSettle req)           => Response.promiseSettle <$> AbstractModel.promiseSettle req now
  | .external (.promiseRegisterCallback req) => Response.promiseRegisterCallback <$> AbstractModel.promiseRegisterCallback req now
  | .external (.promiseRegisterListener req) => Response.promiseRegisterListener <$> AbstractModel.promiseRegisterListener req now
  | .external (.promiseSearch req)           => Response.promiseSearch <$> AbstractModel.promiseSearch req now
  | .external (.scheduleGet req)             => Response.scheduleGet <$> AbstractModel.scheduleGet req now
  | .external (.scheduleCreate req)          => Response.scheduleCreate <$> AbstractModel.scheduleCreate req now
  | .external (.scheduleDelete req)          => Response.scheduleDelete <$> AbstractModel.scheduleDelete req now
  | .external (.scheduleSearch req)          => Response.scheduleSearch <$> AbstractModel.scheduleSearch req now
  | .external (.taskGet req)                 => Response.taskGet <$> AbstractModel.taskGet req now
  | .external (.taskCreate req)              => Response.taskCreate <$> AbstractModel.taskCreate req now
  | .external (.taskAcquire req)             => Response.taskAcquire <$> AbstractModel.taskAcquire req now
  | .external (.taskFence req)               => Response.taskFence <$> AbstractModel.taskFence req now
  | .external (.taskHeartbeat req)           => Response.taskHeartbeat <$> AbstractModel.taskHeartbeat req now
  | .external (.taskSuspend req)             => Response.taskSuspend <$> AbstractModel.taskSuspend req now
  | .external (.taskFulfill req)             => Response.taskFulfill <$> AbstractModel.taskFulfill req now
  | .external (.taskRelease req)             => Response.taskRelease <$> AbstractModel.taskRelease req now
  | .external (.taskHalt req)                => Response.taskHalt <$> AbstractModel.taskHalt req now
  | .external (.taskContinue req)            => Response.taskContinue <$> AbstractModel.taskContinue req now
  | .external (.taskSearch req)              => Response.taskSearch <$> AbstractModel.taskSearch req now
  | .internal a => do handleInternal a now; return .silent
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
