namespace ServerModel

/-- An identifier: an origin and a suffix within it.

    On the wire this is one string, `origin:suffix`, with exactly one
    colon; the suffix may be empty, and an id whose suffix is empty
    renders as the bare origin. None of that is the specification's
    business -- parsing and rendering happen at the edge, in the trace
    codec and in the server. In here an id is a pair, and the only
    question ever asked of its halves is whether two ids share an
    origin.

    `DecidableEq` rather than `deriving BEq`: the lookup and frame
    proofs are stated over `[BEq α] [LawfulBEq α]`, and the instance
    derived from decidable equality is the lawful one. -/
structure Ident where
  origin : String
  suffix : String
  deriving Repr, DecidableEq, Inhabited

instance : BEq Ident := instBEqOfDecidableEq

/-- Two ids in one origin. The doors that refuse cross-origin work --
    callback registration, suspension, fencing -- are exactly this
    comparison, which is why they cost no read. -/
def Ident.sameOrigin (a b : Ident) : Bool := a.origin == b.origin

abbrev Tags := List (String × String)

structure Value where
  headers : Tags          := []
  data    : Option String := none
  deriving Repr, Inhabited

inductive PromiseState
  | pending
  | resolved
  | rejected
  | rejectedCanceled
  | rejectedTimedout
  deriving Repr, DecidableEq

inductive TaskState
  | pending | acquired | suspended | halted | fulfilled
  deriving Repr, DecidableEq

structure PromiseRecord where
  id        : Ident
  state     : PromiseState
  param     : Value
  value     : Value       := {}
  tags      : Tags
  timeoutAt : Nat
  createdAt : Nat
  settledAt : Option Nat  := none
  deriving Repr

structure TaskRecord where
  id      : Ident
  state   : TaskState
  version : Nat
  resumes : Nat
  ttl     : Option Nat    := none
  pid     : Option String := none
  deriving Repr

structure Schedule where
  id             : Ident
  cron           : String
  promiseId      : Ident
  promiseTimeout : Nat
  promiseParam   : Value
  promiseTags    : Tags
  nextRunAt      : Nat
  lastRunAt      : Option Nat := none
  createdAt      : Nat
  deriving Repr

structure PromiseGetReq where
  id : Ident
  deriving Repr

structure PromiseGetRes where
  status  : Nat
  promise : Option PromiseRecord := none
  deriving Repr

structure PromiseCreateReq where
  id        : Ident
  timeoutAt : Nat
  param     : Value
  tags      : Tags
  deriving Repr

structure PromiseCreateRes where
  status  : Nat
  promise : Option PromiseRecord
  deriving Repr

structure PromiseSettleReq where
  id    : Ident
  state : PromiseState
  value : Value
  deriving Repr

structure PromiseSettleRes where
  status  : Nat
  promise : Option PromiseRecord := none
  deriving Repr

structure PromiseRegisterCallbackReq where
  awaited : Ident
  awaiter : Ident
  deriving Repr

structure PromiseRegisterCallbackRes where
  status  : Nat
  promise : Option PromiseRecord := none
  deriving Repr

structure PromiseRegisterListenerReq where
  awaited : Ident
  address : String
  deriving Repr

structure PromiseRegisterListenerRes where
  status  : Nat
  promise : Option PromiseRecord := none
  deriving Repr

structure PromiseSearchReq where
  state  : Option PromiseState := none
  tags   : Tags := []
  limit  : Option Nat := none
  cursor : Option String := none
  deriving Repr

structure PromiseSearchRes where
  status   : Nat
  promises : List PromiseRecord := []
  cursor   : Option String := none
  deriving Repr

structure ScheduleGetReq where
  id : Ident
  deriving Repr

structure ScheduleGetRes where
  status   : Nat
  schedule : Option Schedule := none
  deriving Repr

structure ScheduleCreateReq where
  id             : Ident
  cron           : String
  promiseId      : Ident
  promiseTimeout : Nat
  promiseParam   : Value
  promiseTags    : Tags
  deriving Repr

structure ScheduleCreateRes where
  status   : Nat
  schedule : Option Schedule := none
  deriving Repr

structure ScheduleDeleteReq where
  id : Ident
  deriving Repr

structure ScheduleDeleteRes where
  status : Nat
  deriving Repr

structure ScheduleSearchReq where
  limit  : Option Nat := none
  cursor : Option String := none
  deriving Repr

structure ScheduleSearchRes where
  status    : Nat
  schedules : List Schedule := []
  cursor    : Option String := none
  deriving Repr

structure TaskGetReq where
  id : Ident
  deriving Repr

structure TaskGetRes where
  status : Nat
  task   : Option TaskRecord := none
  deriving Repr

structure TaskCreateReq where
  pid    : String
  ttl    : Nat
  action : PromiseCreateReq
  deriving Repr

structure TaskCreateRes where
  status  : Nat
  task    : Option TaskRecord := none
  promise : Option PromiseRecord := none
  preload : List PromiseRecord := []
  deriving Repr

structure TaskAcquireReq where
  id      : Ident
  version : Nat
  pid     : String
  ttl     : Nat
  deriving Repr

structure TaskAcquireRes where
  status  : Nat
  task    : Option TaskRecord := none
  promise : Option PromiseRecord := none
  preload : List PromiseRecord := []
  deriving Repr

inductive TaskFenceAction
  | create (req : PromiseCreateReq)
  | settle (req : PromiseSettleReq)
  deriving Repr

inductive TaskFenceInnerRes
  | create (res : PromiseCreateRes)
  | settle (res : PromiseSettleRes)
  deriving Repr

structure TaskFenceReq where
  id      : Ident
  version : Nat
  action  : TaskFenceAction
  deriving Repr

structure TaskFenceRes where
  status  : Nat
  action  : Option TaskFenceInnerRes := none
  preload : List PromiseRecord := []
  deriving Repr

structure TaskRef where
  id      : Ident
  version : Nat
  deriving Repr

structure TaskHeartbeatReq where
  pid   : String
  tasks : List TaskRef
  deriving Repr

structure TaskHeartbeatRes where
  status : Nat
  deriving Repr

structure TaskSuspendReq where
  id      : Ident
  version : Nat
  actions : List PromiseRegisterCallbackReq
  deriving Repr

structure TaskSuspendRes where
  status  : Nat
  preload : List PromiseRecord := []
  deriving Repr

structure TaskFulfillReq where
  id      : Ident
  version : Nat
  action  : PromiseSettleReq
  deriving Repr

structure TaskFulfillRes where
  status  : Nat
  promise : Option PromiseRecord := none
  deriving Repr

structure TaskReleaseReq where
  id      : Ident
  version : Nat
  deriving Repr

structure TaskReleaseRes where
  status : Nat
  deriving Repr

structure TaskHaltReq where
  id : Ident
  deriving Repr

structure TaskHaltRes where
  status : Nat
  deriving Repr

structure TaskContinueReq where
  id : Ident
  deriving Repr

structure TaskContinueRes where
  status : Nat
  deriving Repr

structure TaskSearchReq where
  state  : Option TaskState := none
  limit  : Option Nat := none
  cursor : Option String := none
  deriving Repr

structure TaskSearchRes where
  status : Nat
  tasks  : List TaskRecord := []
  cursor : Option String := none
  deriving Repr

structure ResumeReq where
  awaited : String
  awaiter : String
  deriving Repr

/-- Spec artifact for the oracle: a resume never serializes and nobody is
    listening, but the drain has six distinct outcomes and `H Unit` would
    leave it testable only by state diffing. An inductive, not a `Nat`:
    there is no wire format to be compatible with, so name the cases.
    `expired` vs `fulfilled` is the distinction TIMEOUT ALWAYS WINS
    legislates -- representable here, invisible in a unit return. -/
inductive ResumeOutcome
  | resumed     -- suspended -> pending; execute emitted
  | buffered    -- awaiter live but not suspended; trigger recorded
  | duplicate   -- trigger already recorded
  | expired     -- awaiter past its own deadline; the timeout path owns cleanup
  | fulfilled   -- awaiter already settled
  | absent      -- no task, or no promise, for the awaiter
  deriving Repr, DecidableEq

structure ResumeRes where
  outcome : ResumeOutcome
  deriving Repr

end ServerModel
