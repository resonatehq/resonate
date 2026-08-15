import «02-abstract».«internal»

/-!  # The step alphabet

What a run of the machine is made of: the 21 client requests, the
responses they get back, and the steps a trace is a sequence of.

This file is the salvage from `trace.lean`, `alpha.lean` and
`refinement.lean`, which held these definitions when there were two
machines to relate. With one machine the relating is gone and the
vocabulary remains, so it lives here on its own.

Two changes came with the move, and both are subtractions.

`Request` carries only the 21 requests a client can send. It used to
carry five τ constructors and an `idle` as well — `τPromiseTimeout`,
`τTaskRetryTimeout`, `τTaskLeaseTimeout`, `τScheduleTimeout`, `τResume`
— because the concrete driver dispatched everything through one type.
`AStep` already names the internal steps as its own constructors, so
those five were a second, redundant spelling of them. `Request` is now
exactly the external surface, which makes `AStep.isExternal` decidable
by shape rather than by asking the request.

`AStep.r6` no longer carries a next-fire instant. It used to read
`r6 (id : String) (next : Nat)`, and that `next` was the only value in
the whole alphabet that the environment wrote into the store rather
than a name of something to act on. `processRetryTimeout` now computes
its own next instant from the task's `ttl`, so the parameter has no
reader. Every constructor below names an object and nothing else. -/

namespace Equivalence

open ServerModel

/-! ## Structural equality on the wire types

`01-protocol/types.lean` derives `Repr` but not `BEq`: the protocol
does not need to compare two responses, only to produce them. Comparing
is what a HARNESS does, so the instances are declared here rather than
there. Salvaged from `trace.lean`, minus the concrete ones. -/

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

/-- Internal steps are silent: nobody outside is listening, so they all
    answer `τ`. -/
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
  | τ
  deriving Repr, BEq

end Equivalence

namespace Abstraction

open Equivalence

/-- One step of a run. `api` is a client request; `r1`–`r7` are the
    steps the server takes on its own initiative; `idle` is the clock
    moving with nothing else happening.

    The numbering has a hole: `r2` was `taskFulfillment` and was
    deleted. The gap is kept so that `r5` still means the lease
    timeout in every document, trace and checker that names it. -/
inductive AStep
  | api (rq : Request)
  | r1  (id : String)
  | r3  (id address : String)
  | r4  (id awaiter : String)
  | r5  (id : String)
  | r6  (id : String)
  | r7  (id : String)
  | idle

/-- Client-visible steps. Now decidable by shape: with the τ
    constructors gone from `Request`, every `api` step is external. -/
def AStep.isExternal : AStep → Bool
  | .api _ => true
  | _      => false

/-! `PromiseObject` is compared by `promiseEq` below rather than
structurally, because two of its fields are ledgers. `TaskObject` has no
such field, so it compares structurally. -/

deriving instance BEq for AbstractModel.TaskObject

/-! ## What a run is

A trace is a function from ticks to state-action blocks. Each block
records the whole situation at one tick: the state the machine was in,
the step taken, the response it gave, and what the clock read.

`now` lives HERE, in the trace element, and not in `ServerState`. That
is deliberate and it is the answer to an obvious question — the clock
is not part of the store, because a server does not own the time; it
reads it. Two machines at the same state and different instants are at
the same state.

Validity is stated with the driver, in the file that defines the
handler, since it needs to say what a step produces. It has three
conjuncts: the response is the one the step gives, the next state is
the one the step leaves, and the clock does not run backwards. Note
what that third one is — a HYPOTHESIS on traces, not a checkable
property. No walk over states can catch a clock that regresses,
because a walk only ever sees the instants it is handed. An
implementation that lets its clock go backwards is outside what any
property in `02-abstract/properties.lean` can refuse. -/

structure StateAction where
  state : AbstractModel.ServerState
  req   : AStep
  res   : Response
  now   : Nat

abbrev Trace := Nat → StateAction

/-! ## Comparing states

Every component of `ServerState` is a keyed list, so order carries no
information and equality is set equality. `promiseEq` additionally
compares `callbacks` and `listeners` as sets, because those are
ledgers and an implementation may drain them in any order. -/

def eqSet [BEq α] (a b : List α) : Bool :=
  a.all b.contains && b.all a.contains

def eqSetBy (eq : α → α → Bool) (a b : List α) : Bool :=
  a.all (fun x => b.any (eq x)) && b.all (fun x => a.any (eq x))

def promiseEq (a b : AbstractModel.PromiseObject) : Bool :=
  a.id == b.id && a.state == b.state && a.param == b.param
    && a.value == b.value && a.tags == b.tags
    && a.timeoutAt == b.timeoutAt && a.createdAt == b.createdAt
    && a.settledAt == b.settledAt
    && eqSet a.callbacks b.callbacks && eqSet a.listeners b.listeners

def absStateEq (a b : AbstractModel.ServerState) : Bool :=
  eqSetBy promiseEq a.promises b.promises
    && eqSet a.tasks b.tasks
    && eqSet a.schedules b.schedules
    && eqSet a.outbox b.outbox

/-! ## Tag fixtures

The three tag shapes every corpus in this directory is built from. -/

def extTags   : ServerModel.Tags := [("resonate:external", "true")]
def tgtTags   : ServerModel.Tags := [("resonate:target", "w1")]
def timerTags : ServerModel.Tags := [("resonate:timer", "true")]

end Abstraction
