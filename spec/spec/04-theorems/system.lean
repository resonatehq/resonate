import «02-abstract».«internal»

/-!  # The system

What the machine IS, as opposed to what it does. Four things, and the
order is the order you need them in:

  1. **The alphabet** — the 21 requests a client can send, the
     responses they get back, and `AStep`, the steps a run is a
     sequence of.
  2. **The driver** — `handle`, which takes a step to the transition it
     denotes, and `stepOf`, which runs that transition against a state.
  3. **The trace** — `StateAction`, one tick's whole situation, and
     `Trace`, a function from ticks to those.
  4. **Validity** — which traces are runs OF THIS MACHINE, as opposed
     to arbitrary sequences of states.

Nothing else in the repository defines the system. The properties
(`02-abstract/properties.lean`) say what must be true of it; the
harnesses evaluate them; the liveness file quantifies over its traces.
All of them presuppose this file.

These definitions were spread across `trace.lean`, `alpha.lean` and
`refinement.lean`, which existed to RELATE two machines. That the
system's own definition lived inside a refinement was an accident of
where the driver happened to sit — nothing in `StateAction`, `Trace`
or validity mentions a second machine. With one machine the relating
is gone and the system statement is what remains, so it gets a file
named for what it is.

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

/-! ## The driver

What a step DENOTES. `handle` takes an `AStep` to a transition in the
machine's monad; `stepOf` runs that transition against a state and
hands back the response and the state it leaves.

`mat` appears only in `stepOf`, never in `handle`. That is the single
machine paying off: the read discipline is not a different dispatcher,
it is an argument to `run`. There used to be `handleA` and `handleAP`,
two 30-case dispatch tables that differed in which namespace they
named. -/

def handle (st : AStep) (now : Nat) : AbstractModel.H Response :=
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
  | .r1 id      => do AbstractModel.Internal.processPromiseTimeout id now; return .τ
  | .r3 id a    => do AbstractModel.Internal.processListener id a now; return .τ
  | .r4 id x    => do AbstractModel.Internal.processCallback id x now; return .τ
  | .r5 id      => do AbstractModel.Internal.processLeaseTimeout id now; return .τ
  | .r6 id      => do AbstractModel.Internal.processRetryTimeout id now; return .τ
  | .r7 id      => do AbstractModel.Internal.processSchedule id now; return .τ
  | .idle       => return .τ

def stepOf (mat : Bool) (st : AStep) (now : Nat) (s : AbstractModel.ServerState) :
    Response × AbstractModel.ServerState :=
  AbstractModel.run mat (handle st now) s

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


/-! ## Validity

Which traces are runs of this machine. Three conjuncts: the response is
the one the step gives, the next state is the one the step leaves, and
the clock does not run backwards.

The third is a HYPOTHESIS, not a property, and the distinction matters.
It restricts which traces we are talking about; it is not something an
implementation can be caught violating, because every walk only sees
the instants it is handed. A server whose clock regresses is outside
what anything in `02-abstract/properties.lean` can refuse — and that is
a real limit on what conformance to this catalogue buys you.

`mat` is a parameter here too, so there is ONE notion of validity with
a bit in it rather than two notions to keep in step. -/

def Valid (mat : Bool) (tr : Trace) : Prop :=
  ∀ t : Nat,
    (tr t).res = (stepOf mat (tr t).req (tr t).now (tr t).state).1 ∧
    (tr (t + 1)).state = (stepOf mat (tr t).req (tr t).now (tr t).state).2 ∧
    (tr t).now ≤ (tr (t + 1)).now

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
