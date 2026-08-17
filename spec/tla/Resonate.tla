-------------------------------- MODULE Resonate --------------------------------
(***************************************************************************)
(* The Resonate protocol as a state machine, at the altitude of            *)
(* `spec/02-abstract` -- but with the atomicity taken OUT.                 *)
(*                                                                         *)
(* The Lean machine makes one request one transition: `stepOf` runs a      *)
(* handler and `applyAll` folds its whole effect list onto the pre-state,  *)
(* and no other request can be observed in between. That is the right      *)
(* altitude for saying WHAT each request means, and the wrong one for      *)
(* asking what two concurrent requests do to each other -- because at that *)
(* altitude, they cannot do anything to each other.                        *)
(*                                                                         *)
(* Four state components, and each is a different KIND of thing:           *)
(*                                                                         *)
(*     objects    what is true. Durable. A promise and its optional task   *)
(*                are ONE unit -- they are settled together, fulfilled     *)
(*                together, and never observed disagreeing                 *)
(*     timeouts   what is due. An INDEX over the deadlines the objects     *)
(*                already carry, which is exactly why it can drift         *)
(*     outbox     what has been said. Append-only, keyed, and the only     *)
(*                thing that leaves the system                             *)
(*     steps      what is in flight. Volatile: a crash drops one and       *)
(*                nothing durable refers to it                             *)
(*                                                                         *)
(* and five transitions:                                                   *)
(*                                                                         *)
(*     SubmitExternal   a client request arrives                           *)
(*     SubmitInternal   the server takes a step on its own initiative      *)
(*     Process          READ, DECIDE, AND COMMIT -- one atomic action      *)
(*     Perform          say one thing on the wire; the last one retires    *)
(*                      the step                                           *)
(*     Crash            the step vanishes; what it committed stays         *)
(*     Tick             the clock advances                                 *)
(*                                                                         *)
(* WHERE THE INTERLEAVING IS. Between a step's Process and its last        *)
(* Perform, any number of other steps may run to completion. So a step     *)
(* commits its state and then says things about a world that has since     *)
(* moved on: that is stale dispatch -- a drain-issued Execute surviving a  *)
(* settle -- which is one of the four defects the Verus port found, and it *)
(* is a defect this machine can EXHIBIT rather than merely be told about.  *)
(*                                                                         *)
(* WHERE THE INTERLEAVING IS NOT, and what that costs. `Process` commits   *)
(* atomically, so there is no write-fence here -- no etag, no version, no  *)
(* compare-and-swap. That is an ASSUMPTION, not a free lunch: it says the  *)
(* store gives you a transaction over everything one step writes. The      *)
(* Verus executor does not assume it, it EARNS it -- `PutDocument` carries *)
(* `expect: etag_of(held)` and a failed compare sends the step back to     *)
(* `Prepare` with `retries + 1`. Two ways to discharge the assumption:     *)
(*                                                                         *)
(*   - a real transaction, and then this is just what the store does;      *)
(*   - one document per unit and a CAS on it, and then a step must write   *)
(*     exactly one unit. `promiseRegisterCallback` and `taskSuspend` do    *)
(*     not: they write the AWAITED promise while reading the awaiter.      *)
(*     Verus buys this back by making its document a whole `Workflow` --   *)
(*     `promises: Map<Id, Promise>`, plural -- so both ends of a callback  *)
(*     live in one document and the CAS covers them.                       *)
(*                                                                         *)
(* Either way it is a refinement obligation, and the place it bites is     *)
(* named: `MultiUnitWrites` at the bottom of this file lists the steps     *)
(* that need it.                                                           *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    PromiseId,          \* promise identifiers -- also task identifiers
    ScheduleId,         \* schedule identifiers
    Address,            \* message addresses: `resonate:target`, listener addresses
    Pid,                \* worker process identifiers
    Value,              \* opaque payloads: param, value
    Tags,               \* opaque tag maps
    Cron,               \* opaque cron expressions
    Ttl,                \* the lease lengths a worker may ask for
    Response,           \* opaque handler responses (status + records)
    Silent,             \* the response of an internal step: nobody is listening
    Materialise,        \* `Env.mat`: does a read PERSIST a projected settlement?
    RetryTimeout,       \* `ServerConfig.retryTimeout` -- the redispatch cadence
    MaxTime,            \* clock bound, so TLC has a finite state space
    Handle(_, _)        \* the handler table -- see WHAT A HANDLER IS, below

(* A task is keyed by the promise it drives: `setTask { id := p.id, .. }`
   everywhere in the Lean. One identifier, two roles -- and here, one
   object. *)
TaskId == PromiseId

Time == 0 .. MaxTime
Rid  == Nat
None == CHOOSE x : x \notin (Nat \cup Address \cup Pid)

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE OBJECTS                                                             *)
(*                                                                         *)
(* An object is a promise and, if the promise is targeted, the task that   *)
(* drives it -- `PromiseObject` and `TaskObject` from                      *)
(* `02-abstract/state.lean`, fused. The fusion is not a compression: it    *)
(* is the claim that the pair is what gets written, and it makes           *)
(* `setSettled` -- settle the promise, fulfill the task -- ONE write       *)
(* rather than two that a reader could catch half-done.                    *)
(*                                                                         *)
(* Schedules are the second kind of object. They are not promises and      *)
(* have no task, but they are keyed, written, and carry a deadline, which  *)
(* is everything this machine needs of them.                               *)
(*                                                                         *)
(* Two changes from the Lean. `callbacks` and `listeners` are SETS, not    *)
(* lists -- they are ledgers, and `promiseEq` already compares them        *)
(* without regard to order. And an object carries its own `clock`, the     *)
(* Verus `Workflow.clock`: a monotone high-water mark, so that an object's *)
(* notion of time never regresses even if the server's does. `clamp` is    *)
(* what reads it.                                                          *)
(***************************************************************************)

PromiseState == {"pending", "resolved", "rejected",
                 "rejectedCanceled", "rejectedTimedout"}

TaskState == {"pending", "acquired", "suspended", "halted", "fulfilled"}

Promise ==
    [ state     : PromiseState,
      param     : Value,
      value     : Value,
      tags      : Tags,
      timeoutAt : Time,                \* deadline -- armed as "promise"
      createdAt : Time,
      settledAt : Time \cup {None},
      callbacks : SUBSET TaskId,       \* awaiters to resume when this settles
      listeners : SUBSET Address ]     \* addresses to notify when this settles

Task ==
    [ state     : TaskState,
      version   : Nat,                 \* the fencing token
      ttl       : Nat  \cup {None},    \* the WORKER's number: lease length asked for
      pid       : Pid  \cup {None},
      expiresAt : Time \cup {None},    \* deadline -- armed as "lease"
      retryAt   : Time \cup {None},    \* deadline -- armed as "retry"
      resumes   : SUBSET PromiseId ]   \* triggers buffered while not suspended

Schedule ==
    [ cron           : Cron,
      promiseId      : PromiseId,
      promiseTimeout : Nat,
      promiseParam   : Value,
      promiseTags    : Tags,
      nextRunAt      : Time,           \* deadline -- armed as "schedule"
      lastRunAt      : Time \cup {None},
      createdAt      : Time ]

(* What an object is keyed by. Promises and schedules are separate
   namespaces in the protocol, so the key carries which one it is. *)
Origin ==
       [kind : {"promise"},  id : PromiseId]
  \cup [kind : {"schedule"}, id : ScheduleId]

Object ==
       [kind : {"promise"},  promise  : Promise, task : Task \cup {None},
                             clock    : Time]
  \cup [kind : {"schedule"}, schedule : Schedule, clock : Time]

Message ==
       [kind : {"execute"}, taskId  : TaskId, version : Nat]
  \cup [kind : {"unblock"}, promise : PromiseId, state : PromiseState]

OutEntry == [address : Address, message : Message]

(* `OutboxEntry.key`: what a later message REPLACES rather than joins.
   Per task for execute, per (promise, address) for unblock. *)
MsgKey(e) ==
    IF e.message.kind = "execute"
    THEN <<"execute", e.message.taskId>>
    ELSE <<"unblock", e.message.promise, e.address>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE TIMEOUTS                                                            *)
(*                                                                         *)
(* The wheel. Every entry names a deadline that an object already carries: *)
(* `promise` is `Promise.timeoutAt`, `lease` is `Task.expiresAt`, `retry`  *)
(* is `Task.retryAt`, `schedule` is `Schedule.nextRunAt`.                  *)
(*                                                                         *)
(* So the wheel is REDUNDANT, and that is the point of having it. It is an *)
(* index kept by a different mechanism than the thing it indexes, which    *)
(* means it can drift -- and the two directions of drift are not the same  *)
(* failure. An entry left behind after its deadline was cleared fires a    *)
(* step that reads the object, finds nothing due, and does nothing: noise. *)
(* A deadline armed with no entry is a timeout that NEVER FIRES: silence,  *)
(* and unrecoverable. `WheelComplete` below is the invariant that forbids  *)
(* the second, and the effect ordering in `Process` is what maintains it.  *)
(*                                                                         *)
(* Verus arms only `min_deadline(w)`, one entry per origin, and re-arms on *)
(* every write. Here every deadline is its own entry -- the same behaviour *)
(* with the coalescing not yet done, and a strictly easier obligation.     *)
(***************************************************************************)

DeadlineKind == {"promise", "lease", "retry", "schedule"}

Entry == [at : Time, origin : Origin, kind : DeadlineKind]

(* The deadline an object actually carries for a given kind -- what the
   wheel is an index OF. `None` when the object has no such deadline
   live: a settled promise has no timeout, an unacquired task no lease. *)
Deadline(obj, kind) ==
    CASE kind = "promise" ->
             IF obj.kind = "promise" /\ obj.promise.state = "pending"
             THEN obj.promise.timeoutAt ELSE None
      [] kind = "lease" ->
             IF obj.kind = "promise" /\ obj.task /= None
             THEN obj.task.expiresAt ELSE None
      [] kind = "retry" ->
             IF obj.kind = "promise" /\ obj.task /= None
             THEN obj.task.retryAt ELSE None
      [] kind = "schedule" ->
             IF obj.kind = "schedule" THEN obj.schedule.nextRunAt ELSE None

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE ALPHABET                                                            *)
(*                                                                         *)
(* `Equivalence.Request` and the internal constructors of                  *)
(* `Abstraction.Step`, as one set of records tagged by `kind`. The         *)
(* external/internal line is the line between what a client asks for and   *)
(* what the server does on its own initiative -- `Step.isExternal` and     *)
(* `Step.isInternal` -- and it is the line the two Submit transitions are  *)
(* organised around: an external event may arrive at any moment, an        *)
(* internal one only when the server has a reason to take it.              *)
(***************************************************************************)

PromiseCreateReq   == [id : PromiseId, timeoutAt : Time, param : Value, tags : Tags]
PromiseSettleReq   == [id : PromiseId, state : PromiseState, value : Value]
PromiseCallbackReq == [awaited : PromiseId, awaiter : PromiseId]
TaskRef            == [id : TaskId, version : Nat]

ExternalEvent ==
       [kind : {"promiseGet"},              id      : PromiseId]
  \cup [kind : {"promiseCreate"},           req     : PromiseCreateReq]
  \cup [kind : {"promiseSettle"},           req     : PromiseSettleReq]
  \cup [kind : {"promiseRegisterCallback"}, req     : PromiseCallbackReq]
  \cup [kind : {"promiseRegisterListener"}, awaited : PromiseId, address : Address]
  \cup [kind : {"promiseSearch"}]
  \cup [kind : {"scheduleGet"},             id      : ScheduleId]
  \cup [kind : {"scheduleCreate"},          id      : ScheduleId, cron : Cron,
                                            promiseId : PromiseId, promiseTimeout : Nat,
                                            promiseParam : Value, promiseTags : Tags]
  \cup [kind : {"scheduleDelete"},          id      : ScheduleId]
  \cup [kind : {"scheduleSearch"}]
  \cup [kind : {"taskGet"},                 id      : TaskId]
  \cup [kind : {"taskCreate"},              pid     : Pid, ttl : Ttl,
                                            action  : PromiseCreateReq]
  \cup [kind : {"taskAcquire"},             id      : TaskId, version : Nat,
                                            pid     : Pid, ttl : Ttl]
  \cup [kind : {"taskFence"},               id      : TaskId, version : Nat,
                                            action  : PromiseCreateReq \cup PromiseSettleReq]
  \cup [kind : {"taskHeartbeat"},           pid     : Pid, tasks : SUBSET TaskRef]
  \cup [kind : {"taskSuspend"},             id      : TaskId, version : Nat,
                                            actions : SUBSET PromiseCallbackReq]
  \cup [kind : {"taskFulfill"},             id      : TaskId, version : Nat,
                                            action  : PromiseSettleReq]
  \cup [kind : {"taskRelease"},             id      : TaskId, version : Nat]
  \cup [kind : {"taskHalt"},                id      : TaskId]
  \cup [kind : {"taskContinue"},            id      : TaskId]
  \cup [kind : {"taskSearch"}]

(* The steps no client asks for. Two shapes, not six, and the split is by
   WHAT ENABLES THEM rather than by what they do:

     - a due wheel entry. This is the Lean's r1, r5, r6 and r7 -- promise
       timeout, lease timeout, retry dispatch, schedule -- collapsed into
       one event, because with the deadline in the entry there is nothing
       left to distinguish them at this altitude. It is the Verus
       `Input::Tick`, which rides only on an armed entry that is due:
       "everything else the machine does on its own is a TIMEOUT TICK".

     - a non-empty ledger on a settled promise. This is r3 and r4, the
       listener and callback drains, and they are NOT timer-driven: what
       enables them is a settlement that has happened and a name still
       written down next to it. *)
InternalEvent ==
       [kind : {"timeout"},       entry : Entry]
  \cup [kind : {"listenerDrain"}, id    : PromiseId, address : Address]  \* r3
  \cup [kind : {"callbackDrain"}, id    : PromiseId, awaiter : TaskId]   \* r4

Event == ExternalEvent \cup InternalEvent

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE VARIABLES                                                           *)
(***************************************************************************)

VARIABLES
    objects,    \* [Origin -> Object], partial: DOMAIN is what exists
    timeouts,   \* SUBSET Entry -- the wheel
    outbox,     \* SUBSET OutEntry, at most one entry per MsgKey
    steps,      \* [Rid -> InFlight] -- the requests currently in flight
    now,        \* the clock. NOT part of the store: a server does not own
                \* the time, it reads it. Two machines at the same state
                \* and different instants are at the same state
    nextRid     \* fresh step identifiers

vars == <<objects, timeouts, outbox, steps, now, nextRid>>

(* One thing a step still has to say after it has committed. `send` puts a
   message on the wire; `respond` answers the client and is a no-op on
   state -- it is `Effect::Respond` in the Verus executor, and it is here
   for the same reason: it is what makes the pending list NEVER EMPTY, so
   the last Perform always retires the step and there is no separate
   Complete to get wrong. An internal step responds `Silent` to nobody.

   These are the effects, and they are the only ones, because everything
   durable was committed in one action by Process. *)
Effect ==
       [op : {"send"},    entry : OutEntry]
  \cup [op : {"respond"}, res   : Response \cup {Silent}]

InFlight ==
    [ rid     : Rid,
      ev      : Event,
      phase   : {"process", "perform"},
      pending : Seq(Effect) ]

Fresh(r, ev) == [rid |-> r, ev |-> ev, phase |-> "process", pending |-> << >>]

-----------------------------------------------------------------------------
(***************************************************************************)
(* WHAT A HANDLER IS                                                       *)
(*                                                                         *)
(* `Handle(ev, env)` is a pure function of the event and the state, and it *)
(* reads nothing else. It returns what to commit and what to say:          *)
(*                                                                         *)
(*     writes   objects to install, as a function Origin -> Object         *)
(*     arm      entries to add to the wheel                                *)
(*     disarm   entries to remove from the wheel                           *)
(*     sends    messages, IN ORDER -- a crash applies a prefix             *)
(*     res      the response to give                                       *)
(*                                                                         *)
(* This is `AbstractModel.H` with the effect list sorted by kind instead   *)
(* of left in emission order, which is what makes the commit expressible   *)
(* as one action. The order WITHIN `sends` still matters and is kept.      *)
(*                                                                         *)
(* `env.mat` is the read discipline -- whether a read that projects a      *)
(* settled promise also PERSISTS that settlement. It is a parameter, not a *)
(* second machine.                                                         *)
(***************************************************************************)

Env ==
    [ objects  |-> objects,
      timeouts |-> timeouts,
      outbox   |-> outbox,
      now      |-> now,
      mat      |-> Materialise,
      config   |-> [retryTimeout |-> RetryTimeout] ]

-----------------------------------------------------------------------------
(***************************************************************************)
(* WHEN AN INTERNAL EVENT HAS A REASON TO FIRE                             *)
(*                                                                         *)
(* Every internal handler in `internal.lean` re-checks its own condition   *)
(* on the state it reads, and does nothing when it does not hold. So this  *)
(* guard is not a semantic commitment -- dropping it would admit strictly  *)
(* more SubmitInternal steps and every extra one would commit nothing. It  *)
(* is here because it is what a real timer wheel and drain loop do, and    *)
(* because an unguarded submit makes the fairness conditions vacuous.      *)
(*                                                                         *)
(* Note the condition is checked HERE, against the state at submit time,   *)
(* and again inside the handler, against the state at Process time. Those  *)
(* are different states, and the gap between them is not a defect but the  *)
(* subject: a lease that expires and is released by its worker in between  *)
(* submits and does nothing -- exactly the `pure ()` branch in             *)
(* `processLeaseTimeout`.                                                  *)
(***************************************************************************)

Live(id) == [kind |-> "promise", id |-> id] \in DOMAIN objects

Prom(id) == objects[[kind |-> "promise", id |-> id]].promise

(* Settled AS READ: a pending promise past its deadline is settled whether
   or not anyone has written that down yet. `PromiseObject.project`. *)
SettledNow(p) == p.state /= "pending" \/ p.timeoutAt <= now

Fires(ev) ==
    CASE ev.kind = "timeout" ->
             /\ ev.entry \in timeouts
             /\ ev.entry.at <= now
      [] ev.kind = "listenerDrain" ->
             /\ Live(ev.id)
             /\ SettledNow(Prom(ev.id))
             /\ ev.address \in Prom(ev.id).listeners
      [] ev.kind = "callbackDrain" ->
             /\ Live(ev.id)
             /\ SettledNow(Prom(ev.id))
             /\ ev.awaiter \in Prom(ev.id).callbacks

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE TRANSITIONS                                                         *)
(***************************************************************************)

Put(f, k, v) == [x \in (DOMAIN f) \cup {k} |-> IF x = k THEN v ELSE f[x]]
Drop(f, k)   == [x \in (DOMAIN f) \ {k}    |-> f[x]]

(* A client request arrives. Unguarded: a client may send anything at any
   time, including nonsense -- rejecting it is the handler's job, and the
   response it gives for nonsense is part of the protocol. *)
SubmitExternal(ev) ==
    /\ ev \in ExternalEvent
    /\ steps'   = Put(steps, nextRid, Fresh(nextRid, ev))
    /\ nextRid' = nextRid + 1
    /\ UNCHANGED <<objects, timeouts, outbox, now>>

(* The server takes a step on its own initiative: a due entry, or a name
   still written down next to a settled promise. Same shape, guarded, and
   nobody is waiting for the answer. *)
SubmitInternal(ev) ==
    /\ ev \in InternalEvent
    /\ Fires(ev)
    /\ steps'   = Put(steps, nextRid, Fresh(nextRid, ev))
    /\ nextRid' = nextRid + 1
    /\ UNCHANGED <<objects, timeouts, outbox, now>>

(* READ, DECIDE, AND COMMIT -- one atomic action, against one state.
   The read discipline of the Lean monad hoisted to the top level: the
   whole handler sees a single environment, so no read of a step can
   observe a write of its own step.
   The commit is objects and wheel TOGETHER. That is what removes the
   need for a fence, and it is the assumption named in the header.
   Arm before disarm: an entry armed twice is noise, a deadline left
   unarmed is silence. *)
Process(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "process"
    /\ LET out == Handle(steps[r].ev, Env)
       IN  /\ objects'  = [o \in (DOMAIN objects) \cup (DOMAIN out.writes) |->
                              IF o \in DOMAIN out.writes THEN out.writes[o]
                                                         ELSE objects[o]]
           /\ timeouts' = (timeouts \cup out.arm) \ (out.disarm \ out.arm)
           /\ steps'    = [steps EXCEPT
                              ![r].phase   = "perform",
                              ![r].pending =
                                  [i \in 1 .. Len(out.sends) |->
                                      [op |-> "send", entry |-> out.sends[i]]]
                                  \o << [op |-> "respond", res |-> out.res] >>]
    /\ UNCHANGED <<outbox, now, nextRid>>

(* Say one thing. When it was the last thing, the step retires -- which is
   why the pending list always ends with a `respond` and never empties on
   its own. There is no Complete: a step that has said everything is a
   step that is gone. *)
Perform(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ steps[r].pending /= << >>
    /\ LET e == Head(steps[r].pending)
       IN  outbox' = IF e.op = "send"
                     THEN {o \in outbox : MsgKey(o) /= MsgKey(e.entry)}
                              \cup {e.entry}
                     ELSE outbox
    /\ steps' = IF Len(steps[r].pending) = 1
                THEN Drop(steps, r)
                ELSE [steps EXCEPT ![r].pending = Tail(@)]
    /\ UNCHANGED <<objects, timeouts, now, nextRid>>

(* The step disappears. What it committed stays committed; a PREFIX of
   what it had to say was said, and the rest never will be. No response is
   given. This is the whole reason `sends` is a sequence and not a set,
   and it is what a client retrying a request has to survive. *)
Crash(r) ==
    /\ r \in DOMAIN steps
    /\ steps' = Drop(steps, r)
    /\ UNCHANGED <<objects, timeouts, outbox, now, nextRid>>

(* The clock. Free-running and independent of everything else -- what makes
   a deadline pass is time moving, not anyone acting. Bounded for TLC. *)
Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<objects, timeouts, outbox, steps, nextRid>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE SPECIFICATION                                                       *)
(***************************************************************************)

Init ==
    /\ objects  = [x \in {} |-> x]      \* the empty partial function
    /\ timeouts = {}
    /\ outbox   = {}
    /\ steps    = [x \in {} |-> x]
    /\ now      = 0
    /\ nextRid  = 0

Next ==
    \/ \E ev \in ExternalEvent : SubmitExternal(ev)
    \/ \E ev \in InternalEvent : SubmitInternal(ev)
    \/ \E r \in DOMAIN steps : Process(r) \/ Perform(r) \/ Crash(r)
    \/ Tick

(* A step in flight must make progress, or "the server answers" is not a
   claim this machine makes. An internal event that stays enabled must
   eventually be taken, or no timeout is ever taken and every liveness
   property is vacuous. Nothing is assumed of clients (`SubmitExternal`)
   or of failure (`Crash`): a client need never send, a crash need never
   happen. *)
Fairness ==
    /\ \A r  \in Rid           : WF_vars(Process(r) \/ Perform(r))
    /\ \A ev \in InternalEvent : WF_vars(SubmitInternal(ev))
    /\ WF_vars(Tick)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(***************************************************************************)
(* INVARIANTS                                                              *)
(***************************************************************************)

PartialFn(f, D, R) ==
    /\ DOMAIN f \subseteq D
    /\ \A k \in DOMAIN f : f[k] \in R

TypeOK ==
    /\ PartialFn(objects, Origin, Object)
    /\ timeouts \subseteq Entry
    /\ outbox \subseteq OutEntry
    /\ \A a, b \in outbox : MsgKey(a) = MsgKey(b) => a = b
    /\ PartialFn(steps, Rid, InFlight)
    /\ now \in Time
    /\ nextRid \in Nat
    (* an object is of the kind its key says *)
    /\ \A o \in DOMAIN objects : objects[o].kind = o.kind
    (* a step in "process" has decided nothing; a step in "perform" always
       has at least its own response left to give *)
    /\ \A r \in DOMAIN steps :
           IF steps[r].phase = "process" THEN steps[r].pending = << >>
                                         ELSE steps[r].pending /= << >>

(* The wheel names only deadlines that exist. Violations are NOISE: the
   entry fires, the handler reads the object, nothing is due, nothing
   happens. Stated to be measured, not because it must hold. *)
WheelSound ==
    \A e \in timeouts :
        /\ e.origin \in DOMAIN objects
        /\ Deadline(objects[e.origin], e.kind) = e.at

(* Every deadline that exists is on the wheel. Violations are SILENCE: a
   timeout that never fires, and nothing downstream recovers. This is the
   one the effect ordering in `Process` exists to maintain, and the one
   worth model-checking first. *)
WheelComplete ==
    \A o \in DOMAIN objects, k \in DeadlineKind :
        Deadline(objects[o], k) /= None =>
            [at |-> Deadline(objects[o], k), origin |-> o, kind |-> k] \in timeouts

(* The promise and its task are one unit -- the reason for fusing them.
   A settled promise never coexists with an unfulfilled task, at any
   instant, for any reader. In the Lean this is `setSettled`, two writes
   in one transaction; here there is only one write and nothing to
   interleave between. *)
UnitCoherent ==
    \A o \in DOMAIN objects :
        (/\ objects[o].kind = "promise"
         /\ objects[o].promise.state /= "pending"
         /\ objects[o].task /= None) => objects[o].task.state = "fulfilled"

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE REFINEMENT OBLIGATIONS                                              *)
(*                                                                         *)
(* Two, and they pull in opposite directions.                              *)
(*                                                                         *)
(* UPWARD, to `spec/02-abstract`: take Crash away and let each step run    *)
(* Process..Perform with no other step interleaved, and the sequence IS    *)
(* `stepOf` -- one event, one atomic transition. So the Lean machine is    *)
(* this machine under a scheduler that never interleaves and never         *)
(* crashes, and the 95 catalogue properties hold of exactly those runs.    *)
(* Which of them survive the other runs is what this module is for.        *)
(*                                                                         *)
(* DOWNWARD, to an implementation: `Process` commits objects and wheel     *)
(* atomically. A single-document store buys that only for a step that      *)
(* writes ONE unit, and these do not.                                      *)
(***************************************************************************)

MultiUnitWrites ==
    (* `promiseRegisterCallback` writes the awaited promise while reading
       the awaiter; `taskSuspend` writes every awaited promise in its
       action list; `callbackDrain` writes the awaited promise (striking
       the callback) and the awaiter's task (resuming it). Each needs
       either a transaction or a document big enough to hold both ends --
       which is what Verus's `Workflow.promises: Map<Id, Promise>` is. *)
    {"promiseRegisterCallback", "taskSuspend", "callbackDrain"}

Draining(r) == steps[r].phase = "perform"

NoInterleave ==
    \A a, b \in DOMAIN steps : (Draining(a) /\ Draining(b)) => a = b

=============================================================================
