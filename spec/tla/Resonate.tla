-------------------------------- MODULE Resonate --------------------------------
(***************************************************************************)
(* The Resonate protocol as a state machine, at the altitude of            *)
(* `spec/02-abstract` -- but with the atomicity taken OUT.                 *)
(*                                                                         *)
(* The Lean machine makes one request one transition: `stepOf` runs a      *)
(* handler and folds its whole effect list onto the pre-state, and no      *)
(* other request can be observed in between. That is the right altitude    *)
(* for saying WHAT each request means, and the wrong one for asking what   *)
(* two concurrent requests can do to each other -- because at that         *)
(* altitude, they cannot do anything to each other.                        *)
(*                                                                         *)
(* This module keeps the Lean alphabet exactly -- 21 external events, 6    *)
(* internal ones -- and splits each event into the phases the Verus        *)
(* executor uses (`src/concrete_spec/executor.rs`):                        *)
(*                                                                         *)
(*     Submit / Fire   the event arrives and becomes a step in flight      *)
(*     Process         the step READS the store and computes, from that    *)
(*                     read alone, a response and a LIST of effects        *)
(*     Perform         the step applies ONE effect, atomically             *)
(*     Complete        the pending list is empty; the step retires         *)
(*     Crash           the step vanishes; whatever it applied stays        *)
(*     Tick            the clock advances                                  *)
(*                                                                         *)
(* Interleaving lives in exactly one place: between a step's Process and   *)
(* its last Perform, any number of other steps may run to completion.      *)
(* So a step's read can be stale by the time its write lands. That is the  *)
(* hazard the whole module exists to expose, and `ver` is what catches     *)
(* it: Process snapshots the write-counters, Perform refuses a write whose *)
(* counter has moved, and the step restarts from a fresh read -- the       *)
(* `expect: etag_of(held)` / `restart` pair in the Verus executor.         *)
(*                                                                         *)
(* Two things are deliberately NOT here yet:                               *)
(*                                                                         *)
(*   - the handler bodies. `Handle` is a constant operator; the 27 cases   *)
(*     are `external.lean` and `internal.lean` transcribed, and they go in *)
(*     their own module, the way `system.lean` sits on top of the two      *)
(*     handler files rather than inlining them.                            *)
(*                                                                         *)
(*   - uncertain acknowledgement. Verus's `Told` has three values: a write *)
(*     may land and the ack be LOST (`Told::Io`), which is why a restarted *)
(*     step carries `recognize` -- the document it meant to write -- and   *)
(*     answers idempotently if it finds it already there. Here the store   *)
(*     answers truthfully, so a refused write is known to have been        *)
(*     refused and `recognize` has no reader. It is the first thing to add *)
(*     when this model grows a lossy link.                                 *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    PromiseId,          \* promise identifiers
    ScheduleId,         \* schedule identifiers
    Address,            \* message addresses -- `resonate:target`, listener addresses
    Pid,                \* worker process identifiers
    Value,              \* opaque payloads: param, value
    Tags,               \* opaque tag maps
    Cron,               \* opaque cron expressions
    Ttl,                \* the ttls a worker may ask for
    Response,           \* opaque handler responses (status + records)
    Silent,             \* the response of an internal step: nobody is listening
    Materialise,        \* `Env.mat`: does a read PERSIST a projected settlement?
    RetryTimeout,       \* `ServerConfig.retryTimeout` -- the redispatch cadence
    MaxTime,            \* clock bound, so TLC has a finite state space
    Handle(_, _)        \* Handle(ev, env) == [pending |-> Seq(Effect), res |-> Response]

(* A task is keyed by the promise it drives: `setTask { id := p.id, .. }`
   everywhere in the Lean. One name, two roles. *)
TaskId == PromiseId

Time == 0 .. MaxTime
Rid  == Nat
None == CHOOSE x : x \notin (Nat \cup Address \cup Pid)

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE OBJECTS                                                             *)
(*                                                                         *)
(* `PromiseObject` and `TaskObject` from `02-abstract/state.lean`, with    *)
(* one change: `callbacks` and `listeners` are SETS here, not lists. They  *)
(* are ledgers -- the Lean compares them with `promiseEq` rather than      *)
(* structurally, precisely because their order carries nothing.            *)
(***************************************************************************)

PromiseState == {"pending", "resolved", "rejected",
                 "rejectedCanceled", "rejectedTimedout"}

TaskState == {"pending", "acquired", "suspended", "halted", "fulfilled"}

Promise ==
    [ id        : PromiseId,
      state     : PromiseState,
      param     : Value,
      value     : Value,
      tags      : Tags,
      timeoutAt : Time,
      createdAt : Time,
      settledAt : Time \cup {None},
      callbacks : SUBSET TaskId,      \* awaiters to resume when this settles
      listeners : SUBSET Address ]    \* addresses to notify when this settles

Task ==
    [ id        : TaskId,
      state     : TaskState,
      version   : Nat,                \* the fencing token
      ttl       : Nat  \cup {None},   \* the WORKER's number: lease length asked for
      pid       : Pid  \cup {None},
      expiresAt : Time \cup {None},   \* lease deadline    -- drives r5
      retryAt   : Time \cup {None},   \* redispatch clock  -- drives r6
      resumes   : SUBSET PromiseId ]  \* triggers buffered while not suspended

Schedule ==
    [ id             : ScheduleId,
      cron           : Cron,
      promiseId      : PromiseId,
      promiseTimeout : Nat,
      promiseParam   : Value,
      promiseTags    : Tags,
      nextRunAt      : Time,          \* drives r7
      lastRunAt      : Time \cup {None},
      createdAt      : Time ]

Message ==
       [kind : {"execute"}, taskId  : TaskId, version : Nat]
  \cup [kind : {"unblock"}, promise : Promise, address : Address]

OutboxEntry == [address : Address, message : Message]

(* `OutboxEntry.key`: what a later message REPLACES rather than joins.
   Per task for execute, per (promise, address) for unblock. *)
MsgKey(e) ==
    IF e.message.kind = "execute"
    THEN <<"execute", e.message.taskId>>
    ELSE <<"unblock", e.message.promise.id, e.address>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE ALPHABET                                                            *)
(*                                                                         *)
(* `Equivalence.Request` and the internal constructors of                  *)
(* `Abstraction.Step`, as one set of records tagged by `kind`. The         *)
(* external/internal line is the line between what a client asks for and   *)
(* what the server does on its own initiative -- `Step.isExternal` and     *)
(* `Step.isInternal` -- and it is the line the top-level transitions are   *)
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

(* The six steps no client asks for. The names are the Lean's `r1`..`r7`,
   with the hole at `r2` kept -- `r5` means the lease timeout in every
   document that names it, and renumbering would silently move it. *)
InternalEvent ==
       [kind : {"promiseTimeout"}, id : PromiseId]                      \* r1
  \cup [kind : {"listenerDrain"},  id : PromiseId, address : Address]   \* r3
  \cup [kind : {"callbackDrain"},  id : PromiseId, awaiter : TaskId]    \* r4
  \cup [kind : {"leaseTimeout"},   id : TaskId]                         \* r5
  \cup [kind : {"retryDispatch"},  id : TaskId]                         \* r6
  \cup [kind : {"scheduleFire"},   id : ScheduleId]                     \* r7

Event == ExternalEvent \cup InternalEvent

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE EFFECTS                                                             *)
(*                                                                         *)
(* `AbstractModel.Effect`, unchanged. What is new is that they are applied *)
(* ONE AT A TIME: in Lean `applyAll` folds the list onto the pre-state as  *)
(* one transaction, here each element is its own transition and another    *)
(* step may run between any two of them. A crash therefore leaves a        *)
(* PREFIX of the list applied, which is why the order a handler emits them *)
(* in is part of the protocol and not an implementation detail.            *)
(***************************************************************************)

Effect ==
       [op : {"setPromise"},  obj     : Promise]
  \cup [op : {"setTask"},     obj     : Task]
  \cup [op : {"setSchedule"}, obj     : Schedule]
  \cup [op : {"delSchedule"}, id      : ScheduleId]
  \cup [op : {"setMessage"},  address : Address, message : Message]

(* The store keys an effect writes -- the keys its write is fenced on.
   The outbox is not fenced: it is keyed, last-writer-wins by `MsgKey`,
   and no handler reads it. *)
Key ==
       [obj : {"promise"},  id : PromiseId]
  \cup [obj : {"task"},     id : TaskId]
  \cup [obj : {"schedule"}, id : ScheduleId]

Writes(e) ==
    CASE e.op = "setPromise"  -> {[obj |-> "promise",  id |-> e.obj.id]}
      [] e.op = "setTask"     -> {[obj |-> "task",     id |-> e.obj.id]}
      [] e.op = "setSchedule" -> {[obj |-> "schedule", id |-> e.obj.id]}
      [] e.op = "delSchedule" -> {[obj |-> "schedule", id |-> e.id]}
      [] e.op = "setMessage"  -> {}

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE VARIABLES                                                           *)
(***************************************************************************)

VARIABLES
    promises,   \* [PromiseId  -> Promise],  partial: DOMAIN is what exists
    tasks,      \* [TaskId     -> Task],     partial
    schedules,  \* [ScheduleId -> Schedule], partial
    outbox,     \* SUBSET OutboxEntry, at most one entry per MsgKey
    ver,        \* [Key -> Nat] -- the write counter each store key is fenced on
    now,        \* the clock. NOT part of the store: a server does not own the
                \* time, it reads it. Two machines at the same state and
                \* different instants are at the same state.
    steps,      \* [Rid -> InFlight] -- the requests currently in flight.
                \* VOLATILE: Crash drops one, and nothing durable refers to it
    nextRid,    \* fresh step identifiers
    history     \* AUDITING ONLY. Seq of <<event, response>> as steps retire.
                \* No action reads it; it exists to state refinement against
                \* the Lean machine and to phrase response-level properties

store == <<promises, tasks, schedules, outbox, ver>>
vars  == <<promises, tasks, schedules, outbox, ver, now, steps, nextRid, history>>

(* A step in flight. In phase "process" it has read nothing yet, and
   `pending`, `res` and `snap` carry no information. `Process` fills all
   three at once and moves it to "perform"; from then on the step is a
   list of effects being drained and a response waiting to be given. *)
InFlight ==
    [ rid     : Rid,
      ev      : Event,
      phase   : {"process", "perform"},
      pending : Seq(Effect),
      res     : Response \cup {Silent},
      snap    : [Key -> Nat],   \* `ver` as it read it -- the fence
      retries : Nat ]

Fresh(r, ev) ==
    [ rid     |-> r,
      ev      |-> ev,
      phase   |-> "process",
      pending |-> << >>,
      res     |-> Silent,
      snap    |-> [k \in Key |-> 0],
      retries |-> 0 ]

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE ENVIRONMENT A HANDLER READS                                         *)
(*                                                                         *)
(* `AbstractModel.Env`, plus the clock. A handler is a pure function of    *)
(* this and its event: `Handle(ev, env)` yields the effects to apply and   *)
(* the response to give, and reads NOTHING else. `mat` is the read         *)
(* discipline -- whether a read that projects a settled promise also       *)
(* PERSISTS that settlement -- and it is a parameter, not a second         *)
(* machine.                                                                *)
(***************************************************************************)

Env ==
    [ promises  |-> promises,
      tasks     |-> tasks,
      schedules |-> schedules,
      outbox    |-> outbox,
      now       |-> now,
      mat       |-> Materialise,
      config    |-> [retryTimeout |-> RetryTimeout] ]

-----------------------------------------------------------------------------
(***************************************************************************)
(* WHEN AN INTERNAL EVENT HAS A REASON TO FIRE                             *)
(*                                                                         *)
(* Every internal handler in `internal.lean` re-checks its own condition   *)
(* on the state it reads, and does nothing when it does not hold. So this  *)
(* guard is not a semantic commitment -- dropping it would admit strictly  *)
(* more Fire steps and every extra one would Process to an empty effect    *)
(* list. It is here because it is what a real server's timer wheel and     *)
(* drain loop actually do, and because unguarded Fire makes the fairness   *)
(* conditions below vacuous.                                              *)
(*                                                                         *)
(* Note that the condition is checked HERE, against the state at Fire      *)
(* time, and again inside the handler, against the state at Process time.  *)
(* Those are different states. A task whose lease expires and is released  *)
(* by its worker in between fires r5 and does nothing -- exactly the       *)
(* `pure ()` branch in `processLeaseTimeout`.                              *)
(***************************************************************************)

(* Settled AS READ: a pending promise past its deadline is settled whether
   or not anyone has written that down yet. `PromiseObject.project`. *)
SettledNow(p) == p.state /= "pending" \/ p.timeoutAt <= now

Fires(ev) ==
    CASE ev.kind = "promiseTimeout" ->
             /\ ev.id \in DOMAIN promises
             /\ promises[ev.id].state = "pending"
             /\ promises[ev.id].timeoutAt <= now
      [] ev.kind = "listenerDrain" ->
             /\ ev.id \in DOMAIN promises
             /\ SettledNow(promises[ev.id])
             /\ ev.address \in promises[ev.id].listeners
      [] ev.kind = "callbackDrain" ->
             /\ ev.id \in DOMAIN promises
             /\ SettledNow(promises[ev.id])
             /\ ev.awaiter \in promises[ev.id].callbacks
      [] ev.kind = "leaseTimeout" ->
             /\ ev.id \in DOMAIN tasks
             /\ tasks[ev.id].state = "acquired"
             /\ tasks[ev.id].expiresAt /= None
             /\ tasks[ev.id].expiresAt <= now
      [] ev.kind = "retryDispatch" ->
             /\ ev.id \in DOMAIN tasks
             /\ tasks[ev.id].state = "pending"
             /\ tasks[ev.id].retryAt /= None
             /\ tasks[ev.id].retryAt <= now
      [] ev.kind = "scheduleFire" ->
             /\ ev.id \in DOMAIN schedules
             /\ schedules[ev.id].nextRunAt <= now

-----------------------------------------------------------------------------
(***************************************************************************)
(* APPLYING ONE EFFECT                                                     *)
(***************************************************************************)

Put(f, k, v)  == [x \in (DOMAIN f) \cup {k} |-> IF x = k THEN v ELSE f[x]]
Drop(f, k)    == [x \in (DOMAIN f) \ {k}    |-> f[x]]

ApplyEffect(e) ==
    CASE e.op = "setPromise" ->
             /\ promises'  = Put(promises, e.obj.id, e.obj)
             /\ UNCHANGED <<tasks, schedules, outbox>>
      [] e.op = "setTask" ->
             /\ tasks'     = Put(tasks, e.obj.id, e.obj)
             /\ UNCHANGED <<promises, schedules, outbox>>
      [] e.op = "setSchedule" ->
             /\ schedules' = Put(schedules, e.obj.id, e.obj)
             /\ UNCHANGED <<promises, tasks, outbox>>
      [] e.op = "delSchedule" ->
             /\ schedules' = Drop(schedules, e.id)
             /\ UNCHANGED <<promises, tasks, outbox>>
      [] e.op = "setMessage" ->
             /\ LET entry == [address |-> e.address, message |-> e.message]
                IN  outbox' = {o \in outbox : MsgKey(o) /= MsgKey(entry)}
                                  \cup {entry}
             /\ UNCHANGED <<promises, tasks, schedules>>

Bump(e) == [k \in Key |-> IF k \in Writes(e) THEN ver[k] + 1 ELSE ver[k]]

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE TOP-LEVEL TRANSITIONS                                               *)
(***************************************************************************)

(* An external event arrives. Unguarded: a client may send anything at any
   time, including nonsense -- rejecting it is the handler's job, and the
   response it gives is part of the protocol. *)
Submit(ev) ==
    /\ ev \in ExternalEvent
    /\ steps'   = Put(steps, nextRid, Fresh(nextRid, ev))
    /\ nextRid' = nextRid + 1
    /\ UNCHANGED <<promises, tasks, schedules, outbox, ver, now, history>>

(* An internal event fires. Same shape, guarded by `Fires`, and nobody is
   waiting for the answer. *)
Fire(ev) ==
    /\ ev \in InternalEvent
    /\ Fires(ev)
    /\ steps'   = Put(steps, nextRid, Fresh(nextRid, ev))
    /\ nextRid' = nextRid + 1
    /\ UNCHANGED <<promises, tasks, schedules, outbox, ver, now, history>>

(* READ AND DECIDE, in one atomic action and against one state. This is
   the `bind` discipline of the Lean monad hoisted to the top level: the
   whole handler sees a single environment, so no read of a step can
   observe a write of that same step. Everything the step will do is
   settled here; `Perform` only carries it out.
   `snap` freezes the fence: the versions the decision was made under. *)
Process(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "process"
    /\ LET out == Handle(steps[r].ev, Env)
       IN  steps' = [steps EXCEPT ![r].phase   = "perform",
                                  ![r].pending = out.pending,
                                  ![r].res     = out.res,
                                  ![r].snap    = ver]
    /\ UNCHANGED <<promises, tasks, schedules, outbox, ver, now, nextRid, history>>

(* Apply the next effect -- OR discover the decision is stale and redo it.
   The fence is per key: a step that read promise p and writes promise p
   is refused if anyone wrote p in between. It then goes back to "process"
   and re-reads, which is the only reason `retries` exists. *)
Stale(r) ==
    LET e == Head(steps[r].pending)
    IN  \E k \in Writes(e) : ver[k] /= steps[r].snap[k]

Perform(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ steps[r].pending /= << >>
    /\ IF Stale(r)
       THEN /\ steps' = [steps EXCEPT ![r].phase   = "process",
                                      ![r].pending = << >>,
                                      ![r].res     = Silent,
                                      ![r].retries = @ + 1]
            /\ UNCHANGED <<promises, tasks, schedules, outbox, ver>>
       ELSE /\ ApplyEffect(Head(steps[r].pending))
            /\ ver'   = Bump(Head(steps[r].pending))
            /\ steps' = [steps EXCEPT ![r].pending = Tail(steps[r].pending)]
    /\ UNCHANGED <<now, nextRid, history>>

(* Every effect landed. The step retires and its response becomes visible.
   For an internal step the response is `Silent` and this is bookkeeping. *)
Complete(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ steps[r].pending = << >>
    /\ steps'   = Drop(steps, r)
    /\ history' = Append(history, <<steps[r].ev, steps[r].res>>)
    /\ UNCHANGED <<promises, tasks, schedules, outbox, ver, now, nextRid>>

(* The step disappears. A PREFIX of its effects is already applied and
   stays applied; no response is ever given. This is the whole reason the
   effect list is ordered rather than a set, and it is what a client
   retrying a request has to survive. *)
Crash(r) ==
    /\ r \in DOMAIN steps
    /\ steps' = Drop(steps, r)
    /\ UNCHANGED <<promises, tasks, schedules, outbox, ver, now, nextRid, history>>

(* The clock. Free-running and independent of everything else -- what makes
   a deadline pass is time moving, not anyone acting. Bounded for TLC. *)
Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<promises, tasks, schedules, outbox, ver, steps, nextRid, history>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE SPECIFICATION                                                       *)
(***************************************************************************)

Init ==
    /\ promises  = [x \in {} |-> x]            \* the empty partial function
    /\ tasks     = [x \in {} |-> x]
    /\ schedules = [x \in {} |-> x]
    /\ outbox    = {}
    /\ ver       = [k \in Key |-> 0]
    /\ now       = 0
    /\ steps     = [x \in {} |-> x]
    /\ nextRid   = 0
    /\ history   = << >>

Next ==
    \/ \E ev \in ExternalEvent : Submit(ev)
    \/ \E ev \in InternalEvent : Fire(ev)
    \/ \E r \in DOMAIN steps : Process(r) \/ Perform(r) \/ Complete(r) \/ Crash(r)
    \/ Tick

(* Fairness. A step in flight must make progress -- otherwise "the server
   answers" is not a claim this machine makes. An internal event that stays
   enabled must eventually fire -- otherwise no timeout is ever taken and
   every liveness property is vacuous. Nothing is assumed of clients
   (`Submit`) or of failure (`Crash`): a client need never send, and a
   crash need never happen. *)
Fairness ==
    /\ \A r \in Rid : WF_vars(Process(r) \/ Perform(r) \/ Complete(r))
    /\ \A ev \in InternalEvent : WF_vars(Fire(ev))
    /\ WF_vars(Tick)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(***************************************************************************)
(* TYPE INVARIANT                                                          *)
(***************************************************************************)

PartialFn(f, D, R) ==
    /\ DOMAIN f \subseteq D
    /\ \A k \in DOMAIN f : f[k] \in R

TypeOK ==
    /\ PartialFn(promises,  PromiseId,  Promise)
    /\ PartialFn(tasks,     TaskId,     Task)
    /\ PartialFn(schedules, ScheduleId, Schedule)
    /\ outbox \subseteq OutboxEntry
    /\ \A a, b \in outbox : MsgKey(a) = MsgKey(b) => a = b
    /\ ver \in [Key -> Nat]
    /\ now \in Time
    /\ PartialFn(steps, Rid, InFlight)
    /\ nextRid \in Nat
    (* keys are what they say they are *)
    /\ \A k \in DOMAIN promises  : promises[k].id  = k
    /\ \A k \in DOMAIN tasks     : tasks[k].id     = k
    /\ \A k \in DOMAIN schedules : schedules[k].id = k
    (* a step in "process" has decided nothing yet *)
    /\ \A r \in DOMAIN steps :
           steps[r].phase = "process" => steps[r].pending = << >>

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE COLLAPSE                                                            *)
(*                                                                         *)
(* What relates this to `spec/02-abstract`. Take Crash away and let a step *)
(* run Process..Complete with no other step interleaved, and the sequence  *)
(* is `stepOf` -- one event, one atomic transition, effects folded on in   *)
(* order. `history` is then the Lean's trace of (request, response) pairs. *)
(*                                                                         *)
(* So the Lean machine is this machine under a scheduler that never        *)
(* interleaves and never crashes, and everything the catalogue proves      *)
(* about it holds of the runs where that scheduler is in charge. The       *)
(* question this module is for is which of those 95 properties survive     *)
(* when it is not.                                                         *)
(***************************************************************************)

Draining(r) == steps[r].phase = "perform" /\ steps[r].pending /= << >>

NoInterleave ==
    \A a, b \in DOMAIN steps : (Draining(a) /\ Draining(b)) => a = b

=============================================================================
