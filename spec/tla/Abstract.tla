-------------------------------- MODULE Abstract --------------------------------
(***************************************************************************)
(* The Resonate protocol as a state machine: ONE EVENT, ONE TRANSITION.   *)
(*                                                                         *)
(* This is `spec/02-abstract` as it is written. `stepOf` runs a handler    *)
(* and `applyAll` folds its whole effect list onto the pre-state, and no   *)
(* other request can be observed in between. Nothing here is in flight,    *)
(* because at this altitude nothing IS in flight: a machine that takes a   *)
(* request and answers it in one step has no state between the two.        *)
(*                                                                         *)
(* Three variables, and nothing else is protocol:                          *)
(*                                                                         *)
(*     objects    what is true. A promise and its optional task are ONE    *)
(*                unit -- settled together, fulfilled together, never      *)
(*                observed disagreeing                                     *)
(*     outbox     what has been said. Keyed, and the only thing that       *)
(*                leaves the system                                        *)
(*     now        the clock. Not part of the store: a server does not own  *)
(*                the time, it reads it                                    *)
(*                                                                         *)
(* WHAT IS NOT HERE, AND WHY.                                              *)
(*                                                                         *)
(*   - THERE IS NO TIMER WHEEL. An internal step fires because an object   *)
(*     carries a deadline that has come due, which is where the Lean       *)
(*     reads it from, and why nothing in the 95 catalogue entries mentions *)
(*     an index. A wheel is something an executor keeps so it does not     *)
(*     have to scan; that it can drift from the objects is a fact about    *)
(*     the executor, and it is stated in `Concrete`, where the wheel is.   *)
(*                                                                         *)
(*   - THERE ARE NO STEPS IN FLIGHT. No phases, no pending effects, no     *)
(*     crash, no retry. An implementation that decides and then writes has *)
(*     an interval between the two and needs all of that; this machine has *)
(*     no interval, so it needs none of it. Saying otherwise would be      *)
(*     dictating how an implementation tracks work it is in the middle of, *)
(*     which is not the protocol's business.                               *)
(*                                                                         *)
(* Which is what makes the refinement in `Concrete` say something sharp.   *)
(* Every phase, every arm, every refused compare-and-swap is a STUTTER     *)
(* here -- it moves nothing this machine can see -- and the one concrete   *)
(* transition that is not a stutter is the document write. THE WRITE IS    *)
(* THE LINEARISATION POINT, and the fence is what makes it land against    *)
(* the state it was decided against.                                       *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, Variants, Apalache

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE TYPES                                                               *)
(*                                                                         *)
(* Apalache needs every variable's shape declared, and writing them down   *)
(* forced three encodings that plain TLA+ let us leave vague. All three    *)
(* are improvements, not concessions:                                      *)
(*                                                                         *)
(*   - THERE IS NO `None`. An earlier draft had one value standing for     *)
(*     absence in six places at once -- absent settlement, absent lease,   *)
(*     absent worker, absent task -- which is not a value any type system  *)
(*     will admit, and was hiding that those are four different questions. *)
(*     Absent instants are `NoTime`, which is `-1` and cannot be confused  *)
(*     with an instant because instants are non-negative. An absent worker *)
(*     is `NoPid`, a constant ASSUMEd outside `Pid`. And an absent task is *)
(*     a task in state `"none"` -- which is the honest reading: whether a  *)
(*     promise is targeted is part of the unit's state, not a hole in it.  *)
(*                                                                         *)
(*   - THE ALPHABET IS A VARIANT. 20 events with 20 different payloads is  *)
(*     a tagged union, and writing it as a set of records with a `kind`    *)
(*     field only worked because nothing checked that the fields lined up. *)
(*     `$event` says it: one tag, one payload, and a handler that forgets  *)
(*     a case is a type error rather than a silent fallthrough.            *)
(*                                                                         *)
(*   - `taskFence` CARRIES A VARIANT TOO. Its action is a create or a      *)
(*     settle, which is `TaskFenceAction` in the Lean and was an untagged  *)
(*     union of two record types here.                                     *)
(***************************************************************************)

(*
  @typeAlias: id = { origin: ORIGIN, rest: REST };

  @typeAlias: tags = { targeted: Bool, timer: Bool, external: Bool, target: ADDR, delay: Int };

  @typeAlias: promise = {
      state: Str,
      param: VALUE,
      value: VALUE,
      tags: $tags,
      timeoutAt: Int,
      createdAt: Int,
      settledAt: Int,
      callbacks: Set($id),
      listeners: Set(ADDR)
  };

  @typeAlias: task = {
      state: Str,
      version: Int,
      ttl: Int,
      pid: PID,
      expiresAt: Int,
      retryAt: Int,
      resumes: Set($id)
  };

  @typeAlias: object = { promise: $promise, task: $task };

  @typeAlias: entry = { at: Int, id: $id, kind: Str };

  @typeAlias: message =
      Execute({ id: $id, version: Int })
    | Unblock({ id: $id, state: Str });

  @typeAlias: outEntry = { address: ADDR, message: $message };

  @typeAlias: msgKey = { kind: Str, id: $id, address: ADDR };

  @typeAlias: createReq = { id: $id, timeoutAt: Int, param: VALUE, tags: $tags };
  @typeAlias: settleReq = { id: $id, state: Str, value: VALUE };
  @typeAlias: callbackReq = { awaited: $id, awaiter: $id };
  @typeAlias: taskRef = { id: $id, version: Int };

  @typeAlias: fenceAction = Create({ req: $createReq }) | Settle({ req: $settleReq });

  @typeAlias: event =
      PromiseGet({ id: $id })
    | PromiseCreate({ req: $createReq })
    | PromiseSettle({ req: $settleReq })
    | PromiseRegisterCallback({ req: $callbackReq })
    | PromiseRegisterListener({ awaited: $id, address: ADDR })
    | PromiseSearch(UNIT)
    | TaskGet({ id: $id })
    | TaskCreate({ pid: PID, ttl: Int, action: $createReq })
    | TaskAcquire({ id: $id, version: Int, pid: PID, ttl: Int })
    | TaskFence({ id: $id, version: Int, action: $fenceAction })
    | TaskHeartbeat({ pid: PID, tasks: Set($taskRef) })
    | TaskSuspend({ id: $id, version: Int, actions: Set($callbackReq) })
    | TaskFulfill({ id: $id, version: Int, action: $settleReq })
    | TaskRelease({ id: $id, version: Int })
    | TaskHalt({ id: $id })
    | TaskContinue({ id: $id })
    | TaskSearch(UNIT)
    | Timeout({ id: $id, kind: Str })
    | ListenerDrain({ id: $id, address: ADDR })
    | CallbackDrain({ id: $id, awaiter: $id });

  @typeAlias: effect =
      PutObject({ id: $id, obj: $object })
    | ArmTimeout({ entry: $entry })
    | DisarmTimeout({ entry: $entry })
    | Send({ entry: $outEntry });

  @typeAlias: inFlight = {
      ev: $event,
      phase: Str,
      pending: Seq($effect),
      res: RESPONSE
  };

  @typeAlias: env = {
      objects: $id -> $object,
      now: Int,
      mat: Bool,
      config: { retryTimeout: Int }
  };

  @typeAlias: outcome = { effects: Seq($effect), res: RESPONSE };
*)
ResonateAliases == TRUE

-----------------------------------------------------------------------------

CONSTANTS
    \* @type: Set(ORIGIN);
    Origin,
    \* @type: Set(REST);
    Rest,
    \* @type: Set(ADDR);
    Address,
    \* @type: Set(PID);
    Pid,
    \* @type: Set(VALUE);
    Value,
    \* @type: VALUE;
    NoValue,
    \* @type: Set(Int);
    Ttl,
    \* @type: Set(RID);
    Rid,
    \* @type: Set(Str);
    Implemented,
    \* @type: PID;
    NoPid,
    \* @type: ADDR;
    NoAddr,
    \* @type: RESPONSE;
    Silent,
    \* @type: Bool;
    Materialise,
    \* @type: Int;
    RetryTimeout,
    \* @type: Int;
    MaxTime,
    \* @type: Int;
    MaxVersion

ASSUME NoPid   \notin Pid
ASSUME NoAddr  \notin Address
ASSUME NoValue \notin Value
ASSUME MaxTime    >= 0
ASSUME MaxVersion >= 0

Time    == 0 .. MaxTime
Version == 0 .. MaxVersion

(* Absence of an instant. `-1` and not a distinguished constant, because
   instants are non-negative and the arithmetic never reaches it. *)
NoTime == -1

(* The tags, as the questions the protocol actually asks of them. `Tags`
   was an opaque constant while nothing read it; a handler does, and
   `p.tags.has "resonate:target"` is not something an uninterpreted type
   can answer. Verus made the same move -- `PromiseG` carries `external`,
   `targeted` and `timer` as fields rather than a tag map -- for the same
   reason: what the protocol reads is a fixed, small set of predicates,
   and the map they are encoded in is wire format.

   `delay` is pinned to 0. It only shifts the first retry, and none of
   the three handlers below reads it. *)
(* FOUR PROFILES, not the eight-way product of the three flags.
   
   The product is mostly nonsense -- timer AND targeted is refused at the
   door, and external adds nothing to a promise already targeted, since
   `IsExternal` is a disjunction. What the protocol actually distinguishes
   is four kinds of promise, and these are they:
   
     plain     nobody outside settles it, nothing can wait on it
     external  something outside settles it, so it can be awaited
     targeted  a task drives it, and it can be awaited
     timer     the deadline resolves it rather than rejecting it
   
   Enumerating four instead of eight is not a weaker model of the same
   thing; it is the same model with the meaningless combinations left
   out, and it roughly halves an alphabet that had stopped being
   checkable. *)
Tags ==
    LET a == CHOOSE x \in Address : TRUE IN
    { [targeted |-> FALSE, timer |-> FALSE, external |-> FALSE, target |-> a, delay |-> 0],
      [targeted |-> FALSE, timer |-> FALSE, external |-> TRUE,  target |-> a, delay |-> 0],
      [targeted |-> TRUE,  timer |-> FALSE, external |-> FALSE, target |-> a, delay |-> 0],
      [targeted |-> FALSE, timer |-> TRUE,  external |-> FALSE, target |-> a, delay |-> 0] }

(* `PromiseObject.external` -- who is allowed to be awaited. A promise
   nobody outside can settle cannot be waited on, because nothing would
   ever wake the waiter. *)
IsExternal(p) == p.tags.external \/ p.tags.targeted \/ p.tags.timer

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE IDENTIFIER                                                          *)
(*                                                                         *)
(* An identifier is a PAIR, and the pair is what is unique -- `rest` alone *)
(* is not. Both halves are opaque here: what an origin denotes is not      *)
(* something any transition in this module reads, and the moment it        *)
(* becomes something the protocol reads it stops being a constant.         *)
(*                                                                         *)
(* One identifier names one object, and an object is a promise and, if the *)
(* promise is targeted, the task that drives it. That is why there is no   *)
(* separate task identifier: `setTask { id := p.id, .. }` everywhere in    *)
(* the Lean, one name for both roles.                                      *)
(***************************************************************************)

Id == [origin : Origin, rest : Rest]

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
(* There is nothing else. Schedules are not objects and are not modelled   *)
(* here: a schedule creates promises on a cron, which makes it a source of *)
(* events rather than a thing the protocol acts on, and folding it in      *)
(* would have cost a second key space, a second deadline kind and a tagged *)
(* union, to say nothing this module is trying to say. `scheduleGet`,      *)
(* `scheduleCreate`, `scheduleDelete`, `scheduleSearch` and the Lean's     *)
(* `r7` are out of the alphabet with it.                                   *)
(*                                                                         *)
(* One change from the Lean: `callbacks` and `listeners` are SETS, not     *)
(* lists -- they are ledgers, and `promiseEq` already compares them        *)
(* without regard to order.                                                *)
(*                                                                         *)
(* An object carries no clock. The Verus `Workflow.clock` is a per-        *)
(* document high-water mark that exists because that executor writes to a  *)
(* store which may serve a stale read, and `clamp` is what keeps the       *)
(* document's own notion of time from regressing. That is a fact about     *)
(* the store, not about the protocol. Here there is one clock, `now`, and  *)
(* every step reads it directly.                                           *)
(***************************************************************************)

PromiseState == {"pending", "resolved", "rejected",
                 "rejectedCanceled", "rejectedTimedout"}

(* `"none"` is the state of a unit whose promise is not targeted -- there
   is no task. It is a state and not a hole: whether a promise is driven
   by a task is something the unit knows about itself. *)
TaskState == {"none", "pending", "acquired", "suspended", "halted", "fulfilled"}

(* @type: $task; *)
NoTask ==
    [ state |-> "none", version |-> 0, ttl |-> NoTime, pid |-> NoPid,
      expiresAt |-> NoTime, retryAt |-> NoTime, resumes |-> {} ]

Promise == [
    state     : PromiseState,
    param     : Value,
    value     : Value,
    tags      : Tags,
    timeoutAt : Time,                    \* deadline -- armed as "promise"
    createdAt : Time,
    settledAt : Time \cup {NoTime},
    callbacks : SUBSET Id,               \* awaiters to resume when this settles
    listeners : SUBSET Address ]         \* addresses to notify when this settles

Task == [
    state     : TaskState,
    version   : Version,                 \* the fencing token
    ttl       : Ttl \cup {NoTime},       \* the WORKER's number: lease asked for
    pid       : Pid \cup {NoPid},
    expiresAt : Time \cup {NoTime},      \* deadline -- armed as "lease"
    retryAt   : Time \cup {NoTime},      \* deadline -- armed as "retry"
    resumes   : SUBSET Id ]              \* triggers buffered while not suspended

Object == [promise : Promise, task : Task]

Message ==
       { Variant("Execute", [id |-> i, version |-> v]) : i \in Id, v \in Version }
  \cup { Variant("Unblock", [id |-> i, state |-> s]) : i \in Id, s \in PromiseState }

OutEntry == [address : Address, message : Message]

(* `OutboxEntry.key`: what a later message REPLACES rather than joins. Per
   task for execute -- so a second dispatch of the same task supersedes the
   first wherever it was going -- and per (promise, address) for unblock,
   since two listeners on one promise are two separate things to say.
   `NoAddr` is what makes the execute key address-blind while keeping one
   record type for both, which is what a type system asks for and what a
   pair of differently-shaped tuples would not have given.
   @type: $outEntry => $msgKey; *)
MsgKey(e) ==
    IF VariantTag(e.message) = "Execute"
    THEN [kind |-> "execute",
          id      |-> VariantGetUnsafe("Execute", e.message).id,
          address |-> NoAddr]
    ELSE [kind |-> "unblock",
          id      |-> VariantGetUnsafe("Unblock", e.message).id,
          address |-> e.address]

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE TIMEOUTS                                                            *)
(*                                                                         *)
(* The wheel. Every entry names a deadline that an object already carries: *)
(* `promise` is `Promise.timeoutAt`, `lease` is `Task.expiresAt`, and      *)
(* `retry` is `Task.retryAt`.                                              *)
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

DeadlineKind == {"promise", "lease", "retry"}

Entry == [at : Time, id : Id, kind : DeadlineKind]

(* The deadline an object actually carries for a given kind -- what the
   wheel is an index OF. `NoTime` when the object carries no such deadline
   live: a settled promise has no timeout, an unacquired task no lease.
   @type: ($object, Str) => Int; *)
Deadline(obj, kind) ==
    CASE kind = "promise" ->
             IF obj.promise.state = "pending" THEN obj.promise.timeoutAt ELSE NoTime
      [] kind = "lease" -> obj.task.expiresAt
      [] OTHER          -> obj.task.retryAt

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE ALPHABET                                                            *)
(*                                                                         *)
(* `Equivalence.Request` and the internal constructors of                  *)
(* `Abstraction.Step`, as one variant. The external/internal line is the   *)
(* line between what a client asks for and what the server does on its own *)
(* initiative -- `Step.isExternal` and `Step.isInternal` -- and it is the  *)
(* line the two Submit transitions are organised around: an external event *)
(* may arrive at any moment, an internal one only when the server has a    *)
(* reason to take it.                                                      *)
(***************************************************************************)

CreateReq   == [id : Id, timeoutAt : Time, param : Value, tags : Tags]
SettleReq   == [id : Id, state : PromiseState, value : Value]
CallbackReq == [awaited : Id, awaiter : Id]
TaskRefT    == [id : Id, version : Version]

FenceAction ==
       { Variant("Create", [req |-> r]) : r \in CreateReq }
  \cup { Variant("Settle", [req |-> r]) : r \in SettleReq }

(* Which constructors the alphabet actually offers.
   `Implemented` is a CONSTANT, and it is the honest way to say that a
   handler exists yet for three of the twenty events. The gate is per
   CONSTRUCTOR rather than a filter over the whole alphabet, because a
   filter would have to enumerate what it discards -- and enumerating
   `TaskFence` over every create and settle request is most of the cost of
   checking anything at all.

   Both machines read the same constant, so `Abstract` and `Concrete`
   always quantify over exactly the same events, and the line moves by
   editing a config as handlers land.
   @type: (Str, Set($event)) => Set($event); *)
On(tag, S) == IF tag \in Implemented THEN S ELSE {}

ExternalEvent ==
       {}
    \cup On("PromiseGet", { Variant("PromiseGet", [id |-> i]) : i \in Id})
    \cup On("PromiseCreate", { Variant("PromiseCreate", [req |-> r]) : r \in CreateReq})
    \cup On("PromiseSettle", { Variant("PromiseSettle", [req |-> r]) : r \in SettleReq})
    \cup On("PromiseRegisterCallback", { Variant("PromiseRegisterCallback", [req |-> r]) : r \in CallbackReq})
    \cup On("PromiseRegisterListener", { Variant("PromiseRegisterListener", [awaited |-> i, address |-> a]) : i \in Id, a \in Address})
    \cup On("PromiseSearch", { Variant("PromiseSearch", UNIT)})
    \cup On("TaskGet", { Variant("TaskGet", [id |-> i]) : i \in Id})
    \cup On("TaskCreate", { Variant("TaskCreate", [pid |-> p, ttl |-> t, action |-> r]) : p \in Pid, t \in Ttl, r \in CreateReq})
    \cup On("TaskAcquire", { Variant("TaskAcquire", [id |-> i, version |-> v, pid |-> p, ttl |-> t]) : i \in Id, v \in Version, p \in Pid, t \in Ttl})
    \cup On("TaskFence", { Variant("TaskFence", [id |-> i, version |-> v, action |-> a]) : i \in Id, v \in Version, a \in FenceAction})
    \cup On("TaskHeartbeat", { Variant("TaskHeartbeat", [pid |-> p, tasks |-> ts]) : p \in Pid, ts \in SUBSET TaskRefT})
    \cup On("TaskSuspend", { Variant("TaskSuspend", [id |-> i, version |-> v, actions |-> as]) : i \in Id, v \in Version, as \in SUBSET CallbackReq})
    \cup On("TaskFulfill", { Variant("TaskFulfill", [id |-> i, version |-> v, action |-> r]) : i \in Id, v \in Version, r \in SettleReq})
    \cup On("TaskRelease", { Variant("TaskRelease", [id |-> i, version |-> v]) : i \in Id, v \in Version})
    \cup On("TaskHalt", { Variant("TaskHalt", [id |-> i]) : i \in Id})
    \cup On("TaskContinue", { Variant("TaskContinue", [id |-> i]) : i \in Id})
    \cup On("TaskSearch", { Variant("TaskSearch", UNIT)})

(* The events no client asks for. Two shapes, and the split is by WHAT
   MAKES THEM AVAILABLE -- which is, in both cases, something an object
   says about itself:

     - a DEADLINE the object carries. `Promise.timeoutAt`,
       `Task.expiresAt` and `Task.retryAt` are the three, so the event
       names an object and which of its deadlines it is about. It does
       NOT carry the instant: the object already knows it, and an event
       carrying its own copy could disagree with the thing it is about.

       This is the Lean's r1, r5 and r6. An earlier draft made it one
       event carrying a wheel entry -- `{at, id, kind}` -- which quietly
       put an executor's index into the protocol's alphabet. A wheel is
       something an implementation keeps so it need not scan for what is
       due; `Concrete` keeps one, and here there is nothing to keep in
       step with the objects because there is nothing but the objects.

     - a NAME still written down next to a settled promise. This is r3
       and r4, the listener and callback drains, and they were always
       written this way -- `address \in listeners`, `awaiter \in
       callbacks`.

   The deadline case now has the same shape as the drains, which is the
   point: an internal step is available because of what is IN THE STORE,
   never because of what is in a register beside it. *)
InternalEvent ==
       {}
    \cup On("Timeout", { Variant("Timeout", [id |-> i, kind |-> k])
                         : i \in Id, k \in DeadlineKind})
    \cup On("ListenerDrain", { Variant("ListenerDrain", [id |-> i, address |-> a]) : i \in Id, a \in Address})
    \cup On("CallbackDrain", { Variant("CallbackDrain", [id |-> i, awaiter |-> w]) : i \in Id, w \in Id})

Event == ExternalEvent \cup InternalEvent

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE VARIABLES                                                           *)
(***************************************************************************)

VARIABLES
    \* @type: $id -> $object;
    objects,    \* partial: DOMAIN is what exists
    \* @type: Set($outEntry);
    outbox,     \* at most one entry per MsgKey
    \* @type: Int;
    now         \* the clock. NOT part of the store: a server does not own
                \* the time, it reads it. Two machines at the same state
                \* and different instants are at the same state

vars == <<objects, outbox, now>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* WHAT A HANDLER IS                                                       *)
(*                                                                         *)
(* `Handle(ev, env)` is a pure function of the event and the state, and it *)
(* reads nothing else. It returns what to commit and what to say:          *)
(*                                                                         *)
(*     effects  what to do, IN ORDER -- a crash applies a prefix           *)
(*     res      the response to give                                       *)
(*                                                                         *)
(* This is `AbstractModel.H`: a list of effects and a value, which is what *)
(* the Lean monad accumulates and what Verus's `prepare()` returns. An     *)
(* earlier draft sorted the effects into `writes`/`arm`/`disarm`/`sends`   *)
(* so that the durable ones could be committed in a single action. That    *)
(* buried the very thing this module is about: the ORDER is the protocol,  *)
(* and a bag of writes has no order to get right.                          *)
(*                                                                         *)
(* `env.mat` is the read discipline -- whether a read that projects a      *)
(* settled promise also PERSISTS that settlement. It is a parameter, not a *)
(* second machine.                                                         *)
(*                                                                         *)
(* IT IS A STUB. The body below is the handler that does nothing, so the   *)
(* module type-checks and runs today and the machinery around it can be    *)
(* exercised before a single case is written. It could not stay a          *)
(* `CONSTANT Handle(_, _)`: Apalache admits constant VALUES, not constant  *)
(* OPERATORS, and an uninterpreted handler is the one thing in this file   *)
(* it would have refused outright.                                         *)
(*                                                                         *)
(* Swapping the body for `Gen(3)` gives the adversarial handler -- every   *)
(* outcome the type admits, including malicious ones -- which is the right *)
(* way to find out which invariants the MACHINE maintains and which ones   *)
(* only the HANDLERS do. Expect it to refute nearly everything; that is    *)
(* the measurement.                                                        *)
(***************************************************************************)

(* @type: $env; *)
Env ==
    [ objects |-> objects,
      now     |-> now,
      mat     |-> Materialise,
      config  |-> [retryTimeout |-> RetryTimeout] ]

(* Nothing to do, nothing to say. Every event whose handler is not yet
   written answers this, which is honest -- an unimplemented handler does
   not act -- and keeps them out of the way of the ones that are.
   @type: $outcome; *)
NoOp == [ effects |-> << >>, res |-> Silent ]

(* The object at an identifier, or the one that stands for absence: a
   thing carrying no live deadline, so that `Commit` against it arms
   everything the newborn needs and disarms nothing. *)
(* @type: $object; *)
Absent ==
    [ promise |-> [ state |-> "resolved", param |-> NoValue, value |-> NoValue,
                    tags |-> [targeted |-> FALSE, timer |-> FALSE,
                              external |-> FALSE, target |-> NoAddr,
                              delay |-> 0],
                    timeoutAt |-> 0, createdAt |-> 0, settledAt |-> 0,
                    callbacks |-> {}, listeners |-> {} ],
      task    |-> NoTask ]

(* @type: ($env, $id) => $object; *)
Cur(env, i) == IF i \in DOMAIN env.objects THEN env.objects[i] ELSE Absent

(***************************************************************************)
(* COMMITTING ONE OBJECT                                                   *)
(*                                                                         *)
(* Every handler that changes an object goes through here, and the point   *)
(* is the ORDER. `Commit` emits                                            *)
(*                                                                         *)
(*     arms ... , PutObject , disarms ...                                  *)
(*                                                                         *)
(* which is Verus's `sched + put + ack`. Arming first means a crash        *)
(* between the arm and the write leaves an entry for an object that is not *)
(* there -- noise, which `WheelSound` will see and which the handler       *)
(* re-checks away when it fires. Writing first would mean a crash leaves   *)
(* an object carrying a deadline that nothing will fire -- silence, which  *)
(* `WheelComplete` will see and which nothing recovers from.               *)
(*                                                                         *)
(* Flip the two concatenations below and the checker should hand back a    *)
(* trace ending in a `Crash` with a deadline nobody holds. That is the     *)
(* experiment this module was built for, and it is one edit wide.          *)
(***************************************************************************)

(* @type: ($id, $object, $object, Str) => Seq($effect); *)
ArmFor(i, old, new, k) ==
    IF Deadline(new, k) /= NoTime /\ Deadline(new, k) /= Deadline(old, k)
    THEN << Variant("ArmTimeout",
                    [entry |-> [at |-> Deadline(new, k), id |-> i, kind |-> k]]) >>
    ELSE << >>

(* @type: ($id, $object, $object, Str) => Seq($effect); *)
DisarmFor(i, old, new, k) ==
    IF Deadline(old, k) /= NoTime /\ Deadline(old, k) /= Deadline(new, k)
    THEN << Variant("DisarmTimeout",
                    [entry |-> [at |-> Deadline(old, k), id |-> i, kind |-> k]]) >>
    ELSE << >>

(* @type: ($id, $object, $object) => Seq($effect); *)
Commit(i, old, new) ==
       ArmFor(i, old, new, "promise")
    \o ArmFor(i, old, new, "lease")
    \o ArmFor(i, old, new, "retry")
    \o << Variant("PutObject", [id |-> i, obj |-> new]) >>
    \o DisarmFor(i, old, new, "promise")
    \o DisarmFor(i, old, new, "lease")
    \o DisarmFor(i, old, new, "retry")

(* @type: ($id, $object, $object) => $outcome; *)
Write(i, old, new) == [ effects |-> Commit(i, old, new), res |-> Silent ]

(* Many objects, for the handlers whose request names a SET of them.
   This one cannot be written out: `S` is computed from the request, so
   there is no literal list to write. That is the test for whether a
   combinator here is earning its place -- `Commit` derives arms and
   disarms by diffing deadlines, and this folds over a runtime set, but
   a `Write2` that only concatenated two `Commit`s for one call site was
   imitating the Lean's `do` block rather than saying anything, and is
   gone.

   The fold order is `CHOOSE`-determined and therefore arbitrary; that
   is sound only because these writes touch pairwise distinct objects
   and no two of them can interfere. `taskHeartbeat` is the case.
   @type: (Set($id), ($id) => $object, $env) => Seq($effect); *)
CommitAll(S, New(_), env) ==
    ApaFoldSet(LAMBDA acc, i : acc \o Commit(i, Cur(env, i), New(i)),
               << >>, S)

(* @type: (ADDR, $message) => $effect; *)
Say(addr, msg) == Variant("Send", [entry |-> [address |-> addr, message |-> msg]])

(* What a step writes when the handler itself changes nothing: the
   projection, but only if this reading materialises it. `Env.mat` is
   the whole of the difference between the two readings of the machine,
   and this is the one place it acts.
   @type: ($id, $object, $object, $env) => $outcome; *)
Touch(i, old, pr, env) ==
    IF env.mat /\ pr /= old THEN Write(i, old, pr) ELSE NoOp

(* @type: ($object, $id) => $object; *)
AddCallback(obj, w) == [obj EXCEPT !.promise.callbacks = @ \cup {w}]

(* @type: ($object, ADDR) => $object; *)
AddListener(obj, a) == [obj EXCEPT !.promise.listeners = @ \cup {a}]

(* `taskAcquire`'s move, shared with `taskCreate` on an existing task.
   The version increments: that is the fencing token, and incrementing
   it is what invalidates whoever held the task before.
   @type: ($object, PID, Int, Int) => $object; *)
Acquired(obj, pid, ttl, at) ==
    [obj EXCEPT !.task.state     = "acquired",
                !.task.version   = @ + 1,
                !.task.ttl       = ttl,
                !.task.pid       = pid,
                !.task.expiresAt = at,
                !.task.retryAt   = NoTime,
                !.task.resumes   = {}]

(* Back to the queue, with the dispatch clock armed. Shared by
   `taskRelease`, `taskContinue` and the lease timeout.
   @type: ($object, Int) => $object; *)
Requeued(obj, at) ==
    [obj EXCEPT !.task.state     = "pending",
                !.task.pid       = NoPid,
                !.task.ttl       = NoTime,
                !.task.expiresAt = NoTime,
                !.task.retryAt   = at]

(* Whether this unit is driven by a task at all -- the object-level
   reading of `consistent_task_iff_targeted_promise`.
   @type: $object => Bool; *)
Driven(obj) == obj.task.state /= "none"

(***************************************************************************)
(* THE OBJECT CONSTRUCTORS -- `02-abstract/state.lean`                     *)
(***************************************************************************)

(* `TaskObject.fulfill`. Keeps the version -- the fencing token survives
   fulfilment, which is what stops a stale holder reusing it.
   @type: $task => $task; *)
Fulfilled(t) ==
    [t EXCEPT !.state = "fulfilled", !.pid = NoPid, !.ttl = NoTime,
              !.expiresAt = NoTime, !.retryAt = NoTime, !.resumes = {}]

(* `setSettled`: settle the promise AND fulfil its task. One object, so
   one write -- this is where the fusion pays.
   @type: ($object, Str, VALUE, Int) => $object; *)
Settle(obj, st, v, at) ==
    [ promise |-> [obj.promise EXCEPT !.state = st, !.value = v, !.settledAt = at],
      task    |-> IF obj.task.state = "none" THEN NoTask ELSE Fulfilled(obj.task) ]

(* `PromiseObject.project`: a pending promise past its deadline is settled
   whether or not anyone has written that down. A timer resolves; anything
   else times out. The stamp is the DEADLINE, not the clock.
   @type: ($object, Int) => $object; *)
Project(obj, t) ==
    IF obj.promise.state = "pending" /\ obj.promise.timeoutAt <= t
    THEN Settle(obj, IF obj.promise.tags.timer THEN "resolved" ELSE "rejectedTimedout",
                NoValue, obj.promise.timeoutAt)
    ELSE obj

(* `createPromise`, both arms of it.
   @type: ($createReq, Int) => $object; *)
Born(req, t) ==
    IF req.timeoutAt > t
    THEN [ promise |-> [ state |-> "pending", param |-> req.param, value |-> NoValue,
                         tags |-> req.tags, timeoutAt |-> req.timeoutAt,
                         createdAt |-> t, settledAt |-> NoTime,
                         callbacks |-> {}, listeners |-> {} ],
           task    |-> IF req.tags.targeted
                       THEN [NoTask EXCEPT !.state = "pending", !.retryAt = t]
                       ELSE NoTask ]
    ELSE [ promise |-> [ state |-> IF req.tags.timer THEN "resolved"
                                                     ELSE "rejectedTimedout",
                         param |-> req.param, value |-> NoValue, tags |-> req.tags,
                         timeoutAt |-> req.timeoutAt, createdAt |-> req.timeoutAt,
                         settledAt |-> req.timeoutAt,
                         callbacks |-> {}, listeners |-> {} ],
           task    |-> IF req.tags.targeted
                       THEN [NoTask EXCEPT !.state = "fulfilled"]
                       ELSE NoTask ]

(***************************************************************************)
(* THE HANDLERS                                                            *)
(*                                                                         *)
(* Three of the twenty, chosen because between them they create an object, *)
(* arm a deadline, settle an object and disarm one -- which is everything  *)
(* the wheel invariants need in order to stop being vacuous.               *)
(*                                                                         *)
(* Responses are `Silent` throughout. Nothing in this machine reads a      *)
(* response, so status codes would be write-only; the doors that would     *)
(* return 400 or 404 return `NoOp` instead, which is the same thing for    *)
(* every property here -- they write nothing.                              *)
(***************************************************************************)

(* `external.lean` promiseCreate. Timer+targeted is refused at the door.
   An existing promise is returned as-is, except that reading it may
   MATERIALISE a settlement it had already passed -- which is a write, and
   the reason a read is not free.
   @type: ($createReq, $env) => $outcome; *)
HPromiseCreate(req, env) ==
    LET i   == req.id
        old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF req.tags.timer /\ req.tags.targeted
        THEN NoOp
        ELSE IF i \in DOMAIN env.objects
             THEN IF env.mat /\ pr /= old THEN Write(i, old, pr) ELSE NoOp
             ELSE Write(i, Absent, Born(req, env.now))

(* `external.lean` promiseSettle. `rejectedTimedout` is not settable by a
   client -- that verdict belongs to the deadline, and letting a client
   forge it was one of the four defects the Verus port found.
   @type: ($settleReq, $env) => $outcome; *)
HPromiseSettle(req, env) ==
    LET i   == req.id
        old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF req.state \notin {"resolved", "rejected", "rejectedCanceled"}
        THEN NoOp
        ELSE IF i \notin DOMAIN env.objects
             THEN NoOp
             ELSE IF pr.promise.state = "pending"
                  THEN Write(i, old, Settle(pr, req.state, req.value, env.now))
                  ELSE IF env.mat /\ pr /= old THEN Write(i, old, pr) ELSE NoOp

(* `external.lean` promiseGet. A read that can WRITE: projecting a
   settlement the deadline already decided is a change, and `mat` says
   whether it is persisted. Nothing else here writes.
   @type: ($id, $env) => $outcome; *)
HPromiseGet(i, env) ==
    IF i \notin DOMAIN env.objects
    THEN NoOp
    ELSE Touch(i, Cur(env, i), Project(Cur(env, i), env.now), env)

(* `external.lean` promiseRegisterCallback. The awaiter must be a task
   promise and the awaited must be EXTERNAL -- something outside can
   settle it -- because a callback on a promise nothing can settle is a
   waiter nothing will ever wake.

   AND THE TWO MUST SHARE AN ORIGIN. This door is not in the Lean: the
   handler there checks that the awaiter is targeted and the awaited is
   external, and says nothing about where either lives, so a client can
   register a waiter across origins and the specification admits it.
   That is a gap in `spec/02-abstract/external.lean` rather than a
   choice, and it matters more than a missing status code: an await that
   spans origins is a write whose two ends can be in different
   partitions, which is the difference between an implementation that
   may shard and one that may not.

   With the door, every handler touches one origin, and the
   one-document-per-origin executor in `Concrete` is not merely SOUND
   -- declining what it cannot serve -- but COMPLETE, serving everything
   the protocol admits.
   @type: ($callbackReq, $env) => $outcome; *)
HPromiseRegisterCallback(req, env) ==
    LET a   == req.awaited
        w   == req.awaiter
        oa  == Cur(env, a)
        pa  == Project(oa, env.now)
        pw  == Project(Cur(env, w), env.now)
    IN  IF \/ a = w
           \/ a.origin /= w.origin
           \/ a \notin DOMAIN env.objects
           \/ w \notin DOMAIN env.objects
           \/ ~pw.promise.tags.targeted
           \/ ~IsExternal(pa.promise)
        THEN Touch(a, oa, pa, env)
        ELSE IF pa.promise.state = "pending" /\ pw.promise.state = "pending"
             THEN Write(a, oa, AddCallback(pa, w))
             ELSE Touch(a, oa, pa, env)

(* `external.lean` promiseRegisterListener. Same door, one object.
   @type: ($id, ADDR, $env) => $outcome; *)
HPromiseRegisterListener(a, addr, env) ==
    LET oa == Cur(env, a)
        pa == Project(oa, env.now)
    IN  IF a \notin DOMAIN env.objects \/ ~IsExternal(pa.promise)
        THEN Touch(a, oa, pa, env)
        ELSE IF pa.promise.state = "pending"
             THEN Write(a, oa, AddListener(pa, addr))
             ELSE Touch(a, oa, pa, env)

(* `external.lean` taskGet. Reads the unit; the task view fulfils itself
   when the promise has settled, which `Project` already does.
   @type: ($id, $env) => $outcome; *)
HTaskGet(i, env) ==
    IF i \notin DOMAIN env.objects \/ ~Driven(Cur(env, i))
    THEN NoOp
    ELSE Touch(i, Cur(env, i), Project(Cur(env, i), env.now), env)

(* `external.lean` taskCreate. A worker claiming work that may not exist
   yet: if the promise is absent it is born ALREADY ACQUIRED at version
   1, which is why this is not `promiseCreate` followed by
   `taskAcquire` -- there is no instant in between at which someone else
   could take it.
   @type: ($createReq, PID, Int, $env) => $outcome; *)
HTaskCreate(req, pid, ttl, env) ==
    LET i   == req.id
        old == Cur(env, i)
        pr  == Project(old, env.now)
        t   == env.now
    IN  IF ~req.tags.targeted \/ (req.tags.timer /\ req.tags.targeted)
        THEN NoOp
        ELSE IF i \notin DOMAIN env.objects
             THEN LET born == Born(req, t)
                  IN  Write(i, Absent,
                            IF born.promise.state = "pending"
                            THEN Acquired([born EXCEPT !.task.state = "pending"],
                                          pid, ttl, t + ttl)
                            ELSE born)
             ELSE IF ~pr.promise.tags.targeted \/ ~Driven(pr)
                  THEN Touch(i, old, pr, env)
                  ELSE IF pr.task.state = "pending"
                       THEN Write(i, old, Acquired(pr, pid, ttl, t + ttl))
                       ELSE Touch(i, old, pr, env)

(* `external.lean` taskAcquire. Three guards and they are all fences:
   the task must be on offer, the promise must still be pending, and the
   version must be the one the caller was told.
   @type: ($id, Int, PID, Int, $env) => $outcome; *)
HTaskAcquire(i, v, pid, ttl, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects \/ ~Driven(pr)
           \/ pr.task.state /= "pending"
           \/ pr.promise.state /= "pending"
           \/ pr.task.version /= v
        THEN Touch(i, old, pr, env)
        ELSE Write(i, old, Acquired(pr, pid, ttl, env.now + ttl))

(* `external.lean` taskFulfill. The holder settles the promise it was
   given, and the task is fulfilled in the same write because they are
   one object.
   @type: ($id, Int, $settleReq, $env) => $outcome; *)
HTaskFulfill(i, v, act, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ act.state \notin {"resolved", "rejected", "rejectedCanceled"}
           \/ i \notin DOMAIN env.objects \/ ~Driven(pr)
           \/ pr.task.state /= "acquired"
           \/ pr.promise.state /= "pending"
           \/ pr.task.version /= v
        THEN Touch(i, old, pr, env)
        ELSE Write(i, old, Settle(pr, act.state, act.value, env.now))

(* `external.lean` taskRelease. The holder gives it back, and the
   dispatch clock is armed at once so somebody else is offered it.
   @type: ($id, Int, $env) => $outcome; *)
HTaskRelease(i, v, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects \/ ~Driven(pr)
           \/ pr.task.state /= "acquired"
           \/ pr.promise.state /= "pending"
           \/ pr.task.version /= v
        THEN Touch(i, old, pr, env)
        ELSE Write(i, old, Requeued(pr, env.now))

(* `external.lean` taskHalt. Stop offering this task, without settling
   the promise. No version is checked: halting is an operator's act, not
   a holder's.
   @type: ($id, $env) => $outcome; *)
HTaskHalt(i, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects \/ ~Driven(pr)
           \/ pr.task.state \in {"fulfilled", "halted"}
        THEN Touch(i, old, pr, env)
        ELSE Write(i, old, [pr EXCEPT !.task.state     = "halted",
                                      !.task.pid       = NoPid,
                                      !.task.ttl       = NoTime,
                                      !.task.expiresAt = NoTime,
                                      !.task.retryAt   = NoTime])

(* `external.lean` taskContinue. The other half of halt.
   @type: ($id, $env) => $outcome; *)
HTaskContinue(i, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects \/ ~Driven(pr)
           \/ pr.task.state /= "halted"
           \/ pr.promise.state /= "pending"
        THEN Touch(i, old, pr, env)
        ELSE Write(i, old, Requeued(pr, env.now))

(* `external.lean` taskHeartbeat. One request, MANY objects: a worker
   renews every lease it holds in one call. Each renewal is its own
   `PutObject`, so this is the widest multi-unit write in the protocol
   and the one whose objects are guaranteed pairwise distinct, which is
   why folding them in `CHOOSE` order is sound.
   @type: (PID, Set($taskRef), $env) => $outcome; *)
Renewable(pid, refs, env) ==
    { i \in DOMAIN env.objects :
        LET pr == Project(env.objects[i], env.now)
        IN  /\ \E rf \in refs : rf.id = i /\ rf.version = pr.task.version
            /\ Driven(pr)
            /\ pr.task.state = "acquired"
            /\ pr.task.pid = pid
            /\ pr.promise.state = "pending" }

HTaskHeartbeat(pid, refs, env) ==
    [ effects |->
        CommitAll(Renewable(pid, refs, env),
                  LAMBDA i : LET pr == Project(Cur(env, i), env.now)
                             IN  [pr EXCEPT !.task.expiresAt = env.now + pr.task.ttl],
                  env),
      res |-> Silent ]

(* `external.lean` taskSuspend. A worker parks a task until the promises
   it is waiting on settle. Writes the suspended task AND registers a
   callback on every awaited promise -- several objects, and unlike
   heartbeat they are not independent: the awaited promises are what
   will later wake this task.

   The 300 case is the interesting one. If anything awaited has ALREADY
   settled there is nothing to wait for, so the task is not suspended
   and the caller is told to carry on -- suspending would park a task
   nothing is left to wake.
   @type: ($id, Int, Set($callbackReq), $env) => $outcome; *)
Awaiteds(acts) == { a.awaited : a \in acts }

HTaskSuspend(i, v, acts, env) ==
    LET old  == Cur(env, i)
        pr   == Project(old, env.now)
        aw   == Awaiteds(acts)
        seen == { a \in aw : a \in DOMAIN env.objects }
        proj(a) == Project(Cur(env, a), env.now)
    IN  IF \/ acts = {} \/ i \in aw
           \/ \E a \in aw : a.origin /= i.origin   \* the same door
           \/ i \notin DOMAIN env.objects \/ ~Driven(pr)
           \/ pr.task.state /= "acquired"
           \/ pr.promise.state /= "pending"
           \/ pr.task.version /= v
           \/ seen /= aw
           \/ \E a \in aw : ~IsExternal(proj(a).promise)
        THEN Touch(i, old, pr, env)
        ELSE IF \E a \in aw : proj(a).promise.state /= "pending"
             THEN Write(i, old, [pr EXCEPT !.task.resumes = {}])
             ELSE [ effects |->
                      Commit(i, old, [pr EXCEPT !.task.state     = "suspended",
                                                !.task.pid       = NoPid,
                                                !.task.ttl       = NoTime,
                                                !.task.expiresAt = NoTime,
                                                !.task.retryAt   = NoTime,
                                                !.task.resumes   = {}])
                      \o CommitAll(aw, LAMBDA a : AddCallback(proj(a), i), env),
                    res |-> Silent ]

(* `external.lean` taskFence. A holder acts on ANOTHER object, and the
   task it holds is the licence to do so: check the fence, then run
   `promiseCreate` or `promiseSettle` on the target. The target may not
   be the fencing task itself, which is what stops a task from settling
   the promise it is the task of by this route.

   Two objects again -- the fence is read, the target is written -- and
   nothing writes the fence, so the licence is not consumed.
   @type: ($id, Int, $fenceAction, $env) => $outcome; *)
FenceTarget(act) ==
    IF VariantTag(act) = "Create"
    THEN VariantGetUnsafe("Create", act).req.id
    ELSE VariantGetUnsafe("Settle", act).req.id

HTaskFence(i, v, act, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ FenceTarget(act) = i
           \/ i \notin DOMAIN env.objects \/ ~Driven(pr)
           \/ pr.task.state /= "acquired"
           \/ pr.promise.state /= "pending"
           \/ pr.task.version /= v
        THEN Touch(i, old, pr, env)
        ELSE IF VariantTag(act) = "Create"
             THEN HPromiseCreate(VariantGetUnsafe("Create", act).req, env)
             ELSE HPromiseSettle(VariantGetUnsafe("Settle", act).req, env)

(* `internal.lean` processLeaseTimeout. A holder that stopped
   heartbeating loses the task, and it goes back on offer at once.
   Reads with materialisation NOT forced -- `viewPromise` -- so a lease
   timeout never itself writes down a settlement.
   @type: ($id, $env) => $outcome; *)
HLeaseTimeout(i, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects \/ ~Driven(old)
           \/ old.task.state /= "acquired"
           \/ old.task.expiresAt = NoTime
           \/ old.task.expiresAt > env.now
           \/ pr.promise.state /= "pending"
        THEN NoOp
        ELSE Write(i, old, Requeued(old, env.now))

(* `internal.lean` processRetryTimeout. The dispatch: offer a pending
   task to its target, and re-arm the clock so it is offered again if
   nobody takes it.

   The next instant is COMPUTED from the server's own dial, not supplied
   by whoever fired the step -- `config.retryTimeout`. It is not the
   task's `ttl`: that is the WORKER's number, absent on a task nobody has
   acquired yet, which is exactly where retry matters most.

   This is the only handler so far that SAYS anything, and it is where
   stale dispatch lives: the message names a version, and by the time it
   is read the task may have moved.
   @type: ($id, $env) => $outcome; *)
HRetryTimeout(i, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects \/ ~Driven(old)
           \/ old.task.state /= "pending"
           \/ old.task.retryAt = NoTime
           \/ old.task.retryAt > env.now
           \/ pr.promise.state /= "pending"
        THEN NoOp
        ELSE [ effects |->
                 Commit(i, old,
                        [old EXCEPT !.task.retryAt = env.now + env.config.retryTimeout])
                 \o << Say(old.promise.tags.target,
                           Variant("Execute", [id      |-> i,
                                               version |-> old.task.version])) >>,
               res |-> Silent ]

(* `internal.lean` processListener. A settled promise owes a message to
   everyone who registered for it. The name is struck from the ledger in
   the same write, so the message is said once.
   @type: ($id, ADDR, $env) => $outcome; *)
HListenerDrain(i, addr, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects
           \/ pr.promise.state = "pending"
           \/ addr \notin pr.promise.listeners
        THEN NoOp
        ELSE [ effects |->
                 Commit(i, old,
                        [pr EXCEPT !.promise.listeners = @ \ {addr}])
                 \o << Say(addr, Variant("Unblock",
                                         [id |-> i, state |-> pr.promise.state])) >>,
               res |-> Silent ]

(* `internal.lean` processCallback, and `resumeOne` inside it. A settled
   promise wakes the task that was waiting on it: strike the callback
   from the awaited promise, and move the awaiter.

   TWO OBJECTS, and they are different objects -- this is the cross-id
   multi-unit write, the one the catalogue's four cross-id entries are
   about and the one a per-origin document only covers if `SameOrigin`
   holds.

   A suspended awaiter goes back on offer with the trigger recorded. An
   awaiter that is awake but not suspended only has the trigger noted,
   because it has not asked to be woken yet; a fulfilled one is past
   caring.
   @type: ($id, $id, $env) => $outcome; *)
Resumed(w, awaited, env) ==
    LET pw == Project(Cur(env, w), env.now)
    IN  IF pw.task.state = "suspended"
        THEN [Requeued(pw, env.now) EXCEPT !.task.resumes = {awaited}]
        ELSE IF pw.task.state \in {"pending", "acquired", "halted"}
             THEN [pw EXCEPT !.task.resumes = @ \cup {awaited}]
             ELSE pw

HCallbackDrain(i, w, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
        ow  == Cur(env, w)
    IN  IF \/ i \notin DOMAIN env.objects
           \/ pr.promise.state = "pending"
           \/ w \notin pr.promise.callbacks
        THEN NoOp
        ELSE IF w \notin DOMAIN env.objects \/ ~Driven(Project(ow, env.now))
             THEN Write(i, old, [pr EXCEPT !.promise.callbacks = @ \ {w}])
             ELSE [ effects |->
                      Commit(i, old, [pr EXCEPT !.promise.callbacks = @ \ {w}])
                      \o Commit(w, ow, Resumed(w, i, env)),
                    res |-> Silent ]

(* `internal.lean` processPromiseTimeout, which is `touchPromise` and
   nothing else: read the promise with materialisation FORCED, so that a
   settlement the deadline already decided gets written down.

   A due entry whose object has moved on finds nothing to do. The entry is
   still on the wheel and will fire again -- that is the noise arming
   before writing admits, and it is why every internal handler re-checks.
   The lease and retry kinds are not implemented yet and answer `NoOp`.
   @type: ({ id: $id, kind: Str }, $env) => $outcome; *)
HTimeout(e, env) ==
    LET i   == e.id
        old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF e.kind /= "promise" \/ i \notin DOMAIN env.objects
        THEN NoOp
        ELSE IF pr /= old THEN Write(i, old, pr) ELSE NoOp

(* The dispatch table. Twenty constructors, twenty answers.

   `promiseSearch` and `taskSearch` answer `NoOp` and always will: the
   Lean returns 501 for both, so there is nothing to model beyond
   writing nothing.
   @type: ($event, $env) => $outcome; *)
Handle(ev, env) ==
    LET p(tag) == VariantGetUnsafe(tag, ev)
    IN
    CASE VariantTag(ev) = "PromiseGet" ->
             HPromiseGet(p("PromiseGet").id, env)
      [] VariantTag(ev) = "PromiseCreate" ->
             HPromiseCreate(p("PromiseCreate").req, env)
      [] VariantTag(ev) = "PromiseSettle" ->
             HPromiseSettle(p("PromiseSettle").req, env)
      [] VariantTag(ev) = "PromiseRegisterCallback" ->
             HPromiseRegisterCallback(p("PromiseRegisterCallback").req, env)
      [] VariantTag(ev) = "PromiseRegisterListener" ->
             HPromiseRegisterListener(p("PromiseRegisterListener").awaited,
                                      p("PromiseRegisterListener").address, env)
      [] VariantTag(ev) = "TaskGet" ->
             HTaskGet(p("TaskGet").id, env)
      [] VariantTag(ev) = "TaskCreate" ->
             HTaskCreate(p("TaskCreate").action, p("TaskCreate").pid,
                         p("TaskCreate").ttl, env)
      [] VariantTag(ev) = "TaskAcquire" ->
             HTaskAcquire(p("TaskAcquire").id, p("TaskAcquire").version,
                          p("TaskAcquire").pid, p("TaskAcquire").ttl, env)
      [] VariantTag(ev) = "TaskFence" ->
             HTaskFence(p("TaskFence").id, p("TaskFence").version,
                        p("TaskFence").action, env)
      [] VariantTag(ev) = "TaskHeartbeat" ->
             HTaskHeartbeat(p("TaskHeartbeat").pid, p("TaskHeartbeat").tasks, env)
      [] VariantTag(ev) = "TaskSuspend" ->
             HTaskSuspend(p("TaskSuspend").id, p("TaskSuspend").version,
                          p("TaskSuspend").actions, env)
      [] VariantTag(ev) = "TaskFulfill" ->
             HTaskFulfill(p("TaskFulfill").id, p("TaskFulfill").version,
                          p("TaskFulfill").action, env)
      [] VariantTag(ev) = "TaskRelease" ->
             HTaskRelease(p("TaskRelease").id, p("TaskRelease").version, env)
      [] VariantTag(ev) = "TaskHalt" ->
             HTaskHalt(p("TaskHalt").id, env)
      [] VariantTag(ev) = "TaskContinue" ->
             HTaskContinue(p("TaskContinue").id, env)
      [] VariantTag(ev) = "Timeout" ->
             LET d == p("Timeout")
             IN  CASE d.kind = "promise" -> HTimeout(d, env)
                   [] d.kind = "lease"   -> HLeaseTimeout(d.id, env)
                   [] OTHER              -> HRetryTimeout(d.id, env)
      [] VariantTag(ev) = "ListenerDrain" ->
             HListenerDrain(p("ListenerDrain").id, p("ListenerDrain").address, env)
      [] VariantTag(ev) = "CallbackDrain" ->
             HCallbackDrain(p("CallbackDrain").id, p("CallbackDrain").awaiter, env)
      [] OTHER -> NoOp

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
(* fires and does nothing -- exactly the `pure ()` branch in               *)
(* `processLeaseTimeout`.                                                  *)
(***************************************************************************)

(* @type: $id => Bool; *)
Live(id) == id \in DOMAIN objects

(* @type: $id => $promise; *)
Prom(id) == objects[id].promise

(* Settled AS READ: a pending promise past its deadline is settled whether
   or not anyone has written that down yet. `PromiseObject.project`.
   @type: $promise => Bool; *)
SettledNow(p) == p.state /= "pending" \/ p.timeoutAt <= now

(* @type: $event => Bool; *)
Fires(ev) ==
    CASE VariantTag(ev) = "Timeout" ->
             LET d == VariantGetUnsafe("Timeout", ev) IN
             /\ d.id \in DOMAIN objects
             /\ Deadline(objects[d.id], d.kind) /= NoTime
             /\ Deadline(objects[d.id], d.kind) <= now
      [] VariantTag(ev) = "ListenerDrain" ->
             LET d == VariantGetUnsafe("ListenerDrain", ev) IN
             /\ Live(d.id)
             /\ SettledNow(Prom(d.id))
             /\ d.address \in Prom(d.id).listeners
      [] VariantTag(ev) = "CallbackDrain" ->
             LET d == VariantGetUnsafe("CallbackDrain", ev) IN
             /\ Live(d.id)
             /\ SettledNow(Prom(d.id))
             /\ d.awaiter \in Prom(d.id).callbacks
      [] OTHER -> FALSE

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE TRANSITIONS                                                         *)
(***************************************************************************)

(* The writes a handler decided on, and the messages it decided to say.
   The wheel effects are not here: this machine has no wheel, so `Arm` and
   `Disarm` are instructions to an executor that keeps one and mean
   nothing to a machine that does not.
   @type: Seq($effect) => Set({ id: $id, obj: $object }); *)
Puts(fx) ==
    { VariantGetUnsafe("PutObject", fx[i])
      : i \in { j \in DOMAIN fx : VariantTag(fx[j]) = "PutObject" } }

(* @type: Seq($effect) => Set($outEntry); *)
Says(fx) ==
    { VariantGetUnsafe("Send", fx[i]).entry
      : i \in { j \in DOMAIN fx : VariantTag(fx[j]) = "Send" } }

(* ONE EVENT, ONE TRANSITION. Read, decide, and apply the whole decision
   at a single instant -- `stepOf` in `02-abstract/system.lean`, where
   `applyAll` folds the effect list onto the pre-state and no other
   request can be observed in between.

   Nothing here is in flight, because at this altitude nothing IS in
   flight. A machine that takes a request and answers it in one step has
   no state between the two, which is why `steps` is gone and why the
   phases, the crash and the retry live in `Concrete` where they belong.

   `Says` is applied as a set rather than in order. That is exact while a
   handler emits at most one message per `MsgKey` and no handler yet
   emits two; the day one does, the order between same-key messages is a
   claim this operator would silently drop.
   @type: $outcome => Bool; *)
Apply(out) ==
    /\ objects' = LET W == Puts(out.effects) IN
                     [ i \in (DOMAIN objects) \cup {w.id : w \in W} |->
                          IF \E w \in W : w.id = i
                          THEN (CHOOSE w \in W : w.id = i).obj
                          ELSE objects[i] ]
    /\ outbox'  = LET S == Says(out.effects) IN
                     { o \in outbox : ~\E e \in S : MsgKey(o) = MsgKey(e) } \cup S
    /\ UNCHANGED now

(* A client request. Unguarded: a client may send anything at any time,
   including nonsense -- rejecting it is the handler's job, and the
   response it gives for nonsense is part of the protocol.
   @type: $event => Bool; *)
External(ev) ==
    /\ ev \in ExternalEvent
    /\ Apply(Handle(ev, Env))

(* A step the server takes on its own initiative, and only when it has a
   reason to: a deadline that has come due, or a name still written down
   next to a settled promise. The reason is read off the OBJECTS. There
   is no timer wheel here -- a wheel is an index an executor keeps so it
   does not have to scan, and the protocol neither requires one nor says
   what happens if it drifts.
   @type: $event => Bool; *)
Internal(ev) ==
    /\ ev \in InternalEvent
    /\ Fires(ev)
    /\ Apply(Handle(ev, Env))

(* The clock. Free-running and independent of everything else -- what
   makes a deadline pass is time moving, not anyone acting. Bounded so
   the state space is. *)
Clock ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<objects, outbox>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE SPECIFICATION                                                       *)
(***************************************************************************)

Init ==
    /\ objects = SetAsFun({})
    /\ outbox  = {}
    /\ now     = 0

Next ==
    \/ \E ev \in ExternalEvent : External(ev)
    \/ \E ev \in InternalEvent : Internal(ev)
    \/ Clock

(* An internal event that stays enabled must eventually be taken, or no
   timeout is ever taken and every liveness property is vacuous. Nothing
   is assumed of clients: a client need never send. *)
Fairness ==
    /\ \A ev \in InternalEvent : WF_vars(Internal(ev))
    /\ WF_vars(Clock)

Spec == Init /\ [][Next]_vars /\ Fairness

Safety == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(***************************************************************************)
(* INVARIANTS                                                              *)
(*                                                                         *)
(* `TypeOK` is much shorter than it was, and on purpose. Membership in     *)
(* `Promise`, `Task`, `$inFlight` and the rest is what the `@type`         *)
(* annotations assert, checked once by Apalache rather than at every       *)
(* state -- and half of those sets are not enumerable anyway (`Seq(...)`   *)
(* is infinite). What is left here is what a type cannot say: the          *)
(* relationships BETWEEN fields.                                           *)
(***************************************************************************)

TypeOK ==
    /\ now \in Time
    /\ DOMAIN objects \subseteq Id
    (* one entry per key -- the outbox is keyed, not a log *)
    /\ \A a, b \in outbox : MsgKey(a) = MsgKey(b) => a = b
    (* a unit with no task carries the canonical one, so that "no task" is
       one value and not a family of junk ones *)
    /\ \A o \in DOMAIN objects :
           objects[o].task.state = "none" => objects[o].task = NoTask

(* `WheelSound` and `WheelComplete` have moved to `Concrete`. They were
   never claims about the protocol: they say an INDEX agrees with the
   deadlines it indexes, and this machine has no index. Nothing in the 95
   mentions one either, for the same reason. *)

(***************************************************************************)
(* THE CATALOGUE                                                           *)
(*                                                                         *)
(* `spec/02-abstract/properties.lean`, transcribed. These are NOT new      *)
(* claims -- each is one of the 95, under its own name, so a violation     *)
(* here means the same thing it means in Lean, Go, TypeScript and Verus.   *)
(*                                                                         *)
(* Only the entries the three implemented handlers can reach are here.     *)
(* Porting the rest against unimplemented handlers would buy 80 vacuous    *)
(* greens.                                                                 *)
(*                                                                         *)
(* Two things change in transcription and neither is a change of claim:    *)
(*                                                                         *)
(*   - `s.promises.all` and `s.tasks.all` become one walk over `objects`,  *)
(*     because a promise and its task are one. Where the Lean has to LOOK  *)
(*     UP the task by the promise's id and cope with its absence, this has *)
(*     `objects[i].task` and `task.state = "none"`.                        *)
(*                                                                         *)
(*   - a `.trans` property becomes an ACTION invariant -- unprimed for     *)
(*     `a`, primed for `b`. The Lean walk checks it at every pair of       *)
(*     consecutive states the server passes through; here that is every    *)
(*     `Next`, and a server that does not write in one transaction passes  *)
(*     through more of them than the Lean machine does. That is the        *)
(*     question, not a distortion of it.                                   *)
(***************************************************************************)

(* @type: $id => $promise; *)
Pr(i) == objects[i].promise

(* @type: $id => $task; *)
Tk(i) == objects[i].task

(* @type: $id => Bool; *)
HasTask(i) == objects[i].task.state /= "none"

(* --- .state ------------------------------------------------------------ *)

well_formed_promise_created_at_lte_timeout_at ==
    \A i \in DOMAIN objects : Pr(i).createdAt <= Pr(i).timeoutAt

well_formed_promise_settled_at_iff_not_pending ==
    \A i \in DOMAIN objects :
        (Pr(i).state /= "pending") <=> (Pr(i).settledAt /= NoTime)

well_formed_promise_pending_has_no_value ==
    \A i \in DOMAIN objects :
        Pr(i).state = "pending" => Pr(i).value = NoValue

well_formed_promise_settled_at_lte_timeout_at ==
    \A i \in DOMAIN objects :
        Pr(i).settledAt /= NoTime => Pr(i).settledAt <= Pr(i).timeoutAt

well_formed_task_pending_iff_has_retry_at ==
    \A i \in DOMAIN objects :
        HasTask(i) => ((Tk(i).state = "pending") <=> (Tk(i).retryAt /= NoTime))

well_formed_task_acquired_iff_has_expires_at ==
    \A i \in DOMAIN objects :
        HasTask(i) => ((Tk(i).state = "acquired") <=> (Tk(i).expiresAt /= NoTime))

(* The fusion dividend: in Lean this has to find the task by id and admit
   that it might not be there. Here they are one object. *)
consistent_settled_promise_has_fulfilled_task ==
    \A i \in DOMAIN objects :
        (Pr(i).state /= "pending" /\ HasTask(i)) => Tk(i).state = "fulfilled"

consistent_settled_task_promise_settled ==
    \A i \in DOMAIN objects :
        (HasTask(i) /\ Tk(i).state = "fulfilled") => Pr(i).state /= "pending"

consistent_task_iff_targeted_promise ==
    \A i \in DOMAIN objects : HasTask(i) <=> Pr(i).tags.targeted

(* --- .trans ------------------------------------------------------------ *)

preserved_settled_promise_record ==
    \A i \in DOMAIN objects :
        \/ Pr(i).state = "pending"
        \/ /\ i \in DOMAIN objects'
           /\ objects'[i].promise.state     = Pr(i).state
           /\ objects'[i].promise.settledAt = Pr(i).settledAt
           /\ objects'[i].promise.value     = Pr(i).value

consistent_new_promise_born_clean ==
    \A i \in DOMAIN objects' :
        \/ i \in DOMAIN objects
        \/ LET q == objects'[i].promise IN
           /\ q.callbacks = {}
           /\ q.listeners = {}
           /\ q.value = NoValue
           /\ q.createdAt <= now
           /\ \/ /\ q.state = "pending"
                 /\ q.settledAt = NoTime
                 /\ q.createdAt < q.timeoutAt
              \/ /\ q.settledAt = q.timeoutAt
                 /\ q.createdAt = q.timeoutAt
                 /\ q.timeoutAt <= now
                 /\ IF q.tags.timer THEN q.state = "resolved"
                                    ELSE q.state = "rejectedTimedout"

(* The one that reads the clock. It says a settlement is stamped with the
   instant it happened -- and this machine decides at one instant and
   writes at another. *)
consistent_promise_settlement_stamp ==
    \A i \in DOMAIN objects :
        \/ Pr(i).state /= "pending"
        \/ /\ i \in DOMAIN objects'
           /\ LET q == objects'[i].promise IN
              \/ q.state = "pending"
              \/ /\ q.settledAt = now
                 /\ now < q.timeoutAt
                 /\ q.state /= "rejectedTimedout"
              \/ /\ q.settledAt = q.timeoutAt
                 /\ q.timeoutAt <= now
                 /\ (IF q.tags.timer THEN q.state = "resolved"
                                     ELSE q.state = "rejectedTimedout")
                 /\ q.value = Pr(i).value

consistent_settlement_fulfils_task ==
    \A i \in DOMAIN objects :
        \/ Pr(i).state /= "pending"
        \/ i \notin DOMAIN objects'
        \/ objects'[i].promise.state = "pending"
        \/ objects'[i].task.state \in {"none", "fulfilled"}

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE OBLIGATIONS THE CATALOGUE CANNOT STATE                              *)
(*                                                                         *)
(* The Lean has no wheel. Its deadlines live on the task record            *)
(* (`well_formed_task_pending_iff_has_retry_at` above is the shape of it), *)
(* so there is no second store to drift and nothing in the 95 says an      *)
(* index agrees with what it indexes. These two are the price of the       *)
(* premise, and they have to be invented here.                             *)
(***************************************************************************)

(* The promise and its task are one unit -- the reason for fusing them.
   A settled promise never coexists with an unfulfilled task, at any
   instant, for any reader. In the Lean this is `setSettled`, two writes
   in one transaction; here there is only one write and nothing to
   interleave between. *)
UnitCoherent ==
    \A o \in DOMAIN objects :
        (/\ objects[o].promise.state /= "pending"
         /\ objects[o].task.state \notin {"none", "fulfilled"})
            => FALSE

-----------------------------------------------------------------------------
(***************************************************************************)
(* WHAT IS BEING ASKED                                                     *)
(*                                                                         *)
(* UPWARD, to `spec/02-abstract`: take `Crash` away and let each step run  *)
(* Process..Perform with no other step interleaved, and the sequence IS    *)
(* `stepOf` -- one event, one atomic transition, effects folded on in      *)
(* order by `applyAll`. So the Lean machine is this machine under a        *)
(* scheduler that never interleaves and never crashes (`NoInterleave`),    *)
(* and the 95 catalogue properties hold of exactly those runs. WHICH OF    *)
(* THEM SURVIVE THE OTHER RUNS is the question.                            *)
(*                                                                         *)
(* Three answers are possible for each property, and they are not equally  *)
(* interesting:                                                            *)
(*                                                                         *)
(*   - it survives because of how the STATE is shaped. `UnitCoherent` is   *)
(*     the example: a promise and its task are one object, so one          *)
(*     `PutObject` moves both and no interleaving can catch them           *)
(*     disagreeing. Fusing them bought that, and it cost nothing.          *)
(*                                                                         *)
(*   - it survives only if the EFFECTS ARE ORDERED right. `WheelComplete`  *)
(*     is the example: the wheel is a second store, so arming and writing  *)
(*     are two moments, and which comes first decides whether a crash      *)
(*     leaves noise or silence. This is where the handlers have to be      *)
(*     careful, and where a handler that is merely correct in the Lean is  *)
(*     not yet correct here.                                               *)
(*                                                                         *)
(*   - it does not survive at all without something this module does not   *)
(*     have. That is the finding worth having, and the candidate is the    *)
(*     absent fence: two steps read one object, both write, one update is  *)
(*     lost. Whether any catalogue property actually falls to that -- and  *)
(*     which -- is the experiment.                                         *)
(*                                                                         *)
(* MULTI-UNIT WRITES are where the third answer is most likely. These      *)
(* handlers touch more than one object, which now means more than one      *)
(* `PutObject`, applied at different moments with the world running        *)
(* between them:                                                           *)
(*                                                                         *)
(*   `promiseRegisterCallback` writes the awaited promise while reading    *)
(*   the awaiter; `taskSuspend` writes every awaited promise in its action *)
(*   list; `CallbackDrain` writes the awaited promise (striking the        *)
(*   callback) and the awaiter's task (resuming it).                       *)
(*                                                                         *)
(* Verus never has to face this: its document is a whole `Workflow` --     *)
(* `promises: Map<Id, Promise>`, plural -- so both ends of a callback live *)
(* in one document and one `PutDocument` moves them together. That is a    *)
(* fact about how it CHUNKS the store, and this module deliberately does   *)
(* not have it, because the question is what the protocol needs rather     *)
(* than what one storage layout provides.                                  *)
(*                                                                         *)
(* `SameOrigin` is the conjecture that would restore it: if the two ends   *)
(* of every callback share an origin, then chunking by origin makes every  *)
(* multi-unit write single-chunk again. Nothing here makes it true.        *)
(***************************************************************************)

(* REFUTED, in three steps, with two origins:
   
       create o1/a  external
       create o2/a  targeted
       promiseRegisterCallback(awaited |-> o1/a, awaiter |-> o2/a)
   
       o1/a.callbacks = { o2/a }
   
   The doors do not forbid it. `promiseRegisterCallback` asks that the
   awaited be external and the awaiter be targeted, and says nothing
   about where either lives -- which is right for a protocol whose whole
   point is waiting on work somebody else is doing.
   
   So this is not a conjecture that failed to be proved. It is a
   PRECONDITION THE PROTOCOL DOES NOT GRANT, and what it prices is an
   implementation strategy: an executor that keeps one document per
   origin and fences it cannot serve a callback whose two ends are in
   different documents. `Concrete` is exactly that executor, which is
   why it has no cross-origin operation at all -- not a limitation it
   works around, a shape it does not have.
   
   Three ways out, and they are choices, not fixes:
   
     - a door: require awaiter and awaited to share an origin. Cheap to
       state, and it forbids waiting on another workflow's promise.
     - a transaction across documents, which is what the chunking was
       there to avoid.
     - chunk by something coarser than origin, so that the two ends are
       in one document by construction. Verus's `Workflow` is this move
       one level down; it works exactly as far as the chunk reaches. *)
SameOrigin ==
    \A o \in DOMAIN objects :
        \A w \in objects[o].promise.callbacks : w.origin = o.origin

(***************************************************************************)
(* The `.trans` entries, wrapped for TLC.                                  *)
(*                                                                         *)
(* Apalache takes a primed formula as an action invariant and checks it    *)
(* directly. TLC will not accept a prime under `INVARIANT`, so each one    *)
(* becomes `[][P]_vars` under `PROPERTY` -- the same claim in the syntax   *)
(* that checker reads.                                                     *)
(***************************************************************************)

T_preserved_settled_promise_record    == [][preserved_settled_promise_record]_vars
T_consistent_new_promise_born_clean   == [][consistent_new_promise_born_clean]_vars
T_consistent_promise_settlement_stamp == [][consistent_promise_settlement_stamp]_vars
T_consistent_settlement_fulfils_task  == [][consistent_settlement_fulfils_task]_vars

=============================================================================
