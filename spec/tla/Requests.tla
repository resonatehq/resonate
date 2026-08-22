-------------------------------- MODULE Requests --------------------------------
(***************************************************************************)
(* THE VOCABULARY. What an object is, what a request is, what an effect    *)
(* is, and which events exist. Shared, because it is the same protocol     *)
(* being spoken about -- `Abstract` and `Concrete` are two independent     *)
(* machines, and the refinement asks whether their behaviour lines up.     *)
(* Nothing here decides anything: no handler, no transition, no state.     *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, Variants

-----------------------------------------------------------------------------

(*
  @typeAlias: id = { origin: ORIGIN, rest: REST };
  @typeAlias: tags = { targeted: Bool, timer: Bool, external: Bool, target: ADDR, delay: Int };
  @typeAlias: promise = {
  @typeAlias: task = {
  @typeAlias: object = { promise: $promise, task: $task };
  @typeAlias: entry = { at: Int, id: $id, kind: Str };
  @typeAlias: message =
  @typeAlias: outEntry = { address: ADDR, message: $message };
  @typeAlias: msgKey = { kind: Str, id: $id, address: ADDR };
  @typeAlias: createReq = { id: $id, timeoutAt: Int, param: VALUE, tags: $tags };
  @typeAlias: getReq = { id: $id };
  @typeAlias: drainReq = { id: $id, awaiter: $id };
  @typeAlias: listenerDrainReq = { id: $id, address: ADDR };
  @typeAlias: listenerReq = { awaited: $id, address: ADDR };

  @typeAlias: settleReq = { id: $id, state: Str, value: VALUE };
  @typeAlias: callbackReq = { awaited: $id, awaiter: $id };
  @typeAlias: taskRef = { id: $id, version: Int };
  @typeAlias: fenceAction = Create({ req: $createReq }) | Settle({ req: $settleReq });
  @typeAlias: taskCreateReq = { pid: PID, ttl: Int, action: $createReq };
  @typeAlias: taskAcquireReq = { id: $id, version: Int, pid: PID, ttl: Int };
  @typeAlias: taskFenceReq = { id: $id, version: Int, action: $fenceAction };
  @typeAlias: taskBeatReq = { pid: PID, tasks: Set($taskRef) };
  @typeAlias: taskSuspendReq = { id: $id, version: Int, actions: Set($callbackReq) };
  @typeAlias: taskFulfillReq = { id: $id, version: Int, action: $settleReq };
  @typeAlias: event =
  @typeAlias: effect =
  @typeAlias: inFlight = {
  @typeAlias: env = {
  @typeAlias: outcome = { effects: Seq($effect) };
*)
ResonateAliases ==
    TRUE

-----------------------------------------------------------------------------

CONSTANTS
    Origin,
    Rest,
    Address,
    Pid,
    Value,
    NoValue,
    Ttl,
    Rid,
    Implemented,
    NoPid,
    NoAddr,
    RetryTimeout,
    MaxTime,
    MaxVersion,
    MaxBatch

ASSUME NoPid   \notin Pid
ASSUME NoAddr  \notin Address
ASSUME NoValue \notin Value
ASSUME MaxTime    >= 0
ASSUME MaxVersion >= 0
ASSUME MaxBatch   >= 1
(* The sweep is a fixpoint. A retry timeout re-arms at now + RetryTimeout,
   which is only in the future -- and so retires its own trigger -- while
   RetryTimeout is positive. *)
ASSUME RetryTimeout > 0

(* THE UNIQUE FUNCTION WITH EMPTY DOMAIN. A store with nothing in it. TLA+
   writes it as the empty sequence, a sequence being a function on 1..n. *)
EmptyFn ==
    << >>

Write(doc, i, obj) ==
    [ x \in (DOMAIN doc) \cup {i} |-> IF x = i THEN obj ELSE doc[x] ]

FoldSet(Op(_,_), base, S) ==
    LET f[T \in SUBSET S] ==
          IF T = {} THEN
              base
          ELSE
              LET x == CHOOSE y \in T : TRUE
              IN  Op(f[T \ {x}], x)
    IN
        f[S]

Time ==
    0 .. MaxTime
Version ==
    0 .. MaxVersion

NoTime ==
    -1

Tags ==
    LET a == CHOOSE x \in Address : TRUE IN
    { [targeted |-> FALSE, timer |-> FALSE, external |-> FALSE, target |-> a, delay |-> 0],
      [targeted |-> FALSE, timer |-> FALSE, external |-> TRUE,  target |-> a, delay |-> 0],
      [targeted |-> TRUE,  timer |-> FALSE, external |-> FALSE, target |-> a, delay |-> 0],
      [targeted |-> FALSE, timer |-> TRUE,  external |-> FALSE, target |-> a, delay |-> 0] }

IsExternal(p) ==
    p.tags.external \/ p.tags.targeted \/ p.tags.timer

-----------------------------------------------------------------------------

Id ==
    [origin : Origin, rest : Rest]

-----------------------------------------------------------------------------

PromiseState ==
    {"pending", "resolved", "rejected",
     "rejectedCanceled", "rejectedTimedout"}

TaskState ==
    {"none", "pending", "acquired", "suspended", "halted", "fulfilled"}

(* @type: $task; *)
NoTask ==
    [ state |-> "none", version |-> 0, ttl |-> NoTime, pid |-> NoPid,
      expiresAt |-> NoTime, retryAt |-> NoTime, resumes |-> {} ]

Promise ==
    [ state     : PromiseState,
      param     : Value,
      value     : Value,
      tags      : Tags,
      timeoutAt : Time,                  \* deadline -- armed as "promise"
      createdAt : Time,
      settledAt : Time \cup {NoTime},
      callbacks : SUBSET Id,             \* awaiters to resume when this settles
      listeners : SUBSET Address ]       \* addresses to notify when this settles

Task ==
    [ state     : TaskState,
      version   : Version,               \* the fencing token
      ttl       : Ttl \cup {NoTime},     \* the WORKER's number: lease asked for
      pid       : Pid \cup {NoPid},
      expiresAt : Time \cup {NoTime},    \* deadline -- armed as "lease"
      retryAt   : Time \cup {NoTime},    \* deadline -- armed as "retry"
      resumes   : SUBSET Id ]            \* triggers buffered while not suspended

Object ==
    [promise : Promise, task : Task]

Message ==
       { Variant("Execute", [id |-> i, version |-> v]) : i \in Id, v \in Version }
  \cup { Variant("Unblock", [id |-> i, state |-> s]) : i \in Id, s \in PromiseState }

OutEntry ==
    [address : Address, message : Message]

(* @type: $outEntry => $msgKey; *)
MsgKey(e) ==
    IF VariantTag(e.message) = "Execute"
    THEN [kind |-> "execute",
          id      |-> VariantGetUnsafe("Execute", e.message).id,
          address |-> NoAddr]
    ELSE [kind |-> "unblock",
          id      |-> VariantGetUnsafe("Unblock", e.message).id,
          address |-> e.address]

-----------------------------------------------------------------------------

DeadlineKind ==
    {"promise", "lease", "retry"}

Entry ==
    [at : Time, id : Id, kind : DeadlineKind]

(* @type: ($object, Str) => Int; *)
Deadline(obj, kind) ==
    CASE kind = "promise" ->
             IF obj.promise.state = "pending" THEN obj.promise.timeoutAt ELSE NoTime
      [] kind = "lease" -> obj.task.expiresAt
      [] OTHER          -> obj.task.retryAt

-----------------------------------------------------------------------------

SettleState ==
    {"resolved", "rejected", "rejectedCanceled"}

CreateReq ==
    [id : Id, timeoutAt : Time, param : Value, tags : Tags]
SettleReq ==
    [id : Id, state : SettleState, value : Value]
CallbackReq ==
    { r \in [awaited : Id, awaiter : Id] :
        /\ r.awaited /= r.awaiter
        /\ r.awaited.origin = r.awaiter.origin }
TaskRefT ==
    [id : Id, version : Version]

FenceAction ==
       { Variant("Create", [req |-> r]) : r \in CreateReq }
  \cup { Variant("Settle", [req |-> r]) : r \in SettleReq }

(* @type: (Str, Set($event)) => Set($event); *)
On(tag, S) ==
    IF tag \in Implemented THEN
        S
    ELSE
        {}

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

InternalEvent ==
       {}
    \cup On("Timeout", { Variant("Timeout", [id |-> i, kind |-> k])
                         : i \in Id, k \in DeadlineKind})
    \cup On("ListenerDrain", { Variant("ListenerDrain", [id |-> i, address |-> a]) : i \in Id, a \in Address})
    \cup On("CallbackDrain", { Variant("CallbackDrain", [id |-> r.awaited, awaiter |-> r.awaiter]) : r \in CallbackReq})

Event ==
    ExternalEvent \cup InternalEvent

===============================================================================
