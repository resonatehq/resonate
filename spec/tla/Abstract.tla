-------------------------------- MODULE Abstract --------------------------------
EXTENDS Integers, Sequences, FiniteSets, Variants, Apalache

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
  @typeAlias: settleReq = { id: $id, state: Str, value: VALUE };
  @typeAlias: callbackReq = { awaited: $id, awaiter: $id };
  @typeAlias: taskRef = { id: $id, version: Int };
  @typeAlias: fenceAction = Create({ req: $createReq }) | Settle({ req: $settleReq });
  @typeAlias: event =
  @typeAlias: effect =
  @typeAlias: inFlight = {
  @typeAlias: env = {
  @typeAlias: outcome = { effects: Seq($effect) };
*)
ResonateAliases == TRUE

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
    MaxVersion

ASSUME NoPid   \notin Pid
ASSUME NoAddr  \notin Address
ASSUME NoValue \notin Value
ASSUME MaxTime    >= 0
ASSUME MaxVersion >= 0

Time    == 0 .. MaxTime
Version == 0 .. MaxVersion

NoTime == -1

Tags ==
    LET a == CHOOSE x \in Address : TRUE IN
    { [targeted |-> FALSE, timer |-> FALSE, external |-> FALSE, target |-> a, delay |-> 0],
      [targeted |-> FALSE, timer |-> FALSE, external |-> TRUE,  target |-> a, delay |-> 0],
      [targeted |-> TRUE,  timer |-> FALSE, external |-> FALSE, target |-> a, delay |-> 0],
      [targeted |-> FALSE, timer |-> TRUE,  external |-> FALSE, target |-> a, delay |-> 0] }

IsExternal(p) == p.tags.external \/ p.tags.targeted \/ p.tags.timer

-----------------------------------------------------------------------------

Id == [origin : Origin, rest : Rest]

-----------------------------------------------------------------------------

PromiseState == {"pending", "resolved", "rejected",
                 "rejectedCanceled", "rejectedTimedout"}

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

DeadlineKind == {"promise", "lease", "retry"}

Entry == [at : Time, id : Id, kind : DeadlineKind]

(* @type: ($object, Str) => Int; *)
Deadline(obj, kind) ==
    CASE kind = "promise" ->
             IF obj.promise.state = "pending" THEN obj.promise.timeoutAt ELSE NoTime
      [] kind = "lease" -> obj.task.expiresAt
      [] OTHER          -> obj.task.retryAt

-----------------------------------------------------------------------------

SettleState == {"resolved", "rejected", "rejectedCanceled"}

CreateReq   == [id : Id, timeoutAt : Time, param : Value, tags : Tags]
SettleReq   == [id : Id, state : SettleState, value : Value]
CallbackReq == [awaited : Id, awaiter : Id]
TaskRefT    == [id : Id, version : Version]

FenceAction ==
       { Variant("Create", [req |-> r]) : r \in CreateReq }
  \cup { Variant("Settle", [req |-> r]) : r \in SettleReq }

(* @type: (Str, Set($event)) => Set($event); *)
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

InternalEvent ==
       {}
    \cup On("Timeout", { Variant("Timeout", [id |-> i, kind |-> k])
                         : i \in Id, k \in DeadlineKind})
    \cup On("ListenerDrain", { Variant("ListenerDrain", [id |-> i, address |-> a]) : i \in Id, a \in Address})
    \cup On("CallbackDrain", { Variant("CallbackDrain", [id |-> i, awaiter |-> w]) : i \in Id, w \in Id})

Event == ExternalEvent \cup InternalEvent

-----------------------------------------------------------------------------

VARIABLES
    objects,    \* partial: DOMAIN is what exists
    outbox,     \* at most one entry per MsgKey
    now         \* the clock. NOT part of the store: a server does not own

vars == <<objects, outbox, now>>

-----------------------------------------------------------------------------

(* @type: $env; *)
Env ==
    [ objects |-> objects,
      now     |-> now,
      config  |-> [retryTimeout |-> RetryTimeout] ]

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

(* @type: ($id, $object) => Seq($effect); *)
Commit(i, new) == << Variant("PutObject", [id |-> i, obj |-> new]) >>

(* @type: ($id, $object) => $outcome; *)
Write(i, new) == [ effects |-> Commit(i, new) ]

(* @type: (Set($id), ($id) => $object, $env) => Seq($effect); *)
CommitAll(S, Obj(_), env) ==
    ApaFoldSet(LAMBDA acc, i : acc \o Commit(i, Obj(i)),
               << >>, S)

(* @type: (ADDR, $message) => $effect; *)
Say(addr, msg) == Variant("Send", [entry |-> [address |-> addr, message |-> msg]])

(* @type: ($object, $id) => $object; *)
AddCallback(obj, w) == [obj EXCEPT !.promise.callbacks = @ \cup {w}]

(* @type: ($object, ADDR) => $object; *)
AddListener(obj, a) == [obj EXCEPT !.promise.listeners = @ \cup {a}]

(* @type: ($object, PID, Int, Int) => $object; *)
Acquired(obj, pid, ttl, at) ==
    [obj EXCEPT !.task.state     = "acquired",
                !.task.version   = @ + 1,
                !.task.ttl       = ttl,
                !.task.pid       = pid,
                !.task.expiresAt = at,
                !.task.retryAt   = NoTime,
                !.task.resumes   = {}]

(* @type: ($object, Int) => $object; *)
Requeued(obj, at) ==
    [obj EXCEPT !.task.state     = "pending",
                !.task.pid       = NoPid,
                !.task.ttl       = NoTime,
                !.task.expiresAt = NoTime,
                !.task.retryAt   = at]

(* @type: $object => Bool; *)
Driven(obj) == obj.task.state /= "none"

(* @type: $task => $task; *)
Fulfilled(t) ==
    IF t.state = "none" THEN
        t
    ELSE
        [t EXCEPT !.state = "fulfilled", !.pid = NoPid, !.ttl = NoTime,
                  !.expiresAt = NoTime, !.retryAt = NoTime, !.resumes = {}]

(* @type: ($object, Str, VALUE, Int) => $object; *)
Settle(obj, st, v, at) ==
    [ promise |-> [obj.promise EXCEPT !.state = st, !.value = v, !.settledAt = at],
      task    |-> Fulfilled(obj.task) ]

(* @type: ($object, Int) => $object; *)
Project(obj, t) ==
    IF obj.promise.state = "pending" /\ obj.promise.timeoutAt <= t
    THEN Settle(obj, IF obj.promise.tags.timer THEN "resolved" ELSE "rejectedTimedout",
                NoValue, obj.promise.timeoutAt)
    ELSE obj

(* @type: ($createReq, Int) => $object; *)
New(req, t) ==
    IF req.timeoutAt > t THEN
        [ promise |-> [ state |-> "pending", param |-> req.param, value |-> NoValue,
                        tags |-> req.tags, timeoutAt |-> req.timeoutAt,
                        createdAt |-> t, settledAt |-> NoTime,
                        callbacks |-> {}, listeners |-> {} ],
          task    |-> IF req.tags.targeted THEN
                          [NoTask EXCEPT !.state = "pending", !.retryAt = t]
                      ELSE
                          NoTask ]
    ELSE
        [ promise |-> [ state |-> IF req.tags.timer THEN "resolved"
                                                    ELSE "rejectedTimedout",
                        param |-> req.param, value |-> NoValue, tags |-> req.tags,
                        timeoutAt |-> req.timeoutAt, createdAt |-> req.timeoutAt,
                        settledAt |-> req.timeoutAt,
                        callbacks |-> {}, listeners |-> {} ],
          task    |-> IF req.tags.targeted THEN
                          [NoTask EXCEPT !.state = "fulfilled"]
                      ELSE
                          NoTask ]

(* @type: ($createReq, $env) => $outcome; *)
HandlePromiseCreate(req, env) ==
    IF req.id \in DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        Write(req.id, New(req, env.now))

(* @type: ($settleReq, $env) => $outcome; *)
HandlePromiseSettle(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET old == Project(env.objects[req.id], env.now)
        IN
            IF old.promise.state = "pending" THEN
                Write(req.id, Settle(old, req.state, req.value, env.now))
            ELSE
                [ effects |-> << >> ]

(* @type: ($id, $env) => $outcome; *)
HandlePromiseGet(i, env) == [ effects |-> << >> ]

(* @type: ($callbackReq, $env) => $outcome; *)
HandlePromiseRegisterCallback(req, env) ==
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
        THEN [ effects |-> << >> ]
        ELSE IF pa.promise.state = "pending" /\ pw.promise.state = "pending"
             THEN Write(a, AddCallback(pa, w))
             ELSE [ effects |-> << >> ]

(* @type: ($id, ADDR, $env) => $outcome; *)
HandlePromiseRegisterListener(a, addr, env) ==
    LET oa == Cur(env, a)
        pa == Project(oa, env.now)
    IN  IF a \notin DOMAIN env.objects \/ ~IsExternal(pa.promise)
        THEN [ effects |-> << >> ]
        ELSE IF pa.promise.state = "pending"
             THEN Write(a, AddListener(pa, addr))
             ELSE [ effects |-> << >> ]

(* @type: ($id, $env) => $outcome; *)
HandleTaskGet(i, env) ==
    IF i \notin DOMAIN env.objects \/ ~Driven(Cur(env, i))
    THEN [ effects |-> << >> ]
    ELSE [ effects |-> << >> ]

(* @type: ($createReq, PID, Int, $env) => $outcome; *)
HandleTaskCreate(req, pid, ttl, env) ==
    LET i   == req.id
        old == Cur(env, i)
        pr  == Project(old, env.now)
        t   == env.now
    IN  IF ~req.tags.targeted \/ (req.tags.timer /\ req.tags.targeted)
        THEN [ effects |-> << >> ]
        ELSE IF i \notin DOMAIN env.objects
             THEN LET new == New(req, t)
                  IN  Write(i,
                            IF new.promise.state = "pending"
                            THEN Acquired([new EXCEPT !.task.state = "pending"],
                                          pid, ttl, t + ttl)
                            ELSE new)
             ELSE IF ~pr.promise.tags.targeted \/ ~Driven(pr)
                  THEN [ effects |-> << >> ]
                  ELSE IF pr.task.state = "pending"
                       THEN Write(i, Acquired(pr, pid, ttl, t + ttl))
                       ELSE [ effects |-> << >> ]

(* @type: ($id, Int, PID, Int, $env) => $outcome; *)
HandleTaskAcquire(i, v, pid, ttl, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects \/ ~Driven(pr)
           \/ pr.task.state /= "pending"
           \/ pr.promise.state /= "pending"
           \/ pr.task.version /= v
        THEN [ effects |-> << >> ]
        ELSE Write(i, Acquired(pr, pid, ttl, env.now + ttl))

(* @type: ($id, Int, $settleReq, $env) => $outcome; *)
HandleTaskFulfill(i, v, act, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ act.state \notin {"resolved", "rejected", "rejectedCanceled"}
           \/ i \notin DOMAIN env.objects \/ ~Driven(pr)
           \/ pr.task.state /= "acquired"
           \/ pr.promise.state /= "pending"
           \/ pr.task.version /= v
        THEN [ effects |-> << >> ]
        ELSE Write(i, Settle(pr, act.state, act.value, env.now))

(* @type: ($id, Int, $env) => $outcome; *)
HandleTaskRelease(i, v, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects \/ ~Driven(pr)
           \/ pr.task.state /= "acquired"
           \/ pr.promise.state /= "pending"
           \/ pr.task.version /= v
        THEN [ effects |-> << >> ]
        ELSE Write(i, Requeued(pr, env.now))

(* @type: ($id, $env) => $outcome; *)
HandleTaskHalt(i, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects \/ ~Driven(pr)
           \/ pr.task.state \in {"fulfilled", "halted"}
        THEN [ effects |-> << >> ]
        ELSE Write(i, [pr EXCEPT !.task.state     = "halted",
                                      !.task.pid       = NoPid,
                                      !.task.ttl       = NoTime,
                                      !.task.expiresAt = NoTime,
                                      !.task.retryAt   = NoTime])

(* @type: ($id, $env) => $outcome; *)
HandleTaskContinue(i, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects \/ ~Driven(pr)
           \/ pr.task.state /= "halted"
           \/ pr.promise.state /= "pending"
        THEN [ effects |-> << >> ]
        ELSE Write(i, Requeued(pr, env.now))

(* @type: (PID, Set($taskRef), $env) => $outcome; *)
Renewable(pid, refs, env) ==
    { i \in DOMAIN env.objects :
        LET pr == Project(env.objects[i], env.now)
        IN  /\ \E rf \in refs : rf.id = i /\ rf.version = pr.task.version
            /\ Driven(pr)
            /\ pr.task.state = "acquired"
            /\ pr.task.pid = pid
            /\ pr.promise.state = "pending" }

HandleTaskHeartbeat(pid, refs, env) ==
    [ effects |->
        CommitAll(Renewable(pid, refs, env),
                  LAMBDA i : LET pr == Project(Cur(env, i), env.now)
                             IN  [pr EXCEPT !.task.expiresAt = env.now + pr.task.ttl],
                  env) ]

(* @type: ($id, Int, Set($callbackReq), $env) => $outcome; *)
Awaiteds(acts) == { a.awaited : a \in acts }

HandleTaskSuspend(i, v, acts, env) ==
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
        THEN [ effects |-> << >> ]
        ELSE IF \E a \in aw : proj(a).promise.state /= "pending"
             THEN Write(i, [pr EXCEPT !.task.resumes = {}])
             ELSE [ effects |->
                      Commit(i, [pr EXCEPT !.task.state     = "suspended",
                                                !.task.pid       = NoPid,
                                                !.task.ttl       = NoTime,
                                                !.task.expiresAt = NoTime,
                                                !.task.retryAt   = NoTime,
                                                !.task.resumes   = {}])
                      \o CommitAll(aw, LAMBDA a : AddCallback(proj(a), i), env) ]

(* @type: ($id, Int, $fenceAction, $env) => $outcome; *)
FenceTarget(act) ==
    IF VariantTag(act) = "Create"
    THEN VariantGetUnsafe("Create", act).req.id
    ELSE VariantGetUnsafe("Settle", act).req.id

HandleTaskFence(i, v, act, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ FenceTarget(act) = i
           \/ i \notin DOMAIN env.objects \/ ~Driven(pr)
           \/ pr.task.state /= "acquired"
           \/ pr.promise.state /= "pending"
           \/ pr.task.version /= v
        THEN [ effects |-> << >> ]
        ELSE IF VariantTag(act) = "Create"
             THEN HandlePromiseCreate(VariantGetUnsafe("Create", act).req, env)
             ELSE HandlePromiseSettle(VariantGetUnsafe("Settle", act).req, env)

(* @type: ($id, $env) => $outcome; *)
HandleLeaseTimeout(i, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects \/ ~Driven(old)
           \/ old.task.state /= "acquired"
           \/ old.task.expiresAt = NoTime
           \/ old.task.expiresAt > env.now
           \/ pr.promise.state /= "pending"
        THEN [ effects |-> << >> ]
        ELSE Write(i, Requeued(old, env.now))

(* @type: ($id, $env) => $outcome; *)
HandleRetryTimeout(i, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects \/ ~Driven(old)
           \/ old.task.state /= "pending"
           \/ old.task.retryAt = NoTime
           \/ old.task.retryAt > env.now
           \/ pr.promise.state /= "pending"
        THEN [ effects |-> << >> ]
        ELSE [ effects |->
                 Commit(i,
                        [old EXCEPT !.task.retryAt = env.now + env.config.retryTimeout])
                 \o << Say(old.promise.tags.target,
                           Variant("Execute", [id      |-> i,
                                               version |-> old.task.version])) >> ]

(* @type: ($id, ADDR, $env) => $outcome; *)
HandleListenerDrain(i, addr, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF \/ i \notin DOMAIN env.objects
           \/ pr.promise.state = "pending"
           \/ addr \notin pr.promise.listeners
        THEN [ effects |-> << >> ]
        ELSE [ effects |->
                 Commit(i,
                        [pr EXCEPT !.promise.listeners = @ \ {addr}])
                 \o << Say(addr, Variant("Unblock",
                                         [id |-> i, state |-> pr.promise.state])) >> ]

(* @type: ($id, $id, $env) => $outcome; *)
Resumed(w, awaited, env) ==
    LET pw == Project(Cur(env, w), env.now)
    IN  IF pw.task.state = "suspended"
        THEN [Requeued(pw, env.now) EXCEPT !.task.resumes = {awaited}]
        ELSE IF pw.task.state \in {"pending", "acquired", "halted"}
             THEN [pw EXCEPT !.task.resumes = @ \cup {awaited}]
             ELSE pw

HandleCallbackDrain(i, w, env) ==
    LET old == Cur(env, i)
        pr  == Project(old, env.now)
        ow  == Cur(env, w)
    IN  IF \/ i \notin DOMAIN env.objects
           \/ pr.promise.state = "pending"
           \/ w \notin pr.promise.callbacks
        THEN [ effects |-> << >> ]
        ELSE IF w \notin DOMAIN env.objects \/ ~Driven(Project(ow, env.now))
             THEN Write(i, [pr EXCEPT !.promise.callbacks = @ \ {w}])
             ELSE [ effects |->
                      Commit(i, [pr EXCEPT !.promise.callbacks = @ \ {w}])
                      \o Commit(w, Resumed(w, i, env)) ]

(* @type: ({ id: $id, kind: Str }, $env) => $outcome; *)
HandleTimeout(e, env) ==
    LET i   == e.id
        old == Cur(env, i)
        pr  == Project(old, env.now)
    IN  IF e.kind /= "promise" \/ i \notin DOMAIN env.objects
        THEN [ effects |-> << >> ]
        ELSE IF pr /= old THEN Write(i, pr) ELSE [ effects |-> << >> ]

(* @type: ($event, $env) => $outcome; *)
Handle(ev, env) ==
    LET p(tag) == VariantGetUnsafe(tag, ev)
    IN
    CASE VariantTag(ev) = "PromiseGet" ->
             HandlePromiseGet(p("PromiseGet").id, env)
      [] VariantTag(ev) = "PromiseCreate" ->
             HandlePromiseCreate(p("PromiseCreate").req, env)
      [] VariantTag(ev) = "PromiseSettle" ->
             HandlePromiseSettle(p("PromiseSettle").req, env)
      [] VariantTag(ev) = "PromiseRegisterCallback" ->
             HandlePromiseRegisterCallback(p("PromiseRegisterCallback").req, env)
      [] VariantTag(ev) = "PromiseRegisterListener" ->
             HandlePromiseRegisterListener(p("PromiseRegisterListener").awaited,
                                      p("PromiseRegisterListener").address, env)
      [] VariantTag(ev) = "TaskGet" ->
             HandleTaskGet(p("TaskGet").id, env)
      [] VariantTag(ev) = "TaskCreate" ->
             HandleTaskCreate(p("TaskCreate").action, p("TaskCreate").pid,
                         p("TaskCreate").ttl, env)
      [] VariantTag(ev) = "TaskAcquire" ->
             HandleTaskAcquire(p("TaskAcquire").id, p("TaskAcquire").version,
                          p("TaskAcquire").pid, p("TaskAcquire").ttl, env)
      [] VariantTag(ev) = "TaskFence" ->
             HandleTaskFence(p("TaskFence").id, p("TaskFence").version,
                        p("TaskFence").action, env)
      [] VariantTag(ev) = "TaskHeartbeat" ->
             HandleTaskHeartbeat(p("TaskHeartbeat").pid, p("TaskHeartbeat").tasks, env)
      [] VariantTag(ev) = "TaskSuspend" ->
             HandleTaskSuspend(p("TaskSuspend").id, p("TaskSuspend").version,
                          p("TaskSuspend").actions, env)
      [] VariantTag(ev) = "TaskFulfill" ->
             HandleTaskFulfill(p("TaskFulfill").id, p("TaskFulfill").version,
                          p("TaskFulfill").action, env)
      [] VariantTag(ev) = "TaskRelease" ->
             HandleTaskRelease(p("TaskRelease").id, p("TaskRelease").version, env)
      [] VariantTag(ev) = "TaskHalt" ->
             HandleTaskHalt(p("TaskHalt").id, env)
      [] VariantTag(ev) = "TaskContinue" ->
             HandleTaskContinue(p("TaskContinue").id, env)
      [] VariantTag(ev) = "Timeout" ->
             LET d == p("Timeout")
             IN  CASE d.kind = "promise" -> HandleTimeout(d, env)
                   [] d.kind = "lease"   -> HandleLeaseTimeout(d.id, env)
                   [] OTHER              -> HandleRetryTimeout(d.id, env)
      [] VariantTag(ev) = "ListenerDrain" ->
             HandleListenerDrain(p("ListenerDrain").id, p("ListenerDrain").address, env)
      [] VariantTag(ev) = "CallbackDrain" ->
             HandleCallbackDrain(p("CallbackDrain").id, p("CallbackDrain").awaiter, env)
      [] OTHER -> [ effects |-> << >> ]

-----------------------------------------------------------------------------

(* @type: $id => Bool; *)
Live(id) == id \in DOMAIN objects

(* @type: $id => $promise; *)
Prom(id) == objects[id].promise

(* @type: $promise => Bool; *)
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

(* @type: Seq($effect) => Set({ id: $id, obj: $object }); *)
Puts(fx) ==
    { VariantGetUnsafe("PutObject", fx[i])
      : i \in { j \in DOMAIN fx : VariantTag(fx[j]) = "PutObject" } }

(* @type: Seq($effect) => Set($outEntry); *)
Says(fx) ==
    { VariantGetUnsafe("Send", fx[i]).entry
      : i \in { j \in DOMAIN fx : VariantTag(fx[j]) = "Send" } }

(* @type: $outcome => Bool; *)
Apply(out) ==
    /\ objects' = LET W == Puts(out.effects) IN
                     [ i \in (DOMAIN objects) \cup {w.id : w \in W} |->
                          IF \E w \in W : w.id = i
                          THEN (CHOOSE w \in W : w.id = i).obj
                          ELSE objects[i] ]
    /\ outbox'  = LET S == Says(out.effects) IN
                     { o \in outbox : ~\E e \in S : MsgKey(o) = MsgKey(e) } \cup S
    /\ UNCHANGED now

(* @type: $event => Bool; *)
External(ev) ==
    /\ ev \in ExternalEvent
    /\ Apply(Handle(ev, Env))

(* @type: $event => Bool; *)
Internal(ev) ==
    /\ ev \in InternalEvent
    /\ Fires(ev)
    /\ Apply(Handle(ev, Env))

Clock ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<objects, outbox>>

-----------------------------------------------------------------------------

Init ==
    /\ objects = SetAsFun({})
    /\ outbox  = {}
    /\ now     = 0

Next ==
    \/ \E ev \in ExternalEvent : External(ev)
    \/ \E ev \in InternalEvent : Internal(ev)
    \/ Clock

Fairness ==
    /\ \A ev \in InternalEvent : WF_vars(Internal(ev))
    /\ WF_vars(Clock)

Spec == Init /\ [][Next]_vars /\ Fairness

Safety == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

TypeOK ==
    /\ now \in Time
    /\ DOMAIN objects \subseteq Id
        /\ \A a, b \in outbox : MsgKey(a) = MsgKey(b) => a = b
        /\ \A o \in DOMAIN objects :
           objects[o].task.state = "none" => objects[o].task = NoTask

(* @type: $id => $promise; *)
Pr(i) == objects[i].promise

(* @type: $id => $task; *)
Tk(i) == objects[i].task

(* @type: $id => Bool; *)
HasTask(i) == objects[i].task.state /= "none"

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

consistent_settled_promise_has_fulfilled_task ==
    \A i \in DOMAIN objects :
        (Pr(i).state /= "pending" /\ HasTask(i)) => Tk(i).state = "fulfilled"

consistent_settled_task_promise_settled ==
    \A i \in DOMAIN objects :
        (HasTask(i) /\ Tk(i).state = "fulfilled") => Pr(i).state /= "pending"

consistent_task_iff_targeted_promise ==
    \A i \in DOMAIN objects : HasTask(i) <=> Pr(i).tags.targeted

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

UnitCoherent ==
    \A o \in DOMAIN objects :
        (/\ objects[o].promise.state /= "pending"
         /\ objects[o].task.state \notin {"none", "fulfilled"})
            => FALSE

-----------------------------------------------------------------------------

SameOrigin ==
    \A o \in DOMAIN objects :
        \A w \in objects[o].promise.callbacks : w.origin = o.origin

T_preserved_settled_promise_record    == [][preserved_settled_promise_record]_vars
T_consistent_new_promise_born_clean   == [][consistent_new_promise_born_clean]_vars
T_consistent_promise_settlement_stamp == [][consistent_promise_settlement_stamp]_vars
T_consistent_settlement_fulfils_task  == [][consistent_settlement_fulfils_task]_vars

=============================================================================
