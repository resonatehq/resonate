-------------------------------- MODULE Concrete --------------------------------
(***************************************************************************)
(* The same protocol under the Verus executor's discipline: ONE DOCUMENT   *)
(* PER ORIGIN, and a compare-and-swap on it.                               *)
(*                                                                         *)
(* `Abstract` deliberately has no fence, and TLC said what that costs: two *)
(* steps read one object, both write, the second erases the first --       *)
(* `preserved_settled_promise_record` and `WheelComplete` fall together.   *)
(* This module is the other half of the experiment.                        *)
(*                                                                         *)
(* IT CHANGES NOTHING ABOUT THE PROTOCOL. The handlers, the effects, the   *)
(* alphabet, the wheel and the invariants all come from `Abstract` through *)
(* the instance below, so a difference in behaviour is a difference in the *)
(* EXECUTOR and cannot be anything else.                                   *)
(*                                                                         *)
(* Two changes, both from `src/concrete_spec/executor.rs`:                 *)
(*                                                                         *)
(*   - the store is keyed by ORIGIN, and one document holds every object   *)
(*     at that origin. This is `Workflow.promises: Map<Id, Promise>` --    *)
(*     plural -- and it is what makes a multi-unit write single-document   *)
(*     whenever the units share an origin.                                 *)
(*                                                                         *)
(*   - every write carries the etag its decision was read under            *)
(*     (`expect: etag_of(held)`). If the document has moved, the write is  *)
(*     REFUSED and the step goes back to deciding --                       *)
(*     `restart(s, rid, ..)`. Nothing stale lands.                         *)
(*                                                                         *)
(* THE NAMES ARE DELIBERATELY THE SAME. `timeouts`, `outbox`, `steps` and  *)
(* `now` mean here exactly what they mean in `Abstract`, and the instance  *)
(* below leaves them alone. Only `objects` needs saying, because only      *)
(* `objects` is represented differently -- which is the whole content of   *)
(* this refinement, and it should be the only thing in the `WITH`.         *)
(***************************************************************************)
EXTENDS Requests

CONSTANT Fenced

VARIABLES
    docs,       \* [ORIGIN -> ($id -> $object)] -- one document per origin
    timeouts,   \* the wheel. Same variable, same meaning as Abstract's
    outbox,     \* likewise
    steps,      \* likewise, plus `expect` and `org`
    now         \* likewise

vars == <<docs, timeouts, outbox, steps, now>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE ABSTRACTION FUNCTION                                                *)
(*                                                                         *)
(* What a chunked store denotes: the flat one that forgets the chunking.   *)
(* An identifier already carries the origin it belongs to, so finding the  *)
(* document an object lives in is `i.origin` and nothing else -- which is  *)
(* what the pair-shaped identifier was for.                                *)
(*                                                                         *)
(* The etags denote NOTHING. There is no abstract variable they map to,    *)
(* and that is the point of them: a fence is machinery for keeping a       *)
(* promise the abstract machine states without it.                         *)
(***************************************************************************)

Objects == [ i \in UNION { DOMAIN docs[o] : o \in Origin } |-> docs[i.origin][i] ]

A == INSTANCE Abstract WITH objects <- Objects

(***************************************************************************)
(* Three of this machine's six variables map to NOTHING, and that is the   *)
(* whole shape of the result:                                              *)
(*                                                                         *)
(*     expect     a fence. The document a step decided against, compared   *)
(*                on the way in, and machinery for keeping a promise the   *)
(*                abstract machine states without it                       *)
(*     timeouts   an index, so an executor need not scan for what is due.  *)
(*                `Abstract` reads deadlines off the objects               *)
(*     steps      work in flight. `Abstract` has no interval between       *)
(*                deciding and writing, so it has nothing to track         *)
(*                                                                         *)
(* `outbox` and `now` map by name; only `objects` needs saying. So every   *)
(* transition here that touches nothing but the three unmapped variables   *)
(* is a STUTTER upstairs -- submitting, deciding, arming, disarming, and   *)
(* a refused compare-and-swap sending a step back to decide again.         *)
(*                                                                         *)
(* Exactly one transition is not a stutter: the `PutObject` that lands.    *)
(* THAT is where the abstract machine takes its single atomic step, and it *)
(* is why the fence matters -- the etag is what makes the write land       *)
(* against the state it was decided against, which is the only way one     *)
(* instant can stand for the whole decision.                               *)
(*                                                                         *)
(* Nothing above needed a `Restart` in `Abstract`. An earlier draft mapped *)
(* `steps` across, so a refused CAS showed up as an abstract step running  *)
(* backwards and had to be legislated for. Hiding the bookkeeping is what  *)
(* removed the need -- the retry is not a smaller abstract step, it is no  *)
(* abstract step at all.                                                   *)
(***************************************************************************)

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
(***************************************************************************)
(* THIS MACHINE'S OWN PROTOCOL. Not `Abstract`'s: the two are independent  *)
(* specifications of the same protocol, and the refinement asks whether    *)
(* their behaviour lines up. Borrowing the handlers would make that        *)
(* question unaskable -- they would agree by construction.                 *)
(*                                                                         *)
(* Only the VOCABULARY is shared, through `Requests`: what an object is,   *)
(* what a request is, which events exist. That is the same protocol being  *)
(* spoken about, not the same machine speaking.                            *)
(***************************************************************************)

(* @type: $object; *)
Absent ==
    [ promise |-> [ state |-> "resolved", param |-> NoValue, value |-> NoValue,
                    tags |-> [targeted |-> FALSE, timer |-> FALSE,
                              external |-> FALSE, target |-> NoAddr,
                              delay |-> 0],
                    timeoutAt |-> 0, createdAt |-> 0, settledAt |-> 0,
                    callbacks |-> {}, listeners |-> {} ],
      task    |-> NoTask ]

(* @type: (Set($id), ($id) => $object, $env) => Seq($effect); *)
CommitAll(S, Obj(_), env) ==
    ApaFoldSet(LAMBDA acc, i :
                   acc \o << Variant("PutObject", [id |-> i, obj |-> Obj(i)]) >>,
               << >>, S)

(* @type: ($object, Int) => $object; *)
Project(obj, t) ==
    IF obj.promise.state = "pending" /\ obj.promise.timeoutAt <= t THEN
        [ promise |-> [obj.promise EXCEPT
                          !.state     = IF obj.promise.tags.timer THEN "resolved"
                                        ELSE "rejectedTimedout",
                          !.value     = NoValue,
                          !.settledAt = obj.promise.timeoutAt],
          task    |-> [obj.task EXCEPT !.state     = IF @ = "none" THEN "none"
                                                     ELSE "fulfilled",
                                       !.pid       = NoPid,
                                       !.ttl       = NoTime,
                                       !.expiresAt = NoTime,
                                       !.retryAt   = NoTime,
                                       !.resumes   = {}] ]
    ELSE
        obj

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
        LET new == New(req, env.now)
        IN
            [ effects |-> << Variant("PutObject", [id |-> req.id, obj |-> new]) >> ]

(* @type: ($settleReq, $env) => $outcome; *)
HandlePromiseSettle(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET old == Project(env.objects[req.id], env.now)
            new == [ promise |-> [old.promise EXCEPT !.state     = req.state,
                                                     !.value     = req.value,
                                                     !.settledAt = env.now],
                     task    |-> [old.task EXCEPT !.state     = IF @ = "none" THEN "none"
                                                               ELSE "fulfilled",
                                                  !.pid       = NoPid,
                                                  !.ttl       = NoTime,
                                                  !.expiresAt = NoTime,
                                                  !.retryAt   = NoTime,
                                                  !.resumes   = {}] ]
        IN
            IF old.promise.state = "pending" THEN
                [ effects |-> << Variant("PutObject", [id |-> req.id, obj |-> new]) >> ]
            ELSE
                [ effects |-> << >> ]

(* @type: ($getReq, $env) => $outcome; *)
HandlePromiseGet(req, env) == [ effects |-> << >> ]

(* @type: ($callbackReq, $env) => $outcome; *)
HandlePromiseRegisterCallback(req, env) ==
    IF \/ req.awaited = req.awaiter
       \/ req.awaited.origin /= req.awaiter.origin
       \/ req.awaited \notin DOMAIN env.objects
       \/ req.awaiter \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET awaited    == Project(env.objects[req.awaited], env.now)
            awaiter    == Project(env.objects[req.awaiter], env.now)
            newAwaited == [awaited EXCEPT !.promise.callbacks = @ \cup {req.awaiter}]
        IN
            IF \/ ~awaiter.promise.tags.targeted
               \/ ~IsExternal(awaited.promise)
               \/ awaited.promise.state /= "pending"
               \/ awaiter.promise.state /= "pending" THEN
                [ effects |-> << >> ]
            ELSE
                [ effects |-> << Variant("PutObject",
                                         [id |-> req.awaited, obj |-> newAwaited]) >> ]

(* @type: ($listenerReq, $env) => $outcome; *)
HandlePromiseRegisterListener(req, env) ==
    IF req.awaited \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET old == Project(env.objects[req.awaited], env.now)
            new == [old EXCEPT !.promise.listeners = @ \cup {req.address}]
        IN
            IF \/ ~IsExternal(old.promise)
               \/ old.promise.state /= "pending" THEN
                [ effects |-> << >> ]
            ELSE
                [ effects |-> << Variant("PutObject",
                                         [id |-> req.awaited, obj |-> new]) >> ]

(* @type: ($getReq, $env) => $outcome; *)
HandleTaskGet(req, env) == [ effects |-> << >> ]

(* @type: ($taskCreateReq, $env) => $outcome; *)
HandleTaskCreate(req, env) ==
    IF ~req.action.tags.targeted THEN
        [ effects |-> << >> ]
    ELSE IF req.action.id \notin DOMAIN env.objects THEN
        LET born == New(req.action, env.now)
            new  == IF born.promise.state = "pending" THEN
                        [born EXCEPT !.task.state     = "acquired",
                                     !.task.version   = @ + 1,
                                     !.task.ttl       = req.ttl,
                                     !.task.pid       = req.pid,
                                     !.task.expiresAt = env.now + req.ttl,
                                     !.task.retryAt   = NoTime,
                                     !.task.resumes   = {}]
                    ELSE
                        born
        IN
            [ effects |-> << Variant("PutObject",
                                     [id |-> req.action.id, obj |-> new]) >> ]
    ELSE
        LET old == Project(env.objects[req.action.id], env.now)
            new == [old EXCEPT !.task.state     = "acquired",
                               !.task.version   = @ + 1,
                               !.task.ttl       = req.ttl,
                               !.task.pid       = req.pid,
                               !.task.expiresAt = env.now + req.ttl,
                               !.task.retryAt   = NoTime,
                               !.task.resumes   = {}]
        IN
            IF \/ ~old.promise.tags.targeted
               \/ old.task.state /= "pending" THEN
                [ effects |-> << >> ]
            ELSE
                [ effects |-> << Variant("PutObject",
                                         [id |-> req.action.id, obj |-> new]) >> ]

(* @type: ($taskAcquireReq, $env) => $outcome; *)
HandleTaskAcquire(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET old == Project(env.objects[req.id], env.now)
            new == [old EXCEPT !.task.state     = "acquired",
                               !.task.version   = @ + 1,
                               !.task.ttl       = req.ttl,
                               !.task.pid       = req.pid,
                               !.task.expiresAt = env.now + req.ttl,
                               !.task.retryAt   = NoTime,
                               !.task.resumes   = {}]
        IN
            IF \/ old.task.state /= "pending"
               \/ old.promise.state /= "pending"
               \/ old.task.version /= req.version THEN
                [ effects |-> << >> ]
            ELSE
                [ effects |-> << Variant("PutObject", [id |-> req.id, obj |-> new]) >> ]

(* @type: ($taskFulfillReq, $env) => $outcome; *)
HandleTaskFulfill(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET old == Project(env.objects[req.id], env.now)
            new == [ promise |-> [old.promise EXCEPT !.state     = req.action.state,
                                                     !.value     = req.action.value,
                                                     !.settledAt = env.now],
                     task    |-> [old.task EXCEPT !.state     = "fulfilled",
                                                  !.pid       = NoPid,
                                                  !.ttl       = NoTime,
                                                  !.expiresAt = NoTime,
                                                  !.retryAt   = NoTime,
                                                  !.resumes   = {}] ]
        IN
            IF \/ old.task.state /= "acquired"
               \/ old.promise.state /= "pending"
               \/ old.task.version /= req.version THEN
                [ effects |-> << >> ]
            ELSE
                [ effects |-> << Variant("PutObject", [id |-> req.id, obj |-> new]) >> ]

(* @type: ($taskRef, $env) => $outcome; *)
HandleTaskRelease(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET old == Project(env.objects[req.id], env.now)
            new == [old EXCEPT !.task.state     = "pending",
                               !.task.pid       = NoPid,
                               !.task.ttl       = NoTime,
                               !.task.expiresAt = NoTime,
                               !.task.retryAt   = env.now]
        IN
            IF \/ old.task.state /= "acquired"
               \/ old.promise.state /= "pending"
               \/ old.task.version /= req.version THEN
                [ effects |-> << >> ]
            ELSE
                [ effects |-> << Variant("PutObject", [id |-> req.id, obj |-> new]) >> ]

(* @type: ($getReq, $env) => $outcome; *)
HandleTaskHalt(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET old == Project(env.objects[req.id], env.now)
            new == [old EXCEPT !.task.state     = "halted",
                               !.task.pid       = NoPid,
                               !.task.ttl       = NoTime,
                               !.task.expiresAt = NoTime,
                               !.task.retryAt   = NoTime]
        IN
            IF \/ old.task.state = "none"
               \/ old.task.state \in {"fulfilled", "halted"} THEN
                [ effects |-> << >> ]
            ELSE
                [ effects |-> << Variant("PutObject", [id |-> req.id, obj |-> new]) >> ]

(* @type: ($getReq, $env) => $outcome; *)
HandleTaskContinue(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET old == Project(env.objects[req.id], env.now)
            new == [old EXCEPT !.task.state     = "pending",
                               !.task.pid       = NoPid,
                               !.task.ttl       = NoTime,
                               !.task.expiresAt = NoTime,
                               !.task.retryAt   = env.now]
        IN
            IF \/ old.task.state /= "halted"
               \/ old.promise.state /= "pending" THEN
                [ effects |-> << >> ]
            ELSE
                [ effects |-> << Variant("PutObject", [id |-> req.id, obj |-> new]) >> ]

(* @type: (PID, Set($taskRef), $env) => $outcome; *)
Renewable(pid, refs, env) ==
    { i \in DOMAIN env.objects :
        LET pr == Project(env.objects[i], env.now)
        IN  /\ \E rf \in refs : rf.id = i /\ rf.version = pr.task.version
            /\ pr.task.state /= "none"
            /\ pr.task.state = "acquired"
            /\ pr.task.pid = pid
            /\ pr.promise.state = "pending" }

HandleTaskHeartbeat(req, env) ==
    [ effects |->
        CommitAll(Renewable(req.pid, req.tasks, env),
                  LAMBDA i :
                      LET old == Project(env.objects[i], env.now)
                      IN
                          [old EXCEPT !.task.expiresAt = env.now + old.task.ttl],
                  env) ]

HandleTaskSuspend(req, env) ==
    LET aw   == { a.awaited : a \in req.actions }
        seen == { a \in aw : a \in DOMAIN env.objects }
    IN
        IF \/ req.actions = {}
           \/ req.id \in aw
           \/ \E a \in aw : a.origin /= req.id.origin
           \/ req.id \notin DOMAIN env.objects
           \/ seen /= aw THEN
            [ effects |-> << >> ]
        ELSE
            LET old == Project(env.objects[req.id], env.now)
                new == [old EXCEPT !.task.state     = "suspended",
                                   !.task.pid       = NoPid,
                                   !.task.ttl       = NoTime,
                                   !.task.expiresAt = NoTime,
                                   !.task.retryAt   = NoTime,
                                   !.task.resumes   = {}]
            IN
                IF \/ old.task.state /= "acquired"
                   \/ old.promise.state /= "pending"
                   \/ old.task.version /= req.version
                   \/ \E a \in aw :
                        ~IsExternal(Project(env.objects[a], env.now).promise) THEN
                    [ effects |-> << >> ]
                ELSE IF \E a \in aw :
                          Project(env.objects[a], env.now).promise.state /= "pending" THEN
                    [ effects |-> << Variant("PutObject",
                                             [id |-> req.id,
                                              obj |-> [old EXCEPT !.task.resumes = {}]]) >> ]
                ELSE
                    [ effects |->
                        << Variant("PutObject", [id |-> req.id, obj |-> new]) >>
                        \o CommitAll(aw,
                                  LAMBDA a :
                                      [Project(env.objects[a], env.now) EXCEPT
                                           !.promise.callbacks = @ \cup {req.id}],
                                  env) ]

(* @type: ($id, Int, $fenceAction, $env) => $outcome; *)
FenceTarget(act) ==
    IF VariantTag(act) = "Create"
    THEN VariantGetUnsafe("Create", act).req.id
    ELSE VariantGetUnsafe("Settle", act).req.id

HandleTaskFence(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET old == Project(env.objects[req.id], env.now)
        IN
            IF \/ FenceTarget(req.action) = req.id
               \/ old.task.state /= "acquired"
               \/ old.promise.state /= "pending"
               \/ old.task.version /= req.version THEN
                [ effects |-> << >> ]
            ELSE IF VariantTag(req.action) = "Create" THEN
                HandlePromiseCreate(VariantGetUnsafe("Create", req.action).req, env)
            ELSE
                HandlePromiseSettle(VariantGetUnsafe("Settle", req.action).req, env)

(* @type: ($id, $env) => $outcome; *)
ProcessLeaseTimeout(i, env) ==
    IF i \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET old == env.objects[i]
            new == [old EXCEPT !.task.state     = "pending",
                               !.task.pid       = NoPid,
                               !.task.ttl       = NoTime,
                               !.task.expiresAt = NoTime,
                               !.task.retryAt   = env.now]
        IN
            IF \/ old.task.state /= "acquired"
               \/ old.task.expiresAt = NoTime
               \/ old.task.expiresAt > env.now
               \/ Project(old, env.now).promise.state /= "pending" THEN
                [ effects |-> << >> ]
            ELSE
                [ effects |-> << Variant("PutObject", [id |-> i, obj |-> new]) >> ]

(* @type: ($id, $env) => $outcome; *)
ProcessRetryTimeout(i, env) ==
    IF i \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET old == env.objects[i]
            new == [old EXCEPT !.task.retryAt = env.now + env.config.retryTimeout]
        IN
            IF \/ old.task.state /= "pending"
               \/ old.task.retryAt = NoTime
               \/ old.task.retryAt > env.now
               \/ Project(old, env.now).promise.state /= "pending" THEN
                [ effects |-> << >> ]
            ELSE
                [ effects |-> << Variant("PutObject", [id |-> i, obj |-> new]),
                                 Variant("Send",
                                         [entry |->
                                            [address |-> old.promise.tags.target,
                                             message |-> Variant("Execute",
                                                                 [id      |-> i,
                                                                  version |-> old.task.version])]]) >> ]

(* @type: ($listenerDrainReq, $env) => $outcome; *)
ProcessListener(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET awaited    == Project(env.objects[req.id], env.now)
            newAwaited == [awaited EXCEPT !.promise.listeners = @ \ {req.address}]
        IN
            IF \/ awaited.promise.state = "pending"
               \/ req.address \notin awaited.promise.listeners THEN
                [ effects |-> << >> ]
            ELSE
                [ effects |-> << Variant("PutObject",
                                         [id |-> req.id, obj |-> newAwaited]),
                                 Variant("Send",
                                         [entry |->
                                            [address |-> req.address,
                                             message |-> Variant("Unblock",
                                                                 [id    |-> req.id,
                                                                  state |-> awaited.promise.state])]]) >> ]

(* @type: ($drainReq, $env) => $outcome; *)
ProcessCallback(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET awaited    == Project(env.objects[req.id], env.now)
            newAwaited == [awaited EXCEPT !.promise.callbacks = @ \ {req.awaiter}]
        IN
            IF \/ awaited.promise.state = "pending"
               \/ req.awaiter \notin awaited.promise.callbacks THEN
                [ effects |-> << >> ]
            ELSE IF req.awaiter \notin DOMAIN env.objects THEN
                [ effects |-> << Variant("PutObject",
                                         [id |-> req.id, obj |-> newAwaited]) >> ]
            ELSE
                LET awaiter    == Project(env.objects[req.awaiter], env.now)
                    newAwaiter == IF awaiter.task.state = "suspended" THEN
                                      [awaiter EXCEPT !.task.state     = "pending",
                                                      !.task.pid       = NoPid,
                                                      !.task.ttl       = NoTime,
                                                      !.task.expiresAt = NoTime,
                                                      !.task.retryAt   = env.now,
                                                      !.task.resumes   = {req.id}]
                                  ELSE
                                      [awaiter EXCEPT !.task.resumes = @ \cup {req.id}]
                IN
                    IF awaiter.task.state \in {"none", "fulfilled"} THEN
                        [ effects |-> << Variant("PutObject",
                                                 [id |-> req.id, obj |-> newAwaited]) >> ]
                    ELSE
                        [ effects |-> << Variant("PutObject",
                                                 [id |-> req.id, obj |-> newAwaited]),
                                         Variant("PutObject",
                                                 [id |-> req.awaiter, obj |-> newAwaiter]) >> ]

(* @type: ({ id: $id, kind: Str }, $env) => $outcome; *)
ProcessPromiseTimeout(req, env) ==
    IF req.kind /= "promise" \/ req.id \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET old == env.objects[req.id]
            new == Project(old, env.now)
        IN
            IF new /= old THEN
                [ effects |-> << Variant("PutObject", [id |-> req.id, obj |-> new]) >> ]
            ELSE
                [ effects |-> << >> ]

(* @type: ($event, $env) => $outcome; *)
Handle(ev, env) ==
    LET p(tag) == VariantGetUnsafe(tag, ev)
    IN
    CASE VariantTag(ev) = "PromiseGet" ->
             HandlePromiseGet(p("PromiseGet"), env)
      [] VariantTag(ev) = "PromiseCreate" ->
             HandlePromiseCreate(p("PromiseCreate").req, env)
      [] VariantTag(ev) = "PromiseSettle" ->
             HandlePromiseSettle(p("PromiseSettle").req, env)
      [] VariantTag(ev) = "PromiseRegisterCallback" ->
             HandlePromiseRegisterCallback(p("PromiseRegisterCallback").req, env)
      [] VariantTag(ev) = "PromiseRegisterListener" ->
             HandlePromiseRegisterListener(p("PromiseRegisterListener"), env)
      [] VariantTag(ev) = "TaskGet" ->
             HandleTaskGet(p("TaskGet"), env)
      [] VariantTag(ev) = "TaskCreate" ->
             HandleTaskCreate(p("TaskCreate"), env)
      [] VariantTag(ev) = "TaskAcquire" ->
             HandleTaskAcquire(p("TaskAcquire"), env)
      [] VariantTag(ev) = "TaskFence" ->
             HandleTaskFence(p("TaskFence"), env)
      [] VariantTag(ev) = "TaskHeartbeat" ->
             HandleTaskHeartbeat(p("TaskHeartbeat"), env)
      [] VariantTag(ev) = "TaskSuspend" ->
             HandleTaskSuspend(p("TaskSuspend"), env)
      [] VariantTag(ev) = "TaskFulfill" ->
             HandleTaskFulfill(p("TaskFulfill"), env)
      [] VariantTag(ev) = "TaskRelease" ->
             HandleTaskRelease(p("TaskRelease"), env)
      [] VariantTag(ev) = "TaskHalt" ->
             HandleTaskHalt(p("TaskHalt"), env)
      [] VariantTag(ev) = "TaskContinue" ->
             HandleTaskContinue(p("TaskContinue"), env)
      [] VariantTag(ev) = "Timeout" ->
             LET d == p("Timeout")
             IN  CASE d.kind = "promise" -> ProcessPromiseTimeout(d, env)
                   [] d.kind = "lease"   -> ProcessLeaseTimeout(d.id, env)
                   [] OTHER              -> ProcessRetryTimeout(d.id, env)
      [] VariantTag(ev) = "ListenerDrain" ->
             ProcessListener(p("ListenerDrain"), env)
      [] VariantTag(ev) = "CallbackDrain" ->
             ProcessCallback(p("CallbackDrain"), env)
      [] OTHER -> [ effects |-> << >> ]

-----------------------------------------------------------------------------

(* @type: $promise => Bool; *)
SettledNow(p) == p.state /= "pending" \/ p.timeoutAt <= now

(* @type: (($id -> $object), $event) => Bool; *)
FiresIn(objs, ev) ==
    CASE VariantTag(ev) = "Timeout" ->
             LET d == VariantGetUnsafe("Timeout", ev) IN
             /\ d.id \in DOMAIN objs
             /\ Deadline(objs[d.id], d.kind) /= NoTime
             /\ Deadline(objs[d.id], d.kind) <= now
      [] VariantTag(ev) = "ListenerDrain" ->
             LET d == VariantGetUnsafe("ListenerDrain", ev) IN
             /\ d.id \in DOMAIN objs
             /\ SettledNow(objs[d.id].promise)
             /\ d.address \in objs[d.id].promise.listeners
      [] VariantTag(ev) = "CallbackDrain" ->
             LET d == VariantGetUnsafe("CallbackDrain", ev) IN
             /\ d.id \in DOMAIN objs
             /\ SettledNow(objs[d.id].promise)
             /\ d.awaiter \in objs[d.id].promise.callbacks
      [] OTHER -> FALSE


(* @type: Seq($effect) => Set({ id: $id, obj: $object }); *)
Puts(fx) ==
    { VariantGetUnsafe("PutObject", fx[i])
      : i \in { j \in DOMAIN fx : VariantTag(fx[j]) = "PutObject" } }

(* @type: Seq($effect) => Set($outEntry); *)
Says(fx) ==
    { VariantGetUnsafe("Send", fx[i]).entry
      : i \in { j \in DOMAIN fx : VariantTag(fx[j]) = "Send" } }

(* @type: (($id -> $object), Seq($effect)) => ($id -> $object); *)
PutsInto(objs, fx) ==
    LET W == Puts(fx) IN
    [ i \in (DOMAIN objs) \cup {w.id : w \in W} |->
         IF \E w \in W : w.id = i
         THEN (CHOOSE w \in W : w.id = i).obj
         ELSE objs[i] ]

(* @type: (Set($outEntry), Seq($effect)) => Set($outEntry); *)
SaysInto(ob, fx) ==
    LET S == Says(fx) IN
    { o \in ob : ~\E e \in S : MsgKey(o) = MsgKey(e) } \cup S


(***************************************************************************)
(* THE EXECUTOR                                                            *)
(***************************************************************************)

(* Which document a step touches.
   
   THIS IS WHERE THE ONE-DOCUMENT PREMISE BITES. `Process` hands the
   handler `docs[o]` and nothing else, so a handler that reads or writes
   an object at another origin cannot be served. Several now do:
   `promiseRegisterCallback` reads the awaiter, `taskSuspend` writes
   every awaited promise, `CallbackDrain` writes the awaiter's task, and
   `taskFence` names a target that must be a different ID -- which is
   not the same as a different origin, and nothing constrains the
   origin.
   
   With a single origin every object is in one document and all of them
   are single-document operations -- which is `SameOrigin` holding, by
   construction rather than by proof. The model runs that way. Raising
   `|Origin|` above one is exactly the experiment `SameOrigin` names,
   and this operator is where it would have to be answered: a
   cross-origin handler needs either two document reads or a
   transaction, and this executor offers neither.
   @type: $event => ORIGIN; *)
OriginOf(ev) ==
    LET p(tag) == VariantGetUnsafe(tag, ev) IN
    CASE VariantTag(ev) = "PromiseGet"              -> p("PromiseGet").id.origin
      [] VariantTag(ev) = "PromiseCreate"           -> p("PromiseCreate").req.id.origin
      [] VariantTag(ev) = "PromiseSettle"           -> p("PromiseSettle").req.id.origin
      [] VariantTag(ev) = "PromiseRegisterCallback" ->
             p("PromiseRegisterCallback").req.awaited.origin
      [] VariantTag(ev) = "PromiseRegisterListener" ->
             p("PromiseRegisterListener").awaited.origin
      [] VariantTag(ev) = "TaskGet"       -> p("TaskGet").id.origin
      [] VariantTag(ev) = "TaskCreate"    -> p("TaskCreate").action.id.origin
      [] VariantTag(ev) = "TaskAcquire"   -> p("TaskAcquire").id.origin
      [] VariantTag(ev) = "TaskFence"     -> p("TaskFence").id.origin
      [] VariantTag(ev) = "TaskSuspend"   -> p("TaskSuspend").id.origin
      [] VariantTag(ev) = "TaskFulfill"   -> p("TaskFulfill").id.origin
      [] VariantTag(ev) = "TaskRelease"   -> p("TaskRelease").id.origin
      [] VariantTag(ev) = "TaskHalt"      -> p("TaskHalt").id.origin
      [] VariantTag(ev) = "TaskContinue"  -> p("TaskContinue").id.origin
      [] VariantTag(ev) = "Timeout"       -> p("Timeout").id.origin
      [] VariantTag(ev) = "ListenerDrain" -> p("ListenerDrain").id.origin
      [] VariantTag(ev) = "CallbackDrain" -> p("CallbackDrain").id.origin
      \* heartbeat names a SET of tasks and the searches name none
      [] OTHER -> CHOOSE o \in Origin : TRUE

Fresh(ev) ==
    [ ev |-> ev, phase |-> "process", pending |-> << >>,
      expect |-> SetAsFun({}), at |-> 0, org |-> OriginOf(ev) ]

Put(f, k, v) == [x \in (DOMAIN f) \cup {k} |-> IF x = k THEN v ELSE f[x]]
Drop(f, k)   == [x \in (DOMAIN f) \ {k}    |-> f[x]]

SubmitExternal(ev) ==
    /\ ev \in ExternalEvent
    /\ \E r \in Rid \ DOMAIN steps : steps' = Put(steps, r, Fresh(ev))
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

(* THE ASYMMETRY, stated plainly, because it is the point of having two
   machines rather than one:

     Abstract may take an internal step whenever the TIMEOUT VALUES ON AN
     OBJECT allow it.

     Concrete may take one only when there is an ARMED ENTRY for it.

   So this machine is strictly more restricted, and the two directions of
   that are not the same:

     - for SAFETY it is free. Fewer behaviours is what refinement wants,
       and an entry that fires with nothing due merely produces a step
       that writes nothing -- a stutter upstairs.

     - for LIVENESS it is the whole risk. `A!Fairness` says a deadline
       that has come due is eventually acted on. Down here that can only
       happen if the wheel holds an entry for it, so an object carrying a
       deadline the wheel has forgotten is a timeout that never fires and
       an abstract obligation this machine cannot meet.

   Which is why `C_WheelComplete` is not a side condition somebody
   thought to write down. It is exactly what `A!Spec` demands of anything
   that keeps an index instead of reading the objects, and if it holds,
   every internal step `Abstract` may take is one `Concrete` may take too.
   @type: $event => Bool; *)
Fires(ev) ==
    CASE VariantTag(ev) = "Timeout" ->
             LET d == VariantGetUnsafe("Timeout", ev) IN
             \E e \in timeouts : /\ e.id   = d.id
                                /\ e.kind = d.kind
                                /\ e.at  <= now
      [] OTHER -> FALSE

SubmitInternal(ev) ==
    /\ ev \in InternalEvent
    /\ Fires(ev)
    /\ \E r \in Rid \ DOMAIN steps : steps' = Put(steps, r, Fresh(ev))
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

(* WHAT IS DUE IS WHAT THE DOCUMENT SAYS IS DUE. Every deadline is a
   FIELD on an object -- `timeoutAt`, `expiresAt`, `retryAt` -- and the
   document is already in hand, so there is nothing to ask an index
   about. The wheel is a TRIGGER: it says wake up and look at this
   document. It does not decide what the work is.
   @type: (($id -> $object), Int) => Set({ id: $id, kind: Str }); *)
DrainableTimeouts(doc, t) ==
    { d \in [id : DOMAIN doc, kind : DeadlineKind] :
        /\ Deadline(doc[d.id], d.kind) /= NoTime
        /\ Deadline(doc[d.id], d.kind) <= t }

(* NOTHING HAPPENS OUTSIDE A REQUEST OR A TIMER. That is the rule this
   executor lives by, and it is why `Fires` is the wheel and nothing
   else: a drain is not work this machine goes looking for and not an
   event it can schedule.

   It is what happens TO A DOCUMENT when a request or a timer touches
   one. So it is written here as a transformation of the document, with
   no event vocabulary involved -- `drain_doc` inside `prepare_doc`.

   A settled promise still carrying a name owes that name an answer: a
   listener gets `Unblock`, a callback gets its awaiter resumed. Both
   strike the name in the same write that answers it, which is why a
   drain happens once.
   @type: (($id -> $object), Int) => Set({ id: $id, address: ADDR }); *)
DrainableListeners(doc, t) ==
    UNION { { [id |-> i, address |-> a] : a \in doc[i].promise.listeners }
              : i \in { j \in DOMAIN doc :
                            \/ doc[j].promise.state /= "pending"
                            \/ doc[j].promise.timeoutAt <= t } }

(* @type: (($id -> $object), Int) => Set({ id: $id, awaiter: $id }); *)
DrainableCallbacks(doc, t) ==
    UNION { { [id |-> i, awaiter |-> w] : w \in doc[i].promise.callbacks }
              : i \in { j \in DOMAIN doc :
                            \/ doc[j].promise.state /= "pending"
                            \/ doc[j].promise.timeoutAt <= t } }

(* SWEEPING A CHOSEN AMOUNT. The drain itself is PROTOCOL, so it is the
   protocol's own operator that performs it -- `ProcessListener` and
   `ProcessCallback` are functions from a document to effects, not
   events being scheduled, and calling one is not this machine taking an
   internal step.

   The document is threaded so two drains on one promise do not
   overwrite each other's strike. One pass, not a fixpoint: a drain that
   only becomes possible because of another waits for the next access.
   @type: (($id -> $object), Int, Set({ id: $id, address: ADDR }),
           Set({ id: $id, awaiter: $id }))
              => { doc: $id -> $object, fx: Seq($effect) }; *)
Sweep(doc, t, TS, LS, CS) ==
    LET EnvOf(d) == [ objects |-> d, now |-> t,
                      config  |-> [retryTimeout |-> RetryTimeout] ]
        Step(H(_,_), st, req) ==
            LET out == H(req, EnvOf(st.doc))
            IN  [ doc |-> PutsInto(st.doc, out.effects),
                  fx  |-> st.fx \o out.effects ]
        Fire(st, d) ==
            LET out == CASE d.kind = "promise" ->
                                ProcessPromiseTimeout(d, EnvOf(st.doc))
                         [] d.kind = "lease" ->
                                ProcessLeaseTimeout(d.id, EnvOf(st.doc))
                         [] OTHER ->
                                ProcessRetryTimeout(d.id, EnvOf(st.doc))
            IN  [ doc |-> PutsInto(st.doc, out.effects),
                  fx  |-> st.fx \o out.effects ]
        afterT == ApaFoldSet(Fire, [doc |-> doc, fx |-> << >>], TS)
        afterL == ApaFoldSet(LAMBDA st, d : Step(ProcessListener, st, d),
                             afterT, LS)
    IN  ApaFoldSet(LAMBDA st, d : Step(ProcessCallback, st, d), afterL, CS)

(* Read ONE DOCUMENT, decide against it, and remember the etag it was read
   under. The handler is `Abstract`'s: the protocol does not know it is
   being run by a different executor. *)
(* MAINTAINING THE INDEX IS THE EXECUTOR'S JOB, and this is where it
   happens. `Abstract` states what a handler writes and stops there --
   a deadline is a field on an object, and `internal.lean` never says
   arm or disarm. A machine that keeps a WHEEL has to work out what
   that write did to the deadlines it indexes, and the only way to know
   is to compare the document before against the document after.

   Verus does this in `prepare`, from `w0` and `w2`:

     let m1 = state::min_deadline(w2);
     let sched = if m1 is Some
         && (m1 != state::min_deadline(w0) || completing is Some) { ... }

   Two differences from Verus worth naming rather than hiding. It arms
   ONE timer at the earliest deadline and carries a generation to make
   stale firings recognisable; this keeps one entry per object and kind,
   which is a coarser wheel with no generations, so `WheelSound` failing
   here is partly that choice and not only the arm-before-write order.
   And `completing` gives Verus a reason to re-arm even when the minimum
   has not moved, which has no counterpart here.
   @type: (ORIGIN, Set({ id: $id, obj: $object })) => Seq($effect); *)
Before(o, i) == IF i \in DOMAIN docs[o] THEN docs[o][i] ELSE Absent

ArmFor(i, old, new, k) ==
    IF Deadline(new, k) /= NoTime /\ Deadline(new, k) /= Deadline(old, k)
    THEN << Variant("ArmTimeout",
                    [entry |-> [at |-> Deadline(new, k), id |-> i, kind |-> k]]) >>
    ELSE << >>

DisarmFor(i, old, new, k) ==
    IF Deadline(old, k) /= NoTime /\ Deadline(old, k) /= Deadline(new, k)
    THEN << Variant("DisarmTimeout",
                    [entry |-> [at |-> Deadline(old, k), id |-> i, kind |-> k]]) >>
    ELSE << >>

(* Arm before the write and disarm after, which is Verus's
   `sched + put + ack`. Arming first means a crash between the arm and
   the write leaves an entry for an object that is not there -- noise,
   which `C_WheelSound` sees and which the handler re-checks away when
   it fires. Writing first would leave an object carrying a deadline
   nothing will fire -- silence, which `C_WheelComplete` sees and which
   nothing recovers from. Flip the two folds and the checker should hand
   back a trace ending in a `Crash` with a deadline nobody holds.

   This is a claim about EXECUTORS THAT KEEP AN INDEX. It used to sit in
   `Abstract`, which made it look like protocol; it is not, and no
   handler states it. *)
Arms(o, W) ==
    ApaFoldSet(LAMBDA acc, w :
                 acc \o ArmFor(w.id, Before(o, w.id), w.obj, "promise")
                     \o ArmFor(w.id, Before(o, w.id), w.obj, "lease")
                     \o ArmFor(w.id, Before(o, w.id), w.obj, "retry"),
               << >>, W)

Disarms(o, W) ==
    ApaFoldSet(LAMBDA acc, w :
                 acc \o DisarmFor(w.id, Before(o, w.id), w.obj, "promise")
                     \o DisarmFor(w.id, Before(o, w.id), w.obj, "lease")
                     \o DisarmFor(w.id, Before(o, w.id), w.obj, "retry"),
               << >>, W)

Process(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "process"
    /\ \E TS \in SUBSET DrainableTimeouts(docs[steps[r].org], now),
          LS \in SUBSET DrainableListeners(docs[steps[r].org], now),
          CS \in SUBSET DrainableCallbacks(docs[steps[r].org], now) :
       LET o   == steps[r].org
           swept == Sweep(docs[o], now, TS, LS, CS)
           env == [ objects  |-> swept.doc,
                    timeouts |-> timeouts,
                    outbox   |-> outbox,
                    now      |-> now,
                    config   |-> [retryTimeout |-> RetryTimeout] ]
           out == Handle(steps[r].ev, env)
           fx  == swept.fx \o out.effects
           W   == Puts(fx)
       IN  steps' = [steps EXCEPT ![r].phase   = "perform",
                                  ![r].pending = Arms(o, W) \o fx
                                                 \o Disarms(o, W),
                                  ![r].expect  = docs[o],
                                  ![r].at      = now]
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

(* One effect. A write whose document has moved since the read is REFUSED,
   and the step goes back to deciding -- which under the abstraction is
   nothing at all.

   THE FENCE COVERS THE CLOCK AS WELL AS THE DOCUMENT, and that is not
   belt-and-braces -- the refinement demanded it. With only the etag
   compared, TLC returned: decide at `now = 0`, `Clock` ticks, write lands
   at `now = 1`. Upstairs the write IS the whole step, so the abstract
   machine runs the handler at the instant the write lands and gets a
   different answer -- a `createdAt`, a `settledAt`, a born-settled promise
   where the concrete decided a pending one. A decision is only valid at
   the instant it was made, and a store etag says nothing about the clock.

   Verus takes the other road: `Phase::Perform` freezes `at` at prepare
   time and every effect uses it, so the skew is absorbed rather than
   refused. That works against the Lean, where `now` is an argument to
   each step rather than a variable running underneath it. Here it is a
   variable, so the choice is to re-decide, and `steps[r].at` is what the
   comparison is against.

   `Fenced` is a CONSTANT so the fence can be switched OFF and the
   refinement made to prove its own necessity: with `Fenced = FALSE` a
   stale decision lands, and `Spec => A!Spec` should fail and hand back
   the trace that shows why.

   ONE STEP, ONE DOCUMENT WRITE -- WHICH IS WHAT THE EXECUTOR DOES.

   A step lands every `PutObject` it decided on in a single write, and
   every message those writes justify with them. Only the wheel effects
   stay separate, which is the premise the whole model is here to test.

   This operator used to apply one effect per `Perform`, walking the
   list. Two refinement counterexamples came out of that, and both were
   reported as findings about the protocol before they were understood.
   Neither was. They were this module having drifted away from the
   executor it claims to model.

   `Process` hands `pending` the effect list `Abstract!Handle` returned,
   which is the right instinct -- the protocol should not know which
   executor is running it. But `Abstract!Commit` emits a `PutObject` PER
   OBJECT, and upstairs that granularity carries no meaning at all,
   because `Apply` folds the whole list at one instant. Walking the same
   list one entry at a time turns a per-object DESCRIPTION into a
   sequence of per-object STORE OPERATIONS. `Write2` became two document
   writes; `CommitAll` became N.

   The executor emits exactly one, and its body is the whole workflow:

     let put = seq![Effect::PutDocument { key, expect: etag_of(held),
                                          body: w2 }];
     ... sched + put + ack(..) + emits(..) + respond

   The messages are already in that body -- the outbox is a field of the
   workflow, and `lemma_sends_outbox` fixes `step(w, cs, now).outbox` as
   `fold_send(w.outbox, sends_step(w, cs, now))`. `Effect::Send` carries
   an entry from that record onto the WIRE, `s.sent`, which this model
   does not have. So `outbox` here is the record, and the record belongs
   in the write.

   THE LESSON IS ABOUT EFFECT LISTS, not about outboxes. A list written
   for an atomic machine does not carry execution granularity, and
   reading it as though it did is silent: `C_TypeOK` passes, so does
   `C_UnitCoherent`, so do the catalogue properties, because every one
   of them is a statement about states and this was an error about WHEN
   states change. The refinement is what caught it, and catching drift
   between the executor and its model is the thing a refinement against
   a hand-written abstraction function is genuinely FOR.

   `Puts` and `Says` are taken over the whole remaining `pending`, and
   the write is built exactly as `Apply` builds `objects'` -- same fold,
   same `CHOOSE` for two writes to one id -- so the two agree by
   construction rather than by luck. Folding at the write rather than in
   `Process` is a structural difference from the executor, which folds
   when it prepares; the body is fixed at `Process` either way, so the
   two are the same machine.

   The `Send` branch below survives for handlers that emit without
   writing: there the message alone IS the whole decision, and `Apply`
   with an empty `Puts` is exactly a step that moves `outbox` and leaves
   `objects` where it was. The arm and disarm branches survive because
   the wheel is the one thing still allowed to lag, which is the whole
   experiment. *)
Perform(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ IF steps[r].pending = << >>
       THEN /\ steps' = Drop(steps, r)
            /\ UNCHANGED <<docs, timeouts, outbox>>
       ELSE LET e == Head(steps[r].pending)
                o == steps[r].org
            IN CASE VariantTag(e) = "PutObject" ->
                    IF Fenced => (docs[o] = steps[r].expect /\ now = steps[r].at)
                    THEN /\ docs'  = [docs EXCEPT ![o] =
                                         LET W == Puts(steps[r].pending) IN
                                         [ i \in (DOMAIN docs[o])
                                                  \cup {w.id : w \in W} |->
                                             IF \E w \in W : w.id = i
                                             THEN (CHOOSE w \in W : w.id = i).obj
                                             ELSE docs[o][i] ]]
                         /\ outbox' = LET S == Says(steps[r].pending) IN
                                        { x \in outbox :
                                            ~\E m \in S : MsgKey(x) = MsgKey(m) } \cup S
                         /\ steps' = [steps EXCEPT ![r].pending =
                                        SelectSeq(Tail(@),
                                                  LAMBDA f :
                                                    VariantTag(f) \notin
                                                      {"PutObject", "Send"})]
                         /\ UNCHANGED timeouts
                    ELSE /\ steps' = [steps EXCEPT ![r].phase   = "process",
                                                   ![r].pending = << >>]
                         /\ UNCHANGED <<docs, timeouts, outbox>>
                 [] VariantTag(e) = "ArmTimeout" ->
                    /\ timeouts' = timeouts \cup {VariantGetUnsafe("ArmTimeout", e).entry}
                    /\ steps'    = [steps EXCEPT ![r].pending = Tail(@)]
                    /\ UNCHANGED <<docs, outbox>>
                 [] VariantTag(e) = "DisarmTimeout" ->
                    /\ timeouts' = timeouts \ {VariantGetUnsafe("DisarmTimeout", e).entry}
                    /\ steps'    = [steps EXCEPT ![r].pending = Tail(@)]
                    /\ UNCHANGED <<docs, outbox>>
                 [] OTHER ->
                    /\ outbox' = LET en == VariantGetUnsafe("Send", e).entry
                                 IN  {x \in outbox : MsgKey(x) /= MsgKey(en)} \cup {en}
                    /\ steps'  = [steps EXCEPT ![r].pending = Tail(@)]
                    /\ UNCHANGED <<docs, timeouts>>
    /\ UNCHANGED now

Crash(r) ==
    /\ r \in DOMAIN steps
    /\ steps' = Drop(steps, r)
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

Clock ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<docs, timeouts, outbox, steps>>

Init ==
    /\ docs     = [o \in Origin |-> SetAsFun({})]
    /\ timeouts = {}
    /\ outbox   = {}
    /\ steps    = SetAsFun({})
    /\ now      = 0

(***************************************************************************)
(* CRASHES ARE ALWAYS IN THE MACHINE                                       *)
(*                                                                         *)
(* There was a `Crashing` constant here, and switching it off was how       *)
(* liveness got claimed. That is a worse thing to say than it looks: it     *)
(* claims something about an executor that never fails, which is not an     *)
(* executor anyone has. `EventuallyStable` says the thing actually meant    *)
(* -- steps do not fail FOREVER -- so the crash can stay, both claims can   *)
(* be made about the same machine, and the difference between them is one   *)
(* formula rather than one configuration.                                   *)
(***************************************************************************)

Next ==
    \/ \E ev \in ExternalEvent : SubmitExternal(ev)
    \/ \E ev \in InternalEvent : SubmitInternal(ev)
    \/ \E r \in DOMAIN steps : Process(r) \/ Perform(r) \/ Crash(r)
    \/ Clock

(* The fairness `Abstract` asks of itself, asked of this executor. It has
   to be here: `A!Spec` carries `A!Fairness`, and a machine that may
   stall forever cannot implement a machine that may not.

   `SubmitInternal` is STRONGLY fair, and that is not a modelling
   convenience -- weak fairness was not enough and TLC said why. Upstairs
   an internal step is available whenever an object's deadline says so,
   full stop. Down here it is available only when a step slot is free,
   and `|Rid|` is finite. So the counterexample was a cycle in which
   clients kept arriving, the slots kept filling, and a due timeout with
   a properly armed entry was never once submitted -- legal under weak
   fairness, because the action was not CONTINUOUSLY enabled, only
   enabled again and again.

   Strong fairness is the honest reading of what the abstract machine
   demands: an executor must not indefinitely prefer client work to work
   that has come due. That is a scheduling obligation on the real server
   -- a fair choice between the API path and the timer path -- and it is
   the kind of requirement that is invisible until liveness is checked
   against a bounded resource. *)
(***************************************************************************)
(* CRASHES THAT STOP, without a counter                                    *)
(*                                                                         *)
(* `Crashing = FALSE` says an executor never fails, which is not what we   *)
(* mean and not something anyone would claim. What we mean is that steps   *)
(* do not fail FOREVER, and that is sayable directly:                      *)
(*                                                                         *)
(*     <>[][ \A r \in Rid : ~Crash(r) ]_vars                               *)
(*                                                                         *)
(* `[][A]_vars` holds of a step when it satisfies `A` or leaves `vars`     *)
(* alone. A crash does neither -- it removes a step -- so `[][~Crash]_vars`*)
(* forbids crash steps, and `<>[]` says: from some point on, forever.      *)
(* No bound, no counter, no constant. Just "not forever".                  *)
(*                                                                         *)
(* Crashes stay in the machine, safety is still checked against them, and  *)
(* only the liveness claim is conditioned on their ceasing. That is the    *)
(* shape every liveness result under failure has -- an eventual-stability  *)
(* assumption -- written as a formula instead of an English caveat.        *)
(*                                                                         *)
(* WHERE IT GOES IS NOT FREE. The obvious spelling is a hypothesis on the  *)
(* property, `EventuallyStable => A!Spec`, and TLC refuses it:             *)
(*                                                                         *)
(*     Temporal formulas containing actions must be of forms               *)
(*     <>[]A or []<>A.                                                     *)
(*                                                                         *)
(* An action-containing formula is admitted only in those two shapes, and  *)
(* an implication with one as its antecedent is not among them. As a       *)
(* CONJUNCT OF THE SPECIFICATION it is accepted, which is what `SpecStable`*)
(* is. `RefinesSpecStable` is kept because it says what is meant; it is    *)
(* just not the form a checker will take.                                  *)
(*                                                                         *)
(* Note also what this does NOT say. `<>[]` means crashes stop ENTIRELY    *)
(* after some point, which is stronger than "not always". The weaker       *)
(* reading -- infinitely many crashes, but never always the attempt that   *)
(* matters -- is what fairness would express, and it is exactly what the   *)
(* slot-recycling counterexample showed cannot be written over `Rid`:      *)
(* fairness names a slot, and a throwaway no-op step in the same slot      *)
(* discharges it. Eventual stability is the honest thing that is actually  *)
(* expressible here.                                                       *)
(***************************************************************************)

EventuallyStable == <>[][ \A r \in Rid : ~Crash(r) ]_vars


Fairness ==
    /\ \A r  \in Rid             : WF_vars(Process(r) \/ Perform(r))
    /\ \A ev \in InternalEvent : SF_vars(SubmitInternal(ev))
    /\ WF_vars(Clock)
    /\ EventuallyStable

(* The stronger promise, as a SECOND SPECIFICATION rather than a constant
   guard. `StrongSteps => \A r : SF_vars(..)` looked like the tidy way to
   make it switchable, and it is not: TLC recognises fairness only in a
   restricted syntactic form -- WF/SF terms and quantifications over them
   -- and an implication is not in that grammar. It produced a
   "counterexample" that plainly violated the very condition it was
   supposed to assume, which is how the mistake was caught. A fairness
   condition that is not syntactically a fairness condition is not an
   assumption; it is decoration. *)
FairnessSF ==
    /\ Fairness
    /\ \A r \in Rid : SF_vars(Process(r))
    /\ \A r \in Rid : SF_vars(Perform(r))

Spec   == Init /\ [][Next]_vars /\ Fairness
SpecSF == Init /\ [][Next]_vars /\ FairnessSF

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE REFINEMENT                                                          *)
(*                                                                         *)
(* The statement we MEAN is                                                *)
(*                                                                         *)
(*     THEOREM Spec => A!Spec                                              *)
(*                                                                         *)
(* -- every behaviour of the chunked, fenced machine is, once you read a   *)
(* document as the objects it holds, a behaviour of the abstract machine.  *)
(*                                                                         *)
(* No existential is needed any more. `Abstract` has no `steps` and no     *)
(* `timeouts` to fill in, so there is no hidden variable to quantify away  *)
(* and no witness to choose -- the mapping is total and there is only one  *)
(* of it. That is worth more than the convenience: it means the theorem no *)
(* longer depends on a choice I made, and an executor with entirely        *)
(* different bookkeeping is measured by the same statement rather than     *)
(* having to supply its own.                                               *)
(*                                                                         *)
(* `A!Spec` and not `A!Safety`: the obligation includes the abstract       *)
(* machine's FAIRNESS. Refining only safety would let an implementation    *)
(* satisfy the specification by doing nothing -- accepting a request,      *)
(* deciding, and stalling forever is safe, and is not an implementation of *)
(* anything.                                                               *)
(*                                                                         *)
(* Note the direction. It does NOT say the two machines admit the same     *)
(* behaviours; the fence exists precisely to admit FEWER.                  *)
(*                                                                         *)
(* And it is a real obligation rather than a formality: the first time it  *)
(* was checked it FAILED, on a refused compare-and-swap sending a step     *)
(* from "perform" back to "process". `Abstract` had no action that went    *)
(* backwards, so every behaviour containing a retry was outside the        *)
(* specification. `Abstract!Restart` is what that failure bought.          *)
(*                                                                         *)
(* That failure is gone, and the way it went is the lesson. It was never a *)
(* fact about the protocol -- it came from mapping `steps` across, so that *)
(* an executor's retry had to be legislated for upstairs. Take the         *)
(* bookkeeping out of the abstract machine and the retry stops being a     *)
(* step it has to permit. `Abstract!Restart` is gone with it.              *)
(***************************************************************************)

(* Both halves, named separately, so that checking one never means editing
   the module. An earlier version had a single `Refinement` that got
   sed-swapped between the two for one-off runs -- and a commit landed
   while it was swapped. A definition you have to edit to check is a
   definition that will eventually be committed mid-edit. *)

RefinesSafety == A!Safety
RefinesSpec   == A!Spec

THEOREM Spec => A!Spec

(* The invariants the abstract machine fails, asked of this one. That the
   fence restores them is the other half of the result, and it is not
   implied by the refinement -- refinement bounds what this machine MAY
   do, and says nothing about what it GUARANTEES. *)
C_TypeOK          == A!TypeOK
C_UnitCoherent    == A!UnitCoherent

(* The wheel invariants live HERE now, because the wheel does. They are
   not among the 95 and could not be: they say an INDEX agrees with the
   deadlines it indexes, and the protocol has no index.

   `WheelSound` fails, and should: arming before writing admits an entry
   for an object that is not there yet, which is noise the handler
   re-checks away. `WheelComplete` is the one that matters -- a deadline
   nothing holds is a timeout that never fires, and nothing recovers. *)
C_WheelSound ==
    \A e \in timeouts :
        /\ e.id \in DOMAIN Objects
        /\ Deadline(Objects[e.id], e.kind) = e.at

C_WheelComplete ==
    \A o \in DOMAIN Objects, k \in DeadlineKind :
        Deadline(Objects[o], k) /= NoTime =>
            [at |-> Deadline(Objects[o], k), id |-> o, kind |-> k] \in timeouts

(* A WITNESS, NOT AN INVARIANT.

   `CallbackDrain` writes two objects, so it emits two `PutObject`s and
   this executor performs them one at a time. The state in between --
   one object written, the other still in hand -- is the multi-unit
   write the header warns about, and the expectation was that it would
   break the refinement the same way a write with its message still in
   hand does.

   There is a reason it might not, and it is worth stating before the
   answer: refinement asks for SOME abstract step between the two mapped
   states, not the same one. A lone object write is what half the
   alphabet does -- a settle, a create, a claim -- so the half-finished
   pair may be a state `Abstract` reaches all the time, just under a
   different request than the one this machine is running. A message has
   no such cover: nothing in `Abstract` ever says without writing in the
   same breath. If that asymmetry is the whole story then writes may be
   split and messages may not.

   THESE THREE OPERATORS EXIST BECAUSE THE FIRST ANSWER WAS VACUOUS. A
   run with `MaxTime = 0` reported no violation and had never run the
   drain at all: a promise created at time 0 with a timeout of 0 is born
   settled, so no callback is ever registered and the multi-unit write
   never happens. `NoDrainRan` is what caught it. With the clock back,
   all three fire -- the drain runs, a step carries two puts, and the
   half-written state is reached -- so a refinement check against that
   configuration is asking the question rather than missing it.

   With one `Rid` nothing else writes, so an etag past what the step
   read means the step itself wrote; a `PutObject` still pending means
   it has not finished. Check `~SplitWrite` and TLC hands back the
   state, which is the witness. *)
SplitWrite ==
    \E r \in DOMAIN steps :
        /\ steps[r].phase = "perform"
        /\ docs[steps[r].org] /= steps[r].expect
        /\ \E j \in DOMAIN steps[r].pending :
               VariantTag(steps[r].pending[j]) = "PutObject"

NoSplitWrite == ~SplitWrite

DrainRan   == \E r \in DOMAIN steps : VariantTag(steps[r].ev) = "CallbackDrain"
NoDrainRan == ~DrainRan

TwoPuts == \E r \in DOMAIN steps :
    Cardinality({ j \in DOMAIN steps[r].pending :
                    VariantTag(steps[r].pending[j]) = "PutObject" }) > 1
NoTwoPuts == ~TwoPuts

CT_preserved_settled_promise_record    == [][A!preserved_settled_promise_record]_vars
CT_consistent_promise_settlement_stamp == [][A!consistent_promise_settlement_stamp]_vars

=============================================================================
