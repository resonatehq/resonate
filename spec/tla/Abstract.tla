-------------------------------- MODULE Abstract --------------------------------
EXTENDS Requests


-----------------------------------------------------------------------------

VARIABLES
    objects,    \* partial: DOMAIN is what exists
    outbox,     \* at most one entry per MsgKey
    now         \* the clock. NOT part of the store: a server does not own

vars ==
    <<objects, outbox, now>>

-----------------------------------------------------------------------------

(* @type: $env; *)
Env ==
    [ objects |-> objects,
      now     |-> now,
      config  |-> [retryTimeout |-> RetryTimeout] ]

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

(* @type: ($getReq, $env) => $outcome; *)
HandlePromiseGet(req, env) ==
    [ effects |-> << >> ]

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

(* @type: ($callbackReq, $env) => $outcome; *)
HandlePromiseRegisterCallback(req, env) ==
    IF \/ req.awaited \notin DOMAIN env.objects
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
HandleTaskGet(req, env) ==
    [ effects |-> << >> ]

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

HandleTaskFence(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ effects |-> << >> ]
    ELSE
        LET old == Project(env.objects[req.id], env.now)
        IN
            IF \/ (IF VariantTag(req.action) = "Create"
                   THEN VariantGetUnsafe("Create", req.action).req.id
                   ELSE VariantGetUnsafe("Settle", req.action).req.id) = req.id
               \/ old.task.state /= "acquired"
               \/ old.promise.state /= "pending"
               \/ old.task.version /= req.version THEN
                [ effects |-> << >> ]
            ELSE IF VariantTag(req.action) = "Create" THEN
                HandlePromiseCreate(VariantGetUnsafe("Create", req.action).req, env)
            ELSE
                HandlePromiseSettle(VariantGetUnsafe("Settle", req.action).req, env)

HandleTaskHeartbeat(req, env) ==
    [ effects |->
        CommitAll({ i \in DOMAIN env.objects :
                      LET old == Project(env.objects[i], env.now)
                      IN  /\ \E rf \in req.tasks :
                                rf.id = i /\ rf.version = old.task.version
                          /\ old.task.state = "acquired"
                          /\ old.task.pid = req.pid
                          /\ old.promise.state = "pending" },
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
             /\ LET p == objs[d.id].promise
                IN  p.state /= "pending" \/ p.timeoutAt <= now
             /\ d.address \in objs[d.id].promise.listeners
      [] VariantTag(ev) = "CallbackDrain" ->
             LET d == VariantGetUnsafe("CallbackDrain", ev) IN
             /\ d.id \in DOMAIN objs
             /\ LET p == objs[d.id].promise
                IN  p.state /= "pending" \/ p.timeoutAt <= now
             /\ d.awaiter \in objs[d.id].promise.callbacks
      [] OTHER -> FALSE

(* @type: $event => Bool; *)
Fires(ev) ==
    FiresIn(objects, ev)

-----------------------------------------------------------------------------

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

(* ONE INTERNAL EVENT, APPLIED TO A STATE. An event not enabled HERE does
   nothing, which is what lets a batch be any sequence at all: a drain
   that is not due yet simply does not fire, so no ordering constraint is
   needed and every order is meaningful.
   @type: ({ objects: $id -> $object, outbox: Set($outEntry) }, $event)
              => { objects: $id -> $object, outbox: Set($outEntry) }; *)
Advance(st, ev) ==
    IF ~FiresIn(st.objects, ev) THEN
        st
    ELSE
        LET out == Handle(ev, [ objects |-> st.objects,
                                now     |-> now,
                                config  |-> [retryTimeout |-> RetryTimeout] ])
        IN  [ objects |-> PutsInto(st.objects, out.effects),
              outbox  |-> SaysInto(st.outbox, out.effects) ]

(* @type: $outcome => Bool; *)
Apply(out) ==
    /\ objects' = PutsInto(objects, out.effects)
    /\ outbox'  = SaysInto(outbox, out.effects)
    /\ UNCHANGED now

(* A REQUEST, WITH ANY AMOUNT OF DRAINING FOLDED IN. An executor that
   sweeps a document on access does the drains and the request at one
   instant, so the abstract step has to admit that shape or the
   refinement fails for a reason about scheduling rather than protocol.
   `n = 0` is the plain request, which is what this used to be. *)
External(ev) ==
    /\ ev \in ExternalEvent
    /\ \E n \in 0..MaxBatch :
           \E s \in [1 .. n -> InternalEvent] :
               LET mid == ApaFoldSeqLeft(Advance,
                                         [objects |-> objects, outbox |-> outbox],
                                         s)
                   out == Handle(ev, [ objects |-> mid.objects,
                                       now     |-> now,
                                       config  |-> [retryTimeout |-> RetryTimeout] ])
               IN  /\ objects' = PutsInto(mid.objects, out.effects)
                   /\ outbox'  = SaysInto(mid.outbox, out.effects)
                   /\ UNCHANGED now

(* @type: $event => Bool; *)
Internal(ev) ==
    /\ ev \in InternalEvent
    /\ Fires(ev)
    /\ Apply(Handle(ev, Env))

Batch ==
    \E n \in 1..MaxBatch :
        \E s \in [1 .. n -> InternalEvent] :
            LET final == ApaFoldSeqLeft(Advance,
                                        [objects |-> objects, outbox |-> outbox],
                                        s)
            IN  /\ \/ final.objects /= objects
                   \/ final.outbox  /= outbox
                /\ objects' = final.objects
                /\ outbox'  = final.outbox
                /\ UNCHANGED now

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
    \/ Batch
    \/ Clock

Fairness ==
    /\ \A ev \in InternalEvent : WF_vars(Internal(ev))
    /\ WF_vars(Clock)

Spec ==
    Init /\ [][Next]_vars /\ Fairness

Safety ==
    Init /\ [][Next]_vars

-----------------------------------------------------------------------------

TypeOK ==
    /\ now \in Time
    /\ DOMAIN objects \subseteq Id
        /\ \A a, b \in outbox : MsgKey(a) = MsgKey(b) => a = b
        /\ \A o \in DOMAIN objects :
           objects[o].task.state = "none" => objects[o].task = NoTask

well_formed_promise_created_at_lte_timeout_at ==
    \A i \in DOMAIN objects : objects[i].promise.createdAt <= objects[i].promise.timeoutAt

well_formed_promise_settled_at_iff_not_pending ==
    \A i \in DOMAIN objects :
        (objects[i].promise.state /= "pending") <=> (objects[i].promise.settledAt /= NoTime)

well_formed_promise_pending_has_no_value ==
    \A i \in DOMAIN objects :
        objects[i].promise.state = "pending" => objects[i].promise.value = NoValue

well_formed_promise_settled_at_lte_timeout_at ==
    \A i \in DOMAIN objects :
        objects[i].promise.settledAt /= NoTime => objects[i].promise.settledAt <= objects[i].promise.timeoutAt

well_formed_task_pending_iff_has_retry_at ==
    \A i \in DOMAIN objects :
        objects[i].task.state /= "none" => ((objects[i].task.state = "pending") <=> (objects[i].task.retryAt /= NoTime))

well_formed_task_acquired_iff_has_expires_at ==
    \A i \in DOMAIN objects :
        objects[i].task.state /= "none" => ((objects[i].task.state = "acquired") <=> (objects[i].task.expiresAt /= NoTime))

consistent_settled_promise_has_fulfilled_task ==
    \A i \in DOMAIN objects :
        (objects[i].promise.state /= "pending" /\ objects[i].task.state /= "none") => objects[i].task.state = "fulfilled"

consistent_settled_task_promise_settled ==
    \A i \in DOMAIN objects :
        (objects[i].task.state /= "none" /\ objects[i].task.state = "fulfilled") => objects[i].promise.state /= "pending"

consistent_task_iff_targeted_promise ==
    \A i \in DOMAIN objects : objects[i].task.state /= "none" <=> objects[i].promise.tags.targeted

preserved_settled_promise_record ==
    \A i \in DOMAIN objects :
        \/ objects[i].promise.state = "pending"
        \/ /\ i \in DOMAIN objects'
           /\ objects'[i].promise.state     = objects[i].promise.state
           /\ objects'[i].promise.settledAt = objects[i].promise.settledAt
           /\ objects'[i].promise.value     = objects[i].promise.value

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
        \/ objects[i].promise.state /= "pending"
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
                 /\ q.value = objects[i].promise.value

consistent_settlement_fulfils_task ==
    \A i \in DOMAIN objects :
        \/ objects[i].promise.state /= "pending"
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

T_preserved_settled_promise_record ==
    [][preserved_settled_promise_record]_vars
T_consistent_new_promise_born_clean ==
    [][consistent_new_promise_born_clean]_vars
T_consistent_promise_settlement_stamp ==
    [][consistent_promise_settlement_stamp]_vars
T_consistent_settlement_fulfils_task ==
    [][consistent_settlement_fulfils_task]_vars

=============================================================================
