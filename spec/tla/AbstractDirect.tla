----------------------------- MODULE AbstractDirect -----------------------------
EXTENDS Requests, TLC

(*
  @typeAlias: state = { objects: $id -> $object, outbox: Set($outEntry), now: Int };
*)
DirectAliases ==
    TRUE

-----------------------------------------------------------------------------

VARIABLES
    objects,    \* partial: DOMAIN is what exists
    outbox,     \* at most one entry per MsgKey
    now         \* the clock. NOT part of the store: a server does not own

vars ==
    <<objects, outbox, now>>

(* @type: $state; *)
State ==
    [objects |-> objects, outbox |-> outbox, now |-> now]

-----------------------------------------------------------------------------

(* @type: ($state, Set($id), ($id) => $object) => $state; *)
CommitAll(st, S, Obj(_)) ==
    LET objs == ApaFoldSet(LAMBDA acc, i : (i :> Obj(i)) @@ acc, st.objects, S)
    IN
        [st EXCEPT !.objects = objs]

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
                        tags |-> req.tags,
                        timeoutAt |-> req.timeoutAt, createdAt |-> t,
                        settledAt |-> NoTime,
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

(* @type: ($state, $getReq) => $state; *)
HandlePromiseGet(st, req) ==
    st

(* @type: ($state, $createReq) => $state; *)
HandlePromiseCreate(st, req) ==
    IF req.id \in DOMAIN st.objects THEN
        st
    ELSE
        LET new == New(req, st.now)
        IN
            [st EXCEPT !.objects = (req.id :> new) @@ @]

(* @type: ($state, $settleReq) => $state; *)
HandlePromiseSettle(st, req) ==
    IF req.id \notin DOMAIN st.objects THEN
        st
    ELSE
        LET old == Project(st.objects[req.id], st.now)
            new == [ promise |-> [old.promise EXCEPT !.state     = req.state,
                                                     !.value     = req.value,
                                                     !.settledAt = st.now],
                     task    |-> [old.task EXCEPT !.state     = IF @ = "none" THEN "none"
                                                               ELSE "fulfilled",
                                                  !.pid       = NoPid,
                                                  !.ttl       = NoTime,
                                                  !.expiresAt = NoTime,
                                                  !.retryAt   = NoTime,
                                                  !.resumes   = {}] ]
        IN
            IF old.promise.state = "pending" THEN
                [st EXCEPT !.objects = (req.id :> new) @@ @]
            ELSE
                st

(* @type: ($state, $callbackReq) => $state; *)
HandlePromiseRegisterCallback(st, req) ==
    IF \/ req.awaited \notin DOMAIN st.objects
       \/ req.awaiter \notin DOMAIN st.objects THEN
        st
    ELSE
        LET awaited    == Project(st.objects[req.awaited], st.now)
            awaiter    == Project(st.objects[req.awaiter], st.now)
            newAwaited == [awaited EXCEPT !.promise.callbacks = @ \cup {req.awaiter}]
        IN
            IF \/ ~awaiter.promise.tags.targeted
               \/ ~IsExternal(awaited.promise)
               \/ awaited.promise.state /= "pending"
               \/ awaiter.promise.state /= "pending" THEN
                st
            ELSE
                [st EXCEPT !.objects = (req.awaited :> newAwaited) @@ @]

(* @type: ($state, $listenerReq) => $state; *)
HandlePromiseRegisterListener(st, req) ==
    IF req.awaited \notin DOMAIN st.objects THEN
        st
    ELSE
        LET old == Project(st.objects[req.awaited], st.now)
            new == [old EXCEPT !.promise.listeners = @ \cup {req.address}]
        IN
            IF \/ ~IsExternal(old.promise)
               \/ old.promise.state /= "pending" THEN
                st
            ELSE
                [st EXCEPT !.objects = (req.awaited :> new) @@ @]

(* @type: ($state, $getReq) => $state; *)
HandleTaskGet(st, req) ==
    st

(* @type: ($state, $taskCreateReq) => $state; *)
HandleTaskCreate(st, req) ==
    IF ~req.action.tags.targeted THEN
        st
    ELSE IF req.action.id \notin DOMAIN st.objects THEN
        LET born == New(req.action, st.now)
            new  == IF born.promise.state = "pending" THEN
                        [born EXCEPT !.task.state     = "acquired",
                                     !.task.version   = @ + 1,
                                     !.task.ttl       = req.ttl,
                                     !.task.pid       = req.pid,
                                     !.task.expiresAt = st.now + req.ttl,
                                     !.task.retryAt   = NoTime,
                                     !.task.resumes   = {}]
                    ELSE
                        born
        IN
            [st EXCEPT !.objects = (req.action.id :> new) @@ @]
    ELSE
        LET old == Project(st.objects[req.action.id], st.now)
            new == [old EXCEPT !.task.state     = "acquired",
                               !.task.version   = @ + 1,
                               !.task.ttl       = req.ttl,
                               !.task.pid       = req.pid,
                               !.task.expiresAt = st.now + req.ttl,
                               !.task.retryAt   = NoTime,
                               !.task.resumes   = {}]
        IN
            IF \/ ~old.promise.tags.targeted
               \/ old.task.state /= "pending" THEN
                st
            ELSE
                [st EXCEPT !.objects = (req.action.id :> new) @@ @]

(* @type: ($state, $taskAcquireReq) => $state; *)
HandleTaskAcquire(st, req) ==
    IF req.id \notin DOMAIN st.objects THEN
        st
    ELSE
        LET old == Project(st.objects[req.id], st.now)
            new == [old EXCEPT !.task.state     = "acquired",
                               !.task.version   = @ + 1,
                               !.task.ttl       = req.ttl,
                               !.task.pid       = req.pid,
                               !.task.expiresAt = st.now + req.ttl,
                               !.task.retryAt   = NoTime,
                               !.task.resumes   = {}]
        IN
            IF \/ old.task.state /= "pending"
               \/ old.promise.state /= "pending"
               \/ old.task.version /= req.version THEN
                st
            ELSE
                [st EXCEPT !.objects = (req.id :> new) @@ @]

(* @type: ($state, $taskFenceReq) => $state; *)
HandleTaskFence(st, req) ==
    IF req.id \notin DOMAIN st.objects THEN
        st
    ELSE
        LET old == Project(st.objects[req.id], st.now)
        IN
            IF \/ (IF VariantTag(req.action) = "Create"
                   THEN VariantGetUnsafe("Create", req.action).req.id
                   ELSE VariantGetUnsafe("Settle", req.action).req.id) = req.id
               \/ old.task.state /= "acquired"
               \/ old.promise.state /= "pending"
               \/ old.task.version /= req.version THEN
                st
            ELSE IF VariantTag(req.action) = "Create" THEN
                HandlePromiseCreate(st, VariantGetUnsafe("Create", req.action).req)
            ELSE
                HandlePromiseSettle(st, VariantGetUnsafe("Settle", req.action).req)

(* @type: ($state, $taskBeatReq) => $state; *)
HandleTaskHeartbeat(st, req) ==
    CommitAll(st,
              { i \in DOMAIN st.objects :
                  LET old == Project(st.objects[i], st.now)
                  IN  /\ \E rf \in req.tasks :
                            rf.id = i /\ rf.version = old.task.version
                      /\ old.task.state = "acquired"
                      /\ old.task.pid = req.pid
                      /\ old.promise.state = "pending" },
              LAMBDA i :
                  LET old == Project(st.objects[i], st.now)
                  IN
                      [old EXCEPT !.task.expiresAt = st.now + old.task.ttl])

(* @type: ($state, $taskSuspendReq) => $state; *)
HandleTaskSuspend(st, req) ==
    LET aw   == { a.awaited : a \in req.actions }
        seen == { a \in aw : a \in DOMAIN st.objects }
    IN
        IF \/ req.actions = {}
           \/ req.id \in aw
           \/ \E a \in aw : a.origin /= req.id.origin
           \/ req.id \notin DOMAIN st.objects
           \/ seen /= aw THEN
            st
        ELSE
            LET old == Project(st.objects[req.id], st.now)
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
                        ~IsExternal(Project(st.objects[a], st.now).promise) THEN
                    st
                ELSE IF \E a \in aw :
                          Project(st.objects[a], st.now).promise.state /= "pending" THEN
                    [st EXCEPT !.objects =
                                   (req.id :> [old EXCEPT !.task.resumes = {}]) @@ @]
                ELSE
                    CommitAll([st EXCEPT !.objects = (req.id :> new) @@ @],
                              aw,
                              LAMBDA a :
                                  [Project(st.objects[a], st.now) EXCEPT
                                       !.promise.callbacks = @ \cup {req.id}])

(* @type: ($state, $taskFulfillReq) => $state; *)
HandleTaskFulfill(st, req) ==
    IF req.id \notin DOMAIN st.objects THEN
        st
    ELSE
        LET old == Project(st.objects[req.id], st.now)
            new == [ promise |-> [old.promise EXCEPT !.state     = req.action.state,
                                                     !.value     = req.action.value,
                                                     !.settledAt = st.now],
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
                st
            ELSE
                [st EXCEPT !.objects = (req.id :> new) @@ @]

(* @type: ($state, $taskRef) => $state; *)
HandleTaskRelease(st, req) ==
    IF req.id \notin DOMAIN st.objects THEN
        st
    ELSE
        LET old == Project(st.objects[req.id], st.now)
            new == [old EXCEPT !.task.state     = "pending",
                               !.task.pid       = NoPid,
                               !.task.ttl       = NoTime,
                               !.task.expiresAt = NoTime,
                               !.task.retryAt   = st.now]
        IN
            IF \/ old.task.state /= "acquired"
               \/ old.promise.state /= "pending"
               \/ old.task.version /= req.version THEN
                st
            ELSE
                [st EXCEPT !.objects = (req.id :> new) @@ @]

(* @type: ($state, $getReq) => $state; *)
HandleTaskHalt(st, req) ==
    IF req.id \notin DOMAIN st.objects THEN
        st
    ELSE
        LET old == Project(st.objects[req.id], st.now)
            new == [old EXCEPT !.task.state     = "halted",
                               !.task.pid       = NoPid,
                               !.task.ttl       = NoTime,
                               !.task.expiresAt = NoTime,
                               !.task.retryAt   = NoTime]
        IN
            IF \/ old.task.state = "none"
               \/ old.task.state \in {"fulfilled", "halted"} THEN
                st
            ELSE
                [st EXCEPT !.objects = (req.id :> new) @@ @]

(* @type: ($state, $getReq) => $state; *)
HandleTaskContinue(st, req) ==
    IF req.id \notin DOMAIN st.objects THEN
        st
    ELSE
        LET old == Project(st.objects[req.id], st.now)
            new == [old EXCEPT !.task.state     = "pending",
                               !.task.pid       = NoPid,
                               !.task.ttl       = NoTime,
                               !.task.expiresAt = NoTime,
                               !.task.retryAt   = st.now]
        IN
            IF \/ old.task.state /= "halted"
               \/ old.promise.state /= "pending" THEN
                st
            ELSE
                [st EXCEPT !.objects = (req.id :> new) @@ @]

(* @type: ($state, $id) => $state; *)
ProcessLeaseTimeout(st, i) ==
    IF i \notin DOMAIN st.objects THEN
        st
    ELSE
        LET old == st.objects[i]
            new == [old EXCEPT !.task.state     = "pending",
                               !.task.pid       = NoPid,
                               !.task.ttl       = NoTime,
                               !.task.expiresAt = NoTime,
                               !.task.retryAt   = st.now]
        IN
            IF \/ old.task.state /= "acquired"
               \/ old.task.expiresAt = NoTime
               \/ old.task.expiresAt > st.now
               \/ Project(old, st.now).promise.state /= "pending" THEN
                st
            ELSE
                [st EXCEPT !.objects = (i :> new) @@ @]

(* @type: ($state, $id) => $state; *)
ProcessRetryTimeout(st, i) ==
    IF i \notin DOMAIN st.objects THEN
        st
    ELSE
        LET old == st.objects[i]
            new == [old EXCEPT !.task.retryAt = st.now + RetryTimeout]
            msg == [ address |-> old.promise.tags.target,
                     message |-> Variant("Execute",
                                         [id      |-> i,
                                          version |-> old.task.version]) ]
        IN
            IF \/ old.task.state /= "pending"
               \/ old.task.retryAt = NoTime
               \/ old.task.retryAt > st.now
               \/ Project(old, st.now).promise.state /= "pending" THEN
                st
            ELSE
                [st EXCEPT !.objects = (i :> new) @@ @,
                           !.outbox  = { o \in @ : MsgKey(o) /= MsgKey(msg) }
                                           \cup {msg}]

(* @type: ($state, $listenerDrainReq) => $state; *)
ProcessListener(st, req) ==
    IF req.id \notin DOMAIN st.objects THEN
        st
    ELSE
        LET awaited    == Project(st.objects[req.id], st.now)
            newAwaited == [awaited EXCEPT !.promise.listeners = @ \ {req.address}]
            msg        == [ address |-> req.address,
                            message |-> Variant("Unblock",
                                                [id    |-> req.id,
                                                 state |-> awaited.promise.state]) ]
        IN
            IF \/ awaited.promise.state = "pending"
               \/ req.address \notin awaited.promise.listeners THEN
                st
            ELSE
                [st EXCEPT !.objects = (req.id :> newAwaited) @@ @,
                           !.outbox  = { o \in @ : MsgKey(o) /= MsgKey(msg) }
                                           \cup {msg}]

(* @type: ($state, $drainReq) => $state; *)
ProcessCallback(st, req) ==
    IF req.id \notin DOMAIN st.objects THEN
        st
    ELSE
        LET awaited    == Project(st.objects[req.id], st.now)
            newAwaited == [awaited EXCEPT !.promise.callbacks = @ \ {req.awaiter}]
        IN
            IF \/ awaited.promise.state = "pending"
               \/ req.awaiter \notin awaited.promise.callbacks THEN
                st
            ELSE IF req.awaiter \notin DOMAIN st.objects THEN
                [st EXCEPT !.objects = (req.id :> newAwaited) @@ @]
            ELSE
                LET awaiter    == Project(st.objects[req.awaiter], st.now)
                    newAwaiter == IF awaiter.task.state = "suspended" THEN
                                      [awaiter EXCEPT !.task.state     = "pending",
                                                      !.task.pid       = NoPid,
                                                      !.task.ttl       = NoTime,
                                                      !.task.expiresAt = NoTime,
                                                      !.task.retryAt   = st.now,
                                                      !.task.resumes   = {req.id}]
                                  ELSE
                                      [awaiter EXCEPT !.task.resumes = @ \cup {req.id}]
                IN
                    IF awaiter.task.state \in {"none", "fulfilled"} THEN
                        [st EXCEPT !.objects = (req.id :> newAwaited) @@ @]
                    ELSE
                        [st EXCEPT !.objects = (req.id      :> newAwaited)
                                            @@ (req.awaiter :> newAwaiter) @@ @]

(* @type: ($state, { id: $id, kind: Str }) => $state; *)
ProcessPromiseTimeout(st, req) ==
    IF req.kind /= "promise" \/ req.id \notin DOMAIN st.objects THEN
        st
    ELSE
        LET old == st.objects[req.id]
            new == Project(old, st.now)
        IN
            IF new /= old THEN
                [st EXCEPT !.objects = (req.id :> new) @@ @]
            ELSE
                st

(* @type: ($state, $event) => $state; *)
Handle(st, ev) ==
    LET p(tag) == VariantGetUnsafe(tag, ev)
    IN
    CASE VariantTag(ev) = "PromiseGet" ->
             HandlePromiseGet(st, p("PromiseGet"))
      [] VariantTag(ev) = "PromiseCreate" ->
             HandlePromiseCreate(st, p("PromiseCreate").req)
      [] VariantTag(ev) = "PromiseSettle" ->
             HandlePromiseSettle(st, p("PromiseSettle").req)
      [] VariantTag(ev) = "PromiseRegisterCallback" ->
             HandlePromiseRegisterCallback(st, p("PromiseRegisterCallback").req)
      [] VariantTag(ev) = "PromiseRegisterListener" ->
             HandlePromiseRegisterListener(st, p("PromiseRegisterListener"))
      [] VariantTag(ev) = "TaskGet" ->
             HandleTaskGet(st, p("TaskGet"))
      [] VariantTag(ev) = "TaskCreate" ->
             HandleTaskCreate(st, p("TaskCreate"))
      [] VariantTag(ev) = "TaskAcquire" ->
             HandleTaskAcquire(st, p("TaskAcquire"))
      [] VariantTag(ev) = "TaskFence" ->
             HandleTaskFence(st, p("TaskFence"))
      [] VariantTag(ev) = "TaskHeartbeat" ->
             HandleTaskHeartbeat(st, p("TaskHeartbeat"))
      [] VariantTag(ev) = "TaskSuspend" ->
             HandleTaskSuspend(st, p("TaskSuspend"))
      [] VariantTag(ev) = "TaskFulfill" ->
             HandleTaskFulfill(st, p("TaskFulfill"))
      [] VariantTag(ev) = "TaskRelease" ->
             HandleTaskRelease(st, p("TaskRelease"))
      [] VariantTag(ev) = "TaskHalt" ->
             HandleTaskHalt(st, p("TaskHalt"))
      [] VariantTag(ev) = "TaskContinue" ->
             HandleTaskContinue(st, p("TaskContinue"))
      [] VariantTag(ev) = "Timeout" ->
             LET d == p("Timeout")
             IN  CASE d.kind = "promise" -> ProcessPromiseTimeout(st, d)
                   [] d.kind = "lease"   -> ProcessLeaseTimeout(st, d.id)
                   [] OTHER              -> ProcessRetryTimeout(st, d.id)
      [] VariantTag(ev) = "ListenerDrain" ->
             ProcessListener(st, p("ListenerDrain"))
      [] VariantTag(ev) = "CallbackDrain" ->
             ProcessCallback(st, p("CallbackDrain"))
      [] OTHER -> st

-----------------------------------------------------------------------------

ReadyTimeouts(objs, t) ==
    { d \in [id : DOMAIN objs, kind : DeadlineKind] :
        /\ Deadline(objs[d.id], d.kind) /= NoTime
        /\ Deadline(objs[d.id], d.kind) <= t }

Settling(objs, t) ==
    { j \in DOMAIN objs : \/ objs[j].promise.state /= "pending"
                          \/ objs[j].promise.timeoutAt <= t }

ReadyListeners(objs, t) ==
    UNION { { [id |-> i, address |-> a] : a \in objs[i].promise.listeners }
              : i \in Settling(objs, t) }

ReadyCallbacks(objs, t) ==
    UNION { { [id |-> i, awaiter |-> w] : w \in objs[i].promise.callbacks }
              : i \in Settling(objs, t) }

Drain(st, TS, LS, CS) ==
    LET afterT == ApaFoldSet(LAMBDA acc, d :
                                 CASE d.kind = "promise" ->
                                          ProcessPromiseTimeout(acc, d)
                                   [] d.kind = "lease" ->
                                          ProcessLeaseTimeout(acc, d.id)
                                   [] OTHER ->
                                          ProcessRetryTimeout(acc, d.id),
                             st, TS)
        afterL == ApaFoldSet(LAMBDA acc, d : ProcessListener(acc, d), afterT, LS)
    IN
        ApaFoldSet(LAMBDA acc, d : ProcessCallback(acc, d), afterL, CS)

External(ev) ==
    /\ ev \in ExternalEvent
    /\ \E TS \in SUBSET ReadyTimeouts(objects, now),
          LS \in SUBSET ReadyListeners(objects, now),
          CS \in SUBSET ReadyCallbacks(objects, now) :
          LET final == Handle(Drain(State, TS, LS, CS), ev)
          IN
              /\ objects' = final.objects
              /\ outbox'  = final.outbox
              /\ UNCHANGED now

Internal ==
    \E TS \in SUBSET ReadyTimeouts(objects, now),
       LS \in SUBSET ReadyListeners(objects, now),
       CS \in SUBSET ReadyCallbacks(objects, now) :
       LET final == Drain(State, TS, LS, CS)
       IN
           /\ final /= State
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
    \/ Internal
    \/ Clock

Fairness ==
    /\ WF_vars(Internal)
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

================================================================

=============================================================================
