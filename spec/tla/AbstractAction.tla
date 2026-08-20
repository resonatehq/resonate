----------------------------- MODULE AbstractAction -----------------------------
EXTENDS Requests, TLC

VARIABLES
    objects,    \* partial: DOMAIN is what exists
    outbox,     \* at most one entry per MsgKey
    now         \* the clock. NOT part of the store: a server does not own

vars ==
    <<objects, outbox, now>>

(* @typeAlias: state = { objects: $id -> $object, outbox: Set($outEntry), now: Int };
   @type: $state; *)
State ==
    [objects |-> objects, outbox |-> outbox, now |-> now]

-----------------------------------------------------------------------------

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

-----------------------------------------------------------------------------

HandlePromiseCreate(pre, req) ==
    /\ req.id \notin DOMAIN pre.objects
    /\ objects' = (req.id :> New(req, now)) @@ pre.objects
    /\ outbox' = pre.outbox
    /\ UNCHANGED now

HandlePromiseSettle(pre, req) ==
    /\ req.id \in DOMAIN pre.objects
    /\ LET old == Project(pre.objects[req.id], now)
       IN
           /\ old.promise.state = "pending"
           /\ objects' =
                  (req.id :>
                      [ promise |-> [old.promise EXCEPT !.state     = req.state,
                                                        !.value     = req.value,
                                                        !.settledAt = now],
                        task    |-> [old.task EXCEPT
                                        !.state     = IF @ = "none" THEN "none"
                                                      ELSE "fulfilled",
                                        !.pid       = NoPid,
                                        !.ttl       = NoTime,
                                        !.expiresAt = NoTime,
                                        !.retryAt   = NoTime,
                                        !.resumes   = {}] ]) @@ pre.objects
    /\ outbox' = pre.outbox
    /\ UNCHANGED now

HandlePromiseRegisterCallback(pre, req) ==
    /\ req.awaited \in DOMAIN pre.objects
    /\ req.awaiter \in DOMAIN pre.objects
    /\ LET awaited == Project(pre.objects[req.awaited], now)
           awaiter == Project(pre.objects[req.awaiter], now)
       IN
           /\ awaiter.promise.tags.targeted
           /\ IsExternal(awaited.promise)
           /\ awaited.promise.state = "pending"
           /\ awaiter.promise.state = "pending"
           /\ objects' =
                  (req.awaited :>
                      [awaited EXCEPT !.promise.callbacks = @ \cup {req.awaiter}])
                          @@ pre.objects
    /\ outbox' = pre.outbox
    /\ UNCHANGED now

HandlePromiseRegisterListener(pre, req) ==
    /\ req.awaited \in DOMAIN pre.objects
    /\ LET old == Project(pre.objects[req.awaited], now)
       IN
           /\ IsExternal(old.promise)
           /\ old.promise.state = "pending"
           /\ objects' =
                  (req.awaited :>
                      [old EXCEPT !.promise.listeners = @ \cup {req.address}])
                          @@ pre.objects
    /\ outbox' = pre.outbox
    /\ UNCHANGED now

-----------------------------------------------------------------------------

HandleTaskCreate(pre, req) ==
    /\ req.action.tags.targeted
    /\ \/ /\ req.action.id \notin DOMAIN pre.objects
          /\ LET born == New(req.action, now)
             IN
                 objects' =
                     (req.action.id :>
                         IF born.promise.state = "pending" THEN
                             [born EXCEPT !.task.state     = "acquired",
                                          !.task.version   = @ + 1,
                                          !.task.ttl       = req.ttl,
                                          !.task.pid       = req.pid,
                                          !.task.expiresAt = now + req.ttl,
                                          !.task.retryAt   = NoTime,
                                          !.task.resumes   = {}]
                         ELSE
                             born) @@ pre.objects
       \/ /\ req.action.id \in DOMAIN pre.objects
          /\ LET old == Project(pre.objects[req.action.id], now)
             IN
                 /\ old.promise.tags.targeted
                 /\ old.task.state = "pending"
                 /\ objects' =
                        (req.action.id :>
                            [old EXCEPT !.task.state     = "acquired",
                                        !.task.version   = @ + 1,
                                        !.task.ttl       = req.ttl,
                                        !.task.pid       = req.pid,
                                        !.task.expiresAt = now + req.ttl,
                                        !.task.retryAt   = NoTime,
                                        !.task.resumes   = {}]) @@ pre.objects
    /\ outbox' = pre.outbox
    /\ UNCHANGED now

HandleTaskAcquire(pre, req) ==
    /\ req.id \in DOMAIN pre.objects
    /\ LET old == Project(pre.objects[req.id], now)
       IN
           /\ old.task.state = "pending"
           /\ old.promise.state = "pending"
           /\ old.task.version = req.version
           /\ objects' =
                  (req.id :>
                      [old EXCEPT !.task.state     = "acquired",
                                  !.task.version   = @ + 1,
                                  !.task.ttl       = req.ttl,
                                  !.task.pid       = req.pid,
                                  !.task.expiresAt = now + req.ttl,
                                  !.task.retryAt   = NoTime,
                                  !.task.resumes   = {}]) @@ pre.objects
    /\ outbox' = pre.outbox
    /\ UNCHANGED now

HandleTaskFence(pre, i, v) ==
    /\ i \in DOMAIN pre.objects
    /\ LET old == Project(pre.objects[i], now)
       IN
           /\ old.task.state = "acquired"
           /\ old.promise.state = "pending"
           /\ old.task.version = v
           /\ \/ \E req \in CreateReq : req.id /= i /\ HandlePromiseCreate(pre, req)
              \/ \E req \in SettleReq : req.id /= i /\ HandlePromiseSettle(pre, req)

HandleTaskHeartbeat(pre, req) ==
    LET beat == { i \in DOMAIN pre.objects :
                    LET old == Project(pre.objects[i], now)
                    IN  /\ \E rf \in req.tasks :
                              rf.id = i /\ rf.version = old.task.version
                        /\ old.task.state = "acquired"
                        /\ old.task.pid = req.pid
                        /\ old.promise.state = "pending" }
    IN
        /\ beat /= {}
        /\ objects' =
               ApaFoldSet(LAMBDA acc, i :
                              LET old == Project(pre.objects[i], now)
                              IN  (i :> [old EXCEPT !.task.expiresAt =
                                                        now + old.task.ttl]) @@ acc,
                          objects, beat)
        /\ outbox' = pre.outbox
        /\ UNCHANGED now

HandleTaskSuspend(pre, req) ==
    LET aw == { a.awaited : a \in req.actions }
    IN
        /\ req.actions /= {}
        /\ req.id \notin aw
        /\ \A a \in aw : a.origin = req.id.origin
        /\ req.id \in DOMAIN pre.objects
        /\ \A a \in aw : a \in DOMAIN pre.objects
        /\ LET old == Project(pre.objects[req.id], now)
           IN
               /\ old.task.state = "acquired"
               /\ old.promise.state = "pending"
               /\ old.task.version = req.version
               /\ \A a \in aw : IsExternal(Project(pre.objects[a], now).promise)
               /\ IF \E a \in aw :
                        Project(pre.objects[a], now).promise.state /= "pending" THEN
                      objects' =
                          (req.id :> [old EXCEPT !.task.resumes = {}]) @@ pre.objects
                  ELSE
                      objects' =
                          ApaFoldSet(LAMBDA acc, a :
                                         (a :> [Project(pre.objects[a], now) EXCEPT
                                                   !.promise.callbacks =
                                                       @ \cup {req.id}]) @@ acc,
                                     (req.id :>
                                         [old EXCEPT !.task.state     = "suspended",
                                                     !.task.pid       = NoPid,
                                                     !.task.ttl       = NoTime,
                                                     !.task.expiresAt = NoTime,
                                                     !.task.retryAt   = NoTime,
                                                     !.task.resumes   = {}]) @@ pre.pre.objects,
                                     aw)
        /\ outbox' = pre.outbox
        /\ UNCHANGED now

HandleTaskFulfill(pre, req) ==
    /\ req.id \in DOMAIN pre.objects
    /\ LET old == Project(pre.objects[req.id], now)
       IN
           /\ old.task.state = "acquired"
           /\ old.promise.state = "pending"
           /\ old.task.version = req.version
           /\ objects' =
                  (req.id :>
                      [ promise |-> [old.promise EXCEPT
                                        !.state     = req.action.state,
                                        !.value     = req.action.value,
                                        !.settledAt = now],
                        task    |-> [old.task EXCEPT !.state     = "fulfilled",
                                                     !.pid       = NoPid,
                                                     !.ttl       = NoTime,
                                                     !.expiresAt = NoTime,
                                                     !.retryAt   = NoTime,
                                                     !.resumes   = {}] ]) @@ pre.objects
    /\ outbox' = pre.outbox
    /\ UNCHANGED now

HandleTaskRelease(pre, req) ==
    /\ req.id \in DOMAIN pre.objects
    /\ LET old == Project(pre.objects[req.id], now)
       IN
           /\ old.task.state = "acquired"
           /\ old.promise.state = "pending"
           /\ old.task.version = req.version
           /\ objects' =
                  (req.id :>
                      [old EXCEPT !.task.state     = "pending",
                                  !.task.pid       = NoPid,
                                  !.task.ttl       = NoTime,
                                  !.task.expiresAt = NoTime,
                                  !.task.retryAt   = now]) @@ pre.objects
    /\ outbox' = pre.outbox
    /\ UNCHANGED now

HandleTaskHalt(pre, req) ==
    /\ req.id \in DOMAIN pre.objects
    /\ LET old == Project(pre.objects[req.id], now)
       IN
           /\ old.task.state \notin {"none", "fulfilled", "halted"}
           /\ objects' =
                  (req.id :>
                      [old EXCEPT !.task.state     = "halted",
                                  !.task.pid       = NoPid,
                                  !.task.ttl       = NoTime,
                                  !.task.expiresAt = NoTime,
                                  !.task.retryAt   = NoTime]) @@ pre.objects
    /\ outbox' = pre.outbox
    /\ UNCHANGED now

HandleTaskContinue(pre, req) ==
    /\ req.id \in DOMAIN pre.objects
    /\ LET old == Project(pre.objects[req.id], now)
       IN
           /\ old.task.state = "halted"
           /\ old.promise.state = "pending"
           /\ objects' =
                  (req.id :>
                      [old EXCEPT !.task.state     = "pending",
                                  !.task.pid       = NoPid,
                                  !.task.ttl       = NoTime,
                                  !.task.expiresAt = NoTime,
                                  !.task.retryAt   = now]) @@ pre.objects
    /\ outbox' = pre.outbox
    /\ UNCHANGED now

-----------------------------------------------------------------------------

-----------------------------------------------------------------------------

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
-----------------------------------------------------------------------------

Clock ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<objects, outbox>>

-----------------------------------------------------------------------------

Init ==
    /\ objects = SetAsFun({})
    /\ outbox  = {}
    /\ now     = 0

Sweeps(Body(_)) ==
    \E TS \in SUBSET ReadyTimeouts(objects, now),
       LS \in SUBSET ReadyListeners(objects, now),
       CS \in SUBSET ReadyCallbacks(objects, now) :
       Body(Drain(State, TS, LS, CS))

Internal ==
    Sweeps(LAMBDA pre :
               /\ pre /= State
               /\ objects' = pre.objects
               /\ outbox'  = pre.outbox
               /\ UNCHANGED now)

Next ==
    \/ Sweeps(LAMBDA pre :
                  \/ \E req \in CreateReq   : HandlePromiseCreate(pre, req)
                  \/ \E req \in SettleReq   : HandlePromiseSettle(pre, req)
                  \/ \E req \in CallbackReq : HandlePromiseRegisterCallback(pre, req)
                  \/ \E req \in [awaited : Id, address : Address] :
                         HandlePromiseRegisterListener(pre, req)
                  \/ \E req \in [pid : Pid, ttl : Ttl, action : CreateReq] :
                         HandleTaskCreate(pre, req)
                  \/ \E req \in [id : Id, version : Version, pid : Pid, ttl : Ttl] :
                         HandleTaskAcquire(pre, req)
                  \/ \E i \in Id, v \in Version : HandleTaskFence(pre, i, v)
                  \/ \E req \in [pid : Pid, tasks : SUBSET TaskRefT] :
                         HandleTaskHeartbeat(pre, req)
                  \/ \E req \in [id : Id, version : Version,
                                 actions : SUBSET CallbackReq] :
                         HandleTaskSuspend(pre, req)
                  \/ \E req \in [id : Id, version : Version, action : SettleReq] :
                         HandleTaskFulfill(pre, req)
                  \/ \E req \in TaskRefT : HandleTaskRelease(pre, req)
                  \/ \E req \in [id : Id] : HandleTaskHalt(pre, req)
                  \/ \E req \in [id : Id] : HandleTaskContinue(pre, req))
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
