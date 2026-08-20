----------------------------- MODULE AbstractAction -----------------------------
EXTENDS Requests, TLC

VARIABLES
    objects,    \* partial: DOMAIN is what exists
    outbox,     \* at most one entry per MsgKey
    now         \* the clock. NOT part of the store: a server does not own

vars ==
    <<objects, outbox, now>>

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

HandlePromiseGet(req) ==
    FALSE

HandlePromiseCreate(req) ==
    /\ req.id \notin DOMAIN objects
    /\ objects' = (req.id :> New(req, now)) @@ objects
    /\ UNCHANGED <<outbox, now>>

HandlePromiseSettle(req) ==
    /\ req.id \in DOMAIN objects
    /\ LET old == Project(objects[req.id], now)
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
                                        !.resumes   = {}] ]) @@ objects
    /\ UNCHANGED <<outbox, now>>

HandlePromiseRegisterCallback(req) ==
    /\ req.awaited \in DOMAIN objects
    /\ req.awaiter \in DOMAIN objects
    /\ LET awaited == Project(objects[req.awaited], now)
           awaiter == Project(objects[req.awaiter], now)
       IN
           /\ awaiter.promise.tags.targeted
           /\ IsExternal(awaited.promise)
           /\ awaited.promise.state = "pending"
           /\ awaiter.promise.state = "pending"
           /\ objects' =
                  (req.awaited :>
                      [awaited EXCEPT !.promise.callbacks = @ \cup {req.awaiter}])
                          @@ objects
    /\ UNCHANGED <<outbox, now>>

HandlePromiseRegisterListener(req) ==
    /\ req.awaited \in DOMAIN objects
    /\ LET old == Project(objects[req.awaited], now)
       IN
           /\ IsExternal(old.promise)
           /\ old.promise.state = "pending"
           /\ objects' =
                  (req.awaited :>
                      [old EXCEPT !.promise.listeners = @ \cup {req.address}])
                          @@ objects
    /\ UNCHANGED <<outbox, now>>

-----------------------------------------------------------------------------

HandleTaskGet(req) ==
    FALSE

HandleTaskCreate(req) ==
    /\ req.action.tags.targeted
    /\ \/ /\ req.action.id \notin DOMAIN objects
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
                             born) @@ objects
       \/ /\ req.action.id \in DOMAIN objects
          /\ LET old == Project(objects[req.action.id], now)
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
                                        !.task.resumes   = {}]) @@ objects
    /\ UNCHANGED <<outbox, now>>

HandleTaskAcquire(req) ==
    /\ req.id \in DOMAIN objects
    /\ LET old == Project(objects[req.id], now)
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
                                  !.task.resumes   = {}]) @@ objects
    /\ UNCHANGED <<outbox, now>>

HandleTaskFence(req) ==
    /\ req.id \in DOMAIN objects
    /\ LET old == Project(objects[req.id], now)
       IN
           /\ (IF VariantTag(req.action) = "Create"
               THEN VariantGetUnsafe("Create", req.action).req.id
               ELSE VariantGetUnsafe("Settle", req.action).req.id) /= req.id
           /\ old.task.state = "acquired"
           /\ old.promise.state = "pending"
           /\ old.task.version = req.version
           /\ IF VariantTag(req.action) = "Create" THEN
                  HandlePromiseCreate(VariantGetUnsafe("Create", req.action).req)
              ELSE
                  HandlePromiseSettle(VariantGetUnsafe("Settle", req.action).req)

HandleTaskHeartbeat(req) ==
    LET beat == { i \in DOMAIN objects :
                    LET old == Project(objects[i], now)
                    IN  /\ \E rf \in req.tasks :
                              rf.id = i /\ rf.version = old.task.version
                        /\ old.task.state = "acquired"
                        /\ old.task.pid = req.pid
                        /\ old.promise.state = "pending" }
    IN
        /\ beat /= {}
        /\ objects' =
               ApaFoldSet(LAMBDA acc, i :
                              LET old == Project(objects[i], now)
                              IN  (i :> [old EXCEPT !.task.expiresAt =
                                                        now + old.task.ttl]) @@ acc,
                          objects, beat)
        /\ UNCHANGED <<outbox, now>>

HandleTaskSuspend(req) ==
    LET aw == { a.awaited : a \in req.actions }
    IN
        /\ req.actions /= {}
        /\ req.id \notin aw
        /\ \A a \in aw : a.origin = req.id.origin
        /\ req.id \in DOMAIN objects
        /\ \A a \in aw : a \in DOMAIN objects
        /\ LET old == Project(objects[req.id], now)
           IN
               /\ old.task.state = "acquired"
               /\ old.promise.state = "pending"
               /\ old.task.version = req.version
               /\ \A a \in aw : IsExternal(Project(objects[a], now).promise)
               /\ IF \E a \in aw :
                        Project(objects[a], now).promise.state /= "pending" THEN
                      objects' =
                          (req.id :> [old EXCEPT !.task.resumes = {}]) @@ objects
                  ELSE
                      objects' =
                          ApaFoldSet(LAMBDA acc, a :
                                         (a :> [Project(objects[a], now) EXCEPT
                                                   !.promise.callbacks =
                                                       @ \cup {req.id}]) @@ acc,
                                     (req.id :>
                                         [old EXCEPT !.task.state     = "suspended",
                                                     !.task.pid       = NoPid,
                                                     !.task.ttl       = NoTime,
                                                     !.task.expiresAt = NoTime,
                                                     !.task.retryAt   = NoTime,
                                                     !.task.resumes   = {}]) @@ objects,
                                     aw)
        /\ UNCHANGED <<outbox, now>>

HandleTaskFulfill(req) ==
    /\ req.id \in DOMAIN objects
    /\ LET old == Project(objects[req.id], now)
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
                                                     !.resumes   = {}] ]) @@ objects
    /\ UNCHANGED <<outbox, now>>

HandleTaskRelease(req) ==
    /\ req.id \in DOMAIN objects
    /\ LET old == Project(objects[req.id], now)
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
                                  !.task.retryAt   = now]) @@ objects
    /\ UNCHANGED <<outbox, now>>

HandleTaskHalt(req) ==
    /\ req.id \in DOMAIN objects
    /\ LET old == Project(objects[req.id], now)
       IN
           /\ old.task.state \notin {"none", "fulfilled", "halted"}
           /\ objects' =
                  (req.id :>
                      [old EXCEPT !.task.state     = "halted",
                                  !.task.pid       = NoPid,
                                  !.task.ttl       = NoTime,
                                  !.task.expiresAt = NoTime,
                                  !.task.retryAt   = NoTime]) @@ objects
    /\ UNCHANGED <<outbox, now>>

HandleTaskContinue(req) ==
    /\ req.id \in DOMAIN objects
    /\ LET old == Project(objects[req.id], now)
       IN
           /\ old.task.state = "halted"
           /\ old.promise.state = "pending"
           /\ objects' =
                  (req.id :>
                      [old EXCEPT !.task.state     = "pending",
                                  !.task.pid       = NoPid,
                                  !.task.ttl       = NoTime,
                                  !.task.expiresAt = NoTime,
                                  !.task.retryAt   = now]) @@ objects
    /\ UNCHANGED <<outbox, now>>

-----------------------------------------------------------------------------

ProcessPromiseTimeout ==
    \E i \in DOMAIN objects :
        /\ objects[i].promise.state = "pending"
        /\ objects[i].promise.timeoutAt <= now
        /\ objects' = (i :> Project(objects[i], now)) @@ objects
        /\ UNCHANGED <<outbox, now>>

ProcessLeaseTimeout ==
    \E i \in DOMAIN objects :
        LET old == objects[i]
        IN
            /\ old.task.state = "acquired"
            /\ old.task.expiresAt /= NoTime
            /\ old.task.expiresAt <= now
            /\ Project(old, now).promise.state = "pending"
            /\ objects' =
                   (i :> [old EXCEPT !.task.state     = "pending",
                                     !.task.pid       = NoPid,
                                     !.task.ttl       = NoTime,
                                     !.task.expiresAt = NoTime,
                                     !.task.retryAt   = now]) @@ objects
            /\ UNCHANGED <<outbox, now>>

ProcessRetryTimeout ==
    \E i \in DOMAIN objects :
        LET old == objects[i]
            msg == [ address |-> objects[i].promise.tags.target,
                     message |-> Variant("Execute",
                                         [id      |-> i,
                                          version |-> objects[i].task.version]) ]
        IN
            /\ old.task.state = "pending"
            /\ old.task.retryAt /= NoTime
            /\ old.task.retryAt <= now
            /\ Project(old, now).promise.state = "pending"
            /\ objects' =
                   (i :> [old EXCEPT !.task.retryAt = now + RetryTimeout]) @@ objects
            /\ outbox' = { o \in outbox : MsgKey(o) /= MsgKey(msg) } \cup {msg}
            /\ UNCHANGED now

ProcessListener ==
    \E i \in DOMAIN objects :
        \E a \in objects[i].promise.listeners :
            LET awaited == Project(objects[i], now)
                msg     == [ address |-> a,
                             message |-> Variant("Unblock",
                                                 [id    |-> i,
                                                  state |-> Project(objects[i],
                                                                    now).promise.state]) ]
            IN
                /\ awaited.promise.state /= "pending"
                /\ objects' =
                       (i :> [awaited EXCEPT !.promise.listeners = @ \ {a}]) @@ objects
                /\ outbox' = { o \in outbox : MsgKey(o) /= MsgKey(msg) } \cup {msg}
                /\ UNCHANGED now

ProcessCallback ==
    \E i \in DOMAIN objects :
        \E w \in objects[i].promise.callbacks :
            LET awaited    == Project(objects[i], now)
                newAwaited == [Project(objects[i], now) EXCEPT
                                  !.promise.callbacks = @ \ {w}]
            IN
                /\ awaited.promise.state /= "pending"
                /\ \/ /\ w \notin DOMAIN objects
                      /\ objects' = (i :> newAwaited) @@ objects
                   \/ /\ w \in DOMAIN objects
                      /\ LET awaiter == Project(objects[w], now)
                         IN
                             IF awaiter.task.state \in {"none", "fulfilled"} THEN
                                 objects' = (i :> newAwaited) @@ objects
                             ELSE
                                 objects' =
                                     (i :> newAwaited)
                                  @@ (w :>
                                         IF awaiter.task.state = "suspended" THEN
                                             [awaiter EXCEPT
                                                 !.task.state     = "pending",
                                                 !.task.pid       = NoPid,
                                                 !.task.ttl       = NoTime,
                                                 !.task.expiresAt = NoTime,
                                                 !.task.retryAt   = now,
                                                 !.task.resumes   = {i}]
                                         ELSE
                                             [awaiter EXCEPT
                                                 !.task.resumes = @ \cup {i}])
                                  @@ objects
                /\ UNCHANGED <<outbox, now>>

-----------------------------------------------------------------------------

Handle(ev) ==
    LET p(tag) == VariantGetUnsafe(tag, ev)
    IN
    CASE VariantTag(ev) = "PromiseGet" ->
             HandlePromiseGet(p("PromiseGet"))
      [] VariantTag(ev) = "PromiseCreate" ->
             HandlePromiseCreate(p("PromiseCreate").req)
      [] VariantTag(ev) = "PromiseSettle" ->
             HandlePromiseSettle(p("PromiseSettle").req)
      [] VariantTag(ev) = "PromiseRegisterCallback" ->
             HandlePromiseRegisterCallback(p("PromiseRegisterCallback").req)
      [] VariantTag(ev) = "PromiseRegisterListener" ->
             HandlePromiseRegisterListener(p("PromiseRegisterListener"))
      [] VariantTag(ev) = "TaskGet" ->
             HandleTaskGet(p("TaskGet"))
      [] VariantTag(ev) = "TaskCreate" ->
             HandleTaskCreate(p("TaskCreate"))
      [] VariantTag(ev) = "TaskAcquire" ->
             HandleTaskAcquire(p("TaskAcquire"))
      [] VariantTag(ev) = "TaskFence" ->
             HandleTaskFence(p("TaskFence"))
      [] VariantTag(ev) = "TaskHeartbeat" ->
             HandleTaskHeartbeat(p("TaskHeartbeat"))
      [] VariantTag(ev) = "TaskSuspend" ->
             HandleTaskSuspend(p("TaskSuspend"))
      [] VariantTag(ev) = "TaskFulfill" ->
             HandleTaskFulfill(p("TaskFulfill"))
      [] VariantTag(ev) = "TaskRelease" ->
             HandleTaskRelease(p("TaskRelease"))
      [] VariantTag(ev) = "TaskHalt" ->
             HandleTaskHalt(p("TaskHalt"))
      [] VariantTag(ev) = "TaskContinue" ->
             HandleTaskContinue(p("TaskContinue"))
      [] OTHER -> FALSE

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
    \/ /\ \E ev \in ExternalEvent : Handle(ev)
       /\ \/ objects' /= objects
          \/ outbox'  /= outbox
    \/ ProcessPromiseTimeout
    \/ ProcessLeaseTimeout
    \/ ProcessRetryTimeout
    \/ ProcessListener
    \/ ProcessCallback
    \/ Clock

Fairness ==
    /\ WF_vars(ProcessPromiseTimeout)
    /\ WF_vars(ProcessLeaseTimeout)
    /\ WF_vars(ProcessRetryTimeout)
    /\ WF_vars(ProcessListener)
    /\ WF_vars(ProcessCallback)
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
