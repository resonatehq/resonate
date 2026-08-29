----------------------------- MODULE Abstract -----------------------------
EXTENDS Requests

VARIABLES
    objects,    \* partial: DOMAIN is what exists
    outbox,     \* at most one entry per MsgKey
    now         \* the clock. NOT part of the store: a server does not own

vars ==
    <<objects, outbox, now>>

-----------------------------------------------------------------------------

(* @type: ($object, Int) => $object; *)
New(req, t) ==
    IF req.timeoutAt > t THEN
        [ promise |-> [ state |-> "pending", param |-> req.param, value |-> NoValue,
                        tags |-> req.tags,
                        timeoutAt |-> req.timeoutAt, createdAt |-> t,
                        settledAt |-> NoTime,
                        callbacks |-> {}, listeners |-> {} ],
          task    |-> IF req.tags.targeted THEN
                          [NoTask EXCEPT !.state = "pending", !.retryTimeoutAt = t]
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

Project(obj, t) ==
    IF obj.promise.state = "pending" /\ obj.promise.timeoutAt <= t THEN
        [ promise |-> [obj.promise EXCEPT
                          !.state = IF obj.promise.tags.timer THEN "resolved"
                                        ELSE "rejectedTimedout",
                          !.value     = NoValue,
                          !.settledAt = obj.promise.timeoutAt],
          task    |-> [obj.task EXCEPT !.state     = IF @ = "none" THEN "none"
                                                     ELSE "fulfilled",
                                       !.pid            = NoPid,
                                       !.ttl            = NoTime,
                                       !.leaseTimeoutAt = NoTime,
                                       !.retryTimeoutAt = NoTime,
                                       !.resumes        = {}] ]
    ELSE
        obj

(* @type: ($createReq, Int) => $object; *)

-----------------------------------------------------------------------------

HandlePromiseCreate(req) ==
    /\ req.id \notin DOMAIN objects
    /\ objects' = Write(objects, req.id, New(req, now))
    /\ UNCHANGED <<outbox, now>>

HandlePromiseSettle(req) ==
    /\ req.id \in DOMAIN objects
    /\ LET old == Project(objects[req.id], now)
       IN
           /\ old.promise.state = "pending"
           /\ objects' =
                  Write(objects, req.id,
                        [ promise |-> [old.promise EXCEPT !.state     = req.state,
                                                        !.value     = req.value,
                                                        !.settledAt = now],
                        task    |-> [old.task EXCEPT
                                        !.state = IF @ = "none" THEN "none"
                                                      ELSE "fulfilled",
                                        !.pid            = NoPid,
                                        !.ttl            = NoTime,
                                        !.leaseTimeoutAt = NoTime,
                                        !.retryTimeoutAt = NoTime,
                                        !.resumes        = {}] ])
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
                  Write(objects, req.awaited, [awaited EXCEPT !.promise.callbacks = @ \cup {req.awaiter}])
    /\ UNCHANGED <<outbox, now>>

HandlePromiseRegisterListener(req) ==
    /\ req.awaited \in DOMAIN objects
    /\ LET old == Project(objects[req.awaited], now)
       IN
           /\ IsExternal(old.promise)
           /\ old.promise.state = "pending"
           /\ objects' =
                  Write(objects, req.awaited, [old EXCEPT !.promise.listeners = @ \cup {req.address}])
    /\ UNCHANGED <<outbox, now>>

-----------------------------------------------------------------------------

HandleTaskCreate(req) ==
    /\ req.action.tags.targeted
    /\ \/ /\ req.action.id \notin DOMAIN objects
          /\ LET born == New(req.action, now)
             IN
                 objects' =
                     Write(objects, req.action.id,
                           IF born.promise.state = "pending" THEN
                             [born EXCEPT !.task.state     = "acquired",
                                          !.task.version        = @ + 1,
                                          !.task.ttl            = req.ttl,
                                          !.task.pid            = req.pid,
                                          !.task.leaseTimeoutAt = now + req.ttl,
                                          !.task.retryTimeoutAt = NoTime,
                                          !.task.resumes        = {}]
                         ELSE
                             born)
       \/ /\ req.action.id \in DOMAIN objects
          /\ LET old == Project(objects[req.action.id], now)
             IN
                 /\ old.promise.tags.targeted
                 /\ old.task.state = "pending"
                 /\ objects' =
                        Write(objects, req.action.id,
                              [old EXCEPT !.task.state     = "acquired",
                                        !.task.version        = @ + 1,
                                        !.task.ttl            = req.ttl,
                                        !.task.pid            = req.pid,
                                        !.task.leaseTimeoutAt = now + req.ttl,
                                        !.task.retryTimeoutAt = NoTime,
                                        !.task.resumes        = {}])
    /\ UNCHANGED <<outbox, now>>

HandleTaskAcquire(req) ==
    /\ req.id \in DOMAIN objects
    /\ LET old == Project(objects[req.id], now)
       IN
           /\ old.task.state = "pending"
           /\ old.promise.state = "pending"
           /\ old.task.version = req.version
           /\ objects' =
                  Write(objects, req.id,
                        [old EXCEPT !.task.state     = "acquired",
                                  !.task.version        = @ + 1,
                                  !.task.ttl            = req.ttl,
                                  !.task.pid            = req.pid,
                                  !.task.leaseTimeoutAt = now + req.ttl,
                                  !.task.retryTimeoutAt = NoTime,
                                  !.task.resumes        = {}])
    /\ UNCHANGED <<outbox, now>>

HandleTaskFence(i, v) ==
    /\ i \in DOMAIN objects
    /\ LET old == Project(objects[i], now)
       IN
           /\ old.task.state = "acquired"
           /\ old.promise.state = "pending"
           /\ old.task.version = v
           /\ \/ \E req \in CreateReq : req.id /= i /\ HandlePromiseCreate(req)
              \/ \E req \in SettleReq : req.id /= i /\ HandlePromiseSettle(req)

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
               [ i \in DOMAIN objects |->
                    IF i \in beat THEN
                        LET old == Project(objects[i], now)
                        IN  [old EXCEPT !.task.leaseTimeoutAt = now + old.task.ttl]
                    ELSE
                        objects[i] ]
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
                          Write(objects, req.id, [old EXCEPT !.task.resumes = {}])
                  ELSE
                      objects' =
                          [ i \in DOMAIN objects |->
                               IF i = req.id THEN
                                   [old EXCEPT !.task.state     = "suspended",
                                               !.task.pid            = NoPid,
                                               !.task.ttl            = NoTime,
                                               !.task.leaseTimeoutAt = NoTime,
                                               !.task.retryTimeoutAt = NoTime,
                                               !.task.resumes        = {}]
                               ELSE IF i \in aw THEN
                                   [Project(objects[i], now) EXCEPT
                                        !.promise.callbacks = @ \cup {req.id}]
                               ELSE
                                   objects[i] ]
        /\ UNCHANGED <<outbox, now>>

HandleTaskFulfill(req) ==
    /\ req.id \in DOMAIN objects
    /\ LET old == Project(objects[req.id], now)
       IN
           /\ old.task.state = "acquired"
           /\ old.promise.state = "pending"
           /\ old.task.version = req.version
           /\ objects' =
                  Write(objects, req.id,
                        [ promise |-> [old.promise EXCEPT
                                        !.state     = req.action.state,
                                        !.value     = req.action.value,
                                        !.settledAt = now],
                        task    |-> [old.task EXCEPT !.state     = "fulfilled",
                                                     !.pid            = NoPid,
                                                     !.ttl            = NoTime,
                                                     !.leaseTimeoutAt = NoTime,
                                                     !.retryTimeoutAt = NoTime,
                                                     !.resumes        = {}] ])
    /\ UNCHANGED <<outbox, now>>

HandleTaskRelease(req) ==
    /\ req.id \in DOMAIN objects
    /\ LET old == Project(objects[req.id], now)
       IN
           /\ old.task.state = "acquired"
           /\ old.promise.state = "pending"
           /\ old.task.version = req.version
           /\ objects' =
                  Write(objects, req.id,
                        [old EXCEPT !.task.state     = "pending",
                                  !.task.pid            = NoPid,
                                  !.task.ttl            = NoTime,
                                  !.task.leaseTimeoutAt = NoTime,
                                  !.task.retryTimeoutAt = now])
    /\ UNCHANGED <<outbox, now>>

HandleTaskHalt(req) ==
    /\ req.id \in DOMAIN objects
    /\ LET old == Project(objects[req.id], now)
       IN
           /\ old.task.state \notin {"none", "fulfilled", "halted"}
           /\ objects' =
                  Write(objects, req.id,
                        [old EXCEPT !.task.state     = "halted",
                                  !.task.pid            = NoPid,
                                  !.task.ttl            = NoTime,
                                  !.task.leaseTimeoutAt = NoTime,
                                  !.task.retryTimeoutAt = NoTime])
    /\ UNCHANGED <<outbox, now>>

HandleTaskContinue(req) ==
    /\ req.id \in DOMAIN objects
    /\ LET old == Project(objects[req.id], now)
       IN
           /\ old.task.state = "halted"
           /\ old.promise.state = "pending"
           /\ objects' =
                  Write(objects, req.id,
                        [old EXCEPT !.task.state     = "pending",
                                  !.task.pid            = NoPid,
                                  !.task.ttl            = NoTime,
                                  !.task.leaseTimeoutAt = NoTime,
                                  !.task.retryTimeoutAt = now])
    /\ UNCHANGED <<outbox, now>>

-----------------------------------------------------------------------------

ProcessPromiseTimeout ==
    \E i \in DOMAIN objects :
        /\ objects[i].promise.state = "pending"
        /\ objects[i].promise.timeoutAt <= now
        /\ objects' = Write(objects, i, Project(objects[i], now))
        /\ UNCHANGED <<outbox, now>>

ProcessLeaseTimeout ==
    \E i \in DOMAIN objects :
        LET old == objects[i]
        IN
            /\ old.task.state = "acquired"
            /\ old.task.leaseTimeoutAt /= NoTime
            /\ old.task.leaseTimeoutAt <= now
            /\ Project(old, now).promise.state = "pending"
            /\ objects' =
                   Write(objects, i,
                         [old EXCEPT !.task.state     = "pending",
                                     !.task.pid            = NoPid,
                                     !.task.ttl            = NoTime,
                                     !.task.leaseTimeoutAt = NoTime,
                                     !.task.retryTimeoutAt = now])
            /\ UNCHANGED <<outbox, now>>

ProcessRetryTimeout ==
    \E i \in DOMAIN objects :
        LET old == objects[i]
            msg == [ address |-> objects[i].promise.tags.target,
                     message |-> [tag |-> "Execute", id      |-> i,
                                          version |-> objects[i].task.version] ]
        IN
            /\ old.task.state = "pending"
            /\ old.task.retryTimeoutAt /= NoTime
            /\ old.task.retryTimeoutAt <= now
            /\ Project(old, now).promise.state = "pending"
            /\ objects' =
                   Write(objects, i, [old EXCEPT !.task.retryTimeoutAt = now + RetryTimeout])
            /\ outbox' = { o \in outbox : MsgKey(o) /= MsgKey(msg) } \cup {msg}
            /\ UNCHANGED now

ProcessListener ==
    \E i \in DOMAIN objects :
        \E a \in objects[i].promise.listeners :
            LET awaited == Project(objects[i], now)
                msg     == [ address |-> a,
                             message |-> [tag |-> "Unblock", id    |-> i,
                                                  state |-> Project(objects[i],
                                                                    now).promise.state] ]
            IN
                /\ awaited.promise.state /= "pending"
                /\ objects' =
                       Write(objects, i, [awaited EXCEPT !.promise.listeners = @ \ {a}])
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
                      /\ objects' = Write(objects, i, newAwaited)
                   \/ /\ w \in DOMAIN objects
                      /\ LET struck  == Write(objects, i, newAwaited)
                             awaiter == Project(struck[w], now)
                         IN
                             IF awaiter.task.state \in {"none", "fulfilled"} THEN
                                 objects' = Write(struck, w, awaiter)
                             ELSE
                                 objects' =
                                     Write(struck, w,
                                           IF awaiter.task.state = "suspended" THEN
                                             [awaiter EXCEPT
                                                 !.task.state          = "pending",
                                                 !.task.pid            = NoPid,
                                                 !.task.ttl            = NoTime,
                                                 !.task.leaseTimeoutAt = NoTime,
                                                 !.task.retryTimeoutAt = now,
                                                 !.task.resumes        = {i}]
                                         ELSE
                                             [awaiter EXCEPT
                                                 !.task.resumes = @ \cup {i}])
                /\ UNCHANGED <<outbox, now>>

-----------------------------------------------------------------------------

Clock ==
    /\ now' = now + 1
    /\ UNCHANGED <<objects, outbox>>

-----------------------------------------------------------------------------

Init ==
    /\ objects = EmptyFn
    /\ outbox  = {}
    /\ now     = 0

Next ==
    \/ \E req \in CreateReq   : HandlePromiseCreate(req)
    \/ \E req \in SettleReq   : HandlePromiseSettle(req)
    \/ \E req \in CallbackReq : HandlePromiseRegisterCallback(req)
    \/ \E req \in [awaited : Id, address : Address] :
           HandlePromiseRegisterListener(req)
    \/ \E req \in [pid : Pid, ttl : Ttl, action : CreateReq] :
           HandleTaskCreate(req)
    \/ \E req \in [id : Id, version : Version, pid : Pid, ttl : Ttl] :
           HandleTaskAcquire(req)
    \/ \E i \in Id, v \in Version : HandleTaskFence(i, v)
    \/ \E req \in [pid : Pid, tasks : SUBSET TaskRefT] :
           HandleTaskHeartbeat(req)
    \/ \E req \in [id : Id, version : Version, actions : SUBSET CallbackReq] :
           HandleTaskSuspend(req)
    \/ \E req \in [id : Id, version : Version, action : SettleReq] :
           HandleTaskFulfill(req)
    \/ \E req \in TaskRefT : HandleTaskRelease(req)
    \/ \E req \in [id : Id] : HandleTaskHalt(req)
    \/ \E req \in [id : Id] : HandleTaskContinue(req)
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

(* THE MODEL IS FINITE ONLY IF SOMETHING SAYS SO. `Version` is declared as
   0..MaxVersion and then never enforced: acquire and create bump the task
   version with no upper guard, so a task acquired and released and acquired
   again climbs forever and the state space is infinite. TypeOK did not catch
   it -- it never mentioned version -- and no run of this spec has ever
   terminated. This is the bound, as a state constraint rather than a guard,
   because a protocol that stopped bumping at MaxVersion would be a different
   protocol. *)

NowBound ==
    now <= MaxTime

VersionBound ==
    \A i \in DOMAIN objects : objects[i].task.version <= MaxVersion

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
        objects[i].task.state /= "none" => ((objects[i].task.state = "pending") <=> (objects[i].task.retryTimeoutAt /= NoTime))

well_formed_task_acquired_iff_has_expires_at ==
    \A i \in DOMAIN objects :
        objects[i].task.state /= "none" => ((objects[i].task.state = "acquired") <=> (objects[i].task.leaseTimeoutAt /= NoTime))

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
