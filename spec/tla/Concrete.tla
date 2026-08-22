-------------------------------- MODULE Concrete --------------------------------

EXTENDS Requests

CONSTANT Fenced

VARIABLES
    docs,
    timeouts,
    outbox,
    steps,
    now

vars ==
    <<docs, timeouts, outbox, steps, now>>

-----------------------------------------------------------------------------

Objects ==
    [ i \in UNION { DOMAIN docs[o] : o \in Origin } |-> docs[i.origin][i] ]

A ==
    INSTANCE Abstract WITH objects <- Objects

-----------------------------------------------------------------------------

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

HandlePromiseGet(req, doc, t) ==
    [ doc   |-> doc,
      sends |-> << >> ]

HandlePromiseCreate(req, doc, t) ==
    IF req.id \in DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET new == New(req, t)
        IN
            [ doc   |-> Write(doc, req.id, new),
              sends |-> << >> ]

HandlePromiseSettle(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET old == Project(doc[req.id], t)
            new == [ promise |-> [old.promise EXCEPT !.state     = req.state,
                                                     !.value     = req.value,
                                                     !.settledAt = t],
                     task    |-> [old.task EXCEPT !.state     = IF @ = "none" THEN "none"
                                                               ELSE "fulfilled",
                                                  !.pid       = NoPid,
                                                  !.ttl       = NoTime,
                                                  !.expiresAt = NoTime,
                                                  !.retryAt   = NoTime,
                                                  !.resumes   = {}] ]
        IN
            IF old.promise.state = "pending" THEN
                [ doc   |-> Write(doc, req.id, new),
                  sends |-> << >> ]
            ELSE
                [ doc   |-> doc,
                  sends |-> << >> ]

HandlePromiseRegisterCallback(req, doc, t) ==
    IF \/ req.awaited \notin DOMAIN doc
       \/ req.awaiter \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET awaited    == Project(doc[req.awaited], t)
            awaiter    == Project(doc[req.awaiter], t)
            newAwaited == [awaited EXCEPT !.promise.callbacks = @ \cup {req.awaiter}]
        IN
            IF \/ ~awaiter.promise.tags.targeted
               \/ ~IsExternal(awaited.promise)
               \/ awaited.promise.state /= "pending"
               \/ awaiter.promise.state /= "pending" THEN
                [ doc   |-> doc,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(doc, req.awaited, newAwaited),
                  sends |-> << >> ]

HandlePromiseRegisterListener(req, doc, t) ==
    IF req.awaited \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET old == Project(doc[req.awaited], t)
            new == [old EXCEPT !.promise.listeners = @ \cup {req.address}]
        IN
            IF \/ ~IsExternal(old.promise)
               \/ old.promise.state /= "pending" THEN
                [ doc   |-> doc,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(doc, req.awaited, new),
                  sends |-> << >> ]

HandleTaskGet(req, doc, t) ==
    [ doc   |-> doc,
      sends |-> << >> ]

HandleTaskCreate(req, doc, t) ==
    IF ~req.action.tags.targeted THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE IF req.action.id \notin DOMAIN doc THEN
        LET born == New(req.action, t)
            new  == IF born.promise.state = "pending" THEN
                        [born EXCEPT !.task.state     = "acquired",
                                     !.task.version   = @ + 1,
                                     !.task.ttl       = req.ttl,
                                     !.task.pid       = req.pid,
                                     !.task.expiresAt = t + req.ttl,
                                     !.task.retryAt   = NoTime,
                                     !.task.resumes   = {}]
                    ELSE
                        born
        IN
            [ doc   |-> Write(doc, req.action.id, new),
              sends |-> << >> ]
    ELSE
        LET old == Project(doc[req.action.id], t)
            new == [old EXCEPT !.task.state     = "acquired",
                               !.task.version   = @ + 1,
                               !.task.ttl       = req.ttl,
                               !.task.pid       = req.pid,
                               !.task.expiresAt = t + req.ttl,
                               !.task.retryAt   = NoTime,
                               !.task.resumes   = {}]
        IN
            IF \/ ~old.promise.tags.targeted
               \/ old.task.state /= "pending" THEN
                [ doc   |-> doc,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(doc, req.action.id, new),
                  sends |-> << >> ]

HandleTaskAcquire(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET old == Project(doc[req.id], t)
            new == [old EXCEPT !.task.state     = "acquired",
                               !.task.version   = @ + 1,
                               !.task.ttl       = req.ttl,
                               !.task.pid       = req.pid,
                               !.task.expiresAt = t + req.ttl,
                               !.task.retryAt   = NoTime,
                               !.task.resumes   = {}]
        IN
            IF \/ old.task.state /= "pending"
               \/ old.promise.state /= "pending"
               \/ old.task.version /= req.version THEN
                [ doc   |-> doc,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(doc, req.id, new),
                  sends |-> << >> ]

HandleTaskFence(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET old == Project(doc[req.id], t)
        IN
            IF \/ (IF req.action.tag = "Create"
                   THEN req.action.req.id
                   ELSE req.action.req.id) = req.id
               \/ old.task.state /= "acquired"
               \/ old.promise.state /= "pending"
               \/ old.task.version /= req.version THEN
                [ doc   |-> doc,
                  sends |-> << >> ]
            ELSE IF req.action.tag = "Create" THEN
                HandlePromiseCreate(req.action.req, doc, t)
            ELSE
                HandlePromiseSettle(req.action.req, doc, t)

HandleTaskHeartbeat(req, doc, t) ==
    LET beat == { i \in DOMAIN doc :
                    LET old == Project(doc[i], t)
                    IN  /\ \E rf \in req.tasks :
                              rf.id = i /\ rf.version = old.task.version
                        /\ old.task.state = "acquired"
                        /\ old.task.pid = req.pid
                        /\ old.promise.state = "pending" }
    IN
        [ doc   |-> [ i \in DOMAIN doc |->
                         IF i \in beat THEN
                             LET old == Project(doc[i], t)
                             IN  [old EXCEPT !.task.expiresAt =
                                                 t + old.task.ttl]
                         ELSE
                             doc[i] ],
           sends |-> << >> ]

HandleTaskSuspend(req, doc, t) ==
    LET aw   == { a.awaited : a \in req.actions }
        seen == { a \in aw : a \in DOMAIN doc }
    IN
        IF \/ req.actions = {}
           \/ req.id \in aw
           \/ \E a \in aw : a.origin /= req.id.origin
           \/ req.id \notin DOMAIN doc
           \/ seen /= aw THEN
            [ doc   |-> doc,
              sends |-> << >> ]
        ELSE
            LET old == Project(doc[req.id], t)
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
                        ~IsExternal(Project(doc[a], t).promise) THEN
                    [ doc   |-> doc,
                      sends |-> << >> ]
                ELSE IF \E a \in aw :
                          Project(doc[a], t).promise.state /= "pending" THEN
                    [ doc   |-> Write(doc, req.id, [old EXCEPT !.task.resumes = {}]),
                      sends |-> << >> ]
                ELSE
                    [ doc   |-> [ i \in DOMAIN doc |->
                                     IF i = req.id THEN
                                         new
                                     ELSE IF i \in aw THEN
                                         [Project(doc[i], t) EXCEPT
                                              !.promise.callbacks = @ \cup {req.id}]
                                     ELSE
                                         doc[i] ],
                       sends |-> << >> ]

HandleTaskFulfill(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET old == Project(doc[req.id], t)
            new == [ promise |-> [old.promise EXCEPT !.state     = req.action.state,
                                                     !.value     = req.action.value,
                                                     !.settledAt = t],
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
                [ doc   |-> doc,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(doc, req.id, new),
                  sends |-> << >> ]

HandleTaskRelease(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET old == Project(doc[req.id], t)
            new == [old EXCEPT !.task.state     = "pending",
                               !.task.pid       = NoPid,
                               !.task.ttl       = NoTime,
                               !.task.expiresAt = NoTime,
                               !.task.retryAt   = t]
        IN
            IF \/ old.task.state /= "acquired"
               \/ old.promise.state /= "pending"
               \/ old.task.version /= req.version THEN
                [ doc   |-> doc,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(doc, req.id, new),
                  sends |-> << >> ]

HandleTaskHalt(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET old == Project(doc[req.id], t)
            new == [old EXCEPT !.task.state     = "halted",
                               !.task.pid       = NoPid,
                               !.task.ttl       = NoTime,
                               !.task.expiresAt = NoTime,
                               !.task.retryAt   = NoTime]
        IN
            IF \/ old.task.state = "none"
               \/ old.task.state \in {"fulfilled", "halted"} THEN
                [ doc   |-> doc,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(doc, req.id, new),
                  sends |-> << >> ]

HandleTaskContinue(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET old == Project(doc[req.id], t)
            new == [old EXCEPT !.task.state     = "pending",
                               !.task.pid       = NoPid,
                               !.task.ttl       = NoTime,
                               !.task.expiresAt = NoTime,
                               !.task.retryAt   = t]
        IN
            IF \/ old.task.state /= "halted"
               \/ old.promise.state /= "pending" THEN
                [ doc   |-> doc,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(doc, req.id, new),
                  sends |-> << >> ]

ProcessLeaseTimeout(i, doc, t) ==
    IF i \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET old == doc[i]
            new == [old EXCEPT !.task.state     = "pending",
                               !.task.pid       = NoPid,
                               !.task.ttl       = NoTime,
                               !.task.expiresAt = NoTime,
                               !.task.retryAt   = t]
        IN
            IF \/ old.task.state /= "acquired"
               \/ old.task.expiresAt = NoTime
               \/ old.task.expiresAt > t
               \/ Project(old, t).promise.state /= "pending" THEN
                [ doc   |-> doc,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(doc, i, new),
                  sends |-> << >> ]

ProcessRetryTimeout(i, doc, t) ==
    IF i \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET old == doc[i]
            new == [old EXCEPT !.task.retryAt = t + RetryTimeout]
        IN
            IF \/ old.task.state /= "pending"
               \/ old.task.retryAt = NoTime
               \/ old.task.retryAt > t
               \/ Project(old, t).promise.state /= "pending" THEN
                [ doc   |-> doc,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(doc, i, new),
                  sends |-> << [ address |-> old.promise.tags.target,
                                 message |-> [tag |-> "Execute", id      |-> i,
                                                       version |-> old.task.version] ] >> ]

ProcessListener(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET awaited    == Project(doc[req.id], t)
            newAwaited == [awaited EXCEPT !.promise.listeners = @ \ {req.address}]
        IN
            IF \/ awaited.promise.state = "pending"
               \/ req.address \notin awaited.promise.listeners THEN
                [ doc   |-> doc,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(doc, req.id, newAwaited),
                   sends |-> << [ address |-> req.address,
                                 message |-> [tag |-> "Unblock", id    |-> req.id,
                                                       state |-> awaited.promise.state] ] >> ]

ProcessCallback(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET awaited    == Project(doc[req.id], t)
            newAwaited == [awaited EXCEPT !.promise.callbacks = @ \ {req.awaiter}]
        IN
            IF \/ awaited.promise.state = "pending"
               \/ req.awaiter \notin awaited.promise.callbacks THEN
                [ doc   |-> doc,
                  sends |-> << >> ]
            ELSE IF req.awaiter \notin DOMAIN doc THEN
                [ doc   |-> Write(doc, req.id, newAwaited),
                  sends |-> << >> ]
            ELSE
                LET struck     == Write(doc, req.id, newAwaited)
                    awaiter    == Project(struck[req.awaiter], t)
                    newAwaiter == IF awaiter.task.state = "suspended" THEN
                                      [awaiter EXCEPT !.task.state     = "pending",
                                                      !.task.pid       = NoPid,
                                                      !.task.ttl       = NoTime,
                                                      !.task.expiresAt = NoTime,
                                                      !.task.retryAt   = t,
                                                      !.task.resumes   = {req.id}]
                                  ELSE
                                      [awaiter EXCEPT !.task.resumes = @ \cup {req.id}]
                IN
                    IF awaiter.task.state \in {"none", "fulfilled"} THEN
                        [ doc   |-> struck,
                          sends |-> << >> ]
                    ELSE
                        [ doc   |-> Write(struck, req.awaiter, newAwaiter),
                          sends |-> << >> ]

ProcessPromiseTimeout(req, doc, t) ==
    IF req.kind /= "promise" \/ req.id \notin DOMAIN doc THEN
        [ doc   |-> doc,
          sends |-> << >> ]
    ELSE
        LET old == doc[req.id]
            new == Project(old, t)
        IN
            IF new /= old THEN
                [ doc   |-> Write(doc, req.id, new),
                  sends |-> << >> ]
            ELSE
                [ doc   |-> doc,
                  sends |-> << >> ]

Handle(ev, doc, t) ==
    CASE ev.tag = "PromiseGet" ->
             HandlePromiseGet(ev, doc, t)
      [] ev.tag = "PromiseCreate" ->
             HandlePromiseCreate(ev.req, doc, t)
      [] ev.tag = "PromiseSettle" ->
             HandlePromiseSettle(ev.req, doc, t)
      [] ev.tag = "PromiseRegisterCallback" ->
             HandlePromiseRegisterCallback(ev.req, doc, t)
      [] ev.tag = "PromiseRegisterListener" ->
             HandlePromiseRegisterListener(ev, doc, t)
      [] ev.tag = "TaskGet" ->
             HandleTaskGet(ev, doc, t)
      [] ev.tag = "TaskCreate" ->
             HandleTaskCreate(ev, doc, t)
      [] ev.tag = "TaskAcquire" ->
             HandleTaskAcquire(ev, doc, t)
      [] ev.tag = "TaskFence" ->
             HandleTaskFence(ev, doc, t)
      [] ev.tag = "TaskHeartbeat" ->
             HandleTaskHeartbeat(ev, doc, t)
      [] ev.tag = "TaskSuspend" ->
             HandleTaskSuspend(ev, doc, t)
      [] ev.tag = "TaskFulfill" ->
             HandleTaskFulfill(ev, doc, t)
      [] ev.tag = "TaskRelease" ->
             HandleTaskRelease(ev, doc, t)
      [] ev.tag = "TaskHalt" ->
             HandleTaskHalt(ev, doc, t)
      [] ev.tag = "TaskContinue" ->
             HandleTaskContinue(ev, doc, t)
      [] ev.tag = "Timeout" ->
             LET d == ev
             IN  CASE d.kind = "promise" -> ProcessPromiseTimeout(d, doc, t)
                   [] d.kind = "lease"   -> ProcessLeaseTimeout(d.id, doc, t)
                   [] OTHER              -> ProcessRetryTimeout(d.id, doc, t)
      [] ev.tag = "ListenerDrain" ->
             ProcessListener(ev, doc, t)
      [] ev.tag = "CallbackDrain" ->
             ProcessCallback(ev, doc, t)
      [] OTHER -> [ doc   |-> doc,
                    sends |-> << >> ]

-----------------------------------------------------------------------------

OriginOf(ev) ==
    CASE ev.tag = "PromiseGet"              -> ev.id.origin
      [] ev.tag = "PromiseCreate"           -> ev.req.id.origin
      [] ev.tag = "PromiseSettle"           -> ev.req.id.origin
      [] ev.tag = "PromiseRegisterCallback" ->
             ev.req.awaited.origin
      [] ev.tag = "PromiseRegisterListener" ->
             ev.awaited.origin
      [] ev.tag = "TaskGet"       -> ev.id.origin
      [] ev.tag = "TaskCreate"    -> ev.action.id.origin
      [] ev.tag = "TaskAcquire"   -> ev.id.origin
      [] ev.tag = "TaskFence"     -> ev.id.origin
      [] ev.tag = "TaskSuspend"   -> ev.id.origin
      [] ev.tag = "TaskFulfill"   -> ev.id.origin
      [] ev.tag = "TaskRelease"   -> ev.id.origin
      [] ev.tag = "TaskHalt"      -> ev.id.origin
      [] ev.tag = "TaskContinue"  -> ev.id.origin
      [] ev.tag = "Timeout"       -> ev.id.origin
      [] ev.tag = "ListenerDrain" -> ev.id.origin
      [] ev.tag = "CallbackDrain" -> ev.id.origin

      [] OTHER -> CHOOSE o \in Origin : TRUE

Fresh(ev) ==
    [ ev |-> ev, phase |-> "process", pending |-> << >>,
      expect |-> EmptyFn ]

Set(f, k, v) ==
    [x \in (DOMAIN f) \cup {k} |-> IF x = k THEN v ELSE f[x]]
Del(f, k) ==
    [x \in (DOMAIN f) \ {k} |-> f[x]]

SubmitExternal(ev) ==
    /\ ev \in ExternalEvent
    /\ \E r \in Rid \ DOMAIN steps : steps' = Set(steps, r, Fresh(ev))
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

SubmitInternal(e) ==
    /\ e \in timeouts
    /\ e.at <= now
    /\ \E r \in Rid \ DOMAIN steps :
           steps' = Set(steps, r,
                        Fresh([tag |-> "Timeout", id |-> e.id, kind |-> e.kind]))
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

SubmitDue(i, k) ==
    \E e \in timeouts : e.id = i /\ e.kind = k /\ SubmitInternal(e)

Due(doc, t) ==
    { i \in DOMAIN doc :
        /\ doc[i].promise.state = "pending"
        /\ doc[i].promise.timeoutAt <= t }

Settled(doc) ==
    { i \in DOMAIN doc : doc[i].promise.state /= "pending" }

Listening(doc) ==
    { x \in [id : Settled(doc), address : Address] :
        x.address \in doc[x.id].promise.listeners }

Awaiting(doc) ==
    { x \in [id : Settled(doc), awaiter : Id] :
        x.awaiter \in doc[x.id].promise.callbacks }

Leased(doc, t) ==
    { i \in DOMAIN doc :
        /\ doc[i].promise.state = "pending"
        /\ doc[i].task.state = "acquired"
        /\ doc[i].task.expiresAt /= NoTime
        /\ doc[i].task.expiresAt <= t }

Retrying(doc, t) ==
    { i \in DOMAIN doc :
        /\ doc[i].promise.state = "pending"
        /\ doc[i].task.state = "pending"
        /\ doc[i].task.retryAt /= NoTime
        /\ doc[i].task.retryAt <= t }

-----------------------------------------------------------------------------

TimeOut(doc, t) ==
    [ i \in DOMAIN doc |->
        IF i \in Due(doc, t) THEN Project(doc[i], t) ELSE doc[i] ]

Notify(doc, q, t) ==
    [ doc   |-> [ i \in DOMAIN doc |->
                    IF i \in Settled(doc) THEN
                        [doc[i] EXCEPT !.promise.listeners = {}]
                    ELSE
                        doc[i] ],
      sends |-> [ n \in 1 .. Len(q) |->
                    [ address |-> q[n].address,
                      message |-> [ tag   |-> "Unblock",
                                    id    |-> q[n].id,
                                    state |-> doc[q[n].id].promise.state ] ] ] ]

Resume(doc, t) ==
    LET S == Awaiting(doc)
        Resumes(w) == { x.id : x \in { y \in S : y.awaiter = w } }
        Woken      == { x.awaiter : x \in S } \cap DOMAIN doc
    IN
        [ i \in DOMAIN doc |->
            LET struck == IF i \in Settled(doc) THEN
                              [doc[i] EXCEPT !.promise.callbacks = {}]
                          ELSE
                              doc[i]
            IN
                IF i \notin Woken \/ struck.task.state \in {"none", "fulfilled"} THEN
                    struck
                ELSE IF struck.task.state = "suspended" THEN
                    [struck EXCEPT !.task.state     = "pending",
                                   !.task.pid       = NoPid,
                                   !.task.ttl       = NoTime,
                                   !.task.expiresAt = NoTime,
                                   !.task.retryAt   = t,
                                   !.task.resumes   = Resumes(i)]
                ELSE
                    [struck EXCEPT !.task.resumes = @ \cup Resumes(i)] ]

Expire(doc, t) ==
    [ i \in DOMAIN doc |->
        IF i \in Leased(doc, t) THEN
            [ doc[i] EXCEPT !.task.state     = "pending",
                            !.task.pid       = NoPid,
                            !.task.ttl       = NoTime,
                            !.task.expiresAt = NoTime,
                            !.task.retryAt   = t ]
        ELSE
            doc[i] ]

Retry(doc, q, t) ==
    [ doc   |-> [ i \in DOMAIN doc |->
                    IF i \in Retrying(doc, t) THEN
                        [ doc[i] EXCEPT !.task.retryAt = t + RetryTimeout ]
                    ELSE
                        doc[i] ],
      sends |-> [ n \in 1 .. Len(q) |->
                    [ address |-> doc[q[n]].promise.tags.target,
                      message |-> [ tag     |-> "Execute",
                                    id      |-> q[n],
                                    version |-> doc[q[n]].task.version ] ] ] ]

-----------------------------------------------------------------------------

Sweep(doc, t) ==
    LET d1 == TimeOut(doc, t)
        o2 == Notify(d1, SetToSeq(Listening(d1)), t)
        d3 == Resume(o2.doc, t)
        d4 == Expire(d3, t)
        o5 == Retry(d4, SetToSeq(Retrying(d4, t)), t)
    IN
        [ doc   |-> o5.doc,
          sends |-> o2.sends \o o5.sends ]


Was(o, i, k) ==
    IF i \in DOMAIN docs[o] THEN
        Deadline(docs[o][i], k)
    ELSE
        NoTime

PutTimeoutFor(o, i, new, k) ==
    IF Deadline(new, k) /= NoTime /\ Deadline(new, k) /= Was(o, i, k) THEN
        << [tag |-> "PutTimeout", entry |-> [at |-> Deadline(new, k), id |-> i, kind |-> k]] >>
    ELSE
        << >>

DelTimeoutFor(o, i, new, k) ==
    IF Was(o, i, k) /= NoTime /\ Was(o, i, k) /= Deadline(new, k) THEN
        << [tag |-> "DelTimeout", entry |-> [at |-> Was(o, i, k), id |-> i, kind |-> k]] >>
    ELSE
        << >>

PutTimeouts(o, W) ==
    FoldSet(LAMBDA acc, w :
                acc \o PutTimeoutFor(o, w.id, w.obj, "promise")
                    \o PutTimeoutFor(o, w.id, w.obj, "lease")
                    \o PutTimeoutFor(o, w.id, w.obj, "retry"),
            << >>, W)

DelTimeouts(o, W) ==
    FoldSet(LAMBDA acc, w :
                acc \o DelTimeoutFor(o, w.id, w.obj, "promise")
                    \o DelTimeoutFor(o, w.id, w.obj, "lease")
                    \o DelTimeoutFor(o, w.id, w.obj, "retry"),
            << >>, W)

Process(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "process"
    /\ LET o   == OriginOf(steps[r].ev)
           swept == Sweep(docs[o], now)
           out   == Handle(steps[r].ev, swept.doc, now)
           final == out.doc
           sends == swept.sends \o out.sends
           W   == { [id |-> i, obj |-> final[i]] :
                      i \in { j \in DOMAIN final :
                                 \/ j \notin DOMAIN docs[o]
                                 \/ final[j] /= docs[o][j] } }
       IN  steps' = [steps EXCEPT ![r].phase   = "perform",
                                  ![r].pending =
                                      PutTimeouts(o, W)
                                        \o << [tag |-> "PutDocument", body |-> final] >>
                                        \o DelTimeouts(o, W)
                                        \o [ n \in 1 .. Len(sends) |->
                                               [tag |-> "Send", entry |-> sends[n]] ],
                                  ![r].expect  = docs[o]]
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

Ready(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"

Finish(r) ==
    /\ Ready(r)
    /\ steps[r].pending = << >>
    /\ steps' = Del(steps, r)
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

Heads(r, tag) ==
    /\ Ready(r)
    /\ steps[r].pending /= << >>
    /\ Head(steps[r].pending).tag = tag

FenceOk(r) ==
    \/ ~Fenced
    \/ /\ docs[OriginOf(steps[r].ev)] = steps[r].expect

PutDocument(r) ==
    /\ Heads(r, "PutDocument")
    /\ FenceOk(r)
    /\ LET o == OriginOf(steps[r].ev)
       IN
           /\ docs'  = [docs EXCEPT ![o] =
                            Head(steps[r].pending).body]
    /\ steps' = [steps EXCEPT ![r].pending = Tail(@)]
    /\ UNCHANGED <<timeouts, outbox, now>>

Restart(r) ==
    /\ Heads(r, "PutDocument")
    /\ ~FenceOk(r)
    /\ steps' = [steps EXCEPT ![r].phase = "process", ![r].pending = << >>]
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

PutTimeout(r) ==
    /\ Heads(r, "PutTimeout")
    /\ timeouts' = timeouts \cup
                     {Head(steps[r].pending).entry}
    /\ steps'    = [steps EXCEPT ![r].pending = Tail(@)]
    /\ UNCHANGED <<docs, outbox, now>>

DelTimeout(r) ==
    /\ Heads(r, "DelTimeout")
    /\ timeouts' = timeouts \
                     {Head(steps[r].pending).entry}
    /\ steps'    = [steps EXCEPT ![r].pending = Tail(@)]
    /\ UNCHANGED <<docs, outbox, now>>

Send(r) ==
    /\ Heads(r, "Send")
    /\ outbox' = LET en == Head(steps[r].pending).entry
                 IN  {x \in outbox : MsgKey(x) /= MsgKey(en)} \cup {en}
    /\ steps'  = [steps EXCEPT ![r].pending = Tail(@)]
    /\ UNCHANGED <<docs, timeouts, now>>

Perform(r) ==
    \/ PutTimeout(r)
    \/ PutDocument(r)
    \/ DelTimeout(r)
    \/ Send(r)
    \/ Restart(r)
    \/ Finish(r)

Crash(r) ==
    /\ r \in DOMAIN steps
    /\ steps' = Del(steps, r)
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

Clock ==
    /\ now' = now + 1
    /\ UNCHANGED <<docs, timeouts, outbox, steps>>

Init ==
    /\ docs     = [o \in Origin |-> EmptyFn]
    /\ timeouts = {}
    /\ outbox   = {}
    /\ steps    = EmptyFn
    /\ now      = 0

Next ==
    \/ \E ev \in ExternalEvent : SubmitExternal(ev)
    \/ \E e \in timeouts : SubmitInternal(e)
    \/ \E r \in DOMAIN steps : Process(r) \/ Perform(r) \/ Crash(r)
    \/ Clock

EventuallyStable ==
    <>[][ \A r \in Rid : ~Crash(r) ]_vars

Fairness ==
    /\ \A r  \in Rid             : WF_vars(Process(r) \/ Perform(r))
    /\ \A i \in Id, k \in DeadlineKind : SF_vars(SubmitDue(i, k))
    /\ WF_vars(Clock)
    /\ EventuallyStable

FairnessSF ==
    /\ Fairness
    /\ \A r \in Rid : SF_vars(Process(r))
    /\ \A r \in Rid : SF_vars(Perform(r))

Spec ==
    Init /\ [][Next]_vars /\ Fairness
SpecSF ==
    Init /\ [][Next]_vars /\ FairnessSF

-----------------------------------------------------------------------------

C_NowBound ==
    now <= MaxTime

C_VersionBound ==
    \A i \in UNION { DOMAIN docs[o] : o \in Origin } : docs[i.origin][i].task.version <= MaxVersion

C_TypeOK ==
    A!TypeOK
C_UnitCoherent ==
    A!UnitCoherent

C_WheelSound ==
    \A e \in timeouts :
        /\ e.id \in DOMAIN Objects
        /\ Deadline(Objects[e.id], e.kind) = e.at

C_WheelComplete ==
    \A o \in DOMAIN Objects, k \in DeadlineKind :
        Deadline(Objects[o], k) /= NoTime =>
            [at |-> Deadline(Objects[o], k), id |-> o, kind |-> k] \in timeouts

SplitWrite ==
    \E r \in DOMAIN steps :
        /\ steps[r].phase = "perform"
        /\ docs[OriginOf(steps[r].ev)] /= steps[r].expect
        /\ \E j \in DOMAIN steps[r].pending :
               steps[r].pending[j].tag = "PutDocument"

NoSplitWrite ==
    ~SplitWrite

DrainRan ==
    \E r \in DOMAIN steps : steps[r].ev.tag = "CallbackDrain"
NoDrainRan ==
    ~DrainRan

CT_preserved_settled_promise_record ==
    [][A!preserved_settled_promise_record]_vars
CT_consistent_promise_settlement_stamp ==
    [][A!consistent_promise_settlement_stamp]_vars

=============================================================================
