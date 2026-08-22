-------------------------------- MODULE Concrete --------------------------------

EXTENDS Requests

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

Skip(doc) ==
    [ doc   |-> doc,
      puts  |-> << >>,
      dels  |-> << >>,
      sends |-> << >> ]

HandlePromiseGet(req, doc, t) ==
    Skip(doc)

HandlePromiseCreate(req, doc, t) ==
    IF req.id \in DOMAIN doc THEN
        Skip(doc)
    ELSE
        LET new == New(req, t)
        IN
            [ doc   |-> Write(doc, req.id, new),
              puts  |-> (IF new.promise.state = "pending" THEN
                             << [at |-> req.timeoutAt, id |-> req.id, kind |-> "promise"] >>
                         ELSE << >>)
                     \o (IF new.task.state = "pending" THEN
                             << [at |-> t, id |-> req.id, kind |-> "retry"] >>
                         ELSE << >>),
              dels  |-> << >>,
              sends |-> << >> ]

HandlePromiseSettle(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        Skip(doc)
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
            IF old.promise.state /= "pending" THEN
                Skip(doc)
            ELSE
                [ doc   |-> Write(doc, req.id, new),
                  puts  |-> << >>,
                  dels  |-> << [at |-> old.promise.timeoutAt, id |-> req.id, kind |-> "promise"] >>
                         \o (IF old.task.state = "acquired" THEN
                                 << [at |-> old.task.expiresAt, id |-> req.id, kind |-> "lease"] >>
                             ELSE << >>)
                         \o (IF old.task.state = "pending" THEN
                                 << [at |-> old.task.retryAt, id |-> req.id, kind |-> "retry"] >>
                             ELSE << >>),
                  sends |-> << >> ]

HandlePromiseRegisterCallback(req, doc, t) ==
    IF \/ req.awaited \notin DOMAIN doc
       \/ req.awaiter \notin DOMAIN doc THEN
        Skip(doc)
    ELSE
        LET awaited    == Project(doc[req.awaited], t)
            awaiter    == Project(doc[req.awaiter], t)
            newAwaited == [awaited EXCEPT !.promise.callbacks = @ \cup {req.awaiter}]
        IN
            IF \/ ~awaiter.promise.tags.targeted
               \/ ~IsExternal(awaited.promise)
               \/ awaited.promise.state /= "pending"
               \/ awaiter.promise.state /= "pending" THEN
                Skip(doc)
            ELSE
                [ doc   |-> Write(doc, req.awaited, newAwaited),
                  puts  |-> << >>,
                  dels  |-> << >>,
                  sends |-> << >> ]

HandlePromiseRegisterListener(req, doc, t) ==
    IF req.awaited \notin DOMAIN doc THEN
        Skip(doc)
    ELSE
        LET old == Project(doc[req.awaited], t)
            new == [old EXCEPT !.promise.listeners = @ \cup {req.address}]
        IN
            IF \/ ~IsExternal(old.promise)
               \/ old.promise.state /= "pending" THEN
                Skip(doc)
            ELSE
                [ doc   |-> Write(doc, req.awaited, new),
                  puts  |-> << >>,
                  dels  |-> << >>,
                  sends |-> << >> ]

HandleTaskGet(req, doc, t) ==
    Skip(doc)

HandleTaskCreate(req, doc, t) ==
    IF ~req.action.tags.targeted THEN
        Skip(doc)
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
              puts  |-> IF born.promise.state = "pending" THEN
                            << [at |-> req.action.timeoutAt, id |-> req.action.id, kind |-> "promise"],
                               [at |-> t + req.ttl,          id |-> req.action.id, kind |-> "lease"] >>
                        ELSE << >>,
              dels  |-> << >>,
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
                Skip(doc)
            ELSE
                [ doc   |-> Write(doc, req.action.id, new),
                  puts  |-> << [at |-> t + req.ttl, id |-> req.action.id, kind |-> "lease"] >>,
                  dels  |-> << [at |-> old.task.retryAt, id |-> req.action.id, kind |-> "retry"] >>,
                  sends |-> << >> ]

HandleTaskAcquire(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        Skip(doc)
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
                Skip(doc)
            ELSE
                [ doc   |-> Write(doc, req.id, new),
                  puts  |-> << [at |-> t + req.ttl, id |-> req.id, kind |-> "lease"] >>,
                  dels  |-> << [at |-> old.task.retryAt, id |-> req.id, kind |-> "retry"] >>,
                  sends |-> << >> ]

HandleTaskFence(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        Skip(doc)
    ELSE
        LET old == Project(doc[req.id], t)
        IN
            IF \/ (IF req.action.tag = "Create"
                   THEN req.action.req.id
                   ELSE req.action.req.id) = req.id
               \/ old.task.state /= "acquired"
               \/ old.promise.state /= "pending"
               \/ old.task.version /= req.version THEN
                Skip(doc)
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
        q    == SetToSeq({ i \in beat :
                             t + doc[i].task.ttl /= doc[i].task.expiresAt })
    IN
        [ doc   |-> [ i \in DOMAIN doc |->
                         IF i \in beat THEN
                             LET old == Project(doc[i], t)
                             IN  [old EXCEPT !.task.expiresAt =
                                                 t + old.task.ttl]
                         ELSE
                             doc[i] ],
          puts  |-> [ n \in 1 .. Len(q) |->
                        [at |-> t + doc[q[n]].task.ttl, id |-> q[n], kind |-> "lease"] ],
          dels  |-> [ n \in 1 .. Len(q) |->
                        [at |-> doc[q[n]].task.expiresAt, id |-> q[n], kind |-> "lease"] ],
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
            Skip(doc)
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
                    Skip(doc)
                ELSE IF \E a \in aw :
                          Project(doc[a], t).promise.state /= "pending" THEN
                    [ doc   |-> Write(doc, req.id, [old EXCEPT !.task.resumes = {}]),
                      puts  |-> << >>,
                      dels  |-> << >>,
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
                      puts  |-> << >>,
                      dels  |-> << [at |-> old.task.expiresAt, id |-> req.id, kind |-> "lease"] >>,
                      sends |-> << >> ]

HandleTaskFulfill(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        Skip(doc)
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
                Skip(doc)
            ELSE
                [ doc   |-> Write(doc, req.id, new),
                  puts  |-> << >>,
                  dels  |-> << [at |-> old.promise.timeoutAt, id |-> req.id, kind |-> "promise"],
                               [at |-> old.task.expiresAt,    id |-> req.id, kind |-> "lease"] >>,
                  sends |-> << >> ]

HandleTaskRelease(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        Skip(doc)
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
                Skip(doc)
            ELSE
                [ doc   |-> Write(doc, req.id, new),
                  puts  |-> << [at |-> t, id |-> req.id, kind |-> "retry"] >>,
                  dels  |-> << [at |-> old.task.expiresAt, id |-> req.id, kind |-> "lease"] >>,
                  sends |-> << >> ]

HandleTaskHalt(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        Skip(doc)
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
                Skip(doc)
            ELSE
                [ doc   |-> Write(doc, req.id, new),
                  puts  |-> << >>,
                  dels  |-> (IF old.task.state = "acquired" THEN
                                 << [at |-> old.task.expiresAt, id |-> req.id, kind |-> "lease"] >>
                             ELSE << >>)
                         \o (IF old.task.state = "pending" THEN
                                 << [at |-> old.task.retryAt, id |-> req.id, kind |-> "retry"] >>
                             ELSE << >>),
                  sends |-> << >> ]

HandleTaskContinue(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        Skip(doc)
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
                Skip(doc)
            ELSE
                [ doc   |-> Write(doc, req.id, new),
                  puts  |-> << [at |-> t, id |-> req.id, kind |-> "retry"] >>,
                  dels  |-> << >>,
                  sends |-> << >> ]

ProcessLeaseTimeout(i, doc, t) ==
    IF i \notin DOMAIN doc THEN
        Skip(doc)
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
                Skip(doc)
            ELSE
                [ doc   |-> Write(doc, i, new),
                  puts  |-> << [at |-> t, id |-> i, kind |-> "retry"] >>,
                  dels  |-> << [at |-> old.task.expiresAt, id |-> i, kind |-> "lease"] >>,
                  sends |-> << >> ]

ProcessRetryTimeout(i, doc, t) ==
    IF i \notin DOMAIN doc THEN
        Skip(doc)
    ELSE
        LET old == doc[i]
            new == [old EXCEPT !.task.retryAt = t + RetryTimeout]
        IN
            IF \/ old.task.state /= "pending"
               \/ old.task.retryAt = NoTime
               \/ old.task.retryAt > t
               \/ Project(old, t).promise.state /= "pending" THEN
                Skip(doc)
            ELSE
                [ doc   |-> Write(doc, i, new),
                  puts  |-> << [at |-> t + RetryTimeout, id |-> i, kind |-> "retry"] >>,
                  dels  |-> << [at |-> old.task.retryAt, id |-> i, kind |-> "retry"] >>,
                  sends |-> << [ address |-> old.promise.tags.target,
                                 message |-> [tag |-> "Execute", id      |-> i,
                                                       version |-> old.task.version] ] >> ]

ProcessListener(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        Skip(doc)
    ELSE
        LET awaited    == Project(doc[req.id], t)
            newAwaited == [awaited EXCEPT !.promise.listeners = @ \ {req.address}]
        IN
            IF \/ awaited.promise.state = "pending"
               \/ req.address \notin awaited.promise.listeners THEN
                Skip(doc)
            ELSE
                [ doc   |-> Write(doc, req.id, newAwaited),
                  puts  |-> << >>,
                  dels  |-> << >>,
                  sends |-> << [ address |-> req.address,
                                 message |-> [tag |-> "Unblock", id    |-> req.id,
                                                       state |-> awaited.promise.state] ] >> ]

ProcessCallback(req, doc, t) ==
    IF req.id \notin DOMAIN doc THEN
        Skip(doc)
    ELSE
        LET awaited    == Project(doc[req.id], t)
            newAwaited == [awaited EXCEPT !.promise.callbacks = @ \ {req.awaiter}]
        IN
            IF \/ awaited.promise.state = "pending"
               \/ req.awaiter \notin awaited.promise.callbacks THEN
                Skip(doc)
            ELSE IF req.awaiter \notin DOMAIN doc THEN
                [ doc   |-> Write(doc, req.id, newAwaited),
                  puts  |-> << >>,
                  dels  |-> << >>,
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
                          puts  |-> << >>,
                          dels  |-> << >>,
                          sends |-> << >> ]
                    ELSE
                        [ doc   |-> Write(struck, req.awaiter, newAwaiter),
                          puts  |-> IF awaiter.task.state = "suspended" THEN
                                        << [at |-> t, id |-> req.awaiter, kind |-> "retry"] >>
                                    ELSE << >>,
                          dels  |-> << >>,
                          sends |-> << >> ]

ProcessPromiseTimeout(req, doc, t) ==
    IF req.kind /= "promise" \/ req.id \notin DOMAIN doc THEN
        Skip(doc)
    ELSE
        LET old == doc[req.id]
            new == Project(old, t)
        IN
            IF new = old THEN
                Skip(doc)
            ELSE
                [ doc   |-> Write(doc, req.id, new),
                  puts  |-> << >>,
                  dels  |-> << [at |-> old.promise.timeoutAt, id |-> req.id, kind |-> "promise"] >>
                         \o (IF old.task.state = "acquired" THEN
                                 << [at |-> old.task.expiresAt, id |-> req.id, kind |-> "lease"] >>
                             ELSE << >>)
                         \o (IF old.task.state = "pending" THEN
                                 << [at |-> old.task.retryAt, id |-> req.id, kind |-> "retry"] >>
                             ELSE << >>),
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
      [] OTHER -> Skip(doc)

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

-----------------------------------------------------------------------------

Merge(a, b) ==
    [ doc   |-> b.doc,
      puts  |-> a.puts  \o b.puts,
      dels  |-> a.dels  \o b.dels,
      sends |-> a.sends \o b.sends ]

SweepTimeoutAt(doc, t) ==
    LET S  == { i \in DOMAIN doc :
                  doc[i].promise.state = "pending" /\ doc[i].promise.timeoutAt <= t }
        qp == SetToSeq(S)
        ql == SetToSeq({ i \in S : doc[i].task.state = "acquired" })
        qr == SetToSeq({ i \in S : doc[i].task.state = "pending" })
    IN
    [ doc   |-> [ i \in DOMAIN doc |->
                    IF i \in S THEN Project(doc[i], t) ELSE doc[i] ],
      puts  |-> << >>,
      dels  |->    [ n \in 1 .. Len(qp) |->
                       [at |-> doc[qp[n]].promise.timeoutAt, id |-> qp[n], kind |-> "promise"] ]
                \o [ n \in 1 .. Len(ql) |->
                       [at |-> doc[ql[n]].task.expiresAt,    id |-> ql[n], kind |-> "lease"] ]
                \o [ n \in 1 .. Len(qr) |->
                       [at |-> doc[qr[n]].task.retryAt,      id |-> qr[n], kind |-> "retry"] ],
      sends |-> << >> ]

SweepListeners(doc, t) ==
    LET q == SetToSeq({ x \in [id : { i \in DOMAIN doc : doc[i].promise.state /= "pending" },
                              address : Address] :
                          x.address \in doc[x.id].promise.listeners })
    IN
    [ doc   |-> [ i \in DOMAIN doc |->
                    IF doc[i].promise.state /= "pending" THEN
                        [doc[i] EXCEPT !.promise.listeners = {}]
                    ELSE
                        doc[i] ],
      puts  |-> << >>,
      dels  |-> << >>,
      sends |-> [ n \in 1 .. Len(q) |->
                    [ address |-> q[n].address,
                      message |-> [ tag   |-> "Unblock",
                                    id    |-> q[n].id,
                                    state |-> doc[q[n].id].promise.state ] ] ] ]

SweepCallbacks(doc, t) ==
    LET S == { x \in [id : { i \in DOMAIN doc : doc[i].promise.state /= "pending" },
                    awaiter : Id] :
                   x.awaiter \in doc[x.id].promise.callbacks }
        Resumes(w) == { x.id : x \in { y \in S : y.awaiter = w } }
        Woken      == { x.awaiter : x \in S } \cap DOMAIN doc
        q  == SetToSeq({ w \in Woken : doc[w].task.state = "suspended" })
    IN
    [ doc   |-> [ i \in DOMAIN doc |->
                    LET struck == IF doc[i].promise.state /= "pending" THEN
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
                            [struck EXCEPT !.task.resumes = @ \cup Resumes(i)] ],
      puts  |-> [ n \in 1 .. Len(q) |-> [at |-> t, id |-> q[n], kind |-> "retry"] ],
      dels  |-> << >>,
      sends |-> << >> ]

SweepExpiresAt(doc, t) ==
    LET S == { i \in DOMAIN doc :
                 doc[i].task.state = "acquired" /\ doc[i].task.expiresAt <= t }
        q == SetToSeq(S)
    IN
    [ doc   |-> [ i \in DOMAIN doc |->
                    IF i \in S THEN
                        [ doc[i] EXCEPT !.task.state     = "pending",
                                        !.task.pid       = NoPid,
                                        !.task.ttl       = NoTime,
                                        !.task.expiresAt = NoTime,
                                        !.task.retryAt   = t ]
                    ELSE
                        doc[i] ],
      puts  |-> [ n \in 1 .. Len(q) |-> [at |-> t, id |-> q[n], kind |-> "retry"] ],
      dels  |-> [ n \in 1 .. Len(q) |->
                    [at |-> doc[q[n]].task.expiresAt, id |-> q[n], kind |-> "lease"] ],
      sends |-> << >> ]

SweepRetryAt(doc, t) ==
    LET S == { i \in DOMAIN doc :
                 doc[i].task.state = "pending" /\ doc[i].task.retryAt <= t }
        q == SetToSeq(S)
    IN
    [ doc   |-> [ i \in DOMAIN doc |->
                    IF i \in S THEN
                        [ doc[i] EXCEPT !.task.retryAt = t + RetryTimeout ]
                    ELSE
                        doc[i] ],
      puts  |-> [ n \in 1 .. Len(q) |->
                    [at |-> t + RetryTimeout, id |-> q[n], kind |-> "retry"] ],
      dels  |-> [ n \in 1 .. Len(q) |->
                    [at |-> doc[q[n]].task.retryAt, id |-> q[n], kind |-> "retry"] ],
      sends |-> [ n \in 1 .. Len(q) |->
                    [ address |-> doc[q[n]].promise.tags.target,
                      message |-> [ tag     |-> "Execute",
                                    id      |-> q[n],
                                    version |-> doc[q[n]].task.version ] ] ] ]

Sweep(doc, t) ==
    LET s1 == SweepTimeoutAt(doc, t)
        s2 == Merge(s1, SweepListeners(s1.doc, t))
        s3 == Merge(s2, SweepCallbacks(s2.doc, t))
        s4 == Merge(s3, SweepExpiresAt(s3.doc, t))
    IN
        Merge(s4, SweepRetryAt(s4.doc, t))

-----------------------------------------------------------------------------

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

Process(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "process"
    /\ LET o     == OriginOf(steps[r].ev)
           swept == Sweep(docs[o], now)
           out   == Handle(steps[r].ev, swept.doc, now)
           final == out.doc
           puts  == swept.puts  \o out.puts
           dels  == swept.dels  \o out.dels
           sends == swept.sends \o out.sends
       IN  steps' = [steps EXCEPT ![r].phase   = "perform",
                                  ![r].pending =
                                      [ n \in 1 .. Len(puts) |->
                                          [tag |-> "PutTimeout", entry |-> puts[n]] ]
                                        \o << [tag |-> "PutDocument", body |-> final] >>
                                        \o [ n \in 1 .. Len(dels) |->
                                               [tag |-> "DelTimeout", entry |-> dels[n]] ]
                                        \o [ n \in 1 .. Len(sends) |->
                                               [tag |-> "Send", entry |-> sends[n]] ],
                                  ![r].expect  = docs[o]]
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

Finish(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ steps[r].pending = << >>
    /\ steps' = Del(steps, r)
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

PutDocument(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ steps[r].pending /= << >>
    /\ Head(steps[r].pending).tag = "PutDocument"
    /\ docs[OriginOf(steps[r].ev)] = steps[r].expect
    /\ LET o == OriginOf(steps[r].ev)
       IN
           /\ docs'  = [docs EXCEPT ![o] =
                            Head(steps[r].pending).body]
    /\ steps' = [steps EXCEPT ![r].pending = Tail(@)]
    /\ UNCHANGED <<timeouts, outbox, now>>

Restart(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ steps[r].pending /= << >>
    /\ Head(steps[r].pending).tag = "PutDocument"
    /\ docs[OriginOf(steps[r].ev)] /= steps[r].expect
    /\ steps' = [steps EXCEPT ![r].phase = "process", ![r].pending = << >>]
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

PutTimeout(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ steps[r].pending /= << >>
    /\ Head(steps[r].pending).tag = "PutTimeout"
    /\ timeouts' = timeouts \cup
                     {Head(steps[r].pending).entry}
    /\ steps'    = [steps EXCEPT ![r].pending = Tail(@)]
    /\ UNCHANGED <<docs, outbox, now>>

DelTimeout(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ steps[r].pending /= << >>
    /\ Head(steps[r].pending).tag = "DelTimeout"
    /\ timeouts' = timeouts \
                     {Head(steps[r].pending).entry}
    /\ steps'    = [steps EXCEPT ![r].pending = Tail(@)]
    /\ UNCHANGED <<docs, outbox, now>>

Send(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ steps[r].pending /= << >>
    /\ Head(steps[r].pending).tag = "Send"
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
    /\ \A i \in Id, k \in DeadlineKind :
           SF_vars(\E e \in timeouts :
                       e.id = i /\ e.kind = k /\ SubmitInternal(e))
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

Armed(doc) ==
    { e \in { [at |-> Deadline(doc[i], k), id |-> i, kind |-> k] :
                i \in DOMAIN doc, k \in DeadlineKind } :
        e.at /= NoTime }

C_WheelSound ==
    timeouts \subseteq Armed(Objects)

C_WheelComplete ==
    Armed(Objects) \subseteq timeouts

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
