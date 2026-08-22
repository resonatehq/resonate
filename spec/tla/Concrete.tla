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


HandlePromiseGet(req, env) ==
    [ doc   |-> env.objects,
      sends |-> << >> ]

HandlePromiseCreate(req, env) ==
    IF req.id \in DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
    ELSE
        LET new == New(req, env.now)
        IN
            [ doc   |-> Write(env.objects, req.id, new),
              sends |-> << >> ]

HandlePromiseSettle(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
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
                [ doc   |-> Write(env.objects, req.id, new),
                  sends |-> << >> ]
            ELSE
                [ doc   |-> env.objects,
                  sends |-> << >> ]

HandlePromiseRegisterCallback(req, env) ==
    IF \/ req.awaited \notin DOMAIN env.objects
       \/ req.awaiter \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
    ELSE
        LET awaited    == Project(env.objects[req.awaited], env.now)
            awaiter    == Project(env.objects[req.awaiter], env.now)
            newAwaited == [awaited EXCEPT !.promise.callbacks = @ \cup {req.awaiter}]
        IN
            IF \/ ~awaiter.promise.tags.targeted
               \/ ~IsExternal(awaited.promise)
               \/ awaited.promise.state /= "pending"
               \/ awaiter.promise.state /= "pending" THEN
                [ doc   |-> env.objects,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(env.objects, req.awaited, newAwaited),
                  sends |-> << >> ]

HandlePromiseRegisterListener(req, env) ==
    IF req.awaited \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
    ELSE
        LET old == Project(env.objects[req.awaited], env.now)
            new == [old EXCEPT !.promise.listeners = @ \cup {req.address}]
        IN
            IF \/ ~IsExternal(old.promise)
               \/ old.promise.state /= "pending" THEN
                [ doc   |-> env.objects,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(env.objects, req.awaited, new),
                  sends |-> << >> ]

HandleTaskGet(req, env) ==
    [ doc   |-> env.objects,
      sends |-> << >> ]

HandleTaskCreate(req, env) ==
    IF ~req.action.tags.targeted THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
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
            [ doc   |-> Write(env.objects, req.action.id, new),
              sends |-> << >> ]
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
                [ doc   |-> env.objects,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(env.objects, req.action.id, new),
                  sends |-> << >> ]

HandleTaskAcquire(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
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
                [ doc   |-> env.objects,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(env.objects, req.id, new),
                  sends |-> << >> ]

HandleTaskFence(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
    ELSE
        LET old == Project(env.objects[req.id], env.now)
        IN
            IF \/ (IF req.action.tag = "Create"
                   THEN req.action.req.id
                   ELSE req.action.req.id) = req.id
               \/ old.task.state /= "acquired"
               \/ old.promise.state /= "pending"
               \/ old.task.version /= req.version THEN
                [ doc   |-> env.objects,
                  sends |-> << >> ]
            ELSE IF req.action.tag = "Create" THEN
                HandlePromiseCreate(req.action.req, env)
            ELSE
                HandlePromiseSettle(req.action.req, env)

HandleTaskHeartbeat(req, env) ==
    LET beat == { i \in DOMAIN env.objects :
                    LET old == Project(env.objects[i], env.now)
                    IN  /\ \E rf \in req.tasks :
                              rf.id = i /\ rf.version = old.task.version
                        /\ old.task.state = "acquired"
                        /\ old.task.pid = req.pid
                        /\ old.promise.state = "pending" }
    IN
        [ doc   |-> [ i \in DOMAIN env.objects |->
                         IF i \in beat THEN
                             LET old == Project(env.objects[i], env.now)
                             IN  [old EXCEPT !.task.expiresAt =
                                                 env.now + old.task.ttl]
                         ELSE
                             env.objects[i] ],
           sends |-> << >> ]

HandleTaskSuspend(req, env) ==
    LET aw   == { a.awaited : a \in req.actions }
        seen == { a \in aw : a \in DOMAIN env.objects }
    IN
        IF \/ req.actions = {}
           \/ req.id \in aw
           \/ \E a \in aw : a.origin /= req.id.origin
           \/ req.id \notin DOMAIN env.objects
           \/ seen /= aw THEN
            [ doc   |-> env.objects,
              sends |-> << >> ]
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
                    [ doc   |-> env.objects,
                      sends |-> << >> ]
                ELSE IF \E a \in aw :
                          Project(env.objects[a], env.now).promise.state /= "pending" THEN
                    [ doc   |-> Write(env.objects, req.id, [old EXCEPT !.task.resumes = {}]),
                      sends |-> << >> ]
                ELSE
                    [ doc   |-> [ i \in DOMAIN env.objects |->
                                     IF i = req.id THEN
                                         new
                                     ELSE IF i \in aw THEN
                                         [Project(env.objects[i], env.now) EXCEPT
                                              !.promise.callbacks = @ \cup {req.id}]
                                     ELSE
                                         env.objects[i] ],
                       sends |-> << >> ]

HandleTaskFulfill(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
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
                [ doc   |-> env.objects,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(env.objects, req.id, new),
                  sends |-> << >> ]

HandleTaskRelease(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
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
                [ doc   |-> env.objects,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(env.objects, req.id, new),
                  sends |-> << >> ]

HandleTaskHalt(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
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
                [ doc   |-> env.objects,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(env.objects, req.id, new),
                  sends |-> << >> ]

HandleTaskContinue(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
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
                [ doc   |-> env.objects,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(env.objects, req.id, new),
                  sends |-> << >> ]

ProcessLeaseTimeout(i, env) ==
    IF i \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
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
                [ doc   |-> env.objects,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(env.objects, i, new),
                  sends |-> << >> ]

ProcessRetryTimeout(i, env) ==
    IF i \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
    ELSE
        LET old == env.objects[i]
            new == [old EXCEPT !.task.retryAt = env.now + env.config.retryTimeout]
        IN
            IF \/ old.task.state /= "pending"
               \/ old.task.retryAt = NoTime
               \/ old.task.retryAt > env.now
               \/ Project(old, env.now).promise.state /= "pending" THEN
                [ doc   |-> env.objects,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(env.objects, i, new),
                  sends |-> << [ address |-> old.promise.tags.target,
                                 message |-> [tag |-> "Execute", id      |-> i,
                                                       version |-> old.task.version] ] >> ]

ProcessListener(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
    ELSE
        LET awaited    == Project(env.objects[req.id], env.now)
            newAwaited == [awaited EXCEPT !.promise.listeners = @ \ {req.address}]
        IN
            IF \/ awaited.promise.state = "pending"
               \/ req.address \notin awaited.promise.listeners THEN
                [ doc   |-> env.objects,
                  sends |-> << >> ]
            ELSE
                [ doc   |-> Write(env.objects, req.id, newAwaited),
                   sends |-> << [ address |-> req.address,
                                 message |-> [tag |-> "Unblock", id    |-> req.id,
                                                       state |-> awaited.promise.state] ] >> ]

ProcessCallback(req, env) ==
    IF req.id \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
    ELSE
        LET awaited    == Project(env.objects[req.id], env.now)
            newAwaited == [awaited EXCEPT !.promise.callbacks = @ \ {req.awaiter}]
        IN
            IF \/ awaited.promise.state = "pending"
               \/ req.awaiter \notin awaited.promise.callbacks THEN
                [ doc   |-> env.objects,
                  sends |-> << >> ]
            ELSE IF req.awaiter \notin DOMAIN env.objects THEN
                [ doc   |-> Write(env.objects, req.id, newAwaited),
                  sends |-> << >> ]
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
                        [ doc   |-> Write(env.objects, req.id, newAwaited),
                          sends |-> << >> ]
                    ELSE
                        [ doc   |-> Write(Write(env.objects, req.id, newAwaited), req.awaiter, newAwaiter),
                          sends |-> << >> ]

ProcessPromiseTimeout(req, env) ==
    IF req.kind /= "promise" \/ req.id \notin DOMAIN env.objects THEN
        [ doc   |-> env.objects,
          sends |-> << >> ]
    ELSE
        LET old == env.objects[req.id]
            new == Project(old, env.now)
        IN
            IF new /= old THEN
                [ doc   |-> Write(env.objects, req.id, new),
                  sends |-> << >> ]
            ELSE
                [ doc   |-> env.objects,
                  sends |-> << >> ]

Handle(ev, env) ==
    CASE ev.tag = "PromiseGet" ->
             HandlePromiseGet(ev, env)
      [] ev.tag = "PromiseCreate" ->
             HandlePromiseCreate(ev.req, env)
      [] ev.tag = "PromiseSettle" ->
             HandlePromiseSettle(ev.req, env)
      [] ev.tag = "PromiseRegisterCallback" ->
             HandlePromiseRegisterCallback(ev.req, env)
      [] ev.tag = "PromiseRegisterListener" ->
             HandlePromiseRegisterListener(ev, env)
      [] ev.tag = "TaskGet" ->
             HandleTaskGet(ev, env)
      [] ev.tag = "TaskCreate" ->
             HandleTaskCreate(ev, env)
      [] ev.tag = "TaskAcquire" ->
             HandleTaskAcquire(ev, env)
      [] ev.tag = "TaskFence" ->
             HandleTaskFence(ev, env)
      [] ev.tag = "TaskHeartbeat" ->
             HandleTaskHeartbeat(ev, env)
      [] ev.tag = "TaskSuspend" ->
             HandleTaskSuspend(ev, env)
      [] ev.tag = "TaskFulfill" ->
             HandleTaskFulfill(ev, env)
      [] ev.tag = "TaskRelease" ->
             HandleTaskRelease(ev, env)
      [] ev.tag = "TaskHalt" ->
             HandleTaskHalt(ev, env)
      [] ev.tag = "TaskContinue" ->
             HandleTaskContinue(ev, env)
      [] ev.tag = "Timeout" ->
             LET d == ev
             IN  CASE d.kind = "promise" -> ProcessPromiseTimeout(d, env)
                   [] d.kind = "lease"   -> ProcessLeaseTimeout(d.id, env)
                   [] OTHER              -> ProcessRetryTimeout(d.id, env)
      [] ev.tag = "ListenerDrain" ->
             ProcessListener(ev, env)
      [] ev.tag = "CallbackDrain" ->
             ProcessCallback(ev, env)
      [] OTHER -> [ doc   |-> env.objects,
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
      expect |-> EmptyFn, at |-> 0, org |-> OriginOf(ev) ]

Put(f, k, v) ==
    [x \in (DOMAIN f) \cup {k} |-> IF x = k THEN v ELSE f[x]]
Drop(f, k) ==
    [x \in (DOMAIN f) \ {k} |-> f[x]]

SubmitExternal(ev) ==
    /\ ev \in ExternalEvent
    /\ \E r \in Rid \ DOMAIN steps : steps' = Put(steps, r, Fresh(ev))
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

SubmitInternal(e) ==
    /\ e \in timeouts
    /\ e.at <= now
    /\ \E r \in Rid \ DOMAIN steps :
           steps' = Put(steps, r,
                        Fresh([tag |-> "Timeout", id |-> e.id, kind |-> e.kind]))
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

SubmitDue(i, k) ==
    \E e \in timeouts : e.id = i /\ e.kind = k /\ SubmitInternal(e)

DrainableTimeouts(doc, t) ==
    { d \in [id : DOMAIN doc, kind : DeadlineKind] :
        /\ Deadline(doc[d.id], d.kind) /= NoTime
        /\ Deadline(doc[d.id], d.kind) <= t }

DrainableListeners(doc, t) ==
    UNION { { [id |-> i, address |-> a] : a \in doc[i].promise.listeners }
              : i \in { j \in DOMAIN doc :
                            \/ doc[j].promise.state /= "pending"
                            \/ doc[j].promise.timeoutAt <= t } }

DrainableCallbacks(doc, t) ==
    UNION { { [id |-> i, awaiter |-> w] : w \in doc[i].promise.callbacks }
              : i \in { j \in DOMAIN doc :
                            \/ doc[j].promise.state /= "pending"
                            \/ doc[j].promise.timeoutAt <= t } }

EnvAt(d, t) ==
    [ objects |-> d, now |-> t, config |-> [retryTimeout |-> RetryTimeout] ]

Advance(st, out) ==
    LET sends2 == st.sends \o out.sends
    IN
        [ doc   |-> out.doc,
           sends |-> sends2,
          tr    |-> IF out.doc = st.doc /\ out.sends = << >> THEN
                        st.tr
                    ELSE
                        st.tr \o << [doc |-> out.doc, sends |-> sends2] >> ]

RECURSIVE DrainPromises(_, _)
DrainPromises(st, t) ==
    LET d == { e \in DrainableTimeouts(st.doc, t) : e.kind = "promise" }
    IN
        IF d = {} THEN
            st
        ELSE
            DrainPromises(
                Advance(st, ProcessPromiseTimeout(CHOOSE x \in d : TRUE,
                                                  EnvAt(st.doc, t))), t)

RECURSIVE DrainTasks(_, _)
DrainTasks(st, t) ==
    LET d == { e \in DrainableTimeouts(st.doc, t) : e.kind /= "promise" }
    IN
        IF d = {} THEN
            st
        ELSE
            LET e == CHOOSE x \in d : TRUE
            IN
                DrainTasks(
                    Advance(st,
                            IF e.kind = "lease" THEN
                                ProcessLeaseTimeout(e.id, EnvAt(st.doc, t))
                            ELSE
                                ProcessRetryTimeout(e.id, EnvAt(st.doc, t))),
                    t)

RECURSIVE DrainListeners(_, _)
DrainListeners(st, t) ==
    LET d == DrainableListeners(st.doc, t)
    IN
        IF d = {} THEN
            st
        ELSE
            DrainListeners(
                Advance(st, ProcessListener(CHOOSE x \in d : TRUE,
                                            EnvAt(st.doc, t))), t)

RECURSIVE DrainCallbacks(_, _)
DrainCallbacks(st, t) ==
    LET d == DrainableCallbacks(st.doc, t)
    IN
        IF d = {} THEN
            st
        ELSE
            DrainCallbacks(
                Advance(st, ProcessCallback(CHOOSE x \in d : TRUE,
                                            EnvAt(st.doc, t))), t)

Sweep(doc, t) ==
    DrainTasks(
        DrainCallbacks(
            DrainListeners(
                DrainPromises([doc |-> doc, sends |-> << >>, tr |-> << >>], t), t), t), t)

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
    /\ LET o   == steps[r].org
           swept == Sweep(docs[o], now)
           out   == Handle(steps[r].ev, EnvAt(swept.doc, now))
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
                                  ![r].expect  = docs[o],
                                  ![r].at      = now]
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

Ready(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"

Finish(r) ==
    /\ Ready(r)
    /\ steps[r].pending = << >>
    /\ steps' = Drop(steps, r)
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

Heads(r, tag) ==
    /\ Ready(r)
    /\ steps[r].pending /= << >>
    /\ Head(steps[r].pending).tag = tag

FenceOk(r) ==
    \/ ~Fenced
    \/ /\ docs[steps[r].org] = steps[r].expect
       /\ now = steps[r].at

PutDocument(r) ==
    /\ Heads(r, "PutDocument")
    /\ FenceOk(r)
    /\ LET o == steps[r].org
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
    /\ steps' = Drop(steps, r)
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
        /\ docs[steps[r].org] /= steps[r].expect
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
