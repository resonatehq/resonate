------------------------------ MODULE Refinement ------------------------------
(* DOES THE EXECUTOR IMPLEMENT THE PROTOCOL?

   `Abstract` states one protocol step per action. `Concrete` writes a document
   once, and that one write does several protocol steps at a time -- it sweeps
   every due deadline and then serves a request. TLA+ refinement is implication,
   and implication is step by step: each concrete step must be one abstract step
   or a stutter. So the plain answer is no, and TLC says so with a sweep that
   timed out one promise and created another in a single write.

   Abadi and Lamport name this exact case -- "S2 may run slower than S1" -- as
   one of three reasons a refinement mapping fails to exist, and prescribe the
   remedy: an auxiliary variable added to the FASTER machine to slow it down.
   That is `s` below. It walks the stores one write passes through, leaving
   every real variable alone, and the mapping reads it, so the protocol advances
   one action per tick while the document stands still. Lamport and Merz call it
   a stuttering variable; it is not prophecy, which is for the other direction.

   The walk is not stored. It is a function of the document the step decided
   against, the instant it decided, and the request -- and `steps` holds all
   three, none of which move mid-walk. So `s` is a position: who is walking, and
   how far along. That also makes it the well-founded rank the construction
   needs, since `k` climbs to a computable bound.

   (Action composition would say this directly -- `A \cdot B` is one step that
   is A then B, Specifying Systems 7.3, the chapter on the grain of atomicity.
   TLC has an experimental implementation behind a JVM flag, and it cannot be
   used here: it fabricates the intermediate state, and our mapping is computed
   from `docs` rather than a plain variable rename, so there is nothing to
   evaluate it against. It fails with "the identifier docs is either undefined
   or not an operator".) *)

(* TWO MORE AUXILIARIES, both confined to this module.

   `stamp` is a history variable: the instant each request's Process read the
   clock. The executor does not keep it -- its fence is the CAS and nothing
   else -- but the walk must replay the computation, and the computation was a
   function of the document AND the instant. History is the legitimate way to
   remember what the implementation legitimately forgot.

   The outbox mapping reads pending. An abstract message step strikes the
   document and enqueues the message in one action; the executor splits it --
   the write commits the strike, the Send goes later. So the mapping counts a
   message from the moment its write lands: `Outstanding` folds every landed
   request's still-pending sends over the real outbox, and a real Send step
   then moves a message from pending to outbox without changing the mapped
   value at all -- a stutter, as it must be. *)
EXTENDS Concrete

VARIABLES s, stamp, clockA

(* `top` marks "not mid-walk". A record, so it cannot collide with a position. *)
top ==
    [top |-> "top"]

varsS ==
    <<s, stamp, clockA, docs, timeouts, outbox, steps, now>>

-----------------------------------------------------------------------------

(* THE SWEEP, ONE ITEM AT A TIME. `Sweep` does each pass in bulk; the protocol
   does one item per step. So the walk lists the items each pass will do -- in
   the order `SetToSeq` picked, the same order the sweep's sends carry -- and
   `Concrete`'s pass boundaries say where one list ends and the next begins. *)
Due(doc, t) ==
    { i \in DOMAIN doc :
        /\ doc[i].promise.state = "pending"
        /\ doc[i].promise.timeoutAt <= t }

Listening(doc) ==
    { x \in [id : { i \in DOMAIN doc : doc[i].promise.state /= "pending" },
              address : Address] :
        x.address \in doc[x.id].promise.listeners }

Awaiting(doc) ==
    { x \in [id : { i \in DOMAIN doc : doc[i].promise.state /= "pending" },
              awaiter : Id] :
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

Items(d0, t0) ==
    LET q1 == SetToSeq(Due(d0, t0))
        d1 == SweepTimeoutAt(d0, t0).doc
        q2 == SetToSeq(Listening(d1))
        d2 == SweepListeners(d1, t0).doc
        q3 == SetToSeq(Awaiting(d2))
        d3 == SweepCallbacks(d2, t0).doc
        q4 == SetToSeq(Leased(d3, t0))
        d4 == SweepExpiresAt(d3, t0).doc
        q5 == SetToSeq(Retrying(d4, t0))
    IN
           [ n \in 1 .. Len(q1) |-> [kind |-> "promise",  id |-> q1[n]] ]
        \o [ n \in 1 .. Len(q2) |-> [kind |-> "listener",
                                      id   |-> q2[n].id, address |-> q2[n].address] ]
        \o [ n \in 1 .. Len(q3) |-> [kind |-> "callback",
                                      id   |-> q3[n].id, awaiter |-> q3[n].awaiter] ]
        \o [ n \in 1 .. Len(q4) |-> [kind |-> "lease",    id |-> q4[n]] ]
        \o [ n \in 1 .. Len(q5) |-> [kind |-> "retry",    id |-> q5[n]] ]

(* One item is one protocol step. These are the protocol's single-item
   handlers -- the executor no longer has them (its sweep does everything in
   bulk, and a timer request's handler has nothing left to do), but the walk
   replays one item per tick, so they live here now. *)
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
                        [ doc   |-> Write(struck, req.awaiter, awaiter),
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

Step(d, it, t0) ==
    CASE it.kind = "promise"  ->
             ProcessPromiseTimeout([id |-> it.id, kind |-> "promise"], d, t0)
      [] it.kind = "listener" ->
             ProcessListener([id |-> it.id, address |-> it.address], d, t0)
      [] it.kind = "callback" ->
             ProcessCallback([id |-> it.id, awaiter |-> it.awaiter], d, t0)
      [] it.kind = "lease"    ->
             ProcessLeaseTimeout(it.id, d, t0)
      [] OTHER                ->
             ProcessRetryTimeout(it.id, d, t0)

(* THE STORES ONE WRITE PASSES THROUGH, one per abstract step, excluding the
   last -- the last is what the write itself produces, so the write is its own
   final tick. The replay must land where the bulk sweep landed: if it ever
   did not, the walk's last store would fail to meet the one the write
   produces, and the refinement check would say so. *)
Walk(r) ==
    LET d0    == docs[OriginOf(steps[r].ev)]
        t0    == stamp[r]
        its   == Items(d0, t0)
        store[n \in 0 .. Len(its)] ==
            IF n = 0 THEN
                [doc |-> d0, sends |-> << >>]
            ELSE
                LET prev == store[n - 1]
                    out  == Step(prev.doc, its[n], t0)
                IN
                    [doc |-> out.doc, sends |-> prev.sends \o out.sends]
        swept == store[Len(its)]
        out   == Handle(steps[r].ev, swept.doc, t0)
        ticks == [ n \in 1 .. Len(its) |-> store[n] ]
        full  == IF out.doc = swept.doc /\ out.sends = << >> THEN
                     ticks
                 ELSE
                     ticks \o << [ doc   |-> out.doc,
                                   sends |-> swept.sends \o out.sends ] >>
    IN
        IF full = << >> THEN << >> ELSE SubSeq(full, 1, Len(full) - 1)

(* ONE TICK, OR THE WRITE. A walk belongs to the request that started it --
   without that, another request's Perform steps someone else's walk, or closes
   it with its own write. *)
Walked(r) ==
    /\ (s /= top) => (s.req = r)
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ steps[r].pending /= << >>
    /\ Head(steps[r].pending).tag = "PutDocument"
    /\ docs[OriginOf(steps[r].ev)] = steps[r].expect
    /\ clockA <= stamp[r]
    /\ LET k == IF s = top THEN 0 ELSE s.k
       IN
           IF clockA < stamp[r] THEN
               /\ s' = IF s = top THEN [req |-> r, k |-> 0] ELSE s
               /\ clockA' = clockA + 1
               /\ UNCHANGED <<stamp, docs, timeouts, outbox, steps, now>>
           ELSE IF k < Len(Walk(r)) THEN
               /\ s' = IF s = top THEN [req |-> r, k |-> 1]
                        ELSE [s EXCEPT !.k = @ + 1]
               /\ UNCHANGED <<stamp, clockA, docs, timeouts, outbox, steps, now>>
           ELSE
               /\ s' = top
               /\ docs'  = [docs EXCEPT
                                ![OriginOf(steps[r].ev)] = Head(steps[r].pending).body]
               /\ steps' = [steps EXCEPT ![r].pending = Tail(@)]
               /\ UNCHANGED <<stamp, clockA, timeouts, outbox, now>>

(* `Concrete`'s next-state relation, with the write replaced by the walk and
   everything else held still while one is in progress. Nothing may interleave
   with a walk -- not because a walk is delicate, but because it stands in for
   a step that was already atomic, so there was never an interleaving point
   inside it to lose. *)
NextS ==
    \/ /\ s = top
       /\ UNCHANGED <<s, clockA>>
       /\ \/ /\ \/ \E ev \in ExternalEvent : SubmitExternal(ev)
                \/ \E e \in timeouts : SubmitInternal(e)
                \/ Clock
             /\ UNCHANGED stamp
          \/ \E r \in DOMAIN steps :
                \/ /\ Process(r)
                   /\ stamp' = Set(stamp, r, now)
                \/ /\ Crash(r)
                   /\ stamp' = Del(stamp, r)
                \/ /\ Perform(r)
                   /\ ~( /\ steps[r].pending /= << >>
                         /\ Head(steps[r].pending).tag = "PutDocument"
                         /\ docs[OriginOf(steps[r].ev)] = steps[r].expect )
                   /\ stamp' = IF steps[r].pending = << >>
                                THEN Del(stamp, r)
                                ELSE stamp
    \/ \E r \in DOMAIN steps : Walked(r)

InitS ==
    Init /\ s = top /\ stamp = EmptyFn /\ clockA = 0

SpecS ==
    InitS /\ [][NextS]_varsS

-----------------------------------------------------------------------------

SendsInto(ob, ms) ==
    LET upto[n \in 0 .. Len(ms)] ==
          IF n = 0 THEN
              ob
          ELSE
              LET acc == upto[n - 1]
              IN  { x \in acc : MsgKey(x) /= MsgKey(ms[n]) } \cup {ms[n]}
    IN
        upto[Len(ms)]

ObjectsAt(d, o) ==
    LET D == [ p \in Origin |-> IF p = o THEN d ELSE docs[p] ]
    IN  [ i \in UNION { DOMAIN D[p] : p \in Origin } |-> D[i.origin][i] ]

(* Where the walk has got to. Read only while one is in progress. *)
At ==
    Walk(s.req)[s.k]

(* A message counts from the moment its write lands. *)
Landed(r) ==
    /\ steps[r].phase = "perform"
    /\ \A n \in DOMAIN steps[r].pending :
           steps[r].pending[n].tag /= "PutDocument"

SendsOf(r) ==
    LET q == SelectSeq(steps[r].pending, LAMBDA x : x.tag = "Send")
    IN  [ n \in 1 .. Len(q) |-> q[n].entry ]

Outstanding ==
    LET L == { r \in DOMAIN steps : Landed(r) }
        f[T \in SUBSET L] ==
            IF T = {} THEN
                outbox
            ELSE
                LET r == CHOOSE x \in T : TRUE
                IN  SendsInto(f[T \ {r}], SendsOf(r))
    IN
        f[L]

P == INSTANCE Abstract WITH
         now     <- clockA,
         objects <- IF s = top \/ s.k = 0 THEN Objects
                    ELSE ObjectsAt(At.doc, OriginOf(steps[s.req].ev)),
         outbox  <- IF s = top \/ s.k = 0 THEN Outstanding
                    ELSE SendsInto(Outstanding, At.sends)

(* THE PREMISE OF THE LAGGING CLOCK, checked rather than assumed: the abstract
   clock never passes the stamp of a write that can still land. Per document
   the CAS enforces it -- a later-stamped landing kills every earlier-stamped
   holder's expect -- so this can only break across documents, where
   out-of-stamp-order landings would need prophecy. One origin, no prophecy. *)
HoldersWalkable ==
    \A r \in DOMAIN steps :
        ( /\ steps[r].phase = "perform"
          /\ \E n \in DOMAIN steps[r].pending :
                 steps[r].pending[n].tag = "PutDocument"
          /\ docs[OriginOf(steps[r].ev)] = steps[r].expect )
        => clockA <= stamp[r]

Refines ==
    P!Safety

(* THE AUXILIARY VARIABLE MUST BE AUXILIARY. Hide the walk and what is left has
   to be a behaviour the executor already had. `Next` and `vars` here are
   `Concrete`'s own, and `s` is not among them, so every tick is a stuttering
   step and this asks that nothing else was added.

   The other direction -- that nothing was REMOVED -- has no mechanical check.
   It holds because the ticks stand in for a single atomic step, so the guards
   above forbid only interleavings that never existed. *)
Inert ==
    Init /\ [][Next]_vars

===============================================================================
