------------------------------ MODULE RefinementBad ------------------------------
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
EXTENDS Concrete

VARIABLE s

(* `top` marks "not mid-walk". A record, so it cannot collide with a position. *)
top ==
    [top |-> "top"]

varsS ==
    <<s, docs, timeouts, outbox, steps, now>>

-----------------------------------------------------------------------------

(* THE STORES ONE WRITE PASSES THROUGH, one per abstract step, excluding the
   last -- the last is what the write itself produces, so the write is its own
   final tick. *)
Walk(r) ==
    LET doc   == docs[steps[r].org]
        swept == Sweep(doc, now)
        out   == Handle(steps[r].ev,
                        [ objects  |-> swept.doc, timeouts |-> timeouts,
                          outbox   |-> outbox,    now      |-> now,
                          config   |-> [retryTimeout |-> RetryTimeout] ])
        full  == IF out.effects = << >> THEN
                     swept.tr
                 ELSE
                     swept.tr \o << [ doc |-> PutsInto(doc, swept.fx \o out.effects),
                                      fx  |-> swept.fx \o out.effects ] >>
    IN
        IF full = << >> THEN << >> ELSE SubSeq(full, 1, Len(full) - 1)

(* ONE TICK, OR THE WRITE. A walk belongs to the request that started it --
   without that, another request's Perform steps someone else's walk, or closes
   it with its own write. *)
Walked(r) ==
    /\ (s /= top) => (s.req = r)
    /\ Heads(r, "PutObject")
    /\ FenceOk(r)
    /\ IF IF s = top THEN Walk(r) /= << >> ELSE s.k < Len(Walk(r)) THEN
           /\ s' = IF s = top THEN [req |-> r, k |-> 1] ELSE [s EXCEPT !.k = @ + 1]
           /\ UNCHANGED <<docs, timeouts, outbox, steps, now>>
       ELSE
           /\ s' = top
           /\ Commit(r)

(* `Concrete`'s next-state relation, with the write replaced by the walk and
   everything else held still while one is in progress. Nothing may interleave
   with a walk -- not because a walk is delicate, but because it stands in for
   a step that was already atomic, so there was never an interleaving point
   inside it to lose. *)
NextS ==
    \/ /\ s = top
       /\ UNCHANGED s
       /\ \/ \E ev \in ExternalEvent : SubmitExternal(ev)
          \/ \E e \in timeouts : SubmitInternal(e)
          \/ \E r \in DOMAIN steps :
                Process(r) \/ Crash(r) \/ Retire(r) \/ Refuse(r)
                           \/ Arm(r) \/ Disarm(r) \/ Emit(r)
          \/ Clock
    \/ \E r \in DOMAIN steps : Walked(r)

InitS ==
    Init /\ s = top

SpecS ==
    InitS /\ [][NextS]_varsS

-----------------------------------------------------------------------------

ObjectsAt(d, o) ==
    LET D == [ p \in Origin |-> IF p = o THEN d ELSE docs[p] ]
    IN  [ i \in UNION { DOMAIN D[p] : p \in Origin } |-> D[i.origin][i] ]

SaysInto(ob, fx) ==
    LET S == Says(fx)
    IN  { x \in ob : ~\E m \in S : MsgKey(x) = MsgKey(m) } \cup S

(* Where the walk has got to. Read only while one is in progress. *)
At ==
    Walk(s.req)[s.k]

P == INSTANCE AbstractBad WITH
         objects <- IF s = top THEN Objects
                    ELSE ObjectsAt(At.doc, steps[s.req].org),
         outbox  <- IF s = top THEN outbox
                    ELSE SaysInto(outbox, At.fx)

RefinesBad ==
    P!Safety

===============================================================================
