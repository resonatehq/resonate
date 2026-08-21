----------------------------- MODULE RefineStutter -----------------------------
(* THE BATCH, MOVED OUT OF THE PROTOCOL AND INTO THE EXECUTOR'S BOOKKEEPING.

   `AbstractAction` states one protocol step per action -- guards, primed
   variables, nothing else. It cannot match a sweep that drains several
   deadlines and serves a request in one write, and TLC said so: promise `b`
   timed out and promise `a` was created in a single document write, which is
   two abstract steps.

   So the executor walks. `ConcreteS` carries a stuttering variable that steps
   through the intermediate stores that one write passes through, leaving its
   own variables alone; the mapping below reads that variable, so the protocol
   advances one action per tick. Lamport and Merz, `Auxiliary Variables in
   TLA+', section 5 -- the construction for a low-level step coarser than the
   high-level one. Not prophecy, which is for the other direction. *)
EXTENDS ConcreteS

ObjectsAt(d, o) ==
    LET D == [ p \in Origin |-> IF p = o THEN d ELSE docs[p] ]
    IN  [ i \in UNION { DOMAIN D[p] : p \in Origin } |-> D[i.origin][i] ]

SaysInto(ob, fx) ==
    LET S == Says(fx)
    IN  { x \in ob : ~\E m \in S : MsgKey(x) = MsgKey(m) } \cup S

(* Where the walk has got to. Only meaningful while one is in progress, which
   is the only place it is read. *)
At ==
    Walk(docs[steps[s.req].org], steps[s.req].ev, now)[s.k]

AA == INSTANCE Abstract WITH
          objects <- IF s = top THEN Objects
                     ELSE ObjectsAt(At.doc, steps[s.req].org),
          outbox  <- IF s = top THEN outbox
                     ELSE SaysInto(outbox, At.fx)

RefinesStutter ==
    AA!Safety

(* THE AUXILIARY VARIABLE MUST BE AUXILIARY. Projecting the walk away has to
   leave a behaviour the unaugmented executor already had. *)
C == INSTANCE Concrete WITH
         steps <- [ r \in DOMAIN steps |->
                      [ ev      |-> steps[r].ev,
                        phase   |-> steps[r].phase,
                        pending |-> steps[r].pending,
                        expect  |-> steps[r].expect,
                        at      |-> steps[r].at,
                        org     |-> steps[r].org ] ]

Inert ==
    C!Init /\ [][C!Next]_C!vars

==============================================================================
