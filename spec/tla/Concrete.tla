-------------------------------- MODULE Concrete --------------------------------
(***************************************************************************)
(* The same protocol under the Verus executor's discipline: ONE DOCUMENT   *)
(* PER ORIGIN, and a compare-and-swap on it.                               *)
(*                                                                         *)
(* `Abstract` deliberately has no fence, and TLC said what that costs: two *)
(* steps read one object, both write, the second erases the first --       *)
(* `preserved_settled_promise_record` and `WheelComplete` fall together.   *)
(* This module is the other half of the experiment.                        *)
(*                                                                         *)
(* IT CHANGES NOTHING ABOUT THE PROTOCOL. The handlers, the effects, the   *)
(* alphabet, the wheel and the invariants all come from `Abstract` through *)
(* the instance below, so a difference in behaviour is a difference in the *)
(* EXECUTOR and cannot be anything else.                                   *)
(*                                                                         *)
(* Two changes, both from `src/concrete_spec/executor.rs`:                 *)
(*                                                                         *)
(*   - the store is keyed by ORIGIN, and one document holds every object   *)
(*     at that origin. This is `Workflow.promises: Map<Id, Promise>` --    *)
(*     plural -- and it is what makes a multi-unit write single-document   *)
(*     whenever the units share an origin.                                 *)
(*                                                                         *)
(*   - every write carries the etag its decision was read under            *)
(*     (`expect: etag_of(held)`). If the document has moved, the write is  *)
(*     REFUSED and the step goes back to deciding with `retries + 1` --    *)
(*     `restart(s, rid, ..)`. Nothing stale lands.                         *)
(*                                                                         *)
(* THE NAMES ARE DELIBERATELY THE SAME. `timeouts`, `outbox`, `steps` and  *)
(* `now` mean here exactly what they mean in `Abstract`, and the instance  *)
(* below leaves them alone. Only `objects` needs saying, because only      *)
(* `objects` is represented differently -- which is the whole content of   *)
(* this refinement, and it should be the only thing in the `WITH`.         *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, Variants, Apalache

CONSTANTS Origin, Rest, Address, Pid, Value, Ttl, Rid, Implemented, NoPid, NoAddr,
          NoValue, Silent, Materialise, RetryTimeout, MaxTime, MaxVersion

VARIABLES
    docs,       \* [ORIGIN -> ($id -> $object)] -- one document per origin
    etags,      \* [ORIGIN -> Int] -- what a compare-and-swap compares
    timeouts,   \* the wheel. Same variable, same meaning as Abstract's
    outbox,     \* likewise
    steps,      \* likewise, plus `expect`, `org` and `retries`
    now         \* likewise

vars == <<docs, etags, timeouts, outbox, steps, now>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE ABSTRACTION FUNCTION                                                *)
(*                                                                         *)
(* What a chunked store denotes: the flat one that forgets the chunking.   *)
(* An identifier already carries the origin it belongs to, so finding the  *)
(* document an object lives in is `i.origin` and nothing else -- which is  *)
(* what the pair-shaped identifier was for.                                *)
(*                                                                         *)
(* The etags denote NOTHING. There is no abstract variable they map to,    *)
(* and that is the point of them: a fence is machinery for keeping a       *)
(* promise the abstract machine states without it.                         *)
(***************************************************************************)

Objects == [ i \in UNION { DOMAIN docs[o] : o \in Origin } |-> docs[i.origin][i] ]

(* A concrete step denotes the abstract step with its bookkeeping dropped.
   `expect` and `retries` are how this executor keeps its promise, not
   part of what was promised. *)
Steps ==
    [ r \in DOMAIN steps |-> [ ev      |-> steps[r].ev,
                               phase   |-> steps[r].phase,
                               pending |-> steps[r].pending,
                               res     |-> steps[r].res ] ]

A == INSTANCE Abstract WITH objects <- Objects, steps <- Steps

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE EXECUTOR                                                            *)
(***************************************************************************)

(* Which document a step touches. Every implemented handler reads and
   writes one origin; the unimplemented ones write nothing, so any answer
   serves. *)
OriginOf(ev) ==
    CASE VariantTag(ev) = "PromiseCreate" ->
             VariantGetUnsafe("PromiseCreate", ev).req.id.origin
      [] VariantTag(ev) = "PromiseSettle" ->
             VariantGetUnsafe("PromiseSettle", ev).req.id.origin
      [] VariantTag(ev) = "Timeout" ->
             VariantGetUnsafe("Timeout", ev).entry.id.origin
      [] OTHER -> CHOOSE o \in Origin : TRUE

Fresh(ev) ==
    [ ev |-> ev, phase |-> "process", pending |-> << >>, res |-> Silent,
      expect |-> 0, org |-> OriginOf(ev), retries |-> 0 ]

Put(f, k, v) == [x \in (DOMAIN f) \cup {k} |-> IF x = k THEN v ELSE f[x]]
Drop(f, k)   == [x \in (DOMAIN f) \ {k}    |-> f[x]]

SubmitExternal(ev) ==
    /\ ev \in A!ExternalEvent
    /\ \E r \in Rid \ DOMAIN steps : steps' = Put(steps, r, Fresh(ev))
    /\ UNCHANGED <<docs, etags, timeouts, outbox, now>>

SubmitInternal(ev) ==
    /\ ev \in A!InternalEvent
    /\ A!Fires(ev)
    /\ \E r \in Rid \ DOMAIN steps : steps' = Put(steps, r, Fresh(ev))
    /\ UNCHANGED <<docs, etags, timeouts, outbox, now>>

(* Read ONE DOCUMENT, decide against it, and remember the etag it was read
   under. The handler is `Abstract`'s: the protocol does not know it is
   being run by a different executor. *)
Process(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "process"
    /\ LET o   == steps[r].org
           env == [ objects  |-> docs[o],
                    timeouts |-> timeouts,
                    outbox   |-> outbox,
                    now      |-> now,
                    mat      |-> Materialise,
                    config   |-> [retryTimeout |-> RetryTimeout] ]
           out == A!Handle(steps[r].ev, env)
       IN  steps' = [steps EXCEPT ![r].phase   = "perform",
                                  ![r].pending = out.effects,
                                  ![r].res     = out.res,
                                  ![r].expect  = etags[o]]
    /\ UNCHANGED <<docs, etags, timeouts, outbox, now>>

(* One effect. A write whose document has moved since the read is REFUSED,
   and the step goes back to deciding -- which under the abstraction is
   `Abstract!Restart`. *)
Perform(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ IF steps[r].pending = << >>
       THEN /\ steps' = Drop(steps, r)
            /\ UNCHANGED <<docs, etags, timeouts, outbox>>
       ELSE LET e == Head(steps[r].pending)
                o == steps[r].org
            IN CASE VariantTag(e) = "PutObject" ->
                    IF etags[o] = steps[r].expect
                    THEN /\ docs'  = [docs EXCEPT ![o] =
                                         LET w == VariantGetUnsafe("PutObject", e)
                                         IN  Put(docs[o], w.id, w.obj)]
                         /\ etags' = [etags EXCEPT ![o] = @ + 1]
                         /\ steps' = [steps EXCEPT ![r].pending = Tail(@)]
                         /\ UNCHANGED <<timeouts, outbox>>
                    ELSE /\ steps' = [steps EXCEPT ![r].phase   = "process",
                                                   ![r].pending = << >>,
                                                   ![r].res     = Silent,
                                                   ![r].retries = @ + 1]
                         /\ UNCHANGED <<docs, etags, timeouts, outbox>>
                 [] VariantTag(e) = "ArmTimeout" ->
                    /\ timeouts' = timeouts \cup {VariantGetUnsafe("ArmTimeout", e).entry}
                    /\ steps'    = [steps EXCEPT ![r].pending = Tail(@)]
                    /\ UNCHANGED <<docs, etags, outbox>>
                 [] VariantTag(e) = "DisarmTimeout" ->
                    /\ timeouts' = timeouts \ {VariantGetUnsafe("DisarmTimeout", e).entry}
                    /\ steps'    = [steps EXCEPT ![r].pending = Tail(@)]
                    /\ UNCHANGED <<docs, etags, outbox>>
                 [] OTHER ->
                    /\ outbox' = LET en == VariantGetUnsafe("Send", e).entry
                                 IN  {x \in outbox : A!MsgKey(x) /= A!MsgKey(en)} \cup {en}
                    /\ steps'  = [steps EXCEPT ![r].pending = Tail(@)]
                    /\ UNCHANGED <<docs, etags, timeouts>>
    /\ UNCHANGED now

Crash(r) ==
    /\ r \in DOMAIN steps
    /\ steps' = Drop(steps, r)
    /\ UNCHANGED <<docs, etags, timeouts, outbox, now>>

Clock ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<docs, etags, timeouts, outbox, steps>>

Init ==
    /\ docs     = [o \in Origin |-> SetAsFun({})]
    /\ etags    = [o \in Origin |-> 0]
    /\ timeouts = {}
    /\ outbox   = {}
    /\ steps    = SetAsFun({})
    /\ now      = 0

Next ==
    \/ \E ev \in A!ExternalEvent : SubmitExternal(ev)
    \/ \E ev \in A!InternalEvent : SubmitInternal(ev)
    \/ \E r \in DOMAIN steps : Process(r) \/ Perform(r) \/ Crash(r)
    \/ Clock

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE REFINEMENT                                                          *)
(*                                                                         *)
(* This is the statement being made:                                       *)
(*                                                                         *)
(*     THEOREM Spec => A!Safety                                            *)
(*                                                                         *)
(* -- every behaviour of the chunked, fenced machine is, once you read a   *)
(* document as the objects it holds, a behaviour the abstract machine      *)
(* already permits.                                                        *)
(*                                                                         *)
(* Note which direction that runs. It does NOT say the two machines admit  *)
(* the same behaviours; the fence exists precisely to admit FEWER, and     *)
(* `WheelComplete` holds here and fails there. What it says is that the    *)
(* fence never buys its safety by doing something the protocol did not     *)
(* allow -- no extra write, no reordering, no state the abstract machine   *)
(* has no name for.                                                        *)
(*                                                                         *)
(* And it is a real obligation rather than a formality: the first time it  *)
(* was checked it FAILED, on a refused compare-and-swap sending a step     *)
(* from "perform" back to "process". `Abstract` had no action that went    *)
(* backwards, so every behaviour containing a retry was outside the        *)
(* specification. `Abstract!Restart` is what that failure bought.          *)
(***************************************************************************)

Refinement == A!Safety

THEOREM Spec => A!Safety

(* The invariants the abstract machine fails, asked of this one. That the
   fence restores them is the other half of the result, and it is not
   implied by the refinement -- refinement bounds what this machine MAY
   do, and says nothing about what it GUARANTEES. *)
C_TypeOK          == A!TypeOK
C_UnitCoherent    == A!UnitCoherent
C_WheelSound      == A!WheelSound
C_WheelComplete   == A!WheelComplete

CT_preserved_settled_promise_record    == [][A!preserved_settled_promise_record]_vars
CT_consistent_promise_settlement_stamp == [][A!consistent_promise_settlement_stamp]_vars

=============================================================================
