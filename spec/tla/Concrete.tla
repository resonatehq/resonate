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
          NoValue, Silent, Materialise, RetryTimeout, MaxTime, MaxVersion,
          Fenced

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

A == INSTANCE Abstract WITH objects <- Objects

(***************************************************************************)
(* Three of this machine's six variables map to NOTHING, and that is the   *)
(* whole shape of the result:                                              *)
(*                                                                         *)
(*     etags      a fence. Machinery for keeping a promise the abstract    *)
(*                machine states without it                                *)
(*     timeouts   an index, so an executor need not scan for what is due.  *)
(*                `Abstract` reads deadlines off the objects               *)
(*     steps      work in flight. `Abstract` has no interval between       *)
(*                deciding and writing, so it has nothing to track         *)
(*                                                                         *)
(* `outbox` and `now` map by name; only `objects` needs saying. So every   *)
(* transition here that touches nothing but the three unmapped variables   *)
(* is a STUTTER upstairs -- submitting, deciding, arming, disarming, and   *)
(* a refused compare-and-swap sending a step back to decide again.         *)
(*                                                                         *)
(* Exactly one transition is not a stutter: the `PutObject` that lands.    *)
(* THAT is where the abstract machine takes its single atomic step, and it *)
(* is why the fence matters -- the etag is what makes the write land       *)
(* against the state it was decided against, which is the only way one     *)
(* instant can stand for the whole decision.                               *)
(*                                                                         *)
(* Nothing above needed a `Restart` in `Abstract`. An earlier draft mapped *)
(* `steps` across, so a refused CAS showed up as an abstract step running  *)
(* backwards and had to be legislated for. Hiding the bookkeeping is what  *)
(* removed the need -- the retry is not a smaller abstract step, it is no  *)
(* abstract step at all.                                                   *)
(***************************************************************************)

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
             VariantGetUnsafe("Timeout", ev).id.origin
      [] OTHER -> CHOOSE o \in Origin : TRUE

Fresh(ev) ==
    [ ev |-> ev, phase |-> "process", pending |-> << >>, res |-> Silent,
      expect |-> 0, at |-> 0, org |-> OriginOf(ev), retries |-> 0 ]

Put(f, k, v) == [x \in (DOMAIN f) \cup {k} |-> IF x = k THEN v ELSE f[x]]
Drop(f, k)   == [x \in (DOMAIN f) \ {k}    |-> f[x]]

SubmitExternal(ev) ==
    /\ ev \in A!ExternalEvent
    /\ \E r \in Rid \ DOMAIN steps : steps' = Put(steps, r, Fresh(ev))
    /\ UNCHANGED <<docs, etags, timeouts, outbox, now>>

(* THE ASYMMETRY, stated plainly, because it is the point of having two
   machines rather than one:

     Abstract may take an internal step whenever the TIMEOUT VALUES ON AN
     OBJECT allow it.

     Concrete may take one only when there is an ARMED ENTRY for it.

   So this machine is strictly more restricted, and the two directions of
   that are not the same:

     - for SAFETY it is free. Fewer behaviours is what refinement wants,
       and an entry that fires with nothing due merely produces a step
       that writes nothing -- a stutter upstairs.

     - for LIVENESS it is the whole risk. `A!Fairness` says a deadline
       that has come due is eventually acted on. Down here that can only
       happen if the wheel holds an entry for it, so an object carrying a
       deadline the wheel has forgotten is a timeout that never fires and
       an abstract obligation this machine cannot meet.

   Which is why `C_WheelComplete` is not a side condition somebody
   thought to write down. It is exactly what `A!Spec` demands of anything
   that keeps an index instead of reading the objects, and if it holds,
   every internal step `Abstract` may take is one `Concrete` may take too.
   @type: $event => Bool; *)
Fires(ev) ==
    CASE VariantTag(ev) = "Timeout" ->
             LET d == VariantGetUnsafe("Timeout", ev) IN
             \E e \in timeouts : /\ e.id   = d.id
                                /\ e.kind = d.kind
                                /\ e.at  <= now
      [] OTHER -> A!Fires(ev)

SubmitInternal(ev) ==
    /\ ev \in A!InternalEvent
    /\ Fires(ev)
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
                                  ![r].expect  = etags[o],
                                  ![r].at      = now]
    /\ UNCHANGED <<docs, etags, timeouts, outbox, now>>

(* One effect. A write whose document has moved since the read is REFUSED,
   and the step goes back to deciding -- which under the abstraction is
   nothing at all.

   THE FENCE COVERS THE CLOCK AS WELL AS THE DOCUMENT, and that is not
   belt-and-braces -- the refinement demanded it. With only the etag
   compared, TLC returned: decide at `now = 0`, `Clock` ticks, write lands
   at `now = 1`. Upstairs the write IS the whole step, so the abstract
   machine runs the handler at the instant the write lands and gets a
   different answer -- a `createdAt`, a `settledAt`, a born-settled promise
   where the concrete decided a pending one. A decision is only valid at
   the instant it was made, and a store etag says nothing about the clock.

   Verus takes the other road: `Phase::Perform` freezes `at` at prepare
   time and every effect uses it, so the skew is absorbed rather than
   refused. That works against the Lean, where `now` is an argument to
   each step rather than a variable running underneath it. Here it is a
   variable, so the choice is to re-decide, and `steps[r].at` is what the
   comparison is against.

   `Fenced` is a CONSTANT so the fence can be switched OFF and the
   refinement made to prove its own necessity: with `Fenced = FALSE` a
   stale decision lands, and `Spec => A!Spec` should fail and hand back
   the trace that shows why. *)
Perform(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ IF steps[r].pending = << >>
       THEN /\ steps' = Drop(steps, r)
            /\ UNCHANGED <<docs, etags, timeouts, outbox>>
       ELSE LET e == Head(steps[r].pending)
                o == steps[r].org
            IN CASE VariantTag(e) = "PutObject" ->
                    IF Fenced => (etags[o] = steps[r].expect /\ now = steps[r].at)
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

(* The fairness `Abstract` asks of itself, asked of this executor. It has
   to be here: `A!Spec` carries `A!Fairness`, and a machine that may
   stall forever cannot implement a machine that may not.

   `SubmitInternal` is STRONGLY fair, and that is not a modelling
   convenience -- weak fairness was not enough and TLC said why. Upstairs
   an internal step is available whenever an object's deadline says so,
   full stop. Down here it is available only when a step slot is free,
   and `|Rid|` is finite. So the counterexample was a cycle in which
   clients kept arriving, the slots kept filling, and a due timeout with
   a properly armed entry was never once submitted -- legal under weak
   fairness, because the action was not CONTINUOUSLY enabled, only
   enabled again and again.

   Strong fairness is the honest reading of what the abstract machine
   demands: an executor must not indefinitely prefer client work to work
   that has come due. That is a scheduling obligation on the real server
   -- a fair choice between the API path and the timer path -- and it is
   the kind of requirement that is invisible until liveness is checked
   against a bounded resource. *)
Fairness ==
    /\ \A r  \in Rid             : WF_vars(Process(r) \/ Perform(r))
    /\ \A ev \in A!InternalEvent : SF_vars(SubmitInternal(ev))
    /\ WF_vars(Clock)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE REFINEMENT                                                          *)
(*                                                                         *)
(* The statement we MEAN is                                                *)
(*                                                                         *)
(*     THEOREM Spec => A!Spec                                              *)
(*                                                                         *)
(* -- every behaviour of the chunked, fenced machine is, once you read a   *)
(* document as the objects it holds, a behaviour of the abstract machine.  *)
(*                                                                         *)
(* No existential is needed any more. `Abstract` has no `steps` and no     *)
(* `timeouts` to fill in, so there is no hidden variable to quantify away  *)
(* and no witness to choose -- the mapping is total and there is only one  *)
(* of it. That is worth more than the convenience: it means the theorem no *)
(* longer depends on a choice I made, and an executor with entirely        *)
(* different bookkeeping is measured by the same statement rather than     *)
(* having to supply its own.                                               *)
(*                                                                         *)
(* `A!Spec` and not `A!Safety`: the obligation includes the abstract       *)
(* machine's FAIRNESS. Refining only safety would let an implementation    *)
(* satisfy the specification by doing nothing -- accepting a request,      *)
(* deciding, and stalling forever is safe, and is not an implementation of *)
(* anything.                                                               *)
(*                                                                         *)
(* Note the direction. It does NOT say the two machines admit the same     *)
(* behaviours; the fence exists precisely to admit FEWER.                  *)
(*                                                                         *)
(* And it is a real obligation rather than a formality: the first time it  *)
(* was checked it FAILED, on a refused compare-and-swap sending a step     *)
(* from "perform" back to "process". `Abstract` had no action that went    *)
(* backwards, so every behaviour containing a retry was outside the        *)
(* specification. `Abstract!Restart` is what that failure bought.          *)
(*                                                                         *)
(* That failure is gone, and the way it went is the lesson. It was never a *)
(* fact about the protocol -- it came from mapping `steps` across, so that *)
(* an executor's retry had to be legislated for upstairs. Take the         *)
(* bookkeeping out of the abstract machine and the retry stops being a     *)
(* step it has to permit. `Abstract!Restart` is gone with it.              *)
(***************************************************************************)

Refinement == A!Spec

THEOREM Spec => A!Spec

(* The invariants the abstract machine fails, asked of this one. That the
   fence restores them is the other half of the result, and it is not
   implied by the refinement -- refinement bounds what this machine MAY
   do, and says nothing about what it GUARANTEES. *)
C_TypeOK          == A!TypeOK
C_UnitCoherent    == A!UnitCoherent

(* The wheel invariants live HERE now, because the wheel does. They are
   not among the 95 and could not be: they say an INDEX agrees with the
   deadlines it indexes, and the protocol has no index.

   `WheelSound` fails, and should: arming before writing admits an entry
   for an object that is not there yet, which is noise the handler
   re-checks away. `WheelComplete` is the one that matters -- a deadline
   nothing holds is a timeout that never fires, and nothing recovers. *)
C_WheelSound ==
    \A e \in timeouts :
        /\ e.id \in DOMAIN Objects
        /\ A!Deadline(Objects[e.id], e.kind) = e.at

C_WheelComplete ==
    \A o \in DOMAIN Objects, k \in A!DeadlineKind :
        A!Deadline(Objects[o], k) /= A!NoTime =>
            [at |-> A!Deadline(Objects[o], k), id |-> o, kind |-> k] \in timeouts

CT_preserved_settled_promise_record    == [][A!preserved_settled_promise_record]_vars
CT_consistent_promise_settlement_stamp == [][A!consistent_promise_settlement_stamp]_vars

=============================================================================
