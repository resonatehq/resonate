-------------------------------- MODULE Concrete --------------------------------
(***************************************************************************)
(* The same protocol under the Verus executor's discipline: ONE DOCUMENT   *)
(* PER ORIGIN, and a compare-and-swap on it.                               *)
(*                                                                         *)
(* `Resonate` deliberately has no fence, and TLC answered what that costs: *)
(* two steps read one object, both write, the second erases the first --   *)
(* `preserved_settled_promise_record` and `WheelComplete` fall together.   *)
(* This module is the other half of the experiment. It changes NOTHING     *)
(* about the protocol: the handlers, the effects, the alphabet and the     *)
(* wheel come from `Resonate` through the instance below, so a difference  *)
(* in behaviour is a difference in the EXECUTOR and cannot be anything     *)
(* else.                                                                   *)
(*                                                                         *)
(* Two changes, both from `src/concrete_spec/executor.rs`:                 *)
(*                                                                         *)
(*   - the store is keyed by ORIGIN, and one document holds every object   *)
(*     at that origin. This is `Workflow.promises: Map<Id, Promise>` --    *)
(*     plural -- and it is what makes a multi-unit write single-document   *)
(*     whenever the units share an origin.                                 *)
(*                                                                         *)
(*   - every `PutObject` carries the etag its decision was read under      *)
(*     (`expect: etag_of(held)`). If the document has moved, the write is  *)
(*     REFUSED and the step goes back to deciding with `retries + 1` --    *)
(*     `restart(s, rid, ...)`. Nothing is lost, and nothing stale lands.   *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, Variants, Apalache

CONSTANTS Origin, Rest, Address, Pid, Value, Ttl, Rid, NoPid, NoAddr, NoValue,
          Silent, Materialise, RetryTimeout, MaxTime, MaxVersion

VARIABLES
    docs,     \* [ORIGIN -> ($id -> $object)] -- one document per origin
    etags,    \* [ORIGIN -> Int] -- what a CAS compares
    timer,    \* the wheel, unchanged
    sent,     \* the outbox, unchanged
    cs,       \* [RID -> concrete step] -- carries `expect`, `org`, `retries`
    cnow

cvars == <<docs, etags, timer, sent, cs, cnow>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* WHAT THIS STATE DENOTES                                                 *)
(*                                                                         *)
(* The abstraction. A store chunked by origin denotes the flat store that  *)
(* forgets the chunking; the etags denote nothing at all, which is the     *)
(* point of them; a concrete step denotes the abstract step with its       *)
(* bookkeeping dropped.                                                    *)
(***************************************************************************)

Flat == [ i \in UNION { DOMAIN docs[o] : o \in Origin } |-> docs[i.origin][i] ]

AbsSteps ==
    [ r \in DOMAIN cs |-> [ ev      |-> cs[r].ev,
                            phase   |-> cs[r].phase,
                            pending |-> cs[r].pending,
                            res     |-> cs[r].res ] ]

(* `MCResonate` and not `Resonate`, so that both sides quantify over the
   SAME alphabet. Instancing the full machine made the refinement check
   evaluate `\E ev \in ExternalEvent` -- all seventeen kinds, with
   `SUBSET TaskRefT` inside one of them -- at every concrete state, for a
   comparison against three implemented handlers. That is not a stricter
   check, it is the same check with an irrelevant quantifier bolted on. *)
R == INSTANCE MCResonate
     WITH objects  <- Flat,
          timeouts <- timer,
          outbox   <- sent,
          steps    <- AbsSteps,
          now      <- cnow

-----------------------------------------------------------------------------

(* Which document a step touches. Every implemented handler reads and
   writes one origin; the unimplemented ones write nothing, so any answer
   is as good as any other. *)
OriginOf(ev) ==
    CASE VariantTag(ev) = "PromiseCreate" ->
             VariantGetUnsafe("PromiseCreate", ev).req.id.origin
      [] VariantTag(ev) = "PromiseSettle" ->
             VariantGetUnsafe("PromiseSettle", ev).req.id.origin
      [] VariantTag(ev) = "Timeout" ->
             VariantGetUnsafe("Timeout", ev).entry.id.origin
      [] OTHER -> CHOOSE o \in Origin : TRUE

CFresh(ev) ==
    [ ev |-> ev, phase |-> "process", pending |-> << >>, res |-> Silent,
      expect |-> 0, org |-> OriginOf(ev), retries |-> 0 ]

CPut(f, k, v) == [x \in (DOMAIN f) \cup {k} |-> IF x = k THEN v ELSE f[x]]
CDrop(f, k)   == [x \in (DOMAIN f) \ {k}    |-> f[x]]

CSubmitExternal(ev) ==
    /\ ev \in R!ExternalEvent
    /\ \E r \in Rid \ DOMAIN cs : cs' = CPut(cs, r, CFresh(ev))
    /\ UNCHANGED <<docs, etags, timer, sent, cnow>>

CSubmitInternal(ev) ==
    /\ ev \in R!InternalEvent
    /\ R!Fires(ev)
    /\ \E r \in Rid \ DOMAIN cs : cs' = CPut(cs, r, CFresh(ev))
    /\ UNCHANGED <<docs, etags, timer, sent, cnow>>

(* Read one document, decide against it, and remember the etag it was read
   under. The handler is `Resonate`'s -- the protocol does not know it is
   being run by a different executor. *)
CProcess(r) ==
    /\ r \in DOMAIN cs
    /\ cs[r].phase = "process"
    /\ LET o   == cs[r].org
           env == [ objects  |-> docs[o],
                    timeouts |-> timer,
                    outbox   |-> sent,
                    now      |-> cnow,
                    mat      |-> Materialise,
                    config   |-> [retryTimeout |-> RetryTimeout] ]
           out == R!Handle(cs[r].ev, env)
       IN  cs' = [cs EXCEPT ![r].phase   = "perform",
                            ![r].pending = out.effects,
                            ![r].res     = out.res,
                            ![r].expect  = etags[o]]
    /\ UNCHANGED <<docs, etags, timer, sent, cnow>>

(* The compare-and-swap. A write whose document has moved since the read is
   refused, and the step goes back to deciding. *)
CPerform(r) ==
    /\ r \in DOMAIN cs
    /\ cs[r].phase = "perform"
    /\ IF cs[r].pending = << >>
       THEN /\ cs' = CDrop(cs, r)
            /\ UNCHANGED <<docs, etags, timer, sent>>
       ELSE LET e == Head(cs[r].pending)
                o == cs[r].org
            IN CASE VariantTag(e) = "PutObject" ->
                    IF etags[o] = cs[r].expect
                    THEN /\ docs'  = [docs EXCEPT ![o] =
                                         LET w == VariantGetUnsafe("PutObject", e)
                                         IN  CPut(docs[o], w.id, w.obj)]
                         /\ etags' = [etags EXCEPT ![o] = @ + 1]
                         /\ cs'    = [cs EXCEPT ![r].pending = Tail(@)]
                         /\ UNCHANGED <<timer, sent>>
                    ELSE /\ cs' = [cs EXCEPT ![r].phase   = "process",
                                             ![r].pending = << >>,
                                             ![r].res     = Silent,
                                             ![r].retries = @ + 1]
                         /\ UNCHANGED <<docs, etags, timer, sent>>
                 [] VariantTag(e) = "ArmTimeout" ->
                    /\ timer' = timer \cup {VariantGetUnsafe("ArmTimeout", e).entry}
                    /\ cs'    = [cs EXCEPT ![r].pending = Tail(@)]
                    /\ UNCHANGED <<docs, etags, sent>>
                 [] VariantTag(e) = "DisarmTimeout" ->
                    /\ timer' = timer \ {VariantGetUnsafe("DisarmTimeout", e).entry}
                    /\ cs'    = [cs EXCEPT ![r].pending = Tail(@)]
                    /\ UNCHANGED <<docs, etags, sent>>
                 [] OTHER ->
                    /\ sent' = LET en == VariantGetUnsafe("Send", e).entry
                               IN  {x \in sent : R!MsgKey(x) /= R!MsgKey(en)} \cup {en}
                    /\ cs'   = [cs EXCEPT ![r].pending = Tail(@)]
                    /\ UNCHANGED <<docs, etags, timer>>
    /\ UNCHANGED cnow

CCrash(r) ==
    /\ r \in DOMAIN cs
    /\ cs' = CDrop(cs, r)
    /\ UNCHANGED <<docs, etags, timer, sent, cnow>>

CClock ==
    /\ cnow < MaxTime
    /\ cnow' = cnow + 1
    /\ UNCHANGED <<docs, etags, timer, sent, cs>>

CInit ==
    /\ docs  = [o \in Origin |-> SetAsFun({})]
    /\ etags = [o \in Origin |-> 0]
    /\ timer = {}
    /\ sent  = {}
    /\ cs    = SetAsFun({})
    /\ cnow  = 0

CExternal ==
       { Variant("PromiseCreate", [req |-> q]) : q \in R!CreateReq }
  \cup { Variant("PromiseSettle", [req |-> q]) : q \in R!SettleReq }

CInternal == { Variant("Timeout", [entry |-> e]) : e \in R!Entry }

CNext ==
    \/ \E ev \in CExternal : CSubmitExternal(ev)
    \/ \E ev \in CInternal : CSubmitInternal(ev)
    \/ \E r \in DOMAIN cs : CProcess(r) \/ CPerform(r) \/ CCrash(r)
    \/ CClock

CSpec == CInit /\ [][CNext]_cvars

-----------------------------------------------------------------------------
(***************************************************************************)
(* WHAT WE WANT TO KNOW                                                    *)
(*                                                                         *)
(* `Refines` is the refinement obligation: every behaviour of this machine *)
(* is a behaviour of `Resonate` under the abstraction above.               *)
(*                                                                         *)
(* The invariants are the other question, and the more interesting one --  *)
(* `WheelComplete` and `preserved_settled_promise_record` are exactly what *)
(* the abstract machine failed, so whether they hold here is whether the   *)
(* fence is SUFFICIENT.                                                    *)
(***************************************************************************)

Refines == R!Init /\ [][R!MCNext]_<<Flat, timer, sent, AbsSteps, cnow>>

C_WheelComplete   == R!WheelComplete
C_WheelSound      == R!WheelSound
C_UnitCoherent    == R!UnitCoherent
C_TypeOK          == R!TypeOK

CT_preserved_settled_promise_record    == [][R!preserved_settled_promise_record]_cvars
CT_consistent_promise_settlement_stamp == [][R!consistent_promise_settlement_stamp]_cvars

=============================================================================
