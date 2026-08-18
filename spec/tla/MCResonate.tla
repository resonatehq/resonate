------------------------------- MODULE MCResonate -------------------------------
(***************************************************************************)
(* The model. `Resonate` is the machine; this narrows it to something a    *)
(* checker can finish.                                                     *)
(*                                                                         *)
(* The narrowing is the ALPHABET, and it costs nothing today: 14 of the 17 *)
(* external events have no handler yet, so each contributes a `Process`    *)
(* that emits nothing and a step that retires. They multiply the branching *)
(* factor of every `SubmitExternal` and inform nothing. Quantifying over   *)
(* the implemented three instead is not a weaker model of the machine --   *)
(* it is the same model over a smaller alphabet, and the line moves as     *)
(* handlers land.                                                          *)
(*                                                                         *)
(* Everything else -- the phases, the crash, the clock, the wheel -- is    *)
(* `Resonate` unchanged.                                                   *)
(***************************************************************************)
EXTENDS Resonate

MCExternal ==
       { Variant("PromiseCreate", [req |-> r]) : r \in CreateReq }
  \cup { Variant("PromiseSettle", [req |-> r]) : r \in SettleReq }

MCInternal == { Variant("Timeout", [entry |-> e]) : e \in Entry }

MCNext ==
    \/ \E ev \in MCExternal : SubmitExternal(ev)
    \/ \E ev \in MCInternal : SubmitInternal(ev)
    \/ \E r \in DOMAIN steps : Process(r) \/ Perform(r) \/ Crash(r)
    \/ Clock

=============================================================================
