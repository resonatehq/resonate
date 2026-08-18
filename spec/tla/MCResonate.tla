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

MCSpec == Init /\ [][MCNext]_vars

(***************************************************************************)
(* The `.trans` entries, wrapped for TLC.                                  *)
(*                                                                         *)
(* Apalache takes a primed formula as an action invariant and checks it    *)
(* directly. TLC will not accept a prime in `INVARIANT`, so each one       *)
(* becomes `[][P]_vars` under `PROPERTY` -- the same claim, in the syntax  *)
(* that checker reads: P holds across every step that moves the state.     *)
(***************************************************************************)

T_preserved_settled_promise_record    == [][preserved_settled_promise_record]_vars
T_consistent_new_promise_born_clean   == [][consistent_new_promise_born_clean]_vars
T_consistent_promise_settlement_stamp == [][consistent_promise_settlement_stamp]_vars
T_consistent_settlement_fulfils_task  == [][consistent_settlement_fulfils_task]_vars

=============================================================================
