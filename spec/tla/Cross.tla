-------------------------------- MODULE Cross --------------------------------
(* Do the two encodings define the same protocol?

   `Init` and `Next` are AbstractDirect's. `Agree` says that at THIS state,
   for EVERY event, applying Abstract's effect list gives exactly the state
   AbstractDirect writes. Init is identical in both, so if `Agree` holds at
   every reachable state, the two transition relations are equal and the two
   reachable sets are equal -- by induction, not by comparing totals. *)
EXTENDS Requests

VARIABLES objects, outbox, now

A == INSTANCE Abstract
D == INSTANCE AbstractDirect

vars ==
    <<objects, outbox, now>>

Init ==
    D!Init

Next ==
    D!Next

Agree ==
    \A ev \in Event :
        LET out == A!Handle(ev, [ objects |-> objects,
                                  now     |-> now,
                                  config  |-> [retryTimeout |-> RetryTimeout] ])
            d   == D!Handle(D!State, ev)
        IN  /\ A!PutsInto(objects, out.effects) = d.objects
            /\ A!SaysInto(outbox,  out.effects) = d.outbox

(* `Agree` compares the two Handles. It does NOT compare what the batch
   folds: Abstract folds `Advance`, which short-circuits on `FiresIn`,
   while AbstractDirect folds `Handle` with no guard at all. They are the
   same fold only if a non-firing event changes nothing -- which is the
   claim that `FiresIn` merely restates the handlers' own guards. *)
AdvanceAgree ==
    \A ev \in InternalEvent :
        LET a == A!Advance([objects |-> objects, outbox |-> outbox], ev)
            d == D!Handle(D!State, ev)
        IN  /\ a.objects = d.objects
            /\ a.outbox  = d.outbox

==============================================================================
