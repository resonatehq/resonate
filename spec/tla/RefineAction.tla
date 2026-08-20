----------------------------- MODULE RefineAction -----------------------------
(* Does the executor still refine the protocol when the protocol is written
   as plain TLA+ actions -- one event, one step, no batch?

   `Concrete` sweeps a document on access: some number of drains and the
   request land together, in one `PutDocument`. `[][Next]_vars` gives that
   concrete step ONE abstract step or none. AbstractAction has no step that
   is more than one event, so a sweep that drained anything and then served
   a request has no abstract step to be. *)
EXTENDS Concrete

AA == INSTANCE AbstractAction WITH objects <- Objects

RefinesAction ==
    AA!Safety

==============================================================================
