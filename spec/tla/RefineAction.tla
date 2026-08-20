----------------------------- MODULE RefineAction -----------------------------
(* Does the executor refine the protocol stated as actions, with the batch
   as a subset of what is ready?

   The earlier answer, with no batch at all, was no, and TLC said so with a
   sweep that timed out one promise and created another in a single write.
   `Sweeps` gives the request an explicit pre-state, so a drained subset and
   the request that follows it are one step here too. *)
EXTENDS Concrete

AA == INSTANCE AbstractAction WITH objects <- Objects

RefinesAction ==
    AA!Safety

==============================================================================
