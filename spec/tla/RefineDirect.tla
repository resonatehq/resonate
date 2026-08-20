----------------------------- MODULE RefineDirect -----------------------------
(* The batch as a subset of what is ready, rather than a bounded sequence of
   guessed events. `MaxBatch` is gone: a subset of the ready set is already
   finite, so "one at a time, all at once, or anything between" needs no
   window. This asks whether Concrete still refines it. *)
EXTENDS Concrete

AD == INSTANCE AbstractDirect WITH objects <- Objects

RefinesDirect ==
    AD!Safety

==============================================================================
