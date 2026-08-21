----------------------------- MODULE RefineCdot -----------------------------
EXTENDS Concrete
AC == INSTANCE Abstract WITH objects <- Objects
RefinesCdot ==
    AC!Init /\ [][AC!Next \/ (AC!Next \cdot AC!Next)]_AC!vars
==============================================================================
