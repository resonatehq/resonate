----------------------------- MODULE RefineCdot -----------------------------
EXTENDS Concrete
AC == INSTANCE AbstractAction WITH objects <- Objects
RefinesCdot ==
    AC!Init /\ [][AC!Next \/ (AC!Next \cdot AC!Next)]_AC!vars
==============================================================================
