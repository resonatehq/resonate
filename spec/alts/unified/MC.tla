--------------------------------- MODULE MC ---------------------------------
EXTENDS Unified

CONSTANTS a, b, wA, wB, L

MCIds     == {a, b}
MCAddrs   == {L}
MCWorkers == {wA}
MCWorkers2 == {wA, wB}

StateConstraint ==
    /\ now <= MaxTime
    /\ \A i \in MCIds : tasks[i].version <= MaxVersion
    /\ Cardinality(delivered) <= 2

NoSuspended == \A i \in MCIds : tasks[i].state # "suspended"
NoDeadSuspender == \A i \in MCIds : ~(tasks[i].state = "suspended" /\ ~Live(i))

=============================================================================
