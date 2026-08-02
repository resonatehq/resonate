# Scenario: stranded-listener

Model-generated from the counterexample that violates `ObligationsAreDischargeable`.
2 steps. Run this against a real implementation, record the
trace, and replay it: a conformant server cannot produce it.

| # | call | why |
|---|---|---|
| 1 | `promise.create id=a timeoutAt=1 tags=resonate:target` |  |
| 2 | `promise.create id=b timeoutAt=2 tags=(no tags -- INTERNAL)` |  |
| 3 | `promise.register_listener id=a` | **this is the call that must be refused** |

**Violates** `ObligationsAreDischargeable` at step 3.

The recorded prefix is in `traces/gen-stranded-listener.ndjson`; it stops
one step short because TLC's JSON dump omits the violating state.
Run the full call list against a real server and record ITS trace --
that is the artifact worth replaying, because its states come from
the implementation rather than from the model.
