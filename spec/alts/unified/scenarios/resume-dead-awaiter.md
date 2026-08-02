# Scenario: resume-dead-awaiter

Model-generated from the counterexample that violates `NoDeadDispatch`.
5 steps. Run this against a real implementation, record the
trace, and replay it: a conformant server cannot produce it.

| # | call | why |
|---|---|---|
| 1 | `promise.create id=a timeoutAt=1 tags=resonate:target` |  |
| 2 | `promise.create id=b timeoutAt=1 tags=resonate:target` |  |
| 3 | `task.create id=a` |  |
| 4 | `task.suspend id=a` |  |
| 5 | `wait until now = 1` | promise is logically dead: a,b |
| 6 | `(background) promise timeout id=a` | **this is the call that must be refused** |

**Violates** `NoDeadDispatch` at step 6.

The recorded prefix is in `traces/gen-resume-dead-awaiter.ndjson`; it stops
one step short because TLC's JSON dump omits the violating state.
Run the full call list against a real server and record ITS trace --
that is the artifact worth replaying, because its states come from
the implementation rather than from the model.
