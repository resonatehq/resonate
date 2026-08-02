# The model writes the test scenarios

The recorded traces are scripted happy paths, so they never build the
conditions the known bugs need — which is why `COVERAGE.md` has a column of
❌ next to "detected in the implementation". The fix is not to hand-write
cleverer scenarios. The model already knows how to reach every bug condition:
a mutation's **counterexample is the shortest execution that gets there.**

## The loop

```
mutate the spec to reproduce a defect        (run-mutations.sh)
  -> TLC yields the shortest execution reaching it
  -> emit it as a call list                  (gen-scenarios.sh)
  -> run that against the real server        (each repo's harness)
  -> record the trace, replay it             (validate-traces.sh)
  -> refusal = the bug, detected, located at the exact call
```

Nothing in step 3 is automated here — it needs each implementation's harness
— but steps 1, 2 and 5 are, and step 3 is now a short scripted call list
instead of a guess.

## What is generated

`./gen-scenarios.sh` writes, per bug class:

* `scenarios/<bug>.md` — the ordered calls, with the reason each one matters.
* `traces/gen-<bug>.ndjson` — the model's own states along that path.

```
| # | call                                                | why                          |
|---|-----------------------------------------------------|------------------------------|
| 1 | promise.create id=a timeoutAt=1 tags=resonate:target |                              |
| 2 | wait until now = 1                                  | promise is logically dead: a |
| 3 | task.halt id=a                                      | must be refused              |
```

Three calls. A conformant server refuses the third; `resonate.sql` returns
`200`. Compare with the effort of deriving that by hand from
`NoHaltOnDead`'s definition.

| scenario | calls | property it violates | the bug it reproduces |
|---|---|---|---|
| `halt-on-dead` | 3 | `NoHaltOnDead` | resonate-pg BUG-4 |
| `dead-dispatch-claim` | 3 | `NoDeadDispatch` | resonate-pg BUG-2 |
| `dead-redispatch` | 3 | `NoDeadDispatch` | resonate-pg BUG-5 |
| `stranded-listener` | 3 | `ObligationsAreDischargeable` | resonate-pg BUG-1 |
| `resume-dead-awaiter` | 6 | `NoDeadDispatch` | convex BUG-1, and the gap all three share |
| `unprojected-response` | — | `ResponsesAreProjected` | resonate-pg BUG-2, response half |

## Two limits, stated

**The generated NDJSON is a prefix, not a replayable buggy trace.** TLC's
`-dumpTrace json` omits the *violating* state, so the file stops one call
short. The **call list is complete** — it comes from the text output, which
is authoritative — and that is what a harness needs. The trace worth
replaying is the one recorded from the real server anyway: its states come
from the implementation rather than from the model, which is the whole point.

**A model-generated trace replayed against the model proves nothing.** It is
circular by construction. These scenarios are inputs for a real system, not
evidence about one.

## What made this possible

Counterexamples used to be unreadable. With `Next == Step /\ ObsUpdate` TLC
attributed every step to the line of the conjunction, so an error trace said
`<Next line 736>` at every step and never named the action. Each action is
now a named top-level operator and `Next` is their bare disjunction, so
traces read:

```
State 2: <APromiseCreate ...>
State 3: <ATick ...>
State 4: <ATaskHalt ...>
```

That change is what turns a counterexample into a scenario, and it improves
every error trace this model will ever print.

## Two silent failures found while building this

Both are recorded because both would have produced confident wrong answers.

1. **`TaskClaim` was serving the raw promise row.** The hard-wiring pass
   never matched that line, so the committed *specification* model contained
   resonate-pg BUG-2. Worse, the `task.create serves raw row` mutation had
   been anchored on a string that matched `PromiseGet` first, so it mutated
   the wrong site and its `MUTATIONS.md` row was mislabeled. A model-check
   passing is not evidence the model says what you think it says.

2. **Two `gen-scenarios.sh` runs overlapped** and raced on `Unified.tla`,
   leaving a mutation applied in the working tree. Both scripts now take an
   exclusive `flock` and refuse to start if another run holds it.
