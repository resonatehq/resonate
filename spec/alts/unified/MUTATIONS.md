# Mutation experiments

The model has no behaviour switches. That is deliberate — see the header of
`Unified.tla`. But the switches did buy one thing that is worth keeping, and
this file is how to keep it honestly.

## What the switches were actually good for

Two different activities were tangled together:

1. **Encoding an implementation's known defect so its "profile" passes.**
   Self-defeating, and it demonstrably hid real bugs: `MC_convex.cfg` set
   `CallbackExternalGuard = FALSE`, which tells the model that suspending on
   an internal promise is expected. Nothing fired. The divergence was found
   only by replaying a real trace against the specification. **Gone, and good
   riddance.**

2. **Asking what a guard buys.** *"If this line were missing, what breaks,
   and in how many states?"* That is a legitimate and valuable question — it
   is how the resume gap was found — but it is a **mutation experiment**, not
   a conformance configuration, and calling it a profile made a deliberate
   fault look like a supported mode.

## How to run one

Edit the guard out of `Unified.tla`, check, put it back. The diff is the
experiment; record it here with its result.

```bash
# e.g. drop TIMEOUT ALWAYS WINS from the settlement cascade
#   Eligible(j) == Live(j)   ->   Eligible(j) == TRUE
java -XX:+UseParallelGC -cp .tla-lib/tla2tools.jar tlc2.TLC \
     -workers 4 -config MC.cfg MC
```

A mutation is only informative if a **named property** fails. If a guard can
be removed and everything still passes, that says the property set is too
weak — which is a finding about the model, not a licence for the mutant.

## Recorded experiments

Run them with `./run-mutations.sh`, which restores `Unified.tla` on exit.
Each mutation is a defect some implementation actually has, so the run
reproduces that defect **in the specification** and shows which property
catches it — without ever pretending the defect is a supported
configuration.

`MC.cfg` lists the invariants individually rather than as the `Safety`
conjunction, because TLC names the invariant it violated and a conjunction
would throw that attribution away — which is the only thing a mutation
experiment is for. (The first run of this suite reported `Safety` for every
mutation, which was useless.)

| mutation | property that fails | states in counterexample |
|---|---|---|
| drop TIMEOUT-WINS from the cascade | `NoDeadDispatch` | 7 |
| over-arm (arm every promise) | `ArmingIsExternalOnly` | 3 |
| ungate `task.halt` | `NoHaltOnDead` | 4 |
| `task.create` serves the raw row | ⏳ not yet run | |
| ungate `task.continue` | ⏳ not yet run | |
| drop the external guard on callbacks | ⏳ not yet run | |

The first three are from an actual run against this model. The remaining
three are queued, not assumed — they are not claimed until they have run.

## The gap this does not close

Thirteen of the model's properties have never failed under any mutation
recorded here:

`TypeOK`, `TaskHasPromise`, `TaskHasAtMostOneTimer`, `NonAcquiredTaskHasNoPid`,
`CallbackNotSelfReferential`, `SuspendedTaskHasCallback`,
`SettledPromiseHasNoSubscriptions`, `Stickiness`, `NoStrandedListener`,
`NoStrandedTask`, `TaskPromiseCoherence`, `AtMostOneValidClaim`,
`OutboxNeverAhead`

A property that never fails cannot be told apart from one with a typo in it.
Both source models mutation-tested their own checks — resonate-on-convex ran
five mutants, resonate-pg injected two faults — and this model has not been
tested that way. `AtMostOneValidClaim` is the most important of the thirteen,
because it is the only property in this family that states at-most-once
execution at all.
