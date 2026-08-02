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

| mutation (= a defect some implementation has) | property that catches it | states |
|---|---|---|
| resume a dead awaiter | `NoDeadDispatch` | 7 |
| under-arm: arm only targeted | `ObligationsAreDischargeable` | 4 |
| over-arm: arm every promise | `ArmingIsExternalOnly` | 3 |
| listener on an internal promise | `ObligationsAreDischargeable` | 3 |
| callback on an internal promise | `ObligationsAreDischargeable` | 5 |
| ungate `task.create` claim | `NoDeadDispatch` | 4 |
| ungate `task.halt` | `NoHaltOnDead` | 4 |
| ungate `task.continue` | `NoDeadDispatch` | 5 |
| redispatch a dead task (R6) | `NoDeadDispatch` | 4 |
| `task.create` ungated **and** serving the raw row | `ResponsesAreProjected` | see note |

**No mutation survives.** Every one is caught by a named property, in 3-7
states.

### The two halves of resonate-pg BUG-2 are not independent

Serving the raw promise row from `task.create` is **unobservable on its
own**. T-02's liveness guard means the handler only fires when the promise is
live, and for a live promise the stored row EQUALS the projection — so the
"defect" changes no answer. Mutating only the response yields no violation;
TLC searches the entire space and finds nothing.

It becomes observable only in combination with the missing liveness guard,
which is how `resonate.sql` actually ships. The mutation therefore applies
both, and `gen`/`mut` accept several specs for exactly this reason.

An earlier row here claimed this mutation violated `ResponsesAreProjected` in
4 states. That was wrong twice over: the anchor had matched `PromiseGet`
rather than `TaskClaim`, so it mutated the wrong site — and the site it
should have mutated could not have violated anything by itself.

### A correction worth keeping

The first run of this suite silently produced three WRONG rows. Three anchors
failed to match (bad escaping), and `mut` then ran TLC against the *previous*
mutation's file and reported that verdict under the new mutation's name — so
`under-arm`, `over-arm` and `task.create serves the raw row` all reported
`NoDeadDispatch`. Two of those were plausible enough to believe, which is what
made it dangerous.

`mut` now prints `!! ANCHOR DID NOT APPLY` and restores the pristine module.
**An unapplied mutation is an error, never a data point.**

## The gap this does not close

Five properties do the catching: `NoDeadDispatch`,
`ObligationsAreDischargeable`, `NoHaltOnDead`, `ArmingIsExternalOnly`,
`ResponsesAreProjected`. **Sixteen have still never failed under any
mutation**, so nothing yet distinguishes them from properties with a typo in
them:

`TypeOK`, `TaskHasPromise`, `TaskHasAtMostOneTimer`, `NonAcquiredTaskHasNoPid`,
`CallbackNotSelfReferential`, `SuspendedTaskHasCallback`,
`SettledPromiseHasNoSubscriptions`, `Stickiness`, `NoStrandedListener`,
`NoStrandedTask`, `TaskPromiseCoherence`, `AtMostOneValidClaim`,
`OutboxNeverAhead`, `ResponsesNeverRegress`, `TaskResponsesNeverRegress`,
`TaskResponsesAreProjected`

`ResponsesNeverRegress` is a near-miss rather than untested: the raw-row
mutation does violate it, but `ResponsesAreProjected` fails first and TLC
stops there.

`AtMostOneValidClaim` is the most important of the sixteen, because it is the
only property in this family that states at-most-once execution at all — and
no mutation here targets fencing.

## What a mutation does NOT establish

A mutation shows the model **has a property that would catch a defect class**.
It says nothing about any implementation's code. Detecting a bug in a real
implementation needs a recorded trace that exercises it (`TRACES.md`) — and
the 14 traces currently in hand exercise only the external-waiter class.

This is the cost of dropping the profiles, stated plainly: the old
`MC_pg.cfg` model-checked resonate-pg's *semantics* exhaustively and found
`NoHaltOnDead` across the whole reachable space. Nothing does that now,
because the model no longer has a way to express "resonate-pg's semantics".
