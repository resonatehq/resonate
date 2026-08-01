# Should an implementation repo adopt this model?

Short answer: **add it alongside the existing model, do not replace one with
it** — and the deciding fact is at the top.

## The unified model is not validated against any running system

All three source models were. resonate-pg recorded five traces from a live
Postgres running instrumented `resonate.sql` and replayed 39 transitions
through `Trace.tla` with exact post-state agreement on every modelled
variable, plus two injected faults to prove the validator discriminates.
resonate and resonate-on-convex did the equivalent.

This model has none of that. It is exhaustively **checked** and never
**confirmed to correspond to any code**. Its `MC_spec` result says "these
21 properties hold of *this machine*"; it does not say that machine is
`resonate.sql`, `server.ts`, or the Rust server.

So replacing a trace-validated model with this one trades evidence for
expressiveness. That is a bad trade even though this model asks better
questions.

## It is not a drop-in anyway

The state shapes differ, and each repo's `Trace.tla` hard-codes its own:

| | `resonate-pg/base.tla` | `Unified.tla` |
|---|---|---|
| task | `[exists, state, version, timeoutAt]` | `[exists, state, version, pid, timerKind, timerAt]` |
| promise | `[exists, state, timeoutAt, external, isTimer, hasTarget]` | `[exists, state, timeoutAt, kind]` |
| also | — | `delivered`, `claim`, and six response ghosts |

`Trace.tla:145` compares `tasks[i].timeoutAt` directly, and the harness emits
snapshots in that shape. Adoption means rewriting the validator and
re-instrumenting — not editing a config.

## They answer different questions

`base.tla` is annotated `resonate.sql:NNN` line by line. It answers *what does
this SQL actually do*. `Unified.tla` cites specification handler ids (P-02,
T-09, R5). It answers *does this behaviour conform*.

Both are worth having. Losing the code-fidelity anchoring is how a model
quietly stops being about the system it is named after.

## What an implementation repo gains

Real, and different per repo:

- **The response channel.** It found resonate-pg's BUG-2 response half — the
  defect that repo's own report records as beyond its model's reach.
- **A worker and fencing layer.** `AtMostOneValidClaim` is entirely new
  evidence for resonate-pg and resonate-on-convex, neither of which models
  workers at all.
- **Fault injection and liveness**, for the repos lacking them.
- **`NoDeadDispatch` at the resume site** — the divergence all three models
  contained and none probed.

## Recommended shape

1. Keep the existing trace-validated model unchanged. It is the code-fidelity
   artifact.
2. Add this model alongside it, with that repo's profile (`MC_pg.cfg`,
   `MC_server.cfg`, `MC_convex.cfg`) as its conformance statement. A fix
   branch flips the corresponding switches to `TRUE`, so the fix is verified
   rather than argued.
3. **Canonical home is `resonate-specification`.** Vendor or submodule it.
   Three hand-copied forks will drift, and three models that disagree is worse
   than three models with different jobs.
4. **Then port trace validation to this model.** Highest-value item by a wide
   margin: validated against all three real systems, its `MC_spec` result
   would carry weight it currently does not.

   Note what (4) costs. Validating the **response channel** requires the
   harness to record *what each RPC returned*, which no current harness does —
   they snapshot state. That is new instrumentation work in every repo, and it
   is exactly what would make the response channel credible rather than merely
   plausible.

Do 1-3 now; treat 4 as the real project.
