# Differential testing: generator vs async engine

The SDK ships two parallel execution engines that must behave **identically,
including under failure**:

- the **generator** engine (`src/`) — `function*` + `yield* ctx.run`
- the **async** engine (`src/async/`) — `async function` + `await ctx.run`

Both are thin drivers over the **same** server, so the server's durable state —
the promise store, task store, and callback set — is the ground truth for
"behavior" (it is all that survives a crash). The tests below prove the two
engines reach the same durable state on the same workloads, on clean runs and
under injected failures.

The working stance is **detect-and-report**: the harness measures and documents
differences; it does not modify the async engine to fix them.

---

## TL;DR — how to run

```sh
# Layer A — matched-workload equivalence (no failures), under Jest
npx jest tests/equivalence
# (also runs as part of `npm test`)

# Layer B — differential simulation under failure
npm run dst:diff -- --func fibRfc --arg 5 --seeds 100
npm run dst:diff -- --func fibLfc --arg 9 --seeds 100 \
  --dropProb 0.12 --randomDelay 0.15 --duplProb 0.1

# Run a single engine under the deterministic simulator
npm run dst -- --engine async --func fibRfc
npm run dst -- --engine gen   --func fibLfc
```

---

## The equivalence oracle

Two runs are equivalent iff, at quiescence, their **canonical server snapshots**
are deep-equal AND their **root outcome** (resolved value / rejected error)
matches. The canonical snapshot is pulled from the server's `debug.snap` request
and projected into a stable, engine-independent shape (`tests/equivalence/oracle.ts`).

**Strong oracle = promises + tasks + callbacks + root outcome.**

Deliberately excluded from cross-engine comparison, with reasons:

| Excluded | Why |
|----------|-----|
| Absolute time (`timeoutAt`, `createdAt`, `settledAt`, timeouts) | Wall-clock in Layer A; in Layer B each engine emits a different number of ticks → different clock reads. Asserted per-run instead, not cross-engine. |
| Task `version` (fencing epoch) | Counts acquire/resume cycles, which differ with suspend/resume granularity and tick timing (e.g. how a sleep lines up with the server tick). Internal bookkeeping, not durable behavior. |
| `pid`, `corrId`, message head | Per-instance non-determinism. |
| Listeners, outbound messages | Reflect harness subscription and eager-vs-lazy message counts, not engine semantics. |

The harness also forces each workload function's JS `.name` to its registered
name, because local calls store `func.name` in the durable param
(`src/context.ts`) — otherwise `fibGen` vs `fibAsync` would differ purely by
function identity.

---

## Layer A — matched-workload equivalence (no failures)

Location: `tests/equivalence/` (picked up by `npm test`).

Each workload in `workloads.ts` is the same computation written in both engines'
idioms and registered under identical names. The harness (`harness.ts`) runs it
on both engines against fresh in-memory servers, then diffs the canonical
snapshot + root outcome. The generator engine is the reference.

```sh
npx jest tests/equivalence
```

Workloads covered (all pass):

- **fibonacci (local run)** — child id generation, dedup/replay, value propagation
- **fan-out / fan-in** — eager creation ordering + join
- **error propagation (caught / uncaught)** — rejected child, parent catches or rejects
- **sleep** — durable timer created + resolved on the server tick
- **detached** — independent child spawned and run to completion
- **human-in-the-loop (DPC)** — a bare durable promise resolved out of band

Result: the engines produce **identical durable state on every clean workload**.

---

## Layer B — differential simulation under failure

Location: `sim/src/differential.ts` (`npm run dst:diff`).

Runs the same `(seed, workload, fault schedule)` on **both** engines under the
deterministic simulator and asserts they converge. The simulator gives full
determinism (seeded RNG, `StepClock`).

```sh
npm run dst:diff -- [options]
```

| Option | Default | Meaning |
|--------|---------|---------|
| `--func` | `fibRfc` | workload (`fibLfc` local, `fibRfc` remote/suspend-resume) |
| `--arg` | `6` | argument to the workload |
| `--seed` / `--seeds` | `0` / `50` | starting seed and how many consecutive seeds |
| `--steps` | `500000` | max simulation steps before a run is "inconclusive" |
| `--workers` | `3` | worker processes |
| `--dropProb` | `0.05` | message drop probability |
| `--duplProb` | `0.05` | message duplicate probability |
| `--randomDelay` | `0.05` | message delay probability |
| `--deactivateProb` / `--activateProb` | `0.005` / `0.5` | worker crash / recovery |
| `--charFlipProb` | `0` | response corruption (off by default — see findings) |

### Why not compare messages byte-for-byte?

The async engine creates promises eagerly while the generator engine creates
them lazily, so the two emit **different message sequences even on a clean run**.
Fault injection is keyed on messages, so the same seed hits *different* logical
messages on each engine. The invariant that survives is the **final durable
state**: at quiescence the two engines must agree on the canonical
promise/task/callback store and the root outcome, and each run must independently
satisfy structural invariants (root reachability, no task left `acquired`, etc.).

### Results

**0 failures across 160+ seeds**, including heavy faults, for both call modes.
The engines converge whenever both quiesce; `inconclusive` means a run exceeded
the step budget (identical for both engines → not a divergence).

---

## Findings

### Runtime finding: shared `onMessage` assert under corruption — FIXED

Both engines used to guard `onMessage` with `util.assert(msg.kind === "execute")`
(`src/core.ts`, `src/async/core.ts`), and `util.assert` calls `process.exit(1)`.
Message corruption (`--charFlipProb`) could mangle a message's `kind` past the
worker's JSON-only validation, tripping that assert and killing the process.

Fixed by treating the network as a trust boundary instead of an invariant:
both engines' `onMessage` now warn-and-drop unexpected kinds, and the sim
network validates inbound messages with `isMessage` (the same guard production
HTTP uses in `src/network/http.ts`). Corruption is now a usable fault dimension
in both `dst` and `dst:diff`. (Corruption can still persist a mangled tag value —
e.g. `resonate:origin: "fibRfcX0"` — which the comparison correctly flags as a
diff; that is a property of the injected fault, not engine behavior.)

Note: single-engine `npm run dst` draws unspecified fault probabilities from the
seeded RNG (drop up to 0.5, corruption up to 0.15, ...) and now prints the
resolved config as a `[faults]` line at startup.

### Known divergences

1. **Default retry policy** — INTENDED, kept by decision: generator leaf default
   is `Exponential` (`src/computation.ts`), async is `Never` for everything
   (`src/async/context.ts`). Async leaves and workflows are runtime-
   indistinguishable (no `function*` marker), so retries are opt-in via an
   explicit `retryPolicy` in options. This is a documented migration step, not a
   bug. Neutralized in tests by pinning `Never` on every child call.
2. **Eager vs lazy creation** — the async engine has no `begin*` forms and creates
   promises eagerly; the generator engine creates lazily. Changes message
   order/count (not durable state). Cross-engine DST is restricted to
   call-and-await forms; snapshots are sorted before comparison.
3. ~~**No async trace**~~ — RESOLVED: the async engine now emits the full
   lifecycle subset of the trace (spawn/run/rpc/block/dedup/return/suspend, no
   await/resume — see `isWellFormedLifecycle` in `src/trace.ts` and
   `tests/async/async-trace.test.ts`).
4. ~~**Detached id hashing**~~ — RESOLVED: the async engine now threads
   `prefixId` and mints detached ids via `util.detachedId(prefixId, …)`,
   matching the generator; recursive/nested detached ids stay bounded.
5. ~~**`resonate:prefix` root tag**~~ — RESOLVED: the async root
   (`src/async/resonate.ts`) now sets `resonate:prefix`, `AsyncCore` reads it,
   and every child create request propagates it, matching the generator. The
   tag is no longer excluded from the canonical snapshot — the oracle now
   verifies it.

### Suggested follow-ups (separate from this detect-and-report work)

- ~~Converge the detached-id / `resonate:prefix` divergence (#4, #5).~~ Done.
- ~~Align the default retry policy between engines (#1).~~ Decided: keep `Never`
  everywhere in the async engine; retries are explicit (see #1).
- ~~Harden the shared `onMessage` assert against malformed input rather than
  `process.exit`.~~ Done: warn-and-drop in both engines + `isMessage` at the sim
  network boundary.

---

## Files

| Path | Role |
|------|------|
| `tests/equivalence/oracle.ts` | `debug.snap` snapshot + canonicalization + outcome capture |
| `tests/equivalence/harness.ts` | runs a workload on both engines, diffs result |
| `tests/equivalence/workloads.ts` | matched generator/async workload pairs |
| `tests/equivalence/equivalence.test.ts` | Layer A Jest suite |
| `sim/src/worker.ts` | engine-agnostic worker (`EngineFactory`) |
| `sim/src/workloads.ts` | matched DST workloads (`fibLfc`, `fibRfc`) |
| `sim/src/differential.ts` | Layer B differential runner (`npm run dst:diff`) |
| `sim/main.ts` | single-engine DST, now with `--engine gen\|async` |
