# Deterministic simulation (sim/dst)

A deterministic simulation runtime for finding bugs in Resonate workflow code,
modeled on JoJo's simulator. It is separate from the probabilistic simulator in
`sim/src`.

Several simulated workers run the real SDK engine (`Core`) against the existing
in-memory server (`src/network/local.ts`). They communicate only through a
simulated network of envelopes (requests, responses and server messages).
Nothing runs concurrently. Each step applies exactly one `Action`, chosen by a
`Driver` and recorded on a `Tape`:

| Source of nondeterminism  | Controlled by                                               |
| ------------------------- | ----------------------------------------------------------- |
| scheduling, message order | `deliver` (which envelope; which worker for anycast)        |
| clock and timers          | `advance` (to the next deadline), `fire` (a due timer)      |
| randomness in user code   | `random` tape entries feed `Math.random` / `ctx.math.random` |
| faults                    | `crash`, `restart`, `drop`, `delay`                         |

Ids (corrIds, `crypto.randomUUID`) are minted from counters. `Date.now`,
`setTimeout` and `setInterval` are answered from the virtual clock while a step
runs. A run is therefore a pure function of (scenario, tape). No public SDK API
changed: the globals are swapped only for the duration of `simulate`.

## Drivers

- `seeded(seed, faults?)` explores with a mulberry32 PRNG and optional fault
  probabilities (`crash`, `drop`, `delay`, ...).
- `scripted(steps)` follows an explicit scenario, such as
  `{ deliver: { kind: "execute", subject: "t1" }, to: "w1" }`,
  `{ crash: "w2" }`, `{ advance: true }`, `{ fire: "w1" }` or
  `{ random: [0.5] }`. A step that cannot happen fails the run as a
  divergence.
- `replay(tape)` reproduces a recorded run exactly. Any deviation is a
  divergence.

## Example

```ts
import { bank, conserved, functions } from "./examples/bank.js";
import { replay, seeded, serializeTape, simulate } from "./index.js";

const scenario = {
  workers: ["w1", "w2"],
  functions,
  world: bank, // fresh external state per run, via ctx.getDependency("world")
  start: [
    { id: "t1", func: "transferRacy", args: ["alice", "bob", 10] },
    { id: "t2", func: "transferRacy", args: ["bob", "carol", 20] },
  ],
  check: conserved, // throw to fail the run
};

for (let seed = 1; ; seed++) {
  const r = await simulate(scenario, seeded(seed), { requireCompletion: true });
  if (r.status === "failed") {
    console.log(r.failure, r.log.join("\n"));
    const tape = serializeTape(r.tape); // save it; replay(parseTape(tape)) reproduces it
    break;
  }
}
```

See `tests/dst.test.ts` for scripted interleavings, fault injection and replay.
