import { randomUUID } from "node:crypto";
import { StepClock } from "../src/clock.js";
import { Codec } from "../src/codec.js";
import type * as context from "../src/context.js";
import { Registry } from "../src/registry.js";
import { VERSION } from "../src/util.js";
import { ServerProcess } from "./src/server.js";
import { Message, Random, Simulator, unicast } from "./src/simulator.js";
import { WorkerProcess } from "./src/worker.js";

// Concurrent fibonacci using beginRun (local function reference)
function* fibonacci(ctx: context.Context, n: number): Generator<any, number, any> {
  if (n <= 1) {
    return n;
  }
  const p1 = yield ctx.beginRun("fibonacci", n - 1);
  const p2 = yield ctx.beginRun(fibonacci, n - 2);

  return (yield p1) + (yield p2);
}

const seed = Math.floor(Math.random() * 2 ** 32);

const options = { seed, steps: 20_000_000, randomDelay: 0.3, dropProb: 0.3, duplProb: 0.3, charFlipProb: 0.05 };

const rnd = new Random(options.seed);
const clock = new StepClock();
const codec = new Codec();
const registry = new Registry();
registry.add(fibonacci);

const sim = new Simulator(rnd, {
  randomDelay: options.randomDelay,
  dropProb: options.dropProb,
  duplProb: options.duplProb,
});

const server = new ServerProcess(clock, "server");
const worker1 = new WorkerProcess(rnd, clock, registry, { charFlipProb: options.charFlipProb }, "worker-1", "default");
const worker2 = new WorkerProcess(rnd, clock, registry, { charFlipProb: options.charFlipProb }, "worker-2", "default");
const worker3 = new WorkerProcess(rnd, clock, registry, { charFlipProb: options.charFlipProb }, "worker-3", "default");

sim.register(server);
for (const worker of [worker1, worker2, worker3]) {
  sim.register(worker);
}

const n = 5;
const id = `fibonacci-${n}`;

sim.repeat(1, () => {
  sim.send(
    new Message(
      unicast("environment"),
      unicast("server"),
      {
        kind: "debug.tick",
        head: { corrId: randomUUID(), version: VERSION },
        data: { time: clock.time },
      },
      { requ: true },
    ),
  );
});

sim.repeat(1, () => {
  sim.send(
    new Message(
      unicast("environment"),
      unicast("server"),
      {
        kind: "promise.create",
        head: { corrId: randomUUID(), version: VERSION },
        data: {
          id,
          timeoutAt: Number.MAX_SAFE_INTEGER,
          tags: { "resonate:target": "sim://any@default" },
          param: codec.encode({ func: "fibonacci", args: [n], version: 1 }),
        },
      },
      { requ: true },
    ),
  );
});

function f(n: number, memo: Record<number, number> = {}): number {
  if (n <= 1) return n;
  if (memo[n] !== undefined) return memo[n];
  memo[n] = f(n - 1, memo) + f(n - 2, memo);
  return memo[n];
}

const settled = await sim.execUntil(options.steps, () => {
  const promise = server.server.promises.get(id);
  return promise !== undefined && promise.state !== "pending";
});

if (!settled) {
  console.error(
    `fibonacci(${n}) did not settle after ${options.steps} steps (seed=${seed}, randomDelay=${options.randomDelay}, dropProb=${options.dropProb}, duplProb=${options.duplProb}, charFlipProb=${options.charFlipProb})`,
  );
  process.exit(1);
}

const promise = server.server.promises.get(id)!;
const decoded = codec.decode({ headers: promise.value.headers || {}, data: promise.value.data || "" });
if (decoded !== f(n)) {
  console.error(`fibonacci(${n}) expected ${f(n)} but got ${decoded} with seed=${seed}`);
  process.exit(1);
}
