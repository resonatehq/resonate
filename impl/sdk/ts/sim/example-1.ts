import { StepClock } from "../src/clock";
import type * as context from "../src/context";
import { JsonEncoder } from "../src/encoder";
import type { Request } from "../src/network/network";
import { Registry } from "../src/registry";
import * as util from "../src/util";
import { ServerProcess } from "./src/server";
import { Message, Random, Simulator, unicast } from "./src/simulator";
import { WorkerProcess } from "./src/worker";

// Define a resonate function
function* fibonacci(ctx: context.Context, n: number): Generator<any, number, any> {
  if (n <= 1) {
    return n;
  }
  const p1 = yield ctx.beginRun(fibonacci, n - 1);
  const p2 = yield ctx.beginRun(fibonacci, n - 2);

  return (yield p1) + (yield p2);
}

const options = { seed: 0, steps: 1000, randomDelay: 0, dropProb: 0, duplProb: 0, charFlipProb: 0 };

const rnd = new Random(options.seed);
const clock = new StepClock();
const encoder = new JsonEncoder();
const registry = new Registry();
registry.add(fibonacci);

const sim = new Simulator(rnd, {
  randomDelay: options.randomDelay,
  dropProb: options.dropProb,
  duplProb: options.duplProb,
});

const server = new ServerProcess(clock, "server");
const worker1 = new WorkerProcess(
  rnd,
  clock,
  encoder,
  registry,
  { charFlipProb: options.charFlipProb },
  "worker-1",
  "default",
);
const worker2 = new WorkerProcess(
  rnd,
  clock,
  encoder,
  registry,
  { charFlipProb: options.charFlipProb },
  "worker-2",
  "default",
);
const worker3 = new WorkerProcess(
  rnd,
  clock,
  encoder,
  registry,
  { charFlipProb: options.charFlipProb },
  "worker-3",
  "default",
);

const workers = [worker1, worker2, worker3] as const;

sim.register(server);
for (const worker of workers) {
  sim.register(worker);
}

const n = 10;
const id = `fibonacci-${n}`;

sim.delay(0, () => {
  sim.send(
    new Message<Request>(
      unicast("environment"),
      unicast("server"),
      {
        kind: "createPromise",
        id,
        timeout: 10000000,
        iKey: id,
        tags: { "resonate:invoke": "sim://any@default" },
        param: encoder.encode({ func: "fibonacci", args: [n], version: 1 }),
      },
      { requ: true, correlationId: 1 },
    ),
  );
});

sim.exec(options.steps);

function f(n: number, memo: Record<number, number> = {}): number {
  if (n <= 1) return n;

  if (memo[n] !== undefined) {
    return memo[n];
  }

  memo[n] = f(n - 1, memo) + f(n - 2, memo);
  return memo[n];
}

util.assert(encoder.decode(server.server.promises.get(id)?.value) === f(n));
