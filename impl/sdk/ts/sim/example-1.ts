import type * as context from "../src/context";
import type { Request } from "../src/network/network";
import { ServerProcess } from "./src/server";
import { Message, Random, Simulator, unicast } from "./src/simulator";
import { WorkerProcess } from "./src/worker";

import * as util from "../src/util";

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
const sim = new Simulator(rnd, {
  randomDelay: options.randomDelay,
  dropProb: options.dropProb,
  duplProb: options.duplProb,
});

const server = new ServerProcess("server");
const worker1 = new WorkerProcess(
  rnd,
  { charFlipProb: options.charFlipProb ?? rnd.random(0.05) },
  "worker-1",
  "default",
);
const worker2 = new WorkerProcess(
  rnd,
  { charFlipProb: options.charFlipProb ?? rnd.random(0.05) },
  "worker-2",
  "default",
);
const worker3 = new WorkerProcess(
  rnd,
  { charFlipProb: options.charFlipProb ?? rnd.random(0.05) },
  "worker-3",
  "default",
);

const workers = [worker1, worker2, worker3] as const;

// Register defined function to the workers
for (const worker of workers) {
  worker.resonate.register("fibonacci", fibonacci);
}

sim.register(server);
for (const worker of workers) {
  sim.register(worker);
}

const n = 10;
const id = `fibonacci-${n}`;

sim.delay(0, () => {
  const msg = new Message<Request>(
    unicast("environment"),
    unicast("server"),
    {
      kind: "createPromise",
      id,
      timeout: 10000000000,
      iKey: id,
      tags: { "resonate:invoke": "local://any@default" },
      param: { func: "fibonacci", args: [n] },
    },
    { requ: true, correlationId: 1 },
  );

  sim.send(msg);
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
util.assert(server.server.promises.get(id)?.value === f(n));
