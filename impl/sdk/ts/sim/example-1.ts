import type * as context from "../src/context";
import type { RequestMsg } from "../src/network/network";
import { ServerProcess } from "./src/server";
import { Message, Simulator, unicast } from "./src/simulator";
import { WorkerProcess } from "./src/worker";

// Function definition
function* fibonacci(ctx: context.Context, n: number): Generator<any, number, any> {
  if (n <= 1) {
    return n;
  }
  const p1 = yield ctx.beginRpc("fibonacci", n - 1);
  const p2 = yield ctx.beginRpc("fibonacci", n - 2);

  return (yield p1) + (yield p2);
}

const options: {
  seed: number;
  steps: number;
  randomDelay?: number;
  dropProb?: number;
  duplProb?: number;
} = { seed: 0, steps: 100, randomDelay: 0, dropProb: 0, duplProb: 0 };

// Run Simulation

const sim = new Simulator(options.seed, {
  randomDelay: options.randomDelay,
  dropProb: options.dropProb,
  duplProb: options.duplProb,
});

const server = new ServerProcess("server");
const worker1 = new WorkerProcess("worker-1", "default");
const worker2 = new WorkerProcess("worker-2", "default");
const worker3 = new WorkerProcess("worker-3", "default");

const workers = [worker1, worker2, worker3] as const;

for (const worker of workers) {
  worker.resonate.register("fibonacci", fibonacci);
}

sim.register(server);
for (const worker of workers) {
  sim.register(worker);
}

const n = 10;
const id = `fibonacci-${n}`;
const msg = new Message<RequestMsg>(
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

let i = 0;
while (i < options.steps) {
  sim.tick();
  i++;
}

console.log(server.server.promises.get(id));
