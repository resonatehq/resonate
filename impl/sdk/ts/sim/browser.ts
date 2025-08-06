import type { Context } from "../src/context";
import type { RequestMsg } from "../src/network/network";
import { ServerProcess } from "./server";
import { Message, Simulator, unicast } from "./simulator";
import { WorkerProcess } from "./worker";

const sim = new Simulator(100);
const server = new ServerProcess("server");
const worker1 = new WorkerProcess("worker1", "default");
const worker2 = new WorkerProcess("worker2", "default");

function* fib(ctx: Context, n: number): Generator {
  if (n <= 1) {
    return n;
  }

  const p1 = yield ctx.beginRpc("fib", n - 1);
  const p2 = yield ctx.beginRpc("fib", n - 2);
  return (yield p1) + (yield p2);
}

worker1.resonate.register("fib", fib);
worker2.resonate.register("fib", fib);

sim.register(server);
sim.register(worker1);
sim.register(worker2);

sim.send(
  new Message<RequestMsg>(
    unicast("environment"),
    unicast("server"),
    {
      kind: "createPromise",
      id: "fib",
      timeout: 10020001,
      iKey: "fib",
      tags: { "resonate:invoke": "local://any@default" },
      param: { fn: "fib", args: [10] },
    },
    { requ: true, correlationId: 0 },
  ),
);

let i = 0;
while (i < 10000) {
  sim.tick();
  i++;
}
console.log("outbox:", sim.outbox);
