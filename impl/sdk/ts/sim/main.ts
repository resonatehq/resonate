import { Command } from "commander";
import type * as context from "../src/context";
import type { RequestMsg } from "../src/network/network";
import { ServerProcess } from "./src/server";
import { Message, Random, Simulator, unicast } from "./src/simulator";
import { WorkerProcess } from "./src/worker";

// Function definition
function* fibLfi(ctx: context.Context, n: number): Generator<any, number, any> {
  if (n <= 1) {
    return n;
  }
  const p1 = yield ctx.lfi(fibLfi, n - 1, ctx.options({ id: `fibLfi-${n - 1}` }));
  const p2 = yield ctx.lfi(fibLfi, n - 2, ctx.options({ id: `fibLfi-${n - 2}` }));

  return (yield p1) + (yield p2);
}

function* fibRfi(ctx: context.Context, n: number): Generator<any, number, any> {
  if (n <= 1) {
    return n;
  }
  const p1 = yield ctx.rfi("fibRfi", n - 1, ctx.options({ id: `fibRfi-${n - 1}` }));
  const p2 = yield ctx.rfi("fibRfi", n - 2, ctx.options({ id: `fibRfi-${n - 2}` }));

  return (yield p1) + (yield p2);
}

function* fibLfc(ctx: context.Context, n: number): Generator<any, number, any> {
  if (n <= 1) {
    return n;
  }
  const v1 = yield ctx.lfc(fibLfc, n - 1, ctx.options({ id: `fibLfc-${n - 1}` }));
  const v2 = yield ctx.lfc(fibLfc, n - 2, ctx.options({ id: `fibLfc-${n - 2}` }));
  return v1 + v2;
}

function* fibRfc(ctx: context.Context, n: number): Generator<any, number, any> {
  if (n <= 1) {
    return n;
  }
  const v1 = yield ctx.rfc("fibRfc", n - 1, ctx.options({ id: `fibRfc-${n - 1}` }));
  const v2 = yield ctx.rfc("fibRfc", n - 2, ctx.options({ id: `fibRfc-${n - 2}` }));
  return v1 + v2;
}

function* foo(ctx: context.Context): Generator<any, any, any> {
  const p1 = yield ctx.lfi(bar);
  const p2 = yield ctx.rfi("bar");
  yield ctx.lfi(bar);
  yield ctx.rfi("bar");
  yield ctx.lfc(bar);
  yield ctx.rfc("bar");

  return [yield p1, yield p2];
}

function* bar(ctx: context.Context): Generator<any, any, any> {
  const p1 = yield ctx.lfi(baz);
  const p2 = yield ctx.rfi("baz");
  yield ctx.lfi(baz);
  yield ctx.rfi("baz");
  yield ctx.lfc(baz);
  yield ctx.rfc("baz");

  return [yield p1, yield p2];
}

function* baz(ctx: context.Context): Generator<any, any, any> {
  return "baz";
}

const availableFuncs = { fibLfi: fibLfi, fibRfi: fibRfi, fibLfc: fibLfc, fibRfc: fibRfc, foo: foo, bar: bar, baz: baz };
// ------------------------------------------------------------------------------------------

// CLI
const program = new Command();

program
  .name("sim")
  .description("Run the simulator with a given seed and steps")
  .option(
    "--seed <number>",
    "Random seed",
    (value) => {
      const n = Number.parseInt(value, 10);
      if (Number.isNaN(n)) {
        throw new Error(`Invalid seed: ${value}`);
      }
      return n;
    },
    0,
  )
  .option(
    "--steps <number>",
    "Number of steps",
    (value) => {
      const n = Number.parseInt(value, 10);
      if (Number.isNaN(n) || n < 0) {
        throw new Error(`Invalid steps: ${value}`);
      }
      return n;
    },
    10_000,
  )
  .option(
    "--func <name>",
    `Function to run (optional). Choices: ${Object.keys(availableFuncs).join(", ")}`,
    (value) => {
      if (!Object.keys(availableFuncs).includes(value)) {
        throw new Error(`Invalid function: ${value}. Allowed: ${Object.keys(availableFuncs).join(", ")}`);
      }
      return value;
    },
  )
  .option("--randomDelay <number>", "Random delay probability (0-1)", (value) => {
    const n = Number.parseFloat(value);
    if (Number.isNaN(n) || n < 0 || n > 1) {
      throw new Error(`Invalid randomDelay: ${value} (must be 0–1)`);
    }
    return n;
  })
  .option("--dropProb <number>", "Drop probability (0-1)", (value) => {
    const n = Number.parseFloat(value);
    if (Number.isNaN(n) || n < 0 || n > 1) {
      throw new Error(`Invalid dropProb: ${value} (must be 0–1)`);
    }
    return n;
  })
  .option("--duplProb <number>", "Duplicate probability (0-1)", (value) => {
    const n = Number.parseFloat(value);
    if (Number.isNaN(n) || n < 0 || n > 1) {
      throw new Error(`Invalid duplProb: ${value} (must be 0–1)`);
    }
    return n;
  });

program.parse(process.argv);

const options = program.opts<{
  seed: number;
  steps: number;
  func?: string;
  randomDelay?: number;
  dropProb?: number;
  duplProb?: number;
}>();
// ------------------------------------------------------------------------------------------
export function run(options: {
  seed: number;
  steps: number;
  func?: string;
  randomDelay?: number;
  dropProb?: number;
  duplProb?: number;
}) {
  const rnd = new Random(options.seed);
  const sim = new Simulator(options.seed, {
    randomDelay: options.randomDelay ?? rnd.random(0.5),
    dropProb: options.dropProb ?? rnd.random(0.5),
    duplProb: options.duplProb ?? rnd.random(0.5),
  });

  const server = new ServerProcess("server");
  const worker1 = new WorkerProcess("worker-1", "default");
  const worker2 = new WorkerProcess("worker-2", "default");
  const worker3 = new WorkerProcess("worker-3", "default");

  const workers = [worker1, worker2, worker3] as const;

  for (const [name, func] of Object.entries(availableFuncs)) {
    for (const worker of workers) {
      worker.resonate.register(name, func);
    }
  }

  sim.register(server);
  for (const worker of workers) {
    sim.register(worker);
  }

  let i = 0;
  while (i < options.steps) {
    const useExplicit = options.func && i === 0;

    const funcName = useExplicit
      ? options.func
      : Object.keys(availableFuncs)[rnd.randint(0, Object.keys(availableFuncs).length - 1)];

    if (!options.func || i === 0) {
      const id = `${funcName}-${i}`;
      const timeout = rnd.randint(0, options.steps);
      let msg: Message<RequestMsg>;
      switch (funcName) {
        case "fibLfi":
        case "fibLfc":
        case "fibRfi":
        case "fibRfc": {
          msg = new Message<RequestMsg>(
            unicast("environment"),
            unicast("server"),
            {
              kind: "createPromise",
              id,
              timeout,
              iKey: id,
              tags: { "resonate:invoke": "local://any@default" },
              param: { func: funcName, args: [rnd.randint(0, 20)] },
            },
            { requ: true, correlationId: i },
          );
          break;
        }
        case "foo":
        case "bar":
        case "baz": {
          msg = new Message<RequestMsg>(
            unicast("environment"),
            unicast("server"),
            {
              kind: "createPromise",
              id,
              timeout,
              iKey: id,
              tags: { "resonate:invoke": "local://any@default" },
              param: { func: funcName, args: [] },
            },
            { requ: true, correlationId: i },
          );
          break;
        }
        default:
          throw new Error(`unknown function name: ${funcName}`);
      }

      sim.send(msg);
    }

    sim.tick();
    i++;
  }
  console.log("[outbox]: ", sim.outbox);
}

if (require.main === module) {
  run(options);
}
