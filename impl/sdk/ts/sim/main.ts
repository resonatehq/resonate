import { Command } from "commander";
import { StepClock } from "../src/clock";
import type { Context } from "../src/context";
import type { Request } from "../src/network/network";
import { Registry } from "../src/registry";
import { ServerProcess } from "./src/server";
import { Message, Random, Simulator, unicast } from "./src/simulator";
import { WorkerProcess } from "./src/worker";

// Function definition
function* fibLfi(ctx: Context, n: number): Generator<any, number, any> {
  if (n <= 1) {
    return n;
  }
  const p1 = yield ctx.beginRun(fibLfi, n - 1, ctx.options({ id: `fibLfi-${n - 1}` }));
  const p2 = yield ctx.beginRun(fibLfi, n - 2, ctx.options({ id: `fibLfi-${n - 2}` }));

  return (yield p1) + (yield p2);
}

function* fibRfi(ctx: Context, n: number): Generator<any, number, any> {
  if (n <= 1) {
    return n;
  }
  const p1 = yield ctx.beginRpc("fibRfi", n - 1, ctx.options({ id: `fibRfi-${n - 1}` }));
  const p2 = yield ctx.beginRpc("fibRfi", n - 2, ctx.options({ id: `fibRfi-${n - 2}` }));

  return (yield p1) + (yield p2);
}

function* fibLfc(ctx: Context, n: number): Generator<any, number, any> {
  if (n <= 1) {
    return n;
  }
  const v1 = yield ctx.run(fibLfc, n - 1, ctx.options({ id: `fibLfc-${n - 1}` }));
  const v2 = yield ctx.run(fibLfc, n - 2, ctx.options({ id: `fibLfc-${n - 2}` }));
  return v1 + v2;
}

function* fibRfc(ctx: Context, n: number): Generator<any, number, any> {
  if (n <= 1) {
    return n;
  }
  const v1 = yield ctx.rpc("fibRfc", n - 1, ctx.options({ id: `fibRfc-${n - 1}` }));
  const v2 = yield ctx.rpc("fibRfc", n - 2, ctx.options({ id: `fibRfc-${n - 2}` }));
  return v1 + v2;
}

function* foo(ctx: Context): Generator<any, any, any> {
  const p1 = yield ctx.beginRun(bar);
  const p2 = yield ctx.beginRpc("bar");
  yield ctx.beginRun(bar);
  yield ctx.beginRpc("bar");
  yield ctx.run(bar);
  yield ctx.rpc("bar");

  return [yield p1, yield p2];
}

function* bar(ctx: Context): Generator<any, any, any> {
  const p1 = yield ctx.beginRun(baz);
  const p2 = yield ctx.beginRpc("baz");
  yield ctx.beginRun(baz);
  yield ctx.beginRpc("baz");
  yield ctx.run(baz);
  yield ctx.rpc("baz");

  return [yield p1, yield p2];
}

function* baz(ctx: Context): Generator<any, any, any> {
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
  })
  .option("--charFlipProb <number>", "Character flip prob (0-1)", (value) => {
    const n = Number.parseFloat(value);
    if (Number.isNaN(n) || n < 0 || n > 1) {
      throw new Error(`Invalid charFlipProb: ${value} (must be 0–1)`);
    }
    return n;
  })
  .option("--deactivateProb <number>", "Deactivate probability (0-1)", (value) => {
    const n = Number.parseFloat(value);
    if (Number.isNaN(n) || n < 0 || n > 1) {
      throw new Error(`Invalid deactivateProb: ${value} (must be 0–1)`);
    }
    return n;
  })
  .option("--activateProb <number>", "Activate probability (0-1)", (value) => {
    const n = Number.parseFloat(value);
    if (Number.isNaN(n) || n < 0 || n > 1) {
      throw new Error(`Invalid activateProb: ${value} (must be 0–1)`);
    }
    return n;
  });

program.parse(process.argv);

type Options = {
  seed: number;
  steps: number;
  func?: string;
  randomDelay?: number;
  dropProb?: number;
  duplProb?: number;
  charFlipProb?: number;
  deactivateProb?: number;
  activateProb?: number;
};

const options = program.opts<Options>();

export function run(options: Options) {
  // effectively disable queueMicrotask
  process.env.QUEUE_MICROTASK_EVERY_N = Number.MAX_SAFE_INTEGER.toString();

  const rnd = new Random(options.seed);

  const sim = new Simulator(rnd, {
    randomDelay: options.randomDelay ?? rnd.random(0.5),
    dropProb: options.dropProb ?? rnd.random(0.5),
    duplProb: options.duplProb ?? rnd.random(0.5),
    deactivateProb: options.deactivateProb ?? rnd.random(0.01),
    activateProb: options.activateProb ?? rnd.random(0.5),
  });

  const clock = new StepClock();
  const registry = new Registry();

  for (const [name, func] of Object.entries(availableFuncs)) {
    registry.add(func, name);
  }

  // server
  sim.register(new ServerProcess(clock, "server"));

  // workers
  for (let i = 1; i <= 3; i++) {
    sim.register(
      new WorkerProcess(
        rnd,
        clock,
        registry,
        { charFlipProb: options.charFlipProb ?? rnd.random(0.05) },
        `worker-${i}`,
        "default",
      ),
    );
  }

  sim.repeat(1, () => {
    const i = sim.step - 1;
    const useExplicit = options.func && i === 0;

    const funcName = useExplicit
      ? options.func
      : Object.keys(availableFuncs)[rnd.randint(0, Object.keys(availableFuncs).length - 1)];

    if (!options.func || i === 0) {
      const id = `${funcName}-${i}`;
      const timeout = rnd.randint(0, options.steps);
      let msg: Message<Request>;
      switch (funcName) {
        case "fibLfi":
        case "fibLfc":
        case "fibRfi":
        case "fibRfc": {
          msg = new Message<Request>(
            unicast("environment"),
            unicast("server"),
            {
              kind: "createPromise",
              id,
              timeout,
              iKey: id,
              tags: { "resonate:invoke": "local://any@default" },
              param: { func: funcName, args: [rnd.randint(0, 20)], version: 1 },
            },
            { requ: true, correlationId: i },
          );
          break;
        }
        case "foo":
        case "bar":
        case "baz": {
          msg = new Message<Request>(
            unicast("environment"),
            unicast("server"),
            {
              kind: "createPromise",
              id,
              timeout,
              iKey: id,
              tags: { "resonate:invoke": "local://any@default" },
              param: { func: funcName, args: [], version: 1 },
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
  });

  sim.exec(options.steps);
  console.log("[outbox]: ", sim.outbox);
}

if (require.main === module) {
  run(options);
}
