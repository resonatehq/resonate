// error-handling shows how a failure raised in a durable function reaches
// whoever awaits it.
//
// When a durable function like `bar` fails, its exception does not propagate
// in-process the way a normal throw would. Resonate **encodes the failure,
// writes it to a durable promise, and later decodes it** for whoever awaits the
// result -- possibly a different worker, or a process that recovered after a
// crash and never saw the original throw.
//
// The codec encodes an Error's `name`, `message`, and `stack`. On the far side
// the awaiter gets an `Error` whose `.name` is preserved, so you discriminate
// domain failures by name (TS has no pickle, so the concrete class/prototype is
// not reconstructed -- `instanceof` on a custom subclass will not match).
//
//   npx tsx examples/generator/error-handling.ts --mode run --error taken
//   npx tsx examples/generator/error-handling.ts --mode rpc --error value

import { type Context, Never, Resonate } from "../../src/index.js";

class UsernameTakenError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UsernameTakenError";
  }
}

async function bar(_ctx: Context, username: string, age: number): Promise<string> {
  const existingUsers = ["admin", "coder123", "python_fan"];
  if (age < 0) throw new RangeError("Age cannot be negative.");
  if (existingUsers.includes(username.toLowerCase())) {
    throw new UsernameTakenError(`The username '${username}' is already in use.`);
  }
  return `Registration successful for ${username}!`;
}

function* foo(ctx: Context, username: string, age: number, mode: string): Generator<any, void, any> {
  // Never on the leaf call: surface the failure on the first attempt. (The
  // generator engine defaults leaf functions to Exponential retry.)
  const opts = ctx.options({ retryPolicy: new Never() });
  try {
    const v: string = yield* mode === "run"
      ? ctx.run(bar, username, age, opts)
      : ctx.rpc<string>("bar", username, age, opts);
    console.log(`🎉 Success: ${v}`);
  } catch (err) {
    // The original type is NOT reconstructed across the boundary, but `name`
    // survives -- so discriminate on it rather than on `instanceof`.
    const e = err as Error;
    if (e.name === "UsernameTakenError") {
      console.log(`⚠️  Business Rule Error: ${e.message}`);
      console.log("💡 Suggestion: Please try adding numbers to your username.");
    } else if (e.name === "RangeError") {
      console.log(`💥 Validation Error: ${e.message}`);
    } else {
      throw err;
    }
  }
}

function flag(name: string, fallback: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

const mode = flag("mode", "run");
const error = flag("error", "none");
const [username, age] = error === "taken" ? ["admin", 25] : error === "value" ? ["alice", -5] : ["alice", 25];

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.register("foo", foo);
resonate.register("bar", bar); // registered so rpc-by-name can find it

try {
  const id = `error-handling-${Date.now()}`;
  await resonate.run(id, foo, username as string, age as number, mode);
} finally {
  await resonate.stop();
}
