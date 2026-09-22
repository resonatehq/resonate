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
//   npx tsx examples/async/error-handling.ts --mode run --error taken
//   npx tsx examples/async/error-handling.ts --mode rpc --error value

import { type Context, type Info, Resonate } from "../../src/async/index.js";

class UsernameTakenError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UsernameTakenError";
  }
}

async function bar(_info: Info, username: string, age: number): Promise<string> {
  const existingUsers = ["admin", "coder123", "python_fan"];
  if (age < 0) throw new RangeError("Age cannot be negative.");
  if (existingUsers.includes(username.toLowerCase())) {
    throw new UsernameTakenError(`The username '${username}' is already in use.`);
  }
  return `Registration successful for ${username}!`;
}

async function foo(ctx: Context, username: string, age: number, mode: string): Promise<void> {
  try {
    // The async engine defaults to Never retries, so the failure surfaces on
    // the first attempt.
    const v = await (mode === "run" ? ctx.run(bar, username, age) : ctx.rpc<string>("bar", username, age));
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
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  if (hit) return hit.slice(name.length + 3);
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
  await (await resonate.run(id, foo, username as string, age as number, mode)).result();
} finally {
  await resonate.stop();
}
