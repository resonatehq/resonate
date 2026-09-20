// Parallel async/await execution engine for the Resonate SDK.
//
// Users write ordinary `async` functions and use `await`. `ctx.run`/`ctx.rpc`/
// `ctx.sleep` are eager (begin immediately) and return a `DurablePromise<T>` —
// an awaitable branded handle exposing the durable promise id. There are no
// `begin*` variants.

// Retries are opt-in in this engine (default Never) — re-export the policies
// so migrating users don't have to reach into the generator entry point.
export { Constant, Exponential, Linear, Never, type RetryPolicy } from "../retries.js";
export { type AnyFunc, type Context, type DetachedHandle, DurablePromise, type Info } from "./context.js";
// The task-lifecycle orchestrator, exported alongside the root engine's `Core`.
// Stateless per-message hosts (e.g. serverless workers) drive execution by
// constructing this directly and calling `onMessage`, rather than using the
// long-running `Resonate` class with its network recv loop and heartbeat.
export { Core, type Done, type Status, type Suspended } from "./core.js";
export {
  Resonate,
  type ResonateFunc,
  type ResonateHandle,
  type ResonateSchedule,
} from "./resonate.js";
