// Routes the ambient sources of nondeterminism (timers, the wall clock,
// Math.random, crypto.randomUUID) through the simulation while a step runs.
//
// Engine and user code call these globals directly (retry backoff uses
// setTimeout, ctx.math.random uses Math.random, request ids use
// crypto.randomUUID, ...). Inside a step they are answered by the simulation;
// outside a step they fall through to the real implementation, so a test
// runner that shares the process keeps working.

export type Hooks = {
  /** True while a simulation step (and its microtasks) is running. */
  active(): boolean;
  now(): number;
  random(): number;
  uuid(): string;
  setTimer(fn: () => void, ms: number, repeat: boolean): object;
  clearTimer(handle: unknown): boolean;
  exit(code?: number): never;
};

export type Real = {
  setTimeout: typeof setTimeout;
  clearTimeout: typeof clearTimeout;
  setInterval: typeof setInterval;
  clearInterval: typeof clearInterval;
  setImmediate: typeof setImmediate;
  dateNow: () => number;
  mathRandom: () => number;
  exit: typeof process.exit;
};

export const real: Real = {
  setTimeout: globalThis.setTimeout,
  clearTimeout: globalThis.clearTimeout,
  setInterval: globalThis.setInterval,
  clearInterval: globalThis.clearInterval,
  setImmediate: globalThis.setImmediate,
  dateNow: Date.now,
  mathRandom: Math.random,
  exit: process.exit,
};

let installed: Hooks | undefined;

/** Install hooks for the duration of a run. Returns the function that restores the real globals. */
export function install(hooks: Hooks): () => void {
  if (installed) {
    throw new Error("a deterministic simulation is already running in this process");
  }
  installed = hooks;

  const g = globalThis as any;
  const cryptoDescriptor = Object.getOwnPropertyDescriptor(globalThis, "crypto");
  const realCrypto = globalThis.crypto;

  g.setTimeout = (fn: (...a: any[]) => void, ms?: number, ...args: any[]) =>
    hooks.active() ? hooks.setTimer(() => fn(...args), ms ?? 0, false) : real.setTimeout(fn, ms, ...args);
  g.setInterval = (fn: (...a: any[]) => void, ms?: number, ...args: any[]) =>
    hooks.active() ? hooks.setTimer(() => fn(...args), ms ?? 0, true) : real.setInterval(fn, ms, ...args);
  g.clearTimeout = (handle: any) => {
    if (!hooks.clearTimer(handle)) real.clearTimeout(handle);
  };
  g.clearInterval = (handle: any) => {
    if (!hooks.clearTimer(handle)) real.clearInterval(handle);
  };
  Date.now = () => (hooks.active() ? hooks.now() : real.dateNow());
  Math.random = () => (hooks.active() ? hooks.random() : real.mathRandom());
  const simCrypto = new Proxy(realCrypto, {
    get(target, prop) {
      if (prop === "randomUUID" && hooks.active()) return () => hooks.uuid();
      const value = Reflect.get(target, prop, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  Object.defineProperty(globalThis, "crypto", { configurable: true, enumerable: true, get: () => simCrypto });
  (process as any).exit = (code?: number) => (hooks.active() ? hooks.exit(code) : real.exit(code));

  return () => {
    g.setTimeout = real.setTimeout;
    g.setInterval = real.setInterval;
    g.clearTimeout = real.clearTimeout;
    g.clearInterval = real.clearInterval;
    Date.now = real.dateNow;
    Math.random = real.mathRandom;
    if (cryptoDescriptor) Object.defineProperty(globalThis, "crypto", cryptoDescriptor);
    (process as any).exit = real.exit;
    installed = undefined;
  };
}
