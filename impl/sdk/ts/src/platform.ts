// Platform-neutral helpers.
//
// These wrap functionality that historically came from Node-only modules
// (`node:crypto`, `node:timers/promises`, `process.env`) so the SDK can be
// bundled for the browser. Each helper relies only on APIs available in both
// Node (see the `engines` field in package.json) and modern browsers.

// randomUUID — `globalThis.crypto.randomUUID()` is available on Node 19+ and
// all modern browsers, replacing `node:crypto`'s `randomUUID`.
export function randomUUID(): string {
  return globalThis.crypto.randomUUID();
}

// delay — resolves after `ms` milliseconds, replacing `node:timers/promises`'s
// `setTimeout`.
export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// getEnv — safely reads an environment variable, returning undefined when
// `process` is not available (e.g. in the browser).
export function getEnv(name: string): string | undefined {
  if (typeof process === "undefined" || !process.env) return undefined;
  return process.env[name];
}
