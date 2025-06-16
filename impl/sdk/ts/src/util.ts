export function assert(cond: boolean, msg?: string): void {
  if (cond) return; // Early return if assertion passes

  console.assert(cond, "Assertion Failed: %s", msg);
  console.trace();

  if (typeof process !== "undefined" && process.versions.node) {
    process.exit(1);
  }
}
