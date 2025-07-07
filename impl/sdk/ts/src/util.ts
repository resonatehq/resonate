export function assert(cond: boolean, msg?: string): void {
  if (cond) return; // Early return if assertion passes

  console.assert(cond, "Assertion Failed: %s", msg);
  console.trace();

  if (typeof process !== "undefined" && process.versions.node) {
    process.exit(1);
  }
}

// biome-ignore lint/complexity/noBannedTypes: We need to check all possible functions
export function isGeneratorFunction(fn: Function): boolean {
  const GeneratorFunction = Object.getPrototypeOf(function* () {}).constructor;
  const AsyncGeneratorFunction = Object.getPrototypeOf(async function* () {}).constructor;
  return fn instanceof GeneratorFunction || fn instanceof AsyncGeneratorFunction;
}

export function assertDefined<T>(val: T | undefined | null): asserts val is T {
  assert(val !== null && val !== undefined, "value must not be null");
}
