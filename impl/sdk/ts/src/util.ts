import type { DurablePromiseRecord, TaskRecord } from "./network/network";
import { type Options, RESONATE_OPTIONS } from "./types";

// time

export const MS = 1;
export const SEC = 1000;
export const MIN = 60 * SEC;
export const HOUR = 60 * MIN;

// assert

export function assert(cond: boolean, msg?: string): void {
  if (cond) return; // Early return if assertion passes

  console.assert(cond, "Assertion Failed: %s", msg);
  console.trace();

  if (typeof process !== "undefined" && process.versions.node) {
    process.exit(1);
  }
}

export function assertDefined<T>(val: T | undefined | null): asserts val is T {
  assert(val !== null && val !== undefined, "value must not be null");
}

export function isGeneratorFunction(func: Function): boolean {
  const GeneratorFunction = Object.getPrototypeOf(function* () {}).constructor;
  const AsyncGeneratorFunction = Object.getPrototypeOf(async function* () {}).constructor;
  return func instanceof GeneratorFunction || func instanceof AsyncGeneratorFunction;
}

// guards

export function isTaskRecord(obj: any): obj is TaskRecord {
  return (
    typeof obj === "object" &&
    obj !== null &&
    typeof obj.id === "string" &&
    typeof obj.rootPromiseId === "string" &&
    typeof obj.counter === "number" &&
    typeof obj.timeout === "number" &&
    (obj.processId === undefined || typeof obj.processId === "string") &&
    (obj.createdOn === undefined || typeof obj.createdOn === "number") &&
    (obj.completedOn === undefined || typeof obj.completedOn === "number")
  );
}

export function isDurablePromiseRecord(obj: unknown): obj is DurablePromiseRecord {
  return (
    typeof obj === "object" &&
    obj !== null &&
    typeof (obj as any).id === "string" &&
    typeof (obj as any).timeout === "number" &&
    typeof (obj as any).param !== "undefined" && // allow any type
    typeof (obj as any).value !== "undefined" && // allow any type
    typeof (obj as any).tags === "object" &&
    (obj as any).tags !== null &&
    !Array.isArray((obj as any).tags) &&
    Object.values((obj as any).tags).every((v) => typeof v === "string") &&
    ["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"].includes((obj as any).state) &&
    (typeof (obj as any).iKeyForCreate === "undefined" || typeof (obj as any).iKeyForCreate === "string") &&
    (typeof (obj as any).iKeyForComplete === "undefined" || typeof (obj as any).iKeyForComplete === "string") &&
    (typeof (obj as any).createdOn === "undefined" || typeof (obj as any).createdOn === "number") &&
    (typeof (obj as any).completedOn === "undefined" || typeof (obj as any).completedOn === "number")
  );
}

export function isOptions(obj: unknown): obj is Options {
  return typeof obj === "object" && obj !== null && RESONATE_OPTIONS in obj;
}

// helpers

export function splitArgsAndOpts(args: any[], defaults: Options): [any[], Options] {
  const opts = isOptions(args.at(-1)) ? args.pop() : {};
  return [args, { ...defaults, ...opts }];
}
