import { Options, isOptions } from "./options";

export function randomId(): string {
  return Math.floor(Math.random() * Number.MAX_SAFE_INTEGER).toString(16);
}

export function hash(s: string): string {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  }

  // Generate fixed length hexadecimal hash
  const hashString = (Math.abs(h) >>> 0).toString(16); // Convert to unsigned int and then to hexadecimal
  const maxLength = 8;
  return "0".repeat(Math.max(0, maxLength - hashString.length)) + hashString;
}

export function split(argsWithOpts: any[]): { args: any[]; opts: Partial<Options> } {
  const possibleOpts = argsWithOpts.at(-1);
  return isOptions(possibleOpts)
    ? { args: argsWithOpts.slice(0, -1), opts: possibleOpts }
    : { args: argsWithOpts, opts: {} };
}

/**
 * Merges two objects, preferring values from the first object when both are defined.
 * If a property is undefined in the first object, the value from the second object is used.
 *
 * @template T - Type of the first object
 * @template U - Type of the second object
 * @param {T} obj1 - The first object to merge
 * @param {U} obj2 - The second object to merge
 * @returns {T & U} A new object containing all properties from both input objects
 *
 * @example
 * const obj1 = { a: 1, b: undefined };
 * const obj2 = { b: 2, c: 3 };
 * const result = mergeObjects(obj1, obj2);
 * // result is { a: 1, b: 2, c: 3 }
 *
 * @remarks
 * - Properties from obj1 take precedence over obj2 when both are defined.
 * - The function creates a new object and does not modify the input objects.
 * - Nested objects and arrays are not deeply merged, only their references are copied.
 */
export function mergeObjects<T extends object, U extends object>(obj1: T, obj2: U): T & U {
  return Object.entries({ ...obj1, ...obj2 }).reduce((acc, [key, value]) => {
    acc[key as keyof (T & U)] = (
      obj1[key as keyof T] !== undefined ? obj1[key as keyof T] : obj2[key as keyof U]
    ) as any;
    return acc;
  }, {} as any);
}

/**
 * Creates a promise that resolves after a specified delay.
 *
 * @param ms - The delay in milliseconds.
 * @returns A promise that resolves after the specified delay.
 *
 * @example
 * // Basic usage
 * await sleep(1000); // Pauses execution for 1 second
 *
 * @example
 * // Using in an async function
 * async function example() {
 *   console.log('Start');
 *   await sleep(2000);
 *   console.log('2 seconds later');
 * }
 *
 * @example
 * // Using with .then()
 * sleep(3000).then(() => console.log('3 seconds have passed'));
 */
export async function sleep(ms: number): Promise<void> {
  if (ms < 0) {
    throw new Error("ms should be a positive integer");
  }
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Determines the current state of a Promise.
 *
 * @param p - The Promise whose state is to be determined.
 *
 * @returns {Promise<'pending' | 'resolved' | 'rejected'>} A Promise that resolves to a string
 * representing the state of the input Promise:
 *   - 'pending': The input Promise has not settled yet.
 *   - 'resolved': The input Promise has resolved successfully.
 *   - 'rejected': The input Promise has been rejected.
 *
 * @throws {TypeError} If the input is not a Promise.
 *
 * @example
 * const myPromise = new Promise(resolve => setTimeout(() => resolve('done'), 1000));
 *
 * // Check state immediately
 * promiseState(myPromise).then(state => console.log(state)); // Logs: 'pending'
 *
 * // Check state after 2 seconds
 * setTimeout(() => {
 *   promiseState(myPromise).then(state => console.log(state)); // Logs: 'resolved'
 * }, 2000);
 *
 * @remarks
 * This function uses `Promise.race()` internally to determine the state of the input Promise.
 * It does not affect the execution or result of the input Promise in any way.
 *
 * Note that the state of a Promise can change from 'pending' to either 'resolved' or 'rejected',
 * but once it's settled (either 'resolved' or 'rejected'), it cannot change again.
 */
export function promiseState(p: Promise<any>): Promise<"pending" | "resolved" | "rejected"> {
  const t = {};
  return Promise.race([p, t]).then(
    (v) => (v === t ? "pending" : "resolved"), // Resolved branch
    () => "rejected", // Rejected branch
  );
}
