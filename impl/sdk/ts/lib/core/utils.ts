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
