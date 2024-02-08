import { Context } from "../resonate";

// Types

// Resonate supports any function that takes a context as the first
// argument. The generic parameter is used to restrict the return
// type when a generator used.

/**
 * Represents a Resonate function. Must define {@link Context} as the
 * first argument, may by additional arguments.
 *
 * @template T Represents the return type of the function.
 * @param ctx - The Resonate {@link Context}.
 * @param args - Additional function arguments.
 */
export type Func<T = any> = (ctx: Context, ...args: any[]) => T;

// Similar to the built in Parameters type, but ignores the required
// context parameter.

/**
 * Infers the parameters of a Resonate function.
 *
 * @template F The Resonate function type.
 */
export type Params<F extends Func> = F extends (ctx: Context, ...args: infer P) => any ? P : never;

// Similar to the built in ReturnType type, but optionally returns
// the awaited inferred return value of F if F is a generator,
// otherwise returns the awaited inferred return value.

/**
 * Infers the return type of a Resonate function.
 *
 * If a generator is used, the return type will be the return type of
 * the generator, otherwise the return type will be the return type
 * of the function.
 *
 * @template F The Resonate function type.
 */
export type Return<F extends Func> = F extends (ctx: Context, ...args: any) => infer R
  ? R extends Generator<unknown, infer G>
    ? Awaited<G>
    : Awaited<R>
  : never;

// Type Guards

export function isFunction(f: unknown): f is Func {
  return (
    typeof f === "function" &&
    (f.constructor.name === "Function" || f.constructor.name === "AsyncFunction" || f.constructor.name === "")
  );
}

export function isGenerator(f: unknown): f is Func<Generator> {
  return typeof f === "function" && f.constructor.name === "GeneratorFunction";
}
