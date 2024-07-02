import { Context } from "..";
import { Options } from "./options";

// The type of a resonate function
export type Func = (ctx: Context, ...args: any[]) => any;

// The args of a resonate function excluding the context argument
export type Params<F> = F extends (ctx: any, ...args: infer P) => any ? P : never;

// The return type of a resonate function
export type Return<F> = F extends (...args: any[]) => infer T ? Awaited<T> : never;

// A top level function call, to be used in the with `resonate.run`
export type TFC = {
  // Fuction Name of the function to be called, has to be previously registered
  funcName: string;
  // Given id for this execution, ids have to be distinct between top level calls
  id: string;
  // args to be passed to the specified func.
  args?: any[];
  // opts to override the registered Options. Only the subset of options stored
  // with the DurablePromise can be overriden that way we can have the same set
  // of options when running in the recovery path.
  optsOverrides?: Partial<
    Pick<Options, "durable" | "eidFn" | "idempotencyKeyFn" | "retry" | "tags" | "timeout" | "version">
  >;
};

// Remote function call
export type RFC = {
  funcName: string;
  args?: any;
  opts?: Partial<Options>;
};

// Remote function call
export type LFC<F extends Func> = {
  func: F;
  args?: any[];
  opts?: Partial<Options>;
};

// Type guard for RFC
export function isRFC(obj: any): obj is RFC {
  return (
    typeof obj === "object" &&
    obj !== null &&
    typeof obj.funcName === "string" &&
    (obj.args === undefined || typeof obj.args === "object") &&
    (obj.opts === undefined || (typeof obj.opts === "object" && obj.opts !== null))
  );
}
