import type { Context, DIE, Future, LFC, LFI, RFC, RFI } from "./context.js";
import type { PromiseCreateReq, PromiseRecord, PromiseSettleReq } from "./network/types.js";
import type { Options } from "./options.js";

// Resonate functions

export type Func = (ctx: Context, ...args: any[]) => any;

export type Params<F> = F extends (ctx: Context, ...args: infer P) => any ? P : never;
export type ParamsWithOptions<F> = [...Params<F>, Options?];

export type Yieldable<T = any> = LFI<T> | LFC<T> | RFI<T> | RFC<T> | Future<T> | DIE;

export type Return<T> = T extends (...args: any[]) => Generator<infer __, infer R, infer _>
  ? R // Return type of generator
  : T extends (...args: any[]) => infer R
    ? Awaited<R> // Return type of regular function
    : never;

// Result

export type Result<V, E> = { kind: "value"; value: V } | { kind: "error"; error: E };

// Re-export Send and Recv from the Network module for convenience
export type { Recv, Send } from "./network/network.js";

// Effects

export type Effects = {
  promiseCreate: (req: PromiseCreateReq, func?: string) => Promise<PromiseRecord>;
  promiseSettle: (req: PromiseSettleReq, func?: string) => Promise<PromiseRecord>;
};
