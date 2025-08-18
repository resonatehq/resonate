import type { Context } from "./context";
import type { DurablePromiseRecord } from "./network/network";

export type Func = (ctx: Context, ...args: any[]) => any;

export const RESONATE_OPTIONS: unique symbol = Symbol("ResonateOptions");

// The args of a resonate function excluding the context argument
export type Params<F> = F extends (ctx: Context, ...args: infer P) => any ? P : never;
export type ParamsWithOptions<F> = [...Params<F>, (Partial<Options> & { [RESONATE_OPTIONS]: true })?];

export type Options = {
  id: string;
  target: string;
  timeout: number;
};

export type Completed = { kind: "completed"; durablePromise: DurablePromiseRecord };
export type Suspended = { kind: "suspended"; durablePromiseId: string };
export type Failure = { kind: "failure"; task: Task };
export type PlatformError = { kind: "platformError"; cause: any; msg: string };
export type CompResult = Completed | Suspended | Failure | PlatformError;

export type ClaimedTask = {
  kind: "claimed";
  id: string;
  counter: number;
  rootPromiseId: string;
  rootPromise: DurablePromiseRecord;
};

export type UnclaimedTask = {
  kind: "unclaimed";
  id: string;
  counter: number;
  rootPromiseId: string;
};

export type Task = UnclaimedTask | ClaimedTask;

// Return type of a function or a generator
export type Return<T> = T extends (...args: any[]) => Generator<infer __, infer R, infer _>
  ? R // Return type of generator
  : T extends (...args: any[]) => infer R
    ? Awaited<R> // Return type of regular function
    : never;

type Ok<T> = {
  success: true;
  data: T;
};

type Ko = {
  success: false;
  error: any;
};

export type Result<T> = Ok<T> | Ko;

// Expression
export type InternalExpr<T> = InternalAsyncL | InternalAsyncR | InternalAwait<T> | InternalReturn<T>;

export type InternalAsyncR = {
  type: "internal.async.r";
  id: string;
  func: string;
  args: any[];
  opts: Options;
  mode: "eager" | "defer"; // TODO(avillega): Right now it is unused, review its usage
};

export type InternalAsyncL = {
  type: "internal.async.l";
  id: string;
  func: Func;
  args: any[];
  mode: "eager" | "defer"; // TODO(avillega): Right now it is unused, review its usage
};
export type InternalAwait<T> = {
  type: "internal.await";
  id: string;
  promise: Promise<T>;
};

export type InternalReturn<T> = {
  type: "internal.return";
  value: Literal<T>;
};

// Values

export type Value<T> = Nothing | Literal<T> | Promise<T>;

export type Nothing = {
  type: "internal.nothing";
};

export type Promise<T> = PromisePending | PromiseCompleted<T>;

export type PromisePending = {
  type: "internal.promise";
  state: "pending";
  id: string;
};

export type PromiseCompleted<T> = {
  type: "internal.promise";
  state: "completed";
  id: string;
  value: Literal<T>;
};

export type Literal<T> = {
  type: "internal.literal";
  value: Result<T>;
};

// Helper functions to create some of the types defined in this file

export function ok<T>(data: T): Result<T> {
  return { success: true, data };
}

export function ko(error: any): Result<any> {
  return { success: false, error };
}
