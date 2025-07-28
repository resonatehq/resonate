import type { Context } from "./context";

export type Func = (ctx: Context, ...args: any[]) => any;

// The args of a resonate function excluding the context argument
export type Params<F> = F extends (ctx: Context, ...args: infer P) => any ? P : never;

export type RemoteOpts = {
  target: string;
  timeout: number;
};

export type LocalOpts = {
  target: string;
  timeout: number;
};

// Return type of a function or a generator
export type Ret<T> = T extends (...args: any[]) => Generator<infer Y, infer R, infer N>
  ? R // Return type of generator
  : T extends (...args: any[]) => infer R
    ? R // Return type of regular function
    : never;

// Expression
export type InternalExpr<T> = InternalAsyncL<T> | InternalAsyncR<T> | InternalAwait<T> | InternalReturn<T>;

export type InternalAsyncR<T> = {
  type: "internal.async.r";
  id: string;
  func: string;
  args: any[];
  opts: RemoteOpts;
  mode: "eager" | "defer"; // TODO(avillega): Right now it is unused, review its usage
};

export type InternalAsyncL<T> = {
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

export type Literal<T> = {
  type: "internal.literal";
  value: T;
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
