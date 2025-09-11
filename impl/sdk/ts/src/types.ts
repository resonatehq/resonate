import type { Context, Future, LFC, LFI, RFC, RFI } from "./context";
import type { Options } from "./options";

// Resonate functions

export type Func = (ctx: Context, ...args: any[]) => any;

export type Params<F> = F extends (ctx: Context, ...args: infer P) => any ? P : never;
export type ParamsWithOptions<F> = [...Params<F>, Options?];

export type Yieldable<T = any> = LFI<T> | LFC<T> | RFI<T> | RFC<T> | Future<T>;

export type Return<T> = T extends (...args: any[]) => Generator<infer __, infer R, infer _>
  ? R // Return type of generator
  : T extends (...args: any[]) => infer R
    ? Awaited<R> // Return type of regular function
    : never;

// Result

export type Result<T> = Ok<T> | Ko;

type Ok<T> = {
  success: true;
  value: T;
};

type Ko = {
  success: false;
  error: any;
};

export function ok<T>(value: T): Result<T> {
  return { success: true, value };
}

export function ko(error: any): Result<any> {
  return { success: false, error };
}

// Callback

export type Callback<T> = {
  (err: false, res: T): void;
  (err: true, res?: undefined): void;
};
