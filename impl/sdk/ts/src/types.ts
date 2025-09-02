import type { Context, Future, LFC, LFI, RFC, RFI } from "./context";

// Resonate options

export const RESONATE_OPTIONS: unique symbol = Symbol("ResonateOptions");

export type Options = {
  id: string;
  target: string;
  tags: Record<string, string>;
  timeout: number;
};

// Resonate functions

export type Func = (ctx: Context, ...args: any[]) => any;

export type Params<F> = F extends (ctx: Context, ...args: infer P) => any ? P : never;
export type ParamsWithOptions<F> = [...Params<F>, (Partial<Options> & { [RESONATE_OPTIONS]: true })?];

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
