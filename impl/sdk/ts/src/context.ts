import * as util from "./util";
export type AnyGen<T> = (...args: any[]) => Generator<any, T, any>;
export type AnyFun<T> = (...args: any[]) => T;

export type Invocable<T> = AnyGen<T> | AnyFun<T>;
export type Yieldable = Invoke<any> | Future<any> | Call<any>;

export class Invoke<T> implements Iterable<Invoke<T>> {
  public type: "lfi" | "rfi";
  public func: Invocable<T>;
  public args: any[];

  constructor(type: "lfi" | "rfi", func: Invocable<T>, ...args: any[]) {
    this.type = type;
    this.func = func;
    this.args = args;
  }

  *[Symbol.iterator](): Generator<Invoke<T>, Future<T>, any> {
    const v = yield this;
    util.assert(v instanceof Future, "expected Future");
    return v as Future<T>;
  }
}

export class Call<T> implements Iterable<Call<T>> {
  public type: "lfc" | "rfc";
  public func: Invocable<T>;
  public args: any[];

  constructor(type: "lfc" | "rfc", func: Invocable<T>, ...args: any[]) {
    this.type = type;
    this.func = func;
    this.args = args;
  }

  *[Symbol.iterator](): Generator<Call<T>, T, any> {
    const v = yield this;
    util.assert(!(v instanceof Future), "expected a value other than Future");
    return v as T;
  }
}

export class Future<T> implements Iterable<Future<T>> {
  public readonly value?: T;
  private state: "pending" | "completed";

  constructor(
    public id: string,
    state: "pending" | "completed",
    value?: T,
  ) {
    this.value = value;
    this.state = state;
  }

  isCompleted(): boolean {
    return this.state === "completed";
  }

  getValue(): T | undefined {
    return this.value;
  }

  *[Symbol.iterator](): Generator<Future<T>, T, undefined> {
    yield this;
    return this.value!;
  }
}

export function lfi<R>(func: Invocable<R>, ...args: any[]): Invoke<R> {
  return new Invoke<R>("lfi", func, ...args);
}

export function rfi<R>(func: Invocable<R>, ...args: any[]): Invoke<R> {
  return new Invoke<R>("rfi", func, ...args);
}

export function lfc<R>(func: Invocable<R>, ...args: any[]): Call<R> {
  return new Call<R>("lfc", func, ...args);
}

export function rfc<R>(func: Invocable<R>, ...args: any[]): Call<R> {
  return new Call<R>("rfc", func, ...args);
}
