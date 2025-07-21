import * as util from "./util";
export type AnyGen<T> = (...args: any[]) => Generator<any, T, any>;
export type AnyFun<T> = (...args: any[]) => T;

export type Invocable<T> = AnyGen<T> | AnyFun<T>;
export type Yieldable = InvokeLocal<any> | InvokeRemote<any> | Future<any> | CallLocal<any> | CallRemote<any>;

export class InvokeLocal<T> implements Iterable<InvokeLocal<T>> {
  public func: Invocable<T>;
  public args: any[];

  constructor(func: Invocable<T>, ...args: any[]) {
    this.func = func;
    this.args = args;
  }

  *[Symbol.iterator](): Generator<InvokeLocal<T>, Future<T>, any> {
    const v = yield this;
    util.assert(v instanceof Future, "expected Future");
    return v as Future<T>;
  }
}

export class InvokeRemote<T> implements Iterable<InvokeRemote<T>> {
  public func: string;
  public args: any[];

  constructor(func: string, ...args: any[]) {
    this.func = func;
    this.args = args;
  }

  *[Symbol.iterator](): Generator<InvokeRemote<T>, Future<T>, any> {
    const v = yield this;
    util.assert(v instanceof Future, "expected Future");
    return v as Future<T>;
  }
}

export class CallLocal<T> implements Iterable<CallLocal<T>> {
  public func: Invocable<T>;
  public args: any[];

  constructor(func: Invocable<T>, ...args: any[]) {
    this.func = func;
    this.args = args;
  }

  *[Symbol.iterator](): Generator<CallLocal<T>, T, any> {
    const v = yield this;
    util.assert(!(v instanceof Future), "expected a value other than Future");
    return v as T;
  }
}

export class CallRemote<T> implements Iterable<CallRemote<T>> {
  public func: string;
  public args: any[];

  constructor(func: string, ...args: any[]) {
    this.func = func;
    this.args = args;
  }

  *[Symbol.iterator](): Generator<CallRemote<T>, T, any> {
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

export class Context {}

export function lfi<R>(func: Invocable<R>, ...args: any[]): InvokeLocal<R> {
  return new InvokeLocal<R>(func, ...args);
}

export function rfi<R>(func: string, ...args: any[]): InvokeRemote<R> {
  return new InvokeRemote<R>(func, ...args);
}

export function lfc<R>(func: Invocable<R>, ...args: any[]): CallLocal<R> {
  return new CallLocal<R>(func, ...args);
}

export function rfc<R>(func: string, ...args: any[]): CallRemote<R> {
  return new CallRemote<R>(func, ...args);
}
