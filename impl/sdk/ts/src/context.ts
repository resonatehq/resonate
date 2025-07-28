import type { Func, Params, RemoteOpts, Ret } from "./types";
import * as util from "./util";

const defaultRemoteOpts: RemoteOpts = {
  target: "default",
  timeout: Number.MAX_SAFE_INTEGER,
};

export type Yieldable = InvokeLocal<any> | InvokeRemote<any> | Future<any> | CallLocal<any> | CallRemote<any>;

export class InvokeLocal<T> implements Iterable<InvokeLocal<T>> {
  public func: Func;
  public args: any[];

  constructor(func: Func, args: any[]) {
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
  public opts: RemoteOpts;

  constructor(func: string, args: any[]) {
    this.func = func;
    this.args = args;
    this.opts = { ...defaultRemoteOpts };
  }

  options(opts: Partial<RemoteOpts>): InvokeRemote<T> {
    this.opts = { ...this.opts, ...opts };
    return this;
  }

  *[Symbol.iterator](): Generator<InvokeRemote<T>, Future<T>, any> {
    const v = yield this;
    util.assert(v instanceof Future, "expected Future");
    return v as Future<T>;
  }
}

export class CallLocal<T> implements Iterable<CallLocal<T>> {
  public func: Func;
  public args: any[];

  constructor(func: Func, args: any[]) {
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
  public opts: RemoteOpts;

  constructor(func: string, args: any[]) {
    this.func = func;
    this.args = args;
    this.opts = { ...defaultRemoteOpts };
  }

  options(opts: Partial<RemoteOpts>): CallRemote<T> {
    this.opts = { ...this.opts, ...opts };
    return this;
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

export class Context {
  // TODO(avillega): Allow user to define other opts
  beginRun<F extends Func>(func: F, ...args: Params<F>): InvokeLocal<Ret<F>> {
    return new InvokeLocal<Ret<F>>(func, args);
  }

  // TODO(avillega): Allow user to define target, and opts
  beginRpc<R>(func: string, ...args: any[]): InvokeRemote<R> {
    return new InvokeRemote<R>(func, args);
  }

  // TODO(avillega): Allow user to define other opts
  run<F extends Func>(func: F, ...args: Params<F>): CallLocal<Ret<F>> {
    return new CallLocal<Ret<F>>(func, args);
  }

  // TODO(avillega): Allow user to define target and opts
  rpc<R>(func: string, ...args: any[]): CallRemote<R> {
    return new CallRemote<R>(func, args);
  }
}
