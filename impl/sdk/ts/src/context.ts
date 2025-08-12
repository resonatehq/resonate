import { type Func, type Options, type ParamsWithOptions, RESONATE_OPTIONS, type Return } from "./types";
import * as util from "./util";

export type Yieldable = LFI<any> | LFC<any> | RFI<any> | RFC<any> | Future<any>;

export class LFI<T> implements Iterable<LFI<T>> {
  public func: Func;
  public args: any[];
  public opts: Options;

  constructor(func: Func, args: any[], opts: Options) {
    this.func = func;
    this.args = args;
    this.opts = opts;
  }

  *[Symbol.iterator](): Generator<LFI<T>, Future<T>, any> {
    const v = yield this;
    util.assert(v instanceof Future, "expected future");
    return v as Future<T>;
  }
}

export class LFC<T> implements Iterable<LFC<T>> {
  public func: Func;
  public args: any[];
  public opts: Options;

  constructor(func: Func, args: any[], opts: Options) {
    this.func = func;
    this.args = args;
    this.opts = opts;
  }

  *[Symbol.iterator](): Generator<LFC<T>, T, any> {
    const v = yield this;
    util.assert(!(v instanceof Future), "expected value");
    return v as T;
  }
}

export class RFI<T> implements Iterable<RFI<T>> {
  public func: string;
  public args: any[];
  public opts: Options;

  constructor(func: string, args: any[], opts: Options) {
    this.func = func;
    this.args = args;
    this.opts = opts;
  }

  *[Symbol.iterator](): Generator<RFI<T>, Future<T>, any> {
    const v = yield this;
    util.assert(v instanceof Future, "expected future");
    return v as Future<T>;
  }
}

export class RFC<T> implements Iterable<RFC<T>> {
  public func: string;
  public args: any[];
  public opts: Options;

  constructor(func: string, args: any[], opts: Options) {
    this.func = func;
    this.args = args;
    this.opts = opts;
  }

  *[Symbol.iterator](): Generator<RFC<T>, T, any> {
    const v = yield this;
    util.assert(!(v instanceof Future), "expected value");
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
  run = this.lfc;
  rpc = this.rfc;
  beginRun = this.lfi;
  beginRpc = this.rfi;

  lfi<F extends Func>(func: F, ...args: ParamsWithOptions<F>): LFI<Return<F>> {
    return new LFI(func, ...util.splitArgsAndOpts(args, this.options()));
  }

  lfc<F extends Func>(func: F, ...args: ParamsWithOptions<F>): LFC<Return<F>> {
    return new LFC(func, ...util.splitArgsAndOpts(args, this.options()));
  }

  rfi<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFI<Return<F>>;
  rfi<T>(func: string, ...args: any[]): RFI<T>;
  rfi(func: Func | string, ...args: any[]): RFI<any> {
    if (typeof func === "function") {
      throw new Error("not implemented");
    }
    return new RFI(func, ...util.splitArgsAndOpts(args, this.options()));
  }

  rfc<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFC<Return<F>>;
  rfc<T>(func: string, ...args: any[]): RFC<T>;
  rfc(func: Func | string, ...args: any[]): RFC<any> {
    if (typeof func === "function") {
      throw new Error("not implemented");
    }
    return new RFC(func, ...util.splitArgsAndOpts(args, this.options()));
  }

  options(opts: Partial<Options> = {}): Options & { [RESONATE_OPTIONS]: true } {
    return {
      id: "",
      target: "default",
      timeout: 24 * util.HOUR,
      ...opts,
      [RESONATE_OPTIONS]: true,
    };
  }
}
