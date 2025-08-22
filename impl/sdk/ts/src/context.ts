import type { Clock } from "./clock";
import type { CreatePromiseReq } from "./network/network";
import { type Func, type Options, type ParamsWithOptions, RESONATE_OPTIONS, type Result, type Return } from "./types";
import * as util from "./util";

export type Yieldable = LFI<any> | LFC<any> | RFI<any> | RFC<any> | Future<any>;

export class LFI<T> implements Iterable<LFI<T>> {
  public func: Func;
  public args: any[];
  public createReq: CreatePromiseReq;

  constructor(func: Func, args: any[], createReq: CreatePromiseReq) {
    this.func = func;
    this.args = args;
    this.createReq = createReq;
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
  public createReq: CreatePromiseReq;

  constructor(func: Func, args: any[], createReq: CreatePromiseReq) {
    this.func = func;
    this.args = args;
    this.createReq = createReq;
  }

  *[Symbol.iterator](): Generator<LFC<T>, T, any> {
    const v = yield this;
    util.assert(!(v instanceof Future), "expected value");
    return v as T;
  }
}

export class RFI<T> implements Iterable<RFI<T>> {
  public createReq: CreatePromiseReq;

  constructor(createReq: CreatePromiseReq) {
    this.createReq = createReq;
  }

  *[Symbol.iterator](): Generator<RFI<T>, Future<T>, any> {
    const v = yield this;
    util.assert(v instanceof Future, "expected future");
    return v as Future<T>;
  }
}

export class RFC<T> implements Iterable<RFC<T>> {
  public createReq: CreatePromiseReq;

  constructor(createReq: CreatePromiseReq) {
    this.createReq = createReq;
  }

  *[Symbol.iterator](): Generator<RFC<T>, T, any> {
    const v = yield this;
    util.assert(!(v instanceof Future), "expected value");
    return v as T;
  }
}

export class Future<T> implements Iterable<Future<T>> {
  private readonly value?: Result<T>;
  public readonly state: "pending" | "completed";

  constructor(
    public id: string,
    state: "pending" | "completed",
    value?: Result<T>,
  ) {
    this.value = value;
    this.state = state;
  }

  getValue() {
    if (!this.value) {
      throw new Error("Future is not ready");
    }

    if (this.value.success) {
      return this.value.data;
    }
    throw this.value.error; // Should be unreachble
  }

  *[Symbol.iterator](): Generator<Future<T>, T, undefined> {
    yield this;
    util.assertDefined(this.value);
    util.assert(this.value.success, "The value must be and ok result at this point.");
    return this.getValue();
  }
}

export interface Context {
  beginRun<F extends Func>(func: F, ...args: ParamsWithOptions<F>): LFI<Return<F>>;
  run<F extends Func>(func: F, ...args: ParamsWithOptions<F>): LFC<Return<F>>;

  beginRpc<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFI<Return<F>>;
  beginRpc<T>(func: string, ...args: any[]): RFI<T>;
  beginRpc(func: Func | string, ...args: any[]): RFI<any>;

  rpc<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFC<Return<F>>;
  rpc<T>(func: string, ...args: any[]): RFC<T>;
  rpc(func: Func | string, ...args: any[]): RFC<any>;

  promise<T>(options?: Partial<Options> & { [RESONATE_OPTIONS]: true }): RFI<T>;

  sleep(ms: number): RFC<void>;

  options(opts: Partial<Options>): Options & { [RESONATE_OPTIONS]: true };
}

export class InnerContext implements Context {
  run = this.lfc.bind(this);
  rpc = this.rfc.bind(this);
  beginRun = this.lfi.bind(this);
  beginRpc = this.rfi.bind(this);

  private seq: number;

  // TODO(avillega): set the parent timeout to be used to calculate the actual timeout for the createReq
  private constructor(
    private id: string,
    private rId: string,
    private pId: string,
    private clock: Clock,
  ) {
    this.seq = 0;
  }

  static root(id: string, clock: Clock) {
    return new InnerContext(id, id, id, clock);
  }

  child(id: string) {
    return new InnerContext(id, this.rId, this.id, this.clock);
  }

  lfi<F extends Func>(func: F, ...args: ParamsWithOptions<F>): LFI<Return<F>> {
    const [argu, opts] = util.splitArgsAndOpts(args, this.options());
    return new LFI(func, argu, this.localCreateReq(opts));
  }

  lfc<F extends Func>(func: F, ...args: ParamsWithOptions<F>): LFC<Return<F>> {
    const [argu, opts] = util.splitArgsAndOpts(args, this.options());
    return new LFC(func, argu, this.localCreateReq(opts));
  }

  rfi<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFI<Return<F>>;
  rfi<T>(func: string, ...args: any[]): RFI<T>;
  rfi(func: Func | string, ...args: any[]): RFI<any> {
    if (typeof func === "function") {
      throw new Error("not implemented");
    }
    const [argu, opts] = util.splitArgsAndOpts(args, this.options());
    return new RFI(this.remoteCreateReq(func, argu, opts));
  }

  rfc<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFC<Return<F>>;
  rfc<T>(func: string, ...args: any[]): RFC<T>;
  rfc(func: Func | string, ...args: any[]): RFC<any> {
    if (typeof func === "function") {
      throw new Error("not implemented");
    }
    const [argu, opts] = util.splitArgsAndOpts(args, this.options());
    return new RFC(this.remoteCreateReq(func, argu, opts));
  }

  promise<T>(options: Partial<Options> & { [RESONATE_OPTIONS]: true }): RFI<T> {
    return new RFI(this.latentCreateOpts(this.options(options)));
  }

  sleep(ms: number): RFC<void> {
    return new RFC(this.sleepCreateOpts(this.options({ timeout: ms })));
  }

  options(opts: Partial<Options> = {}): Options & { [RESONATE_OPTIONS]: true } {
    return {
      id: this.seqid(),
      target: "poll://any@default",
      timeout: 24 * util.HOUR,
      tags: {},
      ...opts,
      [RESONATE_OPTIONS]: true,
    };
  }

  localCreateReq(opts: Options): CreatePromiseReq {
    const tags = {
      "resonate:scope": "local",
      "resonate:root": this.rId,
      "resonate:parent": this.pId,
      ...opts.tags,
    };

    return {
      kind: "createPromise",
      id: opts.id,
      timeout: opts.timeout + this.clock.now(),
      param: {},
      tags,
      iKey: opts.id,
      strict: false,
    };
  }

  remoteCreateReq(func: string, args: any[], opts: Options): CreatePromiseReq {
    const tags = {
      "resonate:scope": "global",
      "resonate:invoke": opts.target,
      "resonate:root": this.rId,
      "resonate:parent": this.pId,
      ...opts.tags,
    };

    return {
      kind: "createPromise",
      id: opts.id,
      timeout: opts.timeout + this.clock.now(),
      tags,
      param: {
        func,
        args,
      },
      iKey: opts.id,
      strict: false,
    };
  }

  latentCreateOpts(opts: Options): CreatePromiseReq {
    const tags = {
      "resonate:scope": "global",
      "resonate:root": this.rId,
      "resonate:parent": this.pId,
      ...opts.tags,
    };

    return {
      kind: "createPromise",
      id: opts.id,
      timeout: opts.timeout + this.clock.now(),
      param: {},
      tags,
      iKey: opts.id,
      strict: false,
    };
  }

  sleepCreateOpts(opts: Options): CreatePromiseReq {
    const tags = {
      "resonate:scope": "global",
      "resonate:root": this.rId,
      "resonate:parent": this.pId,
      "resonate:timeout": "true",
      ...opts.tags,
    };

    return {
      kind: "createPromise",
      id: opts.id,
      timeout: opts.timeout + this.clock.now(),
      param: {},
      tags,
      iKey: opts.id,
      strict: false,
    };
  }

  seqid(): string {
    return `${this.id}.${this.seq++}`;
  }
}
