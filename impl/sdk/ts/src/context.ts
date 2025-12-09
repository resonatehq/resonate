import type { Clock } from "./clock";
import exceptions, { type ResonateError } from "./exceptions";
import type { CreatePromiseReq } from "./network/network";
import type { Options, OptionsBuilder } from "./options";
import type { Registry } from "./registry";
import { Exponential, Never, type RetryPolicy } from "./retries";
import type { Span } from "./tracer";
import type { Func, ParamsWithOptions, Result, Return } from "./types";
import * as util from "./util";

export class LFI<T> implements Iterable<LFI<T>> {
  public id: string;
  public func: Func;
  public args: any[];
  public version: number;
  public retryPolicy: RetryPolicy;
  public createReq: CreatePromiseReq<any>;

  constructor(
    id: string,
    func: Func,
    args: any[],
    version: number,
    retryPolicy: RetryPolicy,
    createReq: CreatePromiseReq<any>,
  ) {
    this.id = id;
    this.func = func;
    this.args = args;
    this.version = version;
    this.retryPolicy = retryPolicy;
    this.createReq = createReq;
  }

  *[Symbol.iterator](): Generator<LFI<T>, Future<T>, any> {
    const v = yield this;
    util.assert(v instanceof Future, "expected future");
    return v as Future<T>;
  }
}

export class LFC<T> implements Iterable<LFC<T>> {
  public id: string;
  public func: Func;
  public args: any[];
  public version: number;
  public retryPolicy: RetryPolicy;
  public createReq: CreatePromiseReq;

  constructor(
    id: string,
    func: Func,
    args: any[],
    version: number,
    retryPolicy: RetryPolicy,
    createReq: CreatePromiseReq,
  ) {
    this.id = id;
    this.func = func;
    this.args = args;
    this.version = version;
    this.retryPolicy = retryPolicy;
    this.createReq = createReq;
  }

  *[Symbol.iterator](): Generator<LFC<T>, T, any> {
    const v = yield this;
    util.assert(!(v instanceof Future), "expected value");
    return v as T;
  }
}

export class RFI<T> implements Iterable<RFI<T>> {
  public id: string;
  public func: string;
  public version: number;
  public createReq: CreatePromiseReq;
  public mode: "attached" | "detached";

  constructor(
    id: string,
    func: string,
    version: number,
    createReq: CreatePromiseReq,
    mode: "attached" | "detached" = "attached",
  ) {
    this.id = id;
    this.func = func;
    this.version = version;
    this.createReq = createReq;
    this.mode = mode;
  }

  *[Symbol.iterator](): Generator<RFI<T>, Future<T>, any> {
    const v = yield this;
    util.assert(v instanceof Future, "expected future");
    return v as Future<T>;
  }
}

export class RFC<T> implements Iterable<RFC<T>> {
  public id: string;
  public func: string;
  public version: number;
  public createReq: CreatePromiseReq;
  public mode = "attached" as const;

  constructor(id: string, func: string, version: number, createReq: CreatePromiseReq) {
    this.id = id;
    this.func = func;
    this.version = version;
    this.createReq = createReq;
  }

  *[Symbol.iterator](): Generator<RFC<T>, T, any> {
    const v = yield this;
    util.assert(!(v instanceof Future), "expected value");
    return v as T;
  }
}

export class DIE implements Iterable<DIE> {
  public condition: boolean;
  public error: ResonateError;

  constructor(condition: boolean, error: ResonateError) {
    this.condition = condition;
    this.error = error;
  }

  *[Symbol.iterator](): Generator<DIE, void, any> {
    yield this;
    return;
  }
}

export class Future<T> implements Iterable<Future<T>> {
  private readonly value?: Result<T>;
  public readonly state: "pending" | "completed";
  private mode: "attached" | "detached";

  constructor(
    public id: string,
    state: "pending" | "completed",
    value?: Result<T>,
    mode: "attached" | "detached" = "attached",
  ) {
    this.value = value;
    this.state = state;
    this.mode = mode;
  }

  getValue() {
    if (!this.value) {
      throw new Error("Future is not ready");
    }

    if (this.value.success) {
      return this.value.value;
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
  readonly id: string;
  readonly parentId: string;
  readonly rootId: string;
  readonly info: { readonly attempt: number; readonly timeout: number; readonly version: number };

  // core four
  lfi<F extends Func>(func: F, ...args: ParamsWithOptions<F>): LFI<Return<F>>;
  lfi<T>(func: string, ...args: any[]): LFI<T>;
  lfc<F extends Func>(func: F, ...args: ParamsWithOptions<F>): LFC<Return<F>>;
  lfc<T>(func: string, ...args: any[]): LFC<T>;
  rfi<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFI<Return<F>>;
  rfi<T>(func: string, ...args: any[]): RFI<T>;
  rfc<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFC<Return<F>>;
  rfc<T>(func: string, ...args: any[]): RFC<T>;

  // beginRun (lfi alias)
  beginRun<F extends Func>(func: F, ...args: ParamsWithOptions<F>): LFI<Return<F>>;
  beginRun<T>(func: string, ...args: any[]): LFI<T>;

  // run (lfc alias)
  run<F extends Func>(func: F, ...args: ParamsWithOptions<F>): LFC<Return<F>>;
  run<T>(func: string, ...args: any[]): LFC<T>;

  // beginRpc (rfi alias)
  beginRpc<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFI<Return<F>>;
  beginRpc<T>(func: string, ...args: any[]): RFI<T>;

  // rpc (rfc alias)
  rpc<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFC<Return<F>>;
  rpc<T>(func: string, ...args: any[]): RFC<T>;

  // detached
  detached<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFI<Return<F>>;
  detached<T>(func: string, ...args: any[]): RFI<T>;

  // sleep
  sleep(ms: number): RFC<void>;
  sleep(opts: { for?: number; until?: Date }): RFC<void>;
  sleep(msOrOpts: number | { for?: number; until?: Date }): RFC<void>;

  // promise
  promise<T>(): RFI<T>;
  promise<T>({
    id,
    timeout,
    data,
    tags,
  }: {
    id?: string;
    timeout?: number;
    data?: any;
    tags?: Record<string, string>;
  }): RFI<T>;

  // die

  // Aborts the execution of the root promise if condition is true
  panic(condition: boolean, msg?: string): DIE;

  // Aborts the execution of the root promise if condition is false
  assert(condition: boolean, msg?: string): DIE;

  // getDependency
  getDependency<T = any>(key: string): T | undefined;

  // options
  options(opts?: Partial<Options>): Options;

  // date
  date: ResonateDate;

  // random
  math: ResonateMath;
}

export interface ResonateDate {
  now(): LFC<number>;
}

export interface ResonateMath {
  random(): LFC<number>;
}

export class InnerContext implements Context {
  readonly id: string;
  readonly info: { attempt: number; readonly timeout: number; readonly version: number };
  readonly func: string;
  readonly retryPolicy: RetryPolicy;

  readonly rootId: string;
  readonly parentId: string;
  readonly clock: Clock;
  readonly span: Span;
  private registry: Registry;
  private dependencies: Map<string, any>;
  private optsBuilder: OptionsBuilder;
  private seq = 0;

  run = this.lfc.bind(this);
  rpc = this.rfc.bind(this);
  beginRun = this.lfi.bind(this);
  beginRpc = this.rfi.bind(this);

  constructor({
    id,
    rId = id,
    pId = id,
    func,
    clock,
    registry,
    dependencies,
    optsBuilder,
    timeout,
    version,
    retryPolicy,
    span,
  }: {
    id: string;
    rId?: string;
    pId?: string;
    func: string;
    clock: Clock;
    registry: Registry;
    dependencies: Map<string, any>;
    optsBuilder: OptionsBuilder;
    timeout: number;
    version: number;
    retryPolicy: RetryPolicy;
    span: Span;
  }) {
    this.id = id;
    this.rootId = rId;
    this.parentId = pId;
    this.func = func;
    this.clock = clock;
    this.registry = registry;
    this.dependencies = dependencies;
    this.optsBuilder = optsBuilder;
    this.retryPolicy = retryPolicy;
    this.span = span;

    this.info = {
      attempt: 1,
      timeout,
      version,
    };
  }

  child({
    id,
    func,
    timeout,
    version,
    retryPolicy,
    span,
  }: {
    id: string;
    func: string;
    timeout: number;
    version: number;
    retryPolicy: RetryPolicy;
    span: Span;
  }) {
    return new InnerContext({
      id,
      rId: this.rootId,
      pId: this.id,
      func,
      clock: this.clock,
      registry: this.registry,
      dependencies: this.dependencies,
      optsBuilder: this.optsBuilder,
      timeout,
      version,
      retryPolicy,
      span,
    });
  }

  lfi<F extends Func>(func: F, ...args: ParamsWithOptions<F>): LFI<Return<F>>;
  lfi<T>(func: string, ...args: any[]): LFI<T>;
  lfi(funcOrName: Func | string, ...args: any[]): LFI<any> {
    const [argu, opts] = util.splitArgsAndOpts(args, this.options());
    const registered = this.registry.get(funcOrName, opts.version);

    if (typeof funcOrName === "string" && !registered) {
      // This results in a dropped task and a value will never be
      // yielded back to the users coroutine. However, the type
      // system indicates the value is void. Casting to LFI "tricks"
      // the type system to indicate the correct type.
      return new DIE(
        true,
        exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(funcOrName, opts.version),
      ) as unknown as LFI<any>;
    }

    const id = opts.id ?? this.seqid();
    this.seq++;

    const func = registered ? registered.func : (funcOrName as Func);
    const version = registered ? registered.version : 1;

    return new LFI(
      id,
      func,
      argu,
      version,
      opts.retryPolicy ?? (util.isGeneratorFunction(func) ? new Never() : new Exponential()),
      this.localCreateReq(id, { func: func.name, version }, opts),
    );
  }

  lfc<F extends Func>(func: F, ...args: ParamsWithOptions<F>): LFC<Return<F>>;
  lfc<T>(func: string, ...args: any[]): LFC<T>;
  lfc(funcOrName: Func | string, ...args: any[]): LFC<any> {
    const [argu, opts] = util.splitArgsAndOpts(args, this.options());
    const registered = this.registry.get(funcOrName, opts.version);

    if (typeof funcOrName === "string" && !registered) {
      // This results in a dropped task and a value will never be
      // yielded back to the users coroutine. However, the type
      // system indicates the value is void. Casting to LFC "tricks"
      // the type system to indicate the correct type.
      return new DIE(
        true,
        exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(funcOrName, opts.version),
      ) as unknown as LFC<any>;
    }

    const id = opts.id ?? this.seqid();
    this.seq++;

    const func = registered ? registered.func : (funcOrName as Func);
    const version = registered ? registered.version : 1;

    return new LFC(
      id,
      func,
      argu,
      version,
      opts.retryPolicy ?? (util.isGeneratorFunction(func) ? new Never() : new Exponential()),
      this.localCreateReq(id, { func: func.name, version }, opts),
    );
  }

  rfi<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFI<Return<F>>;
  rfi<T>(func: string, ...args: any[]): RFI<T>;
  rfi(funcOrName: Func | string, ...args: any[]): RFI<any> {
    const [argu, opts] = util.splitArgsAndOpts(args, this.options());
    const registered = this.registry.get(funcOrName, opts.version);

    if (typeof funcOrName === "function" && !registered) {
      // This results in a dropped task and a value will never be
      // yielded back to the users coroutine. However, the type
      // system indicates the value is void. Casting to RFI "tricks"
      // the type system to indicate the correct type.
      return new DIE(
        true,
        exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(funcOrName.name, opts.version),
      ) as unknown as RFI<any>;
    }

    const id = opts.id ?? this.seqid();
    this.seq++;

    const func = registered ? registered.name : (funcOrName as string);
    const version = registered ? registered.version : 1;

    const data = {
      func: func,
      args: argu,
      retry: opts.retryPolicy?.encode(),
      version: registered ? registered.version : opts.version || 1,
    };

    return new RFI(id, func, version, this.remoteCreateReq(id, data, opts));
  }

  rfc<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFC<Return<F>>;
  rfc<T>(func: string, ...args: any[]): RFC<T>;
  rfc(funcOrName: Func | string, ...args: any[]): RFC<any> {
    const [argu, opts] = util.splitArgsAndOpts(args, this.options());
    const registered = this.registry.get(funcOrName, opts.version);

    if (typeof funcOrName === "function" && !registered) {
      // This results in a dropped task and a value will never be
      // yielded back to the users coroutine. However, the type
      // system indicates the value is void. Casting to RFC "tricks"
      // the type system to indicate the correct type.
      return new DIE(
        true,
        exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(funcOrName.name, opts.version),
      ) as unknown as RFC<any>;
    }

    const id = opts.id ?? this.seqid();
    this.seq++;

    const func = registered ? registered.name : (funcOrName as string);
    const version = registered ? registered.version : 1;

    const data = {
      func: func,
      args: argu,
      retry: opts.retryPolicy?.encode(),
      version: registered ? registered.version : opts.version || 1,
    };

    return new RFC(id, func, version, this.remoteCreateReq(id, data, opts));
  }

  detached<F extends Func>(func: F, ...args: ParamsWithOptions<F>): RFI<Return<F>>;
  detached<T>(func: string, ...args: any[]): RFI<T>;
  detached(funcOrName: Func | string, ...args: any[]): RFI<any> {
    const [argu, opts] = util.splitArgsAndOpts(args, this.options());
    const registered = this.registry.get(funcOrName, opts.version);

    if (typeof funcOrName === "function" && !registered) {
      // This results in a dropped task and a value will never be
      // yielded back to the users coroutine. However, the type
      // system indicates the value is void. Casting to RFI "tricks"
      // the type system to indicate the correct type.
      return new DIE(
        true,
        exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(funcOrName.name, opts.version),
      ) as unknown as RFI<any>;
    }

    const id = opts.id ?? this.seqid();
    this.seq++;

    const func = registered ? registered.name : (funcOrName as string);
    const version = registered ? registered.version : 1;

    const data = {
      func: func,
      args: argu,
      retry: opts.retryPolicy?.encode(),
      version: registered ? registered.version : opts.version || 1,
    };

    return new RFI(id, func, version, this.remoteCreateReq(id, data, opts, Number.MAX_SAFE_INTEGER), "detached");
  }

  promise<T>({
    id,
    timeout,
    data,
    tags,
  }: {
    id?: string;
    timeout?: number;
    data?: any;
    tags?: Record<string, string>;
  } = {}): RFI<T> {
    id = id ?? this.seqid();
    this.seq++;

    return new RFI(id, "unknown", 1, this.latentCreateOpts(id, timeout, data, tags));
  }

  sleep(msOrOpts: number | { for?: number; until?: Date }): RFC<void> {
    let until: number;

    if (typeof msOrOpts === "number") {
      until = this.clock.now() + msOrOpts;
    } else if (msOrOpts.for != null) {
      until = this.clock.now() + msOrOpts.for;
    } else if (msOrOpts.until != null) {
      until = msOrOpts.until.getTime();
    } else {
      until = 0;
    }

    const id = this.seqid();
    this.seq++;

    return new RFC(id, "sleep", 1, this.sleepCreateOpts(id, until));
  }

  panic(condition: boolean, msg?: string): DIE {
    const src = util.getCallerInfo();
    return new DIE(condition, exceptions.PANIC(src, msg));
  }

  assert(condition: boolean, msg?: string): DIE {
    return this.panic(!condition, msg);
  }

  getDependency<T = any>(name: string): T | undefined {
    return this.dependencies.get(name);
  }

  options(
    opts: Partial<Pick<Options, "id" | "tags" | "target" | "timeout" | "version" | "retryPolicy">> = {},
  ): Options {
    return this.optsBuilder.build(opts);
  }

  readonly date = {
    now: () => this.lfc((this.getDependency<DateConstructor>("resonate:date") ?? Date).now),
  };

  readonly math = {
    random: () => this.lfc((this.getDependency<Math>("resonate:math") ?? Math).random),
  };

  localCreateReq(id: string, data: any, opts: Options): CreatePromiseReq {
    const tags = {
      "resonate:scope": "local",
      "resonate:root": this.rootId,
      "resonate:parent": this.id,
      ...opts.tags,
    };

    // timeout cannot be greater than parent timeout
    const timeout = Math.min(this.clock.now() + opts.timeout, this.info.timeout);

    return {
      kind: "createPromise",
      id,
      timeout: timeout,
      param: { data },
      tags,
      iKey: id,
      strict: false,
    };
  }

  remoteCreateReq(id: string, data: any, opts: Options, maxTimeout = this.info.timeout): CreatePromiseReq {
    const tags = {
      "resonate:scope": "global",
      "resonate:invoke": opts.target,
      "resonate:root": this.rootId,
      "resonate:parent": this.id,
      ...opts.tags,
    };

    // timeout cannot be greater than parent timeout (unless detached)
    const timeout = Math.min(this.clock.now() + opts.timeout, maxTimeout);

    return {
      kind: "createPromise",
      id,
      timeout,
      tags,
      param: { data },
      iKey: id,
      strict: false,
    };
  }

  latentCreateOpts(id: string, timeout?: number, data?: any, tags?: Record<string, string>): CreatePromiseReq {
    const cTags = {
      "resonate:scope": "global",
      "resonate:root": this.rootId,
      "resonate:parent": this.id,
      ...tags,
    };

    // timeout cannot be greater than parent timeout
    const cTimeout = Math.min(this.clock.now() + (timeout ?? 24 * util.HOUR), this.info.timeout);

    return {
      kind: "createPromise",
      id: id,
      timeout: cTimeout,
      param: { data },
      tags: cTags,
      iKey: id,
      strict: false,
    };
  }

  sleepCreateOpts(id: string, time: number): CreatePromiseReq {
    const tags = {
      "resonate:scope": "global",
      "resonate:root": this.rootId,
      "resonate:parent": this.id,
      "resonate:timeout": "true",
    };

    // timeout cannot be greater than parent timeout
    const timeout = Math.min(time, this.info.timeout);

    return {
      kind: "createPromise",
      id: id,
      timeout: timeout,
      param: {},
      tags,
      iKey: id,
      strict: false,
    };
  }

  seqid(): string {
    return `${this.id}.${this.seq}`;
  }
}
