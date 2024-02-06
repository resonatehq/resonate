import { Opts, ContextOpts, isContextOpts } from "./core/opts";
import { IStore } from "./core/store";
import { DurablePromise, isPendingPromise, isResolvedPromise } from "./core/promise";
import { Retry } from "./core/retries/retry";
import { IBucket } from "./core/bucket";
import { Bucket } from "./core/buckets/bucket";
import { LocalStore } from "./core/stores/local";
import { RemoteStore } from "./core/stores/remote";
import { ILogger, ITrace } from "./core/logger";
import { Logger } from "./core/loggers/logger";
import { JSONEncoder } from "./core/encoders/json";
import { ResonateError } from "./core/error";
import { ICache } from "./core/cache";
import { Cache } from "./core/caches/cache";
import { Schedule } from "./core/schedule";
import { IEncoder } from "./core/encoder";

// Types

// Resonate supports any function that takes a context as the first
// argument. The generic parameter is used to restrict the return
// type when a generator used.
type Func<T = any> = (ctx: Context, ...args: any[]) => T;

// Similar to the built in Parameters type, but ignores the required
// context parameter.
type Params<F extends Func> = F extends (ctx: Context, ...args: infer P) => any ? P : never;

// Similar to the built in ReturnType type, but optionally returns
// the awaited inferred return value of F if F is a generator,
// otherwise returns the awaited inferred return value.
type Return<F extends Func> = F extends (ctx: Context, ...args: any) => infer R
  ? R extends Generator<unknown, infer G>
    ? Awaited<G>
    : Awaited<R>
  : never;

function isFunction(f: unknown): f is Func {
  return (
    typeof f === "function" &&
    (f.constructor.name === "Function" || f.constructor.name === "AsyncFunction" || f.constructor.name === "")
  );
}

function isGenerator(f: unknown): f is Func<Generator> {
  return typeof f === "function" && f.constructor.name === "GeneratorFunction";
}

// Resonate

type ResonateOpts = {
  url: string;
  pid: string;
  namespace: string;
  seperator: string;
  store: IStore;
  bucket: IBucket;
  logger: ILogger;
  encoder: IEncoder<unknown, string | undefined>;
  recoveryDelay: number;
};

export class Resonate {
  private functions: Record<string, { func: Func; opts: Opts }> = {};

  private cache: ICache<Promise<any>> = new Cache();

  private recoveryDelay: number;

  private recoveryInterval: number | undefined = undefined;

  private store: IStore;

  private bucket: IBucket;

  readonly logger: ILogger;

  readonly encoder: IEncoder<unknown, string | undefined>;

  readonly pid: string;

  readonly namespace: string;

  readonly seperator: string;

  /**
   * Instantiate a new Resonate instance.
   *
   * @param opts.url optional url of a DurablePromiseStore, if not provided a VolatilePromiseStore will be used
   * @param opts.logger optional logger, defaults to a the default Logger
   * @param opts.timeout optional timeout for function invocations, defaults to 10000ms
   * @param opts.retry optional retry policy constructor, defaults to Retry.atLeastOnce()
   * @param opts.bucket optional default bucket, defaults to "default"
   * @param opts.encoder optional default encoder, defaults to "default"
   */
  constructor({
    url,
    pid = randomId(),
    namespace = "",
    seperator = "/",
    store,
    logger = new Logger(),
    bucket = new Bucket(),
    encoder = new JSONEncoder(),
    recoveryDelay = 1000,
  }: Partial<ResonateOpts> = {}) {
    this.pid = pid;
    this.namespace = namespace;
    this.seperator = seperator;

    this.bucket = bucket;
    this.logger = logger;
    this.encoder = encoder;
    this.recoveryDelay = recoveryDelay;

    // store
    if (store) {
      this.store = store;
    } else if (url) {
      this.store = new RemoteStore(url, this.pid, logger);
    } else {
      this.store = new LocalStore(logger);
    }
  }

  /**
   * Register a function with Resonate. Registered functions can be invoked with {@link run}, or by the returned function.
   *
   * @param name a name to identify the function
   * @param func the function to register with resonate
   * @param opts optional resonate options
   * @returns Resonate function
   */
  register<F extends Func>(
    name: string,
    func: F,
    opts: ContextOpts = new ContextOpts(),
  ): (id: string, ...args: Params<F>) => Promise<Return<F>> {
    if (name in this.functions) {
      throw new Error(`Function ${name} already registered`);
    }

    this.functions[name] = {
      // the function
      func: func,

      // inject sensible defaults
      opts: {
        timeout: 10000,
        retry: Retry.exponential(),
        encoder: new JSONEncoder(),
        eid: randomId(),
        ...opts.all(),
      },
    };

    return (id: string, ...args: Params<F>): Promise<Return<F>> => {
      return this.run(name, id, ...args);
    };
  }

  /**
   * Register a module with Resonate. Registered module functions can be invoked with run.
   *
   * @param module the javascript module
   * @param opts optional resonate options
   */
  registerModule(module: Record<string, Func>, opts: ContextOpts = new ContextOpts()) {
    for (const key in module) {
      this.register(key, module[key], opts);
    }
  }

  /**
   * Invoke a Resonate function.
   *
   * @param name the name of the registered function
   * @param id a unique identifier for the invocation
   * @param args arguments to pass to the function
   * @returns a promise that resolves to the return value of the function
   */
  run<T = any, P extends any[] = any[]>(name: string, id: string, ...args: P): Promise<T> {
    // use a constructed id for both the id and idempotency key
    // TODO: can user override the top level id / idempotency key?
    id = this.id(this.namespace, name, id);
    return this._run(name, id, id, args);
  }

  _run<T = any, P extends any[] = any[]>(
    name: string,
    id: string,
    idempotencyKey: string | undefined,
    args: P,
  ): Promise<T> {
    if (!(name in this.functions)) {
      throw new Error(`Function ${name} not registered`);
    }

    // grab the registered function and options
    const { func, opts } = this.functions[name];

    if (!this.cache.has(id)) {
      const promise = new Promise(async (resolve, reject) => {
        // lock
        while (!(await this.store.locks.tryAcquire(id, opts.eid))) {
          // sleep
          await new Promise((r) => setTimeout(r, 1000));
        }

        const context = new ResonateContext(this, this.store, this.bucket, name, id, idempotencyKey, opts);

        try {
          resolve(await context.execute(func, args));
        } catch (e) {
          reject(e);
        } finally {
          await this.store.locks.release(id, opts.eid);
        }
      });

      this.cache.set(id, promise);
    }

    return this.cache.get(id);
  }

  /**
   * Invoke a function on a recurring schedule.
   *
   * @param name
   * @param cron
   * @param func
   * @param args
   * @param opts
   * @returns Resonate function
   */
  schedule(name: string, cron: string, func: string, ...args: any[]): Promise<Schedule>;
  schedule<F extends Func>(name: string, cron: string, func: F, ...args: Params<F>): Promise<Schedule>;
  schedule<F extends Func>(
    name: string,
    cron: string,
    func: F,
    ...argsAndOpts: [...Params<F>, ContextOpts]
  ): Promise<Schedule>;
  schedule(name: string, cron: string, func: string | Func, ...argsAndOpts: any[]): Promise<Schedule> {
    let args: any[];
    let opts: Partial<Opts>;

    if (typeof func == "string") {
      if (!(func in this.functions)) {
        throw new Error(`Function ${func} not registered`);
      }

      args = argsAndOpts;
      opts = this.functions[func].opts;
    } else {
      ({ args, opts } = split(argsAndOpts));

      // only split the opts if func is provided, otherwise the top
      // level function is already registered with opts
      this.register(name, func, this.options(opts));
    }

    // lazily start the recovery loop
    this.recover();

    return this.store.schedules.create(
      name,
      name,
      undefined,
      cron,
      undefined,
      "{{.id}}/{{.timestamp}}",
      opts.timeout ?? 10000,
      undefined,
      this.encoder.encode({ func: typeof func === "string" ? func : name, args: args }),
      undefined,
    );
  }

  /**
   * Start resonate recovery path.
   *
   * Starts a control loop that polls the promise store for promises
   * that have the "resonate:invocation" tag. When a promise is
   * returned from the search, execute the corresponding function.
   */
  async recover() {
    if (this.recoveryInterval === undefined) {
      // the + converts to a number
      this.recoveryInterval = +setInterval(() => this._recover(), this.recoveryDelay);
    }
  }

  private async _recover() {
    const search = this.store.promises.search(this.id(this.namespace, "*"), "pending", {
      "resonate:invocation": "true",
    });

    for await (const promises of search) {
      for (const promise of promises) {
        try {
          const { func, args } = this.encoder.decode(promise.param.data) as { func: string; args: any[] };

          this._run(func, promise.id, promise.idempotencyKeyForCreate, args);
        } catch (e: unknown) {
          this.logger.warn(`Durable promise ${promise.id} failed on the recovery path`, e);
        }
      }
    }
  }

  /**
   * Construct context opts.
   *
   * @param opts Resonate {@link Opts}
   * @returns an instance of resonate opts
   */
  options(opts: Partial<Opts>): ContextOpts {
    return new ContextOpts(opts);
  }

  private id(...parts: string[]): string {
    return parts.filter((p) => p !== "").join(this.seperator);
  }
}

// Context

export interface Context {
  /**
   * The unique identifier of the context.
   */
  readonly id: string;

  /**
   * The idempotency key of the context, defaults to the id.
   */
  readonly idempotencyKey: string | undefined;

  /**
   * The absolute time the context will expire.
   */
  readonly timeout: number;

  /**
   * The time the context was created.
   */
  readonly created: number;

  /**
   * The count of all durable function calls made by from the context.
   */
  readonly counter: number;

  /**
   * The current attempt of the context.
   */
  readonly attempt: number;

  /**
   * Indicates whether or not this context has been killed.
   */
  readonly killed: boolean;

  /**
   * Indicates whether or not this context has been canceled.
   */
  readonly canceled: boolean;

  /**
   * A reference to the parent context. If undefined, this is the root context.
   */
  readonly parent?: Context;

  /**
   * A reference to all child contexts.
   */
  readonly children: Context[];

  /**
   * Invoke a function durably.
   *
   * @param func the function to invoke
   * @param args arguments to pass to the function, optionally followed by Resonate {@link Opts}
   * @returns a promise that resolves to the return value of the function
   */
  run<F extends Func>(func: F, ...args: Params<F>): Promise<Return<F>>;
  run<F extends Func>(func: F, ...args: [...Params<F>, ContextOpts]): Promise<Return<F>>;
  run<T = any, P = any>(func: string, args: P): Promise<T>;
  run<T = any, P = any>(func: string, args: P, opts: ContextOpts): Promise<T>;

  /**
   * Construct context opts.
   *
   * @param opts Resonate {@link Opts}
   * @returns an instance of resonate opts
   */
  options(opts: Partial<Opts>): ContextOpts;

  /**
   * Wraps an array of durable promises into a new durable promise that fulfills when all input
   * durable promises fulfill.
   *
   * See [Promise.all()](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/all) for more information.
   *
   * @param values array of durable promises
   * @param opts resonate options {@link Opts}
   * @returns a promise that resolves with an array of all resolved values, or rejects with the reason of the first rejected durable promise
   */
  all<T extends readonly unknown[] | []>(values: T): Promise<{ -readonly [P in keyof T]: Awaited<T[P]> }>;
  all<T extends readonly unknown[] | []>(
    values: T,
    opts?: ContextOpts,
  ): Promise<{ -readonly [P in keyof T]: Awaited<T[P]> }>;

  /**
   * Wraps an array of durable promises into a new durable promise that fulfills when the first
   * durable promise resolves.
   *
   * See [Promise.any()](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/any) for more information.
   *
   * @param values array of durable promises
   * @param opts resonate options {@link Opts}
   * @returns a promise that resolves with the first resolved value, or rejects with an aggregate error if all durable promises reject
   */
  any<T extends readonly unknown[] | []>(values: T): Promise<Awaited<T[number]>>;
  any<T extends readonly unknown[] | []>(values: T, opts?: ContextOpts): Promise<Awaited<T[number]>>;

  /**
   * Wraps an array of durable promises into a new durable promise that fulfills when the first
   * durable promise either resolves or rejects.
   *
   * See [Promise.race()](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/race) for more information.
   *
   * @param values array of durable promises
   * @param opts resonate options {@link Opts}
   * @returns a promise that fulfills with the first durable promise that resolves or rejects
   */
  race<T extends readonly unknown[] | []>(values: T): Promise<Awaited<T[number]>>;
  race<T extends readonly unknown[] | []>(values: T, opts?: ContextOpts): Promise<Awaited<T[number]>>;
}

class ResonateContext implements Context {
  readonly created: number = Date.now();

  counter: number = 0;

  attempt: number = 0;

  killed: boolean = false;

  canceled: boolean = false;

  parent?: ResonateContext;

  children: ResonateContext[] = [];

  private reject?: (e: unknown) => void;

  private traces: ITrace[] = [];

  constructor(
    private readonly resonate: Resonate,
    private readonly store: IStore,
    private readonly bucket: IBucket,
    public readonly name: string,
    public readonly id: string,
    public readonly idempotencyKey: string | undefined,
    public readonly defaults: Opts,
    public readonly opts: Opts = defaults,
  ) {}

  get timeout(): number {
    return Math.min(this.created + this.opts.timeout, this.parent?.timeout ?? Infinity);
  }

  startTrace(id: string, i: number = this.traces.length - 1): ITrace {
    const trace = this.traces[i]?.start(id) ?? this.parent?.startTrace(id, 0) ?? this.resonate.logger.startTrace(id);
    this.traces.unshift(trace);

    return trace;
  }

  private addChild(context: ResonateContext) {
    context.parent = this;
    this.children.push(context);
  }

  get isRoot(): boolean {
    return this.parent === undefined;
  }

  options(opts: Partial<Opts>): ContextOpts {
    return new ContextOpts(opts);
  }

  run<F extends Func>(func: F, ...args: Params<F>): Promise<Return<F>>;
  run<F extends Func>(func: F, ...args: [...Params<F>, ContextOpts]): Promise<Return<F>>;
  run<T = any, P = any>(func: string, args: P): Promise<T>;
  run<T = any, P = any>(func: string, args: P, opts: ContextOpts): Promise<T>;
  run(func: Func | string, ...argsAndOpts: [...any, ContextOpts?]): Promise<any> {
    const { args, opts } = split(argsAndOpts);

    const id = opts.id ?? [this.id, this.counter++].join(this.resonate.seperator);
    const idempotencyKey = opts.idempotencyKey ?? id;
    const name = typeof func === "string" ? func : func.name;

    const context = new ResonateContext(
      this.resonate,
      this.store,
      this.bucket,
      name,
      id,
      idempotencyKey,
      this.defaults,
      { ...this.defaults, ...opts },
    );
    this.addChild(context);

    return context.execute(func, args);
  }

  execute(func: Func | string, args: any[]): Promise<any> {
    return new Promise(async (resolve, reject) => {
      // set reject for cancel
      this.reject = reject;

      // generator
      let generator: AsyncGenerator<DurablePromise, DurablePromise, DurablePromise>;

      if (typeof func === "string") {
        generator = this.remoteExecution(func, args);
      } else if (isGenerator(func)) {
        generator = this.localExecution(this.name, args, new GInvocation(func, this.bucket));
      } else if (isFunction(func)) {
        generator = this.localExecution(this.name, args, new AInvocation(func, this.bucket));
      } else {
        throw new Error("Invalid function");
      }

      // trace
      const trace = this.startTrace(this.id);

      // invoke
      try {
        let r = await generator.next();
        while (!r.done) {
          r = await generator.next(r.value);
        }

        const promise = r.value;

        if (isPendingPromise(promise)) {
          throw new Error("Invalid state");
        } else if (isResolvedPromise(promise)) {
          resolve(this.opts.encoder.decode(promise.value.data));
        } else {
          reject(this.opts.encoder.decode(promise.value.data));
        }
      } catch (e: unknown) {
        // kill the promise when an error is thrown
        // note that this is not the same as a failed function invocation
        // which will return a promise
        this.kill(ResonateError.fromError(e));
      } finally {
        trace.end();
      }
    });
  }

  async all<T extends readonly unknown[] | []>(values: T): Promise<{ -readonly [P in keyof T]: Awaited<T[P]> }>;
  async all<T extends readonly unknown[] | []>(
    values: T,
    opts: ContextOpts,
  ): Promise<{ -readonly [P in keyof T]: Awaited<T[P]> }>;
  async all<T extends readonly unknown[] | []>(
    values: T,
    opts: ContextOpts = new ContextOpts(),
  ): Promise<{ -readonly [P in keyof T]: Awaited<T[P]> }> {
    // Promise.all handles rejected promises, however, on the recovery path
    // the resolved/rejected value may be retrieved from the promise store,
    // circumventing Promise.all. To avoid unhandled rejections, we attach
    // a noop catch handler to each promise.
    for (const value of values) {
      if (value instanceof Promise) {
        value.catch(() => {}); // noop
      }
    }

    // Use a generator instead of a function for future proofing
    return this.run(
      function* () {
        return Promise.all(values);
      },
      opts.merge({ retry: Retry.never() }),
    );
  }

  async any<T extends readonly unknown[] | []>(values: T): Promise<Awaited<T[number]>>;
  async any<T extends readonly unknown[] | []>(values: T, opts: ContextOpts): Promise<Awaited<T[number]>>;
  async any<T extends readonly unknown[] | []>(
    values: T,
    opts: ContextOpts = new ContextOpts(),
  ): Promise<Awaited<T[number]>> {
    // Promise.any handles rejected promises, however, on the recovery path
    // the resolved/rejected value may be retrieved from the promise store,
    // circumventing Promise.any. To avoid unhandled rejections, we attach
    // a noop catch handler to each promise.
    for (const value of values) {
      if (value instanceof Promise) {
        value.catch(() => {}); // noop
      }
    }

    // Use a generator instead of a function for future proofing
    return this.run(
      function* () {
        return Promise.any(values);
      },
      opts.merge({ retry: Retry.never() }),
    );
  }

  async race<T extends readonly unknown[] | []>(values: T): Promise<Awaited<T[number]>>;
  async race<T extends readonly unknown[] | []>(values: T, opts: ContextOpts): Promise<Awaited<T[number]>>;
  async race<T extends readonly unknown[] | []>(
    values: T,
    opts: ContextOpts = new ContextOpts(),
  ): Promise<Awaited<T[number]>> {
    // Promise.race handles rejected promises, however, on the recovery path
    // the resolved/rejected value may be retrieved from the promise store,
    // circumventing Promise.race. To avoid unhandled rejections, we attach
    // a noop catch handler to each promise.
    for (const value of values) {
      if (value instanceof Promise) {
        value.catch(() => {}); // noop
      }
    }

    // Use a generator instead of a function for future proofing
    return this.run(
      function* () {
        return Promise.race(values);
      },
      opts.merge({ retry: Retry.never() }),
    );
  }

  private async *localExecution<F extends Func>(
    func: string,
    args: Params<F>,
    invocation: AInvocation<F> | GInvocation<F>,
  ): AsyncGenerator<DurablePromise, DurablePromise, DurablePromise> {
    const data = this.isRoot ? this.opts.encoder.encode({ func, args }) : undefined;
    const tags = this.isRoot ? { "resonate:invocation": "true" } : undefined;

    // create durable promise
    const promise = yield this.store.promises.create(
      this.id,
      this.idempotencyKey,
      false,
      undefined,
      data,
      this.timeout,
      tags,
    );

    if (isPendingPromise(promise)) {
      try {
        // TODO
        // decode the arguments from the promise
        // if root context, this normalizes the information across
        // the initial invocation and the recovery path

        // invoke function
        const r = await this.withRetry((delay) => invocation.invoke(this, args, delay));

        // encode value
        const data = this.opts.encoder.encode(r);

        // resolve durable promise
        return yield this.store.promises.resolve(this.id, this.idempotencyKey, false, undefined, data);
      } catch (e: unknown) {
        // encode error
        const data = this.opts.encoder.encode(e);

        // reject durable promise
        return yield this.store.promises.reject(this.id, this.idempotencyKey, false, undefined, data);
      }
    }

    return promise;
  }

  private async *remoteExecution(
    func: string,
    args: any,
  ): AsyncGenerator<DurablePromise, DurablePromise, DurablePromise> {
    // context run captures arguments in a rest parameter, for remote
    // invocation we only care about the first argument
    const data = this.opts.encoder.encode(args[0]);

    // create durable promise
    let promise = yield this.store.promises.create(
      func,
      this.idempotencyKey,
      false,
      undefined,
      data,
      this.timeout,
      undefined,
    );

    while (isPendingPromise(promise)) {
      await new Promise((resolve) => setTimeout(resolve, 1000));

      try {
        promise = await this.store.promises.get(func);
      } catch (e: unknown) {
        // TODO
      }
    }

    return promise;
  }

  private async withRetry<T>(retriable: (d?: number) => Promise<T>): Promise<T> {
    let error;

    for (const delay of this.opts.retry.iterator(this)) {
      const trace = this.startTrace(`${this.id}:${this.attempt}`);

      try {
        return await retriable(delay);
      } catch (e: unknown) {
        error = e;

        this.attempt++;
        this.counter = 0;
      } finally {
        trace.end();
      }
    }

    throw error;
  }

  private kill(e: ResonateError) {
    this.killed = true;

    if (this.parent) {
      this.parent.kill(e);
    } else {
      // when we reach the root context, we can reject
      this.reject?.(e);
    }
  }

  private cancel() {
    for (const child of this.children) {
      child.cancel();
    }

    // TODO: store.cancel

    this.canceled = true;
  }
}

// Invocations

class AInvocation<F extends Func> {
  constructor(
    private func: F,
    private bucket: IBucket,
  ) {}

  async invoke(ctx: Context, args: Params<F>, delay?: number): Promise<Return<F>> {
    return await this.bucket.schedule(() => this.func(ctx, ...args), delay);
  }
}

class GInvocation<F extends Func<Generator>> {
  constructor(
    private func: F,
    private bucket: IBucket,
  ) {}

  async invoke(ctx: Context, args: Params<F>, delay?: number): Promise<Return<F>> {
    const generator = this.func(ctx, ...args);

    let lastValue: unknown;
    let lastError: unknown;

    let g = await this.bucket.schedule(() => generator.next(), delay);

    while (!g.done) {
      try {
        lastValue = await g.value;
      } catch (e: unknown) {
        lastError = e;
      }

      let next: () => IteratorResult<unknown, any>;
      if (lastError) {
        next = () => generator.throw(lastError);
      } else {
        next = () => generator.next(lastValue);
      }

      g = await this.bucket.schedule(next);
    }

    return await g.value;
  }
}

// Utils

function split<T extends any[]>(args: [...T, ContextOpts?]): { args: T; opts: Partial<Opts> } {
  let opts: Partial<Opts> = {};

  if (isContextOpts(args[args.length - 1])) {
    opts = args[args.length - 1].all();
    args.pop();
  }

  return { args: args as unknown as T, opts: opts };
}

function randomId(): string {
  return Math.floor(Math.random() * Number.MAX_SAFE_INTEGER).toString(16);
}
