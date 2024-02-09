import { ResonateOptions, ContextOptions, Options, isContextOpts } from "./core/opts";
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
import { ResonateError, ResonateTestCrash } from "./core/error";
import { ICache } from "./core/cache";
import { Cache } from "./core/caches/cache";
import { Schedule } from "./core/schedule";
import { IEncoder } from "./core/encoder";
import { IRetry } from "./core/retry";
import { Func, Params, Return, isFunction, isGenerator } from "./core/types";

// Resonate

export class Resonate {
  private cache: ICache<{ context: Context; promise: Promise<any> }> = new Cache();
  private functions: Record<string, { func: Func; opts: ContextOptions }> = {};
  private recoveryInterval: number | undefined = undefined;

  readonly bucket: IBucket;
  readonly encoder: IEncoder<unknown, string | undefined>;
  readonly logger: ILogger;
  readonly namespace: string;
  readonly recoveryDelay: number;
  readonly retry: IRetry;
  readonly pid: string;
  readonly separator: string;
  readonly store: IStore;
  readonly timeout: number;

  /**
   * Creates a Resonate instance. This is the starting point for using Resonate.
   *
   * @constructor
   * @param opts - A partial resonate options object.
   */
  constructor({
    bucket = new Bucket(),
    encoder = new JSONEncoder(),
    logger = new Logger(),
    namespace = "",
    pid = rid(),
    recoveryDelay = 1000,
    retry = Retry.exponential(),
    separator: seperator = "/",
    store = undefined,
    timeout = 1000,
    url = undefined,
  }: Partial<ResonateOptions> = {}) {
    this.bucket = bucket;
    this.encoder = encoder;
    this.logger = logger;
    this.namespace = namespace;
    this.pid = pid;
    this.recoveryDelay = recoveryDelay;
    this.retry = retry;
    this.separator = seperator;
    this.timeout = timeout;

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
   * @param name A unique name to identify the function.
   * @param func The function to register with resonate.
   * @param opts Resonate options, can be constructed by calling {@link options}.
   * @returns Resonate function
   */
  register<F extends Func>(
    name: string,
    func: F,
    opts: Options = new Options(),
  ): (id: string, ...args: Params<F>) => Promise<Return<F>> {
    if (name in this.functions) {
      throw new Error(`Function ${name} already registered`);
    }

    this.functions[name] = {
      // the function
      func: func,

      // default opts
      opts: {
        bucket: this.bucket,
        eid: rid(),
        encoder: this.encoder,
        retry: Retry.exponential(),
        timeout: this.timeout,
        test: { p: 0, generator: Math.random },
        ...opts.all(),
      },
    };

    return (id: string, ...args: Params<F>): Promise<Return<F>> => {
      return this.run(name, id, ...args);
    };
  }

  /**
   * Register a module with Resonate. Registered module functions can be invoked with {@link run}.
   *
   * @param module the javascript module
   * @param opts optional resonate options
   */
  registerModule(module: Record<string, Func>, opts: Options = new Options()) {
    for (const key in module) {
      this.register(key, module[key], opts);
    }
  }

  /**
   * Invoke a Resonate function.
   *
   * @template T The return type of function.
   * @template P The type of parameters to be passed to the function.
   * @param name The name of the function registered with Resonate.
   * @param id A unique identifier for this invocation.
   * @param args The arguments to pass to the function.
   * @returns A promise that resolves to the return value of the function.
   */
  run<T = any, P extends any[] = any[]>(name: string, id: string, ...args: P): Promise<T> {
    return this.runWithContext(name, id, ...args).promise;
  }

  /**
   * Invoke a Resonate function.
   *
   * @template T The return type of function.
   * @template P The type of parameters to be passed to the function.
   * @param name The name of the function registered with Resonate.
   * @param id A unique identifier for this invocation.
   * @param args The arguments to pass to the function.
   * @returns An object containing the context and a promise that resolves to the return value of the function and the context.
   */
  runWithContext<T = any, P extends any[] = any[]>(
    name: string,
    id: string,
    ...args: P
  ): { context: Context; promise: Promise<T> } {
    // use a constructed id for both the id and idempotency key
    // TODO: can user override the top level id / idempotency key?
    id = this.id(this.namespace, name, id);
    return this._run(name, id, id, args);
  }

  private _run<T = any, P extends any[] = any[]>(
    name: string,
    id: string,
    idempotencyKey: string | undefined,
    args: P,
  ): { context: Context; promise: Promise<T> } {
    if (!(name in this.functions)) {
      throw new Error(`Function ${name} not registered`);
    }

    // grab the registered function and options
    const { func, opts } = this.functions[name];

    if (!this.cache.has(id)) {
      const context = new ResonateContext(this, this.store, this.bucket, name, id, idempotencyKey, opts);

      const promise = new Promise(async (resolve, reject) => {
        // lock
        while (!(await this.store.locks.tryAcquire(id, opts.eid))) {
          // sleep
          await new Promise((r) => setTimeout(r, 1000));
        }

        let err: unknown;
        let isrejected = false;
        let result: any;
        try {
          result = await context.execute(func, args);
        } catch (e) {
          err = e;
          isrejected = true;
        }
        // unlock
        await this.store.locks.release(id, opts.eid);

        // TODO : This is just for testing purposes.
        if (err instanceof ResonateTestCrash) {
          this.cache.delete(id);
        }

        if (isrejected) {
          reject(err);
        } else {
          resolve(result);
        }
      });

      this.cache.set(id, { context, promise });
    }

    return this.cache.get(id);
  }

  /**
   * Invoke a function on a recurring schedule.
   *
   * @param name The name of the schedule.
   * @param cron The cron expression for the schedule.
   * @param funcName The name of the function registered with Resonate to invoke on a schedule.
   * @param args The arguments to pass to the function.
   * @returns The Resonate schedule.
   */
  schedule(name: string, cron: string, funcName: string, ...args: any[]): Promise<Schedule>;

  /**
   * Invoke a function on a recurring schedule.
   *
   * @param name The name of the schedule, will also be used as the name for the function registered with Resonate.
   * @param cron The cron expression for the schedule.
   * @param func The function to register with Resonate.
   * @param argsAndOpts The arguments to pass to the function, followed by {@link options}.
   * @returns The Resonate schedule.
   */
  schedule<F extends Func>(
    name: string,
    cron: string,
    func: F,
    ...argsAndOpts: [...Params<F>, Options?]
  ): Promise<Schedule>;
  schedule(name: string, cron: string, func: string | Func, ...argsAndOpts: any[]): Promise<Schedule> {
    let args: any[];
    let opts: Partial<ContextOptions>;

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
   * Start the Resonate recovery control loop.
   *
   * Polls the promise store for pending promises that have the
   * "resonate:invocation" tag. When a promise is matches, execute
   * the corresponding function.
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
   * Helper function that maps a partial {@link ContextOptions} to
   * Options.
   *
   * @param opts A partial Resonate {@link ContextOptions}
   * @returns An instance of Options.
   */
  options(opts: Partial<ContextOptions>): Options {
    return new Options(opts);
  }

  private id(...parts: string[]): string {
    return parts.filter((p) => p !== "").join(this.separator);
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
   * The absolute time in ms that the context will expire.
   */
  readonly timeout: number;

  /**
   * The time the context was created.
   */
  readonly created: number;

  /**
   * The count of all function calls made by this context.
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
   * @param func The function to invoke.
   * @param args The function arguments, optionally followed by context {@link options}.
   * @returns A promise that resolves to the return value of the function.
   */
  run<F extends Func>(func: F, ...args: [...Params<F>, Options?]): Promise<Return<F>>;

  /**
   * Invoke a remote function.
   *
   * @template T The return type of the remote function.
   * @template P The type of parameters to be passed to the remote function.
   * @param id The id of the remote function.
   * @param args The arguments to pass to the remote function.
   * @param opts The optional context {@link options}.
   * @returns A promise that resolves to the resolved value of the remote function.
   */
  run<T = any, P = any>(id: string, args: P, opts?: Options): Promise<T>;

  /**
   * Helper function that maps a partial {@link ContextOptions} to
   * Options.
   *
   * @param opts A partial Resonate {@link ContextOptions}
   * @returns An instance of Options.
   */
  options(opts: Partial<ContextOptions>): Options;

  /**
   * Wraps an array of durable promises into a new durable promise that fulfills when all input
   * durable promises fulfill.
   *
   * See [Promise.all()](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/all) for more information.
   *
   * @param values Array of durable promises.
   * @param opts Optional resonate {@link options}.
   * @returns A promise that resolves with an array of all resolved values, or rejects with the reason of the first rejected durable promise.
   */
  all<T extends readonly unknown[] | []>(
    values: T,
    opts?: Options,
  ): Promise<{ -readonly [P in keyof T]: Awaited<T[P]> }>;

  /**
   * Wraps an array of durable promises into a new durable promise that fulfills when the first
   * durable promise resolves.
   *
   * See [Promise.any()](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/any) for more information.
   *
   * @param values Array of durable promises.
   * @param opts Optional resonate {@link options}.
   * @returns A promise that resolves with the first resolved value, or rejects with an aggregate error if all durable promises reject.
   */
  any<T extends readonly unknown[] | []>(values: T, opts?: Options): Promise<Awaited<T[number]>>;

  /**
   * Wraps an array of durable promises into a new durable promise that fulfills when the first
   * durable promise either resolves or rejects.
   *
   * See [Promise.race()](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/race) for more information.
   *
   * @param values Array of durable promises.
   * @param opts Optional resonate {@link options}.
   * @returns A promise that fulfills with the first durable promise that resolves or rejects.
   */
  race<T extends readonly unknown[] | []>(values: T, opts?: Options): Promise<Awaited<T[number]>>;
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
    public readonly defaults: ContextOptions,
    public readonly opts: ContextOptions = defaults,
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

  options(opts: Partial<ContextOptions>): Options {
    return new Options(opts);
  }

  run<F extends Func>(func: F, ...args: [...Params<F>, Options?]): Promise<Return<F>>;
  run<T = any, P = any>(func: string, args: P, opts?: Options): Promise<T>;
  run(func: Func | string, ...argsAndOpts: [...any, Options?]): Promise<any> {
    const { args, opts } = split(argsAndOpts);

    const id = opts.id ?? [this.id, this.counter++].join(this.resonate.separator);
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

  async all<T extends readonly unknown[] | []>(
    values: T,
    opts: Options = new Options(),
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

  async any<T extends readonly unknown[] | []>(values: T, opts: Options = new Options()): Promise<Awaited<T[number]>> {
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

  async race<T extends readonly unknown[] | []>(values: T, opts: Options = new Options()): Promise<Awaited<T[number]>> {
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
      const fail = this.opts.test.generator() < this.opts.test.p;
      const branch = Math.floor(this.opts.test.generator() * 2);

      // generate failure before invoking the function
      if (fail && branch === 0) {
        throw new ResonateTestCrash(this.opts.test.p);
      }

      try {
        // TODO
        // decode the arguments from the promise
        // if root context, this normalizes the information across
        // the initial invocation and the recovery path

        // invoke function
        const r = await this.withRetry((delay) => invocation.invoke(this, args, delay));

        // encode value
        const data = this.opts.encoder.encode(r);

        // generate failure after invoking the function but before
        // resolve the promise
        if (fail && branch === 1) {
          throw new ResonateTestCrash(this.opts.test.p);
        }

        // resolve durable promise
        return yield this.store.promises.resolve(this.id, this.idempotencyKey, false, undefined, data);
      } catch (e: unknown) {
        // encode error
        const data = this.opts.encoder.encode(e);

        // generate failure after invoking the function but before
        // rejecting the promise
        if (fail && branch === 1) {
          throw new ResonateTestCrash(this.opts.test.p);
        }

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

function split<T extends any[]>(args: [...T, Options?]): { args: T; opts: Partial<ContextOptions> } {
  let opts: Partial<ContextOptions> = {};

  if (isContextOpts(args[args.length - 1])) {
    opts = args[args.length - 1].all();
    args.pop();
  }

  return { args: args as unknown as T, opts: opts };
}

function rid(): string {
  return Math.floor(Math.random() * Number.MAX_SAFE_INTEGER).toString(16);
}
