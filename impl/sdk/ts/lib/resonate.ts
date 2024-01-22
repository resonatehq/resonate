import { Opts } from "./core/opts";
import { IStore } from "./core/store";
import { DurablePromise, isPendingPromise, isResolvedPromise } from "./core/promise";
import { ExponentialRetry } from "./core/retries/exponential";
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

// Types

type F<A extends any[], R> = (c: Context, ...a: A) => R;
type G<A extends any[], R> = (c: Context, ...a: A) => Generator<Promise<any>, R, any>;

type DurableFunction<A extends any[], R> = G<A, R> | F<A, R>;

type Invocation<A extends any[], R> = AInvocation<A, R> | GInvocation<A, R>;

function isF<A extends any[], R>(f: unknown): f is F<A, R> {
  return (
    typeof f === "function" &&
    (f.constructor.name === "Function" || f.constructor.name === "AsyncFunction" || f.constructor.name === "")
  );
}

function isG<A extends any[], R>(g: unknown): g is G<A, R> {
  return typeof g === "function" && g.constructor.name === "GeneratorFunction";
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
};

export class Resonate {
  private functions: Record<string, { func: DurableFunction<any, any>; opts: Opts }> = {};

  private cache: ICache<Promise<any>> = new Cache();

  private stores: Record<string, IStore> = {};

  private buckets: Record<string, IBucket> = {};

  readonly logger: ILogger;

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
  }: Partial<ResonateOpts> = {}) {
    this.pid = pid;
    this.namespace = namespace;
    this.seperator = seperator;

    this.logger = logger;

    // store
    let defaultStore: IStore;
    if (store) {
      defaultStore = store;
    } else if (url) {
      defaultStore = new RemoteStore(url, logger);
    } else {
      defaultStore = new LocalStore(logger);
    }

    this.addStore("default", defaultStore);
    this.addBucket("default", bucket);
  }

  /**
   * Add a store.
   *
   * @param name
   * @param store
   */
  addStore(name: string, store: IStore) {
    this.stores[name] = store;
  }

  /**
   * Get a store by name.
   *
   * @param name
   * @returns instance of IPromiseStore
   */
  store(name: string): IStore {
    return this.stores[name];
  }

  /**
   * Add a bucket.
   *
   * @param name
   * @param bucket
   */
  addBucket(name: string, bucket: IBucket) {
    this.buckets[name] = bucket;
  }

  /**
   * Get a bucket by name.
   *
   * @param name
   * @returns instance of IBucket
   */
  bucket(name: string): IBucket {
    return this.buckets[name];
  }

  /**
   * Register a function with Resonate. Registered functions can be invoked with {@link run}, or by the returned function.
   *
   * @param name
   * @param func
   * @returns Resonate function
   */
  register<A extends any[], R>(
    name: string,
    func: DurableFunction<A, R>,
    opts: Partial<Opts> = {},
  ): (id: string, ...args: A) => Promise<R> {
    if (name in this.functions) {
      throw new Error(`Function ${name} already registered`);
    }

    this.functions[name] = {
      // the function
      func: func,

      // default opts
      opts: {
        timeout: 10000,
        store: "default",
        bucket: "default",
        retry: ExponentialRetry.atLeastOnce(),
        encoder: new JSONEncoder(),
        eid: randomId(),
        ...opts,
      },
    };

    return (id: string, ...args: A): Promise<R> => {
      return this.run(name, id, ...args);
    };
  }

  /**
   * Register a module with Resonate. Registered module functions can be invoked with run.
   *
   * @param module
   */
  registerModule(module: Record<string, DurableFunction<any, any>>) {
    for (const key in module) {
      this.register(key, module[key]);
    }
  }

  /**
   * Invoke a Resonate function.
   *
   * @param name the name of the registered function
   * @param id a unique identifier for the invocation
   * @param args arguments to pass to the function, optionally followed by Resonate {@link Opts}
   * @returns a promise that resolves to the return value of the function
   */
  run<R>(name: string, id: string, ...argsAndOpts: any[]): Promise<R> {
    if (!(name in this.functions)) {
      throw new Error(`Function ${name} not registered`);
    }

    const { func, opts: defaults } = this.functions[name];
    const { args, opts } = split(func, argsAndOpts, defaults);

    id = this.id(this.namespace, name, opts.id ?? id);
    const idempotencyKey = opts.idempotencyKey ?? id;

    const locks = this.store(opts.store).locks;

    if (!this.cache.has(id)) {
      const promise = new Promise(async (resolve, reject) => {
        // lock
        while (!locks.tryAcquire(id, this.pid, opts.eid)) {
          // sleep
          await new Promise((r) => setTimeout(r, 1000));
        }

        const context = new ResonateContext(this, name, id, idempotencyKey, opts);

        try {
          resolve(await context.execute(func, args));
        } catch (e) {
          reject(e);
        } finally {
          locks.release(id, opts.eid);
        }
      });

      this.cache.set(id, promise);
    }

    return this.cache.get(id);
  }

  async start(store: string = "default") {
    setTimeout(() => this.start(store), 1000);

    const encoder = new JSONEncoder();

    const search = this.store(store).promises.search(this.id(this.namespace, "*"), "pending", {
      "resonate:invocation": "true",
    });

    for await (const promises of search) {
      for (const promise of promises) {
        const { func, args } = encoder.decode(promise.value.data) as { func: string; args: any[] };

        try {
          this.run(func, promise.id, ...args, {
            id: promise.id,
            idempotencyKey: promise.idempotencyKeyForCreate,
          });
        } catch (e: unknown) {
          this.logger.warn(`Function ${func} with id ${promise.id} failed on the recovery path`, e);
        }
      }
    }
  }

  id(...parts: string[]): string {
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
  readonly idempotencyKey: string;

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
  run<A extends any[], R>(func: DurableFunction<A, R>, ...args: A): Promise<R>;
  run<A extends any[], R>(func: DurableFunction<A, R>, ...args: [...A, Partial<Opts>]): Promise<R>;
  run<R>(func: string, args: any): Promise<R>;
  run<R>(func: string, args: any, opts: Partial<Opts>): Promise<R>;
  run<A extends any[], R>(func: DurableFunction<A, R> | string, ...args: [...A, Partial<Opts>?]): Promise<R>;
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

  private store: IStore;

  private bucket: IBucket;

  constructor(
    private readonly resonate: Resonate,
    public readonly name: string,
    public readonly id: string,
    public readonly idempotencyKey: string,
    public readonly opts: Opts,
  ) {
    this.store = resonate.store(opts.store);
    this.bucket = resonate.bucket(opts.bucket);
  }

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

  run<A extends any[], R>(func: DurableFunction<A, R>, ...args: A): Promise<R>;
  run<A extends any[], R>(func: DurableFunction<A, R>, ...args: [...A, Partial<Opts>]): Promise<R>;
  run<R>(func: string, args: any): Promise<R>;
  run<R>(func: string, args: any, opts: Partial<Opts>): Promise<R>;
  run<A extends any[], R>(func: DurableFunction<A, R> | string, ...argsAndOpts: [...A, Partial<Opts>?]): Promise<R> {
    const { args, opts } = split(func, argsAndOpts, this.opts);

    const id = opts.id ?? this.resonate.id(this.id, `${this.counter++}`);
    const idempotencyKey = opts.idempotencyKey ?? id;
    const name = typeof func === "string" ? func : func.name;

    const context = new ResonateContext(this.resonate, name, id, idempotencyKey, opts);
    this.addChild(context);

    return context.execute(func, args);
  }

  execute<A extends any[], R>(func: DurableFunction<A, R> | string, args: A): Promise<R> {
    return new Promise(async (resolve, reject) => {
      // set reject for cancel
      this.reject = reject;

      // generator
      let generator: AsyncGenerator<DurablePromise, DurablePromise, DurablePromise>;

      if (typeof func === "string") {
        generator = this.remoteExecution(func, args[0]);
      } else if (isF<A, R>(func)) {
        generator = this.localExecution(this.name, args, new AInvocation(func, this.bucket));
      } else if (isG<A, R>(func)) {
        generator = this.localExecution(this.name, args, new GInvocation(func, this.bucket));
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
          resolve(this.opts.encoder.decode(promise.value.data) as R);
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

  private async *localExecution<A extends any[], R>(
    func: string,
    args: A,
    invocation: Invocation<A, R>,
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
    const data = this.opts.encoder.encode(args);

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

class AInvocation<A extends any[], R> {
  constructor(
    private func: F<A, R>,
    private bucket: IBucket,
  ) {}

  async invoke(ctx: Context, args: A, delay?: number): Promise<R> {
    return await this.bucket.schedule(() => this.func(ctx, ...args), delay);
  }
}

class GInvocation<A extends any[], R> {
  constructor(
    private func: G<A, R>,
    private bucket: IBucket,
  ) {}

  async invoke(ctx: Context, args: A, delay?: number): Promise<R> {
    const generator = this.func(ctx, ...args);

    let lastValue: any;
    let lastError: unknown;

    let g = await this.bucket.schedule(() => generator.next(), delay);

    while (!g.done) {
      try {
        lastValue = await g.value;
      } catch (e: unknown) {
        lastError = e;
      }

      let next: () => IteratorResult<Promise<any>, R>;
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

function split<A extends any[], R>(
  func: DurableFunction<A, R> | string,
  args: [...A, Partial<Opts>?],
  defaults: Opts,
): { args: A; opts: Opts } {
  const len = typeof func === "string" ? 1 : func.length - 1;
  const opts = args.length > len ? { ...defaults, ...args.pop() } : defaults;

  return { args: args as unknown as A, opts: opts };
}

function randomId(): string {
  return Math.floor(Math.random() * Number.MAX_SAFE_INTEGER).toString(16);
}
