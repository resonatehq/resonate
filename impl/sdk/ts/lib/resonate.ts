import { Opts, isPartialOpts } from "./opts";
import { IPromiseStore } from "./store";
import {
  DurablePromise,
  isCanceledPromise,
  isPendingPromise,
  isRejectedPromise,
  isResolvedPromise,
  isTimedoutPromise,
} from "./promise";
import { IRetry } from "./retry";
import { Retry } from "./retries/retry";
import { IBucket } from "./bucket";
import { Bucket } from "./buckets/bucket";
import { VolatilePromiseStore } from "./stores/volatile";
import { DurablePromiseStore } from "./stores/durable";
import { ILogger, ITrace } from "./logger";
import { Logger } from "./loggers/logger";
import { IEncoder } from "./encoder";
import { JSONEncoder } from "./encoders/json";
import { ErrorEncoder } from "./encoders/error";
import { ErrorCodes, ResonateError } from "./error";

// Types

type F<A extends any[], R> = (c: Context, ...a: A) => R;
type G<A extends any[], R> = (c: Context, ...a: A) => Generator<Promise<any>, R, any>;

type DurableFunction<A extends any[], R> = G<A, R> | F<A, R>;
type Invocation<A extends any[], R> = AInvocation<A, R> | GInvocation<A, R>;

type WithOpts<A extends any[]> = [...A, Partial<Opts>?];

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
  logger: ILogger;
  timeout: number;
  retry: () => IRetry;
  bucket: IBucket;
  encoder: IEncoder<any, string>;
};

export class Resonate {
  private functions: Record<string, DurableFunction<any, any>> = {};

  private stores: Record<string, IPromiseStore> = {};

  private buckets: Record<string, IBucket> = {};

  readonly logger: ILogger;

  readonly defaults: Partial<Opts>;

  readonly defaultRetry: () => IRetry;

  readonly defaultEncoder: IEncoder<any, string>;

  readonly valueEncoders: IEncoder<any, string>[] = [];

  readonly errorEncoders: IEncoder<any, string>[] = [new ErrorEncoder()];

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
    logger = new Logger(),
    timeout = 10000,
    retry = () => Retry.atLeastOnce(),
    bucket = new Bucket(),
    encoder = new JSONEncoder(),
  }: Partial<ResonateOpts> = {}) {
    this.logger = logger;
    this.defaultRetry = retry;
    this.defaultEncoder = encoder;

    this.addStore("default", url ? new DurablePromiseStore(url) : new VolatilePromiseStore());
    this.addBucket("default", bucket);

    this.defaults = {
      timeout: timeout,
      store: "default",
      bucket: "default",
    };
  }

  /**
   * Add a store.
   *
   * @param name
   * @param store
   */
  addStore(name: string, store: IPromiseStore) {
    this.stores[name] = store;
  }

  /**
   * Get a store by name.
   *
   * @param name
   * @returns instance of IPromiseStore
   */
  store(name: string): IPromiseStore {
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
   * Add a value encoder. Value encoders are used to encode and decode values returned from Resonate functions.
   *
   * @param encoder
   */
  addValueEncoder(encoder: IEncoder<any, string>) {
    this.valueEncoders.unshift(encoder);
  }

  /**
   * Add an error encoder. Error encoders are used to encode and decode errors thrown by Resonate functions.
   *
   * @param encoder
   */
  addErrorEncoder(encoder: IEncoder<any, string>) {
    this.errorEncoders.unshift(encoder);
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
  ): (id: string, ...args: WithOpts<A>) => Promise<R> {
    if (name in this.functions) {
      throw new Error(`Function ${name} already registered`);
    }

    this.functions[name] = func;
    return (id: string, ...args: WithOpts<A>): Promise<R> => {
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
      this.functions[key] = module[key];
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
  run<R>(name: string, id: string, ...argsWithOpts: WithOpts<any[]>): Promise<R> {
    if (!(name in this.functions)) {
      throw new Error(`Function ${name} not registered`);
    }

    const [args, opts] = splitArgs(argsWithOpts);

    const context = new ResonateContext(this, {
      id: id,
      idempotencyKey: id,
      timeout: 10000,
      store: "default",
      bucket: "default",
      retry: this.defaultRetry(),
      ...this.defaults,
      ...opts,
    });

    return context.execute(this.functions[name], args);
  }

  /**
   * Get a promise from the store
   * @param id
   * @param store
   * @returns a durable promise
   */
  get(id: string, store: string = "default"): Promise<DurablePromise> {
    return this.store(store).get(id);
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
  run<A extends any[], R>(func: DurableFunction<A, R>, ...args: WithOpts<A>): Promise<R>;
}

class ResonateContext implements Context {
  readonly created: number = Date.now();

  counter: number = 0;

  attempt: number = 0;

  killed: boolean = false;

  canceled: boolean = false;

  parent?: ResonateContext;

  children: ResonateContext[] = [];

  trace?: ITrace;

  private reject?: (e: unknown) => void;

  constructor(
    public readonly resonate: Resonate,
    public readonly opts: Opts,
  ) {}

  get id(): string {
    return this.opts.id;
  }

  get idempotencyKey(): string {
    return this.opts.idempotencyKey;
  }

  get timeout(): number {
    return this.created + this.opts.timeout;
  }

  private addChild(context: ResonateContext) {
    context.parent = this;
    this.children.push(context);
  }

  run<A extends any[], R>(func: DurableFunction<A, R>, ...argsWithOpts: WithOpts<A>): Promise<R> {
    const id = `${this.opts.id}.${this.counter++}`;
    const [args, opts] = splitArgs(argsWithOpts);

    const context = new ResonateContext(this.resonate, {
      id: id,
      idempotencyKey: id,
      timeout: 10000,
      store: "default",
      bucket: "default",
      retry: this.resonate.defaultRetry(),
      ...this.resonate.defaults,
      ...opts,
    });

    this.addChild(context);

    return context.execute(func, args);
  }

  execute<A extends any[], R>(func: DurableFunction<A, R>, args: A): Promise<R> {
    return new Promise(async (resolve, reject) => {
      // set reject for kill/cancel
      this.reject = reject;

      const store = this.resonate.store(this.opts.store);
      const bucket = this.resonate.bucket(this.opts.bucket);

      const trace = this.parent?.trace ? this.parent.trace.start(this.id) : this.resonate.logger.startTrace(this.id);

      let invocation: Invocation<A, R>;
      if (isF<A, R>(func)) {
        invocation = new AInvocation(func, args, this.opts.retry, trace);
      } else if (isG<A, R>(func)) {
        invocation = new GInvocation(func, args, this.opts.retry, trace);
      } else {
        trace.end();
        throw new Error("Invalid function");
      }

      // create durable promise
      try {
        const p = await store.create(
          this.id,
          this.idempotencyKey,
          false,
          undefined,
          undefined,
          this.timeout,
          undefined,
        );

        if (isPendingPromise(p)) {
          try {
            // invoke function
            const r = await invocation.invoke(this, bucket);

            try {
              // serialize value
              const { key, value } = this.encode(r, this.resonate.valueEncoders);
              const headers: Record<string, string> = {};

              if (key !== undefined) {
                headers["Content-Encoding"] = key;
              }

              // resolve durable promise
              const p = await store.resolve(this.id, this.idempotencyKey, false, headers, value);

              if (isResolvedPromise(p)) {
                resolve(
                  this.decode<R>(p.value.data, p.value.headers?.["Content-Encoding"], this.resonate.valueEncoders),
                );
                return;
              } else if (isRejectedPromise(p)) {
                reject(this.decode(p.value.data, p.value.headers?.["Content-Encoding"], this.resonate.errorEncoders));
                return;
              } else if (isCanceledPromise(p)) {
                reject(this.decode(p.value.data, p.value.headers?.["Content-Encoding"], this.resonate.errorEncoders));
                return;
              } else if (isTimedoutPromise(p)) {
                reject();
                return;
              }
            } catch (e: unknown) {
              this.kill(ResonateError.fromError(e));
              return;
            }
          } catch (e: unknown) {
            try {
              // serialize error
              const { key, value } = this.encode(e, this.resonate.errorEncoders);
              const headers: Record<string, string> = {};

              if (key !== undefined) {
                headers["Content-Encoding"] = key;
              }

              // reject durable promise
              const p = await store.reject(this.id, this.idempotencyKey, false, headers, value);

              if (isResolvedPromise(p)) {
                resolve(
                  this.decode<R>(p.value.data, p.value.headers?.["Content-Encoding"], this.resonate.valueEncoders),
                );
                return;
              } else if (isRejectedPromise(p)) {
                reject(this.decode(p.value.data, p.value.headers?.["Content-Encoding"], this.resonate.errorEncoders));
                return;
              } else if (isCanceledPromise(p)) {
                reject(this.decode(p.value.data, p.value.headers?.["Content-Encoding"], this.resonate.errorEncoders));
                return;
              } else if (isTimedoutPromise(p)) {
                reject();
                return;
              }
            } catch (e: unknown) {
              this.kill(ResonateError.fromError(e));
              return;
            }
          }
        } else if (isResolvedPromise(p)) {
          resolve(this.decode<R>(p.value.data, p.value.headers?.["Content-Encoding"], this.resonate.valueEncoders));
          return;
        } else if (isRejectedPromise(p)) {
          reject(this.decode(p.value.data, p.value.headers?.["Content-Encoding"], this.resonate.errorEncoders));
          return;
        } else if (isCanceledPromise(p)) {
          reject(this.decode(p.value.data, p.value.headers?.["Content-Encoding"], this.resonate.errorEncoders));
          return;
        } else if (isTimedoutPromise(p)) {
          reject();
          return;
        }
      } catch (e: unknown) {
        this.kill(ResonateError.fromError(e));
        return;
      } finally {
        trace.end();
      }
    });
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

  private encode<T>(value: T, encoders: IEncoder<any, string>[] = []): { key: string | undefined; value: string } {
    const encoder = this.findEncoder<T>(undefined, value, encoders);

    try {
      return {
        key: encoder.key,
        value: encoder.encode(value),
      };
    } catch (e: unknown) {
      throw new ResonateError("Encode error", ErrorCodes.ENCODER, e);
    }
  }

  private decode<T>(value: string, key?: string, encoders: IEncoder<any, string>[] = []): T {
    const encoder = this.findEncoder<T>(key, undefined, encoders);

    try {
      return encoder.decode(value);
    } catch (e: unknown) {
      throw new ResonateError("Decode error", ErrorCodes.ENCODER, e);
    }
  }

  private findEncoder<T>(key?: string, value?: T, encoders: IEncoder<any, string>[] = []): IEncoder<T, string> {
    for (const encoder of encoders) {
      if (key !== undefined && encoder.key === key) {
        return encoder;
      }
      if (value !== undefined && encoder.match(value)) {
        return encoder;
      }
    }

    return this.resonate.defaultEncoder;
  }
}

// Invocations

class AInvocation<A extends any[], R> {
  constructor(
    private func: F<A, R>,
    private args: A,
    private retry: IRetry,
    private trace: ITrace,
  ) {}

  invoke(context: ResonateContext, bucket: Bucket): Promise<R> {
    return new Promise(async (resolve, reject) => {
      let r = this.retry.next(context);

      while (!r.done) {
        const thunk = () => {
          // set the current trace on the context
          // this will be used to create child traces
          context.trace = this.trace.start(`${context.id}:${context.attempt}`);
          return this.func(context, ...this.args);
        };

        try {
          resolve(await bucket.schedule(thunk, r.delay));
          return;
        } catch (e: unknown) {
          context.attempt++;
          r = this.retry.next(context);

          if (r.done) {
            reject(e);
            return;
          }
        } finally {
          context.trace?.end();
        }
      }
    });
  }
}

class GInvocation<A extends any[], R> {
  constructor(
    private func: G<A, R>,
    private args: A,
    private retry: IRetry,
    private trace: ITrace,
  ) {}

  invoke(context: ResonateContext, bucket: Bucket): Promise<R> {
    return new Promise(async (resolve, reject) => {
      let r = this.retry.next(context);

      while (!r.done) {
        try {
          const generator = this.func(context, ...this.args);

          let lastValue: any;
          let lastError: unknown;
          const thunk = () => {
            // set the current trace on the context
            // this will be used to create child traces
            context.trace = this.trace.start(`${context.id}:${context.attempt}`);

            return generator.next();
          };

          let g = await bucket.schedule(thunk, r.delay);

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

            g = await bucket.schedule(next);
          }

          resolve(await g.value);
          return;
        } catch (e: unknown) {
          context.attempt++;
          r = this.retry.next(context);

          if (r.done) {
            reject(e);
            return;
          }
        } finally {
          context.trace?.end();
        }
      }
    });
  }
}

// Utils

function splitArgs<A extends any[]>(argsWithOpts: WithOpts<A>): [A, Partial<Opts>] {
  let args: A;
  let opts: Partial<Opts>;

  if (argsWithOpts.length > 0) {
    const last = argsWithOpts[argsWithOpts.length - 1];
    if (isPartialOpts(last)) {
      args = argsWithOpts.slice(0, argsWithOpts.length - 1) as A;
      opts = last;
    } else {
      args = argsWithOpts as unknown as A;
      opts = {};
    }
  } else {
    args = argsWithOpts as unknown as A;
    opts = {};
  }

  return [args, opts];
}
