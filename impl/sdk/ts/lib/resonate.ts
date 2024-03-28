import { IEncoder } from "./core/encoder";
import { JSONEncoder } from "./core/encoders/json";
import { ResonatePromise } from "./core/future";
import { ILogger } from "./core/logger";
import { Logger } from "./core/loggers/logger";
import { ResonateOptions, Options } from "./core/options";
import * as promises from "./core/promises/promises";
import { Retry } from "./core/retries/retry";
import { IRetry } from "./core/retry";
import * as schedules from "./core/schedules/schedules";
import { IStore } from "./core/store";
import { LocalStore } from "./core/stores/local";
import { RemoteStore } from "./core/stores/remote";
import * as utils from "./core/utils";

/////////////////////////////////////////////////////////////////////
// Types
/////////////////////////////////////////////////////////////////////

type Func = (...args: any[]) => any;

/////////////////////////////////////////////////////////////////////
// Resonate
/////////////////////////////////////////////////////////////////////

export abstract class ResonateBase {
  private readonly functions: Record<string, Record<number, { version: number; func: Func; opts: Options }>> = {};

  public readonly promises: ResonatePromises;
  public readonly schedules: ResonateSchedules;

  public readonly pid: string;
  public readonly timeout: number;
  public readonly tags: Record<string, string>;

  protected readonly encoder: IEncoder<unknown, string | undefined>;
  protected readonly logger: ILogger;
  protected readonly retry: IRetry;
  protected readonly store: IStore;

  private interval: NodeJS.Timeout | undefined;

  constructor({
    encoder = new JSONEncoder(),
    logger = new Logger(),
    pid = utils.randomId(),
    retry = Retry.exponential(),
    store = undefined,
    tags = {},
    timeout = 10000, // 10s
    url = undefined,
  }: Partial<ResonateOptions> = {}) {
    this.encoder = encoder;
    this.logger = logger;
    this.pid = pid;
    this.retry = retry;
    this.tags = tags;
    this.timeout = timeout;

    if (store) {
      this.store = store;
    } else if (url) {
      this.store = new RemoteStore(url, this.pid, this.logger);
    } else {
      this.store = new LocalStore(this.logger);
    }

    // promises
    this.promises = {
      create: <T>(id: string, timeout: number, opts: Partial<promises.CreateOptions> = {}) =>
        promises.DurablePromise.create<T>(this.store.promises, this.encoder, id, timeout, opts),

      resolve: <T>(id: string, value: T, opts: Partial<promises.CompleteOptions> = {}) =>
        promises.DurablePromise.resolve<T>(this.store.promises, this.encoder, id, value, opts),

      reject: <T>(id: string, error: any, opts: Partial<promises.CompleteOptions> = {}) =>
        promises.DurablePromise.reject<T>(this.store.promises, this.encoder, id, error, opts),

      cancel: <T>(id: string, error: any, opts: Partial<promises.CompleteOptions> = {}) =>
        promises.DurablePromise.cancel<T>(this.store.promises, this.encoder, id, error, opts),

      get: <T>(id: string) => promises.DurablePromise.get<T>(this.store.promises, this.encoder, id),

      search: (id: string, state?: string, tags?: Record<string, string>, limit?: number) =>
        promises.DurablePromise.search(this.store.promises, this.encoder, id, state, tags, limit),
    };

    // schedules
    this.schedules = {
      create: (
        id: string,
        cron: string,
        promiseId: string,
        promiseTimeout: number,
        opts: Partial<schedules.Options> = {},
      ) => schedules.Schedule.create(this.store.schedules, this.encoder, id, cron, promiseId, promiseTimeout, opts),

      get: (id: string) => schedules.Schedule.get(this.store.schedules, this.encoder, id),
      search: (id: string, tags?: Record<string, string>, limit?: number) =>
        schedules.Schedule.search(this.store.schedules, this.encoder, id, tags, limit),
    };
  }

  protected abstract execute(
    name: string,
    version: number,
    id: string,
    idempotencyKey: string | undefined,
    func: Func,
    args: any[],
    opts: Options,
  ): ResonatePromise<any>;

  register(
    name: string,
    funcOrVersion: Func | number,
    funcOrOpts?: Func | Partial<Options>,
  ): (id: string, ...args: any) => ResonatePromise<any> {
    const version = typeof funcOrVersion === "number" ? funcOrVersion : 1;
    const func =
      typeof funcOrVersion === "function" ? funcOrVersion : typeof funcOrOpts === "function" ? funcOrOpts : null;
    const opts = typeof funcOrOpts === "object" ? this.options(funcOrOpts) : this.options({});

    if (!func) {
      throw new Error("Must provide value for func");
    }

    if (version <= 0) {
      throw new Error("Version must be greater than 0");
    }

    if (!this.functions[name]) {
      this.functions[name] = {};
    }

    if (this.functions[name][version]) {
      throw new Error(`Function ${name} version ${version} already registered`);
    }

    // register as latest (0) if version is greatest so far
    if (version > Math.max(...Object.values(this.functions[name]).map((f) => f.version))) {
      this.functions[name][0] = { version, func, opts };
    }

    // register specific version
    this.functions[name][version] = { version, func, opts };

    return (id: string, ...args: any[]) => this.run(name, version, id, ...args);
  }

  registerModule(module: Record<string, Func>, opts: Partial<Options> = {}) {
    for (const key in module) {
      this.register(key, module[key], opts);
    }
  }

  /**
   * Run a Resonate function. Functions must first be registered with {@link register}.
   *
   * @template T The return type of the function.
   * @param id A unique id for the function invocation.
   * @param name The function name.
   * @param args The function arguments.
   * @returns A promise that resolve to the function return value.
   */
  run<T>(name: string, id: string, ...args: any[]): ResonatePromise<T>;

  /**
   * Run a Resonate function. Functions must first be registered with {@link register}.
   *
   * @template T The return type of the function.
   * @param name The function name.
   * @param version The function version.
   * @param id A unique id for the function invocation.
   * @param args The function arguments.
   * @returns A promise that resolve to the function return value.
   */
  run<T>(name: string, version: number, id: string, ...args: any[]): ResonatePromise<T>;
  run<T>(name: string, idOrVersion: string | number, ...args: any[]): ResonatePromise<T> {
    const v = typeof idOrVersion === "number" ? idOrVersion : 0;

    const id = typeof idOrVersion === "string" ? idOrVersion : args.shift();
    const idempotencyKey = utils.hash(id);

    if (!this.functions[name] || !this.functions[name][v]) {
      throw new Error(`Function ${name} version ${v} not registered`);
    }

    const { version, func, opts } = this.functions[name][v];
    return this.execute(name, version, id, idempotencyKey, func, args, opts);
  }

  /**
   * Start the resonate service.
   *
   * @param delay Frequency in ms to check for pending promises.
   */
  start(delay: number = 5000) {
    clearInterval(this.interval);
    this.interval = setInterval(() => this._start(), delay);
  }

  /**
   * Stop the resonate service.
   */
  stop() {
    clearInterval(this.interval);
  }

  private async _start() {
    try {
      for await (const promises of this.promises.search("*", "pending", { "resonate:invocation": "true" })) {
        for (const promise of promises) {
          const param = promise.param();
          if (
            param &&
            typeof param === "object" &&
            "func" in param &&
            typeof param.func === "string" &&
            "version" in param &&
            typeof param.version === "number" &&
            "args" in param &&
            Array.isArray(param.args)
          ) {
            const { func, opts } = this.functions[param.func][param.version];
            this.execute(
              param.func,
              param.version,
              promise.id,
              promise.idempotencyKeyForCreate,
              func,
              param.args,
              opts,
            );
          }
        }
      }
    } catch (e) {
      // squash all errors and log,
      // transient errors will be ironed out in the next interval
      this.logger.error(e);
    }
  }

  schedule(
    name: string,
    cron: string,
    func: Func | string,
    version: number = 1,
    ...args: any[]
  ): Promise<schedules.Schedule> {
    if (typeof func === "function") {
      this.register(name, version, func);
    }

    const funcName = typeof func === "string" ? func : name;

    if (!this.functions[funcName] || !this.functions[funcName][version]) {
      throw new Error(`Function ${funcName} version ${version} not registered`);
    }

    const { opts } = this.functions[funcName][version];

    const idempotencyKey = utils.hash(funcName);

    const promiseParam = {
      func: funcName,
      version,
      args,
    };

    return this.schedules.create(name, cron, "{{.id}}.{{.timestamp}}", opts.timeout, {
      idempotencyKey,
      promiseParam,
    });
  }

  /**
   * Construct options.
   *
   * @param opts A partial {@link Options} object.
   * @returns Options with the __resonate flag set.
   */
  options({
    encoder = this.encoder,
    poll = 5000, // every 5s
    retry = this.retry,
    store = this.store,
    tags = {},
    timeout = this.timeout,
  }: Partial<Options>): Options {
    // merge tags
    tags = { ...this.tags, ...tags };

    return {
      __resonate: true,
      encoder,
      poll,
      retry,
      store,
      tags,
      timeout,
    };
  }
}

export interface ResonatePromises {
  /**
   * Create a durable promise.
   *
   * @template T The type of the promise.
   * @param id Unique identifier for the promise.
   * @param timeout Time (in milliseconds) after which the promise is considered expired.
   * @param opts Additional options.
   * @returns A durable promise.
   */
  create<T>(id: string, timeout: number, opts?: Partial<promises.CreateOptions>): Promise<promises.DurablePromise<T>>;

  /**
   * Resolve a durable promise.
   *
   * @template T The type of the promise.
   * @param id Unique identifier for the promise.
   * @param value The resolved value.
   * @param opts Additional options.
   * @returns A durable promise.
   */
  resolve<T>(id: string, value: T, opts?: Partial<promises.CompleteOptions>): Promise<promises.DurablePromise<T>>;

  /**
   * Reject a durable promise.
   *
   * @template T The type of the promise.
   * @param id Unique identifier for the promise.
   * @param error The reject value.
   * @param opts Additional options.
   * @returns A durable promise.
   */
  reject<T>(id: string, error: any, opts?: Partial<promises.CompleteOptions>): Promise<promises.DurablePromise<T>>;

  /**
   * Cancel a durable promise.
   *
   * @template T The type of the promise.
   * @param id Unique identifier for the promise.
   * @param error The cancel value.
   * @param opts Additional options.
   * @returns A durable promise.
   */
  cancel<T>(id: string, error: any, opts?: Partial<promises.CompleteOptions>): Promise<promises.DurablePromise<T>>;

  /**
   * Get a durable promise.
   *
   * @template T The type of the promise.
   * @param id Id of the promise.
   * @returns A durable promise.
   */
  get<T>(id: string): Promise<promises.DurablePromise<T>>;

  /**
   * Search durable promises.
   *
   * @param id Ids to match, can include wildcards.
   * @param state State to match.
   * @param tags Tags to match.
   * @param limit Maximum number of promises to return.
   * @returns A generator that yields durable promises.
   */
  search(
    id: string,
    state?: string,
    tags?: Record<string, string>,
    limit?: number,
  ): AsyncGenerator<promises.DurablePromise<any>[]>;
}

export interface ResonateSchedules {
  /**
   * Create a new schedule.
   *
   * @param id Unique identifier for the schedule.
   * @param cron CRON expression defining the schedule's execution time.
   * @param promiseId Unique identifier for the associated promise.
   * @param promiseTimeout Timeout for the associated promise in milliseconds.
   * @param opts Additional options.
   * @returns A schedule.
   */
  create(
    id: string,
    cron: string,
    promiseId: string,
    promiseTimeout: number,
    opts?: Partial<schedules.Options>,
  ): Promise<schedules.Schedule>;

  /**
   * Get a schedule.
   *
   * @param id Id of the schedule.
   * @returns A schedule.
   */
  get(id: string): Promise<schedules.Schedule>;

  /**
   * Search for schedules.
   *
   * @param id Ids to match, can include wildcards.
   * @param tags Tags to match.
   * @param limit Maximum number of schedules to return.
   * @returns A generator that yields schedules.
   */
  search(
    id: string,
    tags: Record<string, string> | undefined,
    limit?: number,
  ): AsyncGenerator<schedules.Schedule[], void>;
}
