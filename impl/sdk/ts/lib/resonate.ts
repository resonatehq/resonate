import { IEncoder } from "./core/encoder";
import { JSONEncoder } from "./core/encoders/json";
import { ResonatePromise } from "./core/future";
import { ILogger } from "./core/logger";
import { Logger } from "./core/loggers/logger";
import { ResonateOptions, Options } from "./core/options";
import { DurablePromise, CreateOptions, CompleteOptions } from "./core/promises/promises";
import { Retry } from "./core/retries/retry";
import { IRetry } from "./core/retry";
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
  }

  register(
    name: string,
    funcOrVersion: Func | number,
    funcOrOpts: Func | Partial<Options>,
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
    const id = typeof idOrVersion === "string" ? idOrVersion : args.shift();
    const v = typeof idOrVersion === "number" ? idOrVersion : 0;

    if (!this.functions[name] || !this.functions[name][v]) {
      throw new Error(`Function ${name} version ${v} not registered`);
    }

    const { version, func, opts } = this.functions[name][v];
    return this.execute(name, version, id, func, args, opts);
  }

  protected abstract execute(
    name: string,
    version: number,
    id: string,
    func: Func,
    args: any[],
    opts: Options,
  ): ResonatePromise<any>;

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
    for await (const promises of this.promises.search("*", "pending", { "resonate:invocation": "true" })) {
      for (const promise of promises) {
        try {
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
            this.run(param.func, param.version, promise.id, param.args);
          }
        } catch (e) {
          this.logger.warn(e);
        }
      }
    }
  }

  get promises() {
    return {
      create: <T>(id: string, timeout: number, opts: Partial<CreateOptions> = {}) =>
        DurablePromise.create<T>(this.store.promises, this.encoder, id, timeout, opts),

      resolve: <T>(id: string, value: T, opts: Partial<CompleteOptions> = {}) =>
        DurablePromise.resolve<T>(this.store.promises, this.encoder, id, value, opts),

      reject: <T>(id: string, error: any, opts: Partial<CompleteOptions> = {}) =>
        DurablePromise.reject<T>(this.store.promises, this.encoder, id, error, opts),

      cancel: <T>(id: string, error: any, opts: Partial<CompleteOptions> = {}) =>
        DurablePromise.cancel<T>(this.store.promises, this.encoder, id, error, opts),

      get: <T>(id: string) => DurablePromise.get<T>(this.store.promises, this.encoder, id),

      search: (id: string, state?: string, tags?: Record<string, string>, limit?: number) =>
        DurablePromise.search(this.store.promises, this.encoder, id, state, tags, limit),
    };
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
