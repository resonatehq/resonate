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
  private readonly functions: Record<string, { func: Func; opts: Options }> = {};

  public readonly pid: string;
  public readonly timeout: number;

  protected readonly encoder: IEncoder<unknown, string | undefined>;
  protected readonly logger: ILogger;
  protected readonly retry: IRetry;
  protected readonly store: IStore;

  constructor({
    encoder = new JSONEncoder(),
    logger = new Logger(),
    pid = utils.randomId(),
    retry = Retry.exponential(),
    store = undefined,
    timeout = 10000, // 10s
    url = undefined,
  }: Partial<ResonateOptions> = {}) {
    this.encoder = encoder;
    this.logger = logger;
    this.pid = pid;
    this.retry = retry;
    this.timeout = timeout;

    if (store) {
      this.store = store;
    } else if (url) {
      this.store = new RemoteStore(url, this.pid, this.logger);
    } else {
      this.store = new LocalStore(this.logger);
    }
  }

  register(name: string, func: Func, opts: Partial<Options> = {}): (id: string, ...args: any) => ResonatePromise<any> {
    if (this.functions[name]) {
      throw new Error(`Function ${name} already registered`);
    }

    this.functions[name] = { func, opts: this.options(opts) };
    return (id: string, ...args: any[]) => this.run(name, id, ...args);
  }

  registerModule(module: Record<string, Func>, opts: Partial<Options> = {}) {
    for (const key in module) {
      this.register(key, module[key], opts);
    }
  }

  run<T>(name: string, id: string, ...args: any[]): ResonatePromise<T> {
    if (!this.functions[name]) {
      throw new Error(`Function ${name} not registered`);
    }

    const { func, opts } = this.functions[name];
    return this.execute(name, 1, id, func, args, opts);
  }

  protected abstract execute(
    name: string,
    version: number,
    id: string,
    func: Func,
    args: any[],
    opts: Options,
  ): ResonatePromise<any>;

  get promises() {
    return {
      get: <T>(id: string) => DurablePromise.get<T>(this.store.promises, this.encoder, id),

      create: <T>(id: string, timeout: number, opts: Partial<CreateOptions> = {}) =>
        DurablePromise.create<T>(this.store.promises, this.encoder, id, timeout, opts),

      resolve: <T>(id: string, value: T, opts: Partial<CompleteOptions> = {}) =>
        DurablePromise.resolve<T>(this.store.promises, this.encoder, id, value, opts),

      reject: <T>(id: string, error: any, opts: Partial<CompleteOptions> = {}) =>
        DurablePromise.reject<T>(this.store.promises, this.encoder, id, error, opts),

      cancel: <T>(id: string, error: any, opts: Partial<CompleteOptions> = {}) =>
        DurablePromise.cancel<T>(this.store.promises, this.encoder, id, error, opts),
    };
  }

  options({
    encoder = this.encoder,
    retry = this.retry,
    store = this.store,
    timeout = this.timeout,
  }: Partial<Options>): Options {
    return {
      __resonate: true,
      encoder,
      retry,
      store,
      timeout,
    };
  }
}
