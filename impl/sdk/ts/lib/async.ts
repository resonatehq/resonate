import { Func, isRFC, LFC, Params, Return, RFC } from "./core/calls";
import { Execution, OrdinaryExecution, DeferredExecution } from "./core/execution";
import { ResonatePromise } from "./core/future";
import { Invocation } from "./core/invocation";
import { ResonateOptions, Options, PartialOptions } from "./core/options";
import { DurablePromise } from "./core/promises/promises";
import * as retryPolicy from "./core/retry";
import * as schedules from "./core/schedules/schedules";
import * as utils from "./core/utils";
import { ResonateBase } from "./resonate";

/////////////////////////////////////////////////////////////////////
// Types
/////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////
// Resonate
/////////////////////////////////////////////////////////////////////

export class Resonate extends ResonateBase {
  private scheduler: Scheduler;

  /**
   * Creates a Resonate instance. This is the starting point for using Resonate.
   *
   * @constructor
   * @param opts - A partial {@link ResonateOptions} object.
   */
  constructor(opts: Partial<ResonateOptions> = {}) {
    super(opts);
    this.scheduler = new Scheduler(this);
  }

  protected execute<F extends Func>(
    name: string,
    id: string,
    func: F,
    args: Params<F>,
    opts: Options,
    durablePromise?: DurablePromise<any>,
  ): ResonatePromise<Return<F>> {
    return this.scheduler.add(name, id, func, args, opts, durablePromise);
  }

  /**
   * Register a function with Resonate. Registered functions can be invoked by calling {@link run}, or by the returned function.
   *
   * @template F The type of the function.
   * @param name A unique name to identify the function.
   * @param func The function to register with Resonate.
   * @param opts Resonate options, can be constructed by calling {@link options}.
   * @returns Resonate function
   */
  register<F extends Func>(
    name: string,
    func: F,
    opts?: Partial<Options>,
  ): (id: string, ...args: Params<F>) => ResonatePromise<Return<F>> {
    return super.register(name, func, opts);
  }

  /**
   * Register a module with Resonate. Registered functions can be invoked by calling {@link run}.
   *
   * @template F The type of the function.
   * @param module The module to register with Resonate.
   * @param opts Resonate options, can be constructed by calling {@link options}.
   * @returns Resonate function
   */
  registerModule<F extends Func>(module: Record<string, F>, opts: Partial<Options> = {}) {
    super.registerModule(module, opts);
  }

  /**
   * Schedule a resonate function.
   *
   * @param name The schedule name.
   * @param cron The schedule cron expression.
   * @param func The function to schedule.
   * @param args The function arguments.
   * @returns The schedule object.
   */
  schedule<F extends Func>(
    name: string,
    cron: string,
    func: F,
    ...args: [...Params<F>, PartialOptions?]
  ): Promise<schedules.Schedule>;

  /**
   * Schedule a resonate function that is already registered.
   *
   * @param name The schedule name.
   * @param cron The schedule cron expression.
   * @param func The registered function name.
   * @param args The function arguments.
   * @returns The schedule object.
   */
  schedule(name: string, cron: string, func: string, ...args: [...any, PartialOptions?]): Promise<schedules.Schedule>;
  schedule(name: string, cron: string, func: Func | string, ...args: any[]): Promise<schedules.Schedule> {
    return super.schedule(name, cron, func, ...args);
  }
}

/////////////////////////////////////////////////////////////////////
// Context
/////////////////////////////////////////////////////////////////////

export class Context {
  constructor(
    private resonate: Resonate,
    private invocation: Invocation<any>,
  ) {}

  /**
   * The running count of function execution attempts.
   */
  get attempt() {
    return this.invocation.attempt;
  }

  /**
   * The running count of child function invocations.
   */
  get counter() {
    return this.invocation.counter;
  }

  /**
   * The time the invocation was created. Will use the durable promise creation time if available.
   */
  get createdOn() {
    return this.invocation.createdOn;
  }

  /**
   * Uniquely identifies the function invocation.
   */
  get id() {
    return this.invocation.id;
  }

  /**
   * Deduplicates function invocations with the same id.
   */
  get idempotencyKey() {
    return this.invocation.idempotencyKey;
  }

  /**
   * All configured options for this context.
   */
  get opts() {
    return this.invocation.opts;
  }

  /**
   * The timestamp in ms, once this time elapses the function invocation will timeout.
   */
  get timeout() {
    return this.invocation.timeout;
  }

  /**
   * The resonate function version.
   */
  get version() {
    return this.invocation.root.opts.version;
  }

  /**
   * Invoke a function.
   *
   * @template F The type of the function.
   * @param func The function to invoke.
   * @param args The function arguments, optionally followed by {@link options}.
   * @returns A promise that resolves to the return value of the function.
   */
  run<F extends Func>(func: F, ...args: [...Params<F>, PartialOptions?]): ResonatePromise<Return<F>>;

  /**
   * Invoke a Local Function Call (LFC).
   *
   * @template F - A type extending Function
   * @param {LFC} lfc - The Local Function Call configuration
   * @returns {ReturnType<F>} The result of the function execution
   */
  run<F extends Func>(lfc: LFC<F>): ResonatePromise<Return<F>>;

  /**
   * Invoke a remote function.
   *
   * @template T The return type of the remote function.
   * @param func The id of the remote function.
   * @param args The arguments to pass to the remote function.
   * @param opts Optional {@link options}.
   * @returns A promise that resolves to the resolved value of the remote function.
   */
  run<T>(func: string, args: any, opts?: PartialOptions): ResonatePromise<T>;

  /**
   * Invoke a Remote Function Call (RFC).
   *
   * @param {RFC} rfc - The Remote Function Call configuration
   * @returns {ResonatePromise<T>} A promise that resolves with the result of the remote execution
   *
   * @description
   * This function takes a Remote Function Call (RFC) configuration and executes the specified function
   * remotely with the provided options.
   */
  run<T>(rfc: RFC): ResonatePromise<T>;

  /**
   * Invoke a remote function without arguments.
   *
   * @template T The return type of the remote function.
   * @param func The id of the remote function.
   * @param opts Optional {@link options}.
   * @returns A promise that resolves to the resolved value of the remote function.
   */
  run<T>(func: string, opts?: PartialOptions): ResonatePromise<T>;

  run<F extends Func, T>(funcOrFc: F | LFC<F> | RFC | string, ...argsWithOpts: any[]): ResonatePromise<Return<F> | T> {
    // Function with args and possibly options
    if (typeof funcOrFc === "function") {
      const { args, opts } = utils.split(argsWithOpts);
      const lfc: LFC<F> = {
        func: funcOrFc,
        args,
        opts: opts,
      };

      return this._run<F, T>(lfc);
    }

    // String (function name) with args and possibly options
    if (typeof funcOrFc === "string") {
      const funcName = funcOrFc;
      const { args, opts } = utils.split(argsWithOpts);

      const rfc: RFC = {
        funcName,
        args,
        opts,
      };

      return this._run<F, T>(rfc);
    }

    // We are sure it is an FC
    return this._run<F, T>(funcOrFc);
  }

  private _run<F extends Func, T>(fc: LFC<F> | RFC): ResonatePromise<Return<F> | T> {
    // the parent is the current invocation
    const parent = this.invocation;

    // human readable name of the function
    let name!: string;

    // the id is either:
    // 1. a provided string in the case of a deferred execution
    // 2. a generated string in the case of an ordinary execution
    let id!: string;

    // Params to store with the durable promise. For RemoteExecutions we just encode the
    // given arg(s). Otherwise we store nothing since it is unncesary.
    let param: any | undefined;

    if (isRFC(fc)) {
      name = fc.funcName;
      id = name;
      param = fc.args;
    } else {
      name = fc.func.name;
      id = `${parent.id}.${parent.counter}.${name}`;
      param = undefined;
    }

    const registeredOptions = this.resonate.registeredOptions(
      this.invocation.root.name,
      this.invocation.root.opts.version,
    );

    const resonateOptions = this.resonate.defaults();

    const opts = {
      ...resonateOptions,
      ...registeredOptions,
      ...fc.opts,
    };

    // Merge the tags
    opts.tags = { ...resonateOptions.tags, ...registeredOptions.tags, ...fc.opts?.tags };

    // Default lock is false for children execution
    opts.shouldLock = opts.shouldLock ?? false;

    // create a new invocation
    const invocation = new Invocation(name, id, undefined, param, opts, parent);

    let execution: Execution<any>;
    if (isRFC(fc)) {
      // create a deferred execution
      // this execution will be fulfilled out-of-process
      execution = new DeferredExecution(this.resonate, invocation);
    } else {
      // create an ordinary execution
      // this execution wraps a user-provided function
      const ctx = new Context(this.resonate, invocation);
      execution = new OrdinaryExecution(this.resonate, invocation, () => fc.func(ctx, ...(fc.args ?? [])));
    }

    // bump the counter
    parent.counter++;

    // return a resonate promise
    return execution.execute();
  }

  /**
   * Creates a Promise that is resolved with an array of results when all of the provided Promises
   * resolve, or rejected when any Promise is rejected.
   *
   * @param values An array of Promises.
   * @param opts Optional {@link options}.
   * @returns A new ResonatePromise.
   */
  all<T extends readonly unknown[] | []>(
    values: T,
    opts: Partial<Options> = {},
  ): ResonatePromise<{ -readonly [P in keyof T]: Awaited<T[P]> }> {
    // catch all promises to prevent unhandled promise rejections,
    // since Promise.all will not be called in the case where the
    // durable promise already completed
    for (const value of values) {
      if (value instanceof Promise) {
        value.catch(() => {});
      }
    }

    // prettier-ignore
    return this.run(() => Promise.all(values), this.options({
      retryPolicy: retryPolicy.never(),
      ...opts,
    }));
  }

  /**
   * Creates a Promise that is fulfilled by the first given promise to be fulfilled, or rejected
   * with an AggregateError.
   *
   * @param values An array of Promises.
   * @param opts Optional {@link options}.
   * @returns A new ResonatePromise.
   */
  any<T extends readonly unknown[] | []>(values: T, opts: Partial<Options> = {}): ResonatePromise<Awaited<T[number]>> {
    // catch all promises to prevent unhandled promise rejections,
    // since Promise.any will not be called in the case where the
    // durable promise already completed
    for (const value of values) {
      if (value instanceof Promise) {
        value.catch(() => {});
      }
    }

    // prettier-ignore
    return this.run(() => Promise.any(values), this.options({
      retryPolicy: retryPolicy.never(),
      ...opts,
    }));
  }

  /**
   * Creates a Promise that is resolved or rejected when any of the provided Promises are resolved
   * or rejected.
   *
   * @param values An array of Promises.
   * @param opts Optional {@link options}.
   * @returns A new ResonatePromise.
   */
  race<T extends readonly unknown[] | []>(values: T, opts: Partial<Options> = {}): ResonatePromise<Awaited<T[number]>> {
    // catch all promises to prevent unhandled promise rejections,
    // since Promise.race will not be called in the case where the
    // durable promise already completed
    for (const value of values) {
      if (value instanceof Promise) {
        value.catch(() => {});
      }
    }

    // prettier-ignore
    return this.run(() => Promise.race(values), this.options({
      retryPolicy: retryPolicy.never(),
      ...opts,
    }));
  }

  /**
   * Creates a Promise that is resolved with an array of results when all of the provided Promises
   * resolve or reject.
   *
   * @param values An array of Promises.
   * @param opts Optional {@link options}.
   * @returns A new ResonatePromise.
   */
  allSettled<T extends readonly unknown[] | []>(
    values: T,
    opts: Partial<Options> = {},
  ): ResonatePromise<{ -readonly [P in keyof T]: PromiseSettledResult<Awaited<T[P]>> }> {
    // catch all promises to prevent unhandled promise rejections,
    // since Promise.allSettled will not be called in the case where the
    // durable promise already completed
    for (const value of values) {
      if (value instanceof Promise) {
        value.catch(() => {});
      }
    }

    // prettier-ignore
    return this.run(() => Promise.allSettled(values), this.options({
      retryPolicy: retryPolicy.never(),
      ...opts,
    }));
  }

  /**
   * Sleep for the specified time.
   *
   * @param ms Amount of time to sleep in milliseconds.
   * @returns A Promise that resolves after the specified time has elapsed.
   */
  async sleep(ms: number): Promise<void> {
    // generate id
    const id = `${this.invocation.id}.${this.invocation.counter++}`;

    // create a promise that resolves when it times out
    const promise = await this.resonate.promises.create(id, Date.now() + ms, {
      tags: { "resonate:timeout": "true" },
    });

    // wait for the promise to resolve
    // if wait time < 1, delay will be set to 1
    if (promise.pending) {
      await new Promise((resolve) => setTimeout(resolve, promise.timeout - Date.now()));
    }

    // tight loop in case the promise is not yet resolved
    await promise.wait(Infinity, this.invocation.opts.pollFrequency);
  }

  /**
   * Generate a deterministic random number.
   *
   * @returns A random number.
   */
  random(): ResonatePromise<number> {
    return this.run(Math.random);
  }

  /**
   * Get a deterministic timestamp.
   *
   * @returns A timestamp in ms.
   */
  now(): ResonatePromise<number> {
    return this.run(Date.now);
  }

  /**
   * Run a Resonate function in detached mode. Functions must first be registered with Resonate.
   *
   * @template T The return type of the function.
   * @param id A unique id for the function invocation.
   * @param name The function name.
   * @param args The function arguments.
   * @returns A ResonatePromise.
   */
  detached<T>(name: string, id: string, ...args: [...any, PartialOptions?]): ResonatePromise<T> {
    return this.resonate.run(name, id, ...args);
  }

  /**
   * Construct options.
   *
   * @param opts A partial {@link Options} object.
   * @returns PartialOptions.
   */
  options(opts: Partial<Options> = {}): PartialOptions {
    return this.resonate.options(opts);
  }
}

/////////////////////////////////////////////////////////////////////
// Scheduler
/////////////////////////////////////////////////////////////////////

class Scheduler {
  private cache: Record<string, Execution<any>> = {};

  constructor(private resonate: Resonate) {}

  add<F extends Func>(
    name: string,
    id: string,
    func: F,
    args: Params<F>,
    opts: Options,
    durablePromise?: DurablePromise<any>,
  ): ResonatePromise<Return<F>> {
    // if the execution is already running, and not killed,
    // return the promise
    if (opts.durable && this.cache[id] && !this.cache[id].killed) {
      // execute is idempotent
      return this.cache[id].execute();
    }

    // params, used for recovery
    const param = {
      func: name,
      version: opts.version,
      retryPolicy: opts.retryPolicy,
      args,
    };

    // create a new invocation
    const invocation = new Invocation<Return<F>>(name, id, undefined, param, opts);

    // create a new execution
    const ctx = new Context(this.resonate, invocation);
    const execution = new OrdinaryExecution(this.resonate, invocation, () => func(ctx, ...args), durablePromise);

    // store the execution,
    // will be used if run is called again with the same id
    this.cache[id] = execution;

    return execution.execute();
  }
}
