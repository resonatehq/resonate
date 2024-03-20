import { Execution, OrdinaryExecution, DeferredExecution } from "./core/execution";
import { ResonatePromise } from "./core/future";
import { Invocation } from "./core/invocation";
import { ResonateOptions, Options, PartialOptions } from "./core/options";
import * as utils from "./core/utils";
import { ResonateBase } from "./resonate";

/////////////////////////////////////////////////////////////////////
// Types
/////////////////////////////////////////////////////////////////////

export type AFunc = (ctx: Context, ...args: any[]) => any;

export type IFunc = (info: Info, ...args: any[]) => any;

export type Params<F> = F extends (ctx: any, ...args: infer P) => any ? P : never;

export type Return<F> = F extends (...args: any[]) => infer T ? Awaited<T> : never;

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
    this.scheduler = new Scheduler();
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
  register<F extends AFunc>(
    name: string,
    func: F,
    opts: Partial<Options> = {},
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
  registerModule<F extends AFunc>(module: Record<string, F>, opts: Partial<Options> = {}) {
    super.registerModule(module, opts);
  }

  protected execute<F extends AFunc>(
    name: string,
    version: number,
    id: string,
    func: F,
    args: Params<F>,
    opts: Options,
  ): ResonatePromise<Return<F>> {
    return this.scheduler.add(name, version, id, func, args, opts);
  }
}

/////////////////////////////////////////////////////////////////////
// Context
/////////////////////////////////////////////////////////////////////

export class Info {
  constructor(private invocation: Invocation<any>) {}

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
   * The timestamp in ms, once this time elapses the function invocation will timeout.
   */
  get timeout() {
    return this.invocation.timeout;
  }

  /**
   * The running count of function execution attempts.
   */
  get attempt() {
    return this.invocation.attempt;
  }
}

export class Context {
  constructor(private invocation: Invocation<any>) {}

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
   * The timestamp in ms, once this time elapses the function invocation will timeout.
   */
  get timeout() {
    return this.invocation.timeout;
  }

  /**
   * The running count of child function invocations.
   */
  get counter() {
    return this.invocation.counter;
  }

  /**
   * Invoke a function.
   *
   * @template F The type of the function.
   * @param func The function to invoke.
   * @param args The function arguments, optionally followed by {@link options}.
   * @returns A promise that resolves to the return value of the function.
   */
  run<F extends AFunc>(func: F, ...args: [...Params<F>, PartialOptions?]): ResonatePromise<Return<F>>;

  /**
   * Invoke a remote function.
   *
   * @template T The return type of the remote function.
   * @param func The id of the remote function.
   * @param args The arguments to pass to the remote function.
   * @param opts Optional {@link options}.
   * @returns A promise that resolves to the resolved value of the remote function.
   */
  run<T>(func: string, args?: any, opts?: PartialOptions): ResonatePromise<T>;
  run(func: string | ((...args: any[]) => any), ...argsWithOpts: any[]): ResonatePromise<any> {
    // the id is either:
    // 1. a provided string in the case of a deferred execution
    // 2. a generated string in the case of an ordinary execution
    const id = typeof func === "string" ? func : `${this.invocation.id}.${this.invocation.counter}`;

    // the idempotency key is a hash of the id
    const idempotencyKey = utils.hash(id);

    // opts are optional and can be provided as the last arg
    const { args, opts } = this.invocation.split(argsWithOpts);

    // create a new invocation
    const invocation = new Invocation(id, idempotencyKey, opts, this.invocation);

    let execution: Execution<any>;
    if (typeof func === "string") {
      // create a deferred execution
      // this execution will be fulfilled out-of-process
      execution = new DeferredExecution(invocation);
    } else {
      const ctx = new Context(invocation);

      // create an ordinary execution
      // this execution wraps a user-provided function
      execution = new OrdinaryExecution(invocation, () => func(ctx, ...args));
    }

    // bump the invocation counter
    this.invocation.counter++;

    // return a resonate promise
    return execution.execute();
  }

  /**
   * Invoke an io function.
   *
   * @template F The type of the function.
   * @param func The function to invoke.
   * @param args The function arguments, optionally followed by {@link options}.
   * @returns A promise that resolves to the return value of the function.
   */
  io<F extends IFunc>(func: F, ...args: [...Params<F>, PartialOptions?]): Promise<Return<F>>;
  io(func: (...args: any[]) => any, ...argsWithOpts: any[]): ResonatePromise<any> {
    const id = `${this.invocation.id}.${this.invocation.counter}`;
    const idempotencyKey = utils.hash(id);
    const { args, opts } = this.invocation.split(argsWithOpts);

    const invocation = new Invocation(id, idempotencyKey, opts, this.invocation);
    const info = new Info(invocation);

    // create an ordinary execution
    // this execution wraps a user-provided io function
    // unlike run, an io function can not create child invocations
    const execution = new OrdinaryExecution(invocation, () => func(info, ...args));

    // bump the invocation counter
    this.invocation.counter++;

    // return a resonate promise
    return execution.execute();
  }

  /**
   * Construct options.
   *
   * @param opts A partial {@link Options} object.
   * @returns Options with the __resonate flag set.
   */
  options(opts: Partial<Options> = {}): Partial<Options> & { __resonate: true } {
    return { ...opts, __resonate: true };
  }
}

/////////////////////////////////////////////////////////////////////
// Scheduler
/////////////////////////////////////////////////////////////////////

class Scheduler {
  private executions: Record<string, { execution: Execution<any>; promise: ResonatePromise<any> }> = {};

  add<F extends AFunc>(
    name: string,
    version: number,
    id: string,
    func: F,
    args: Params<F>,
    opts: Options,
  ): ResonatePromise<Return<F>> {
    // if the execution is already running, and not killed,
    // return the promise
    if (this.executions[id] && !this.executions[id].execution.invocation.killed) {
      return this.executions[id].promise;
    }

    // the idempotency key is a hash of the id
    const idempotencyKey = utils.hash(id);

    // create a new invocation
    const invocation = new Invocation<Return<F>>(id, idempotencyKey, opts);

    // create a new execution
    const ctx = new Context(invocation);
    const execution = new OrdinaryExecution(invocation, () => func(ctx, ...args));
    const promise = execution.execute();

    // store the execution and promise,
    // will be used if run is called again with the same id
    this.executions[id] = { execution, promise };

    return promise;
  }
}
