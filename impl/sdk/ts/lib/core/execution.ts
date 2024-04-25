import { ResonateBase } from "../resonate";
import { ErrorCodes, ResonateError } from "./errors";
import { Future, ResonatePromise } from "./future";
import { Invocation } from "./invocation";
import { DurablePromise } from "./promises/promises";

/////////////////////////////////////////////////////////////////////
// Execution
/////////////////////////////////////////////////////////////////////

export abstract class Execution<T> {
  private promise: ResonatePromise<T> | null = null;

  /**
   * Represents an execution of a Resonate function invocation.
   *
   * @constructor
   * @param invocation - An invocation correpsonding to the Resonate function.
   */
  constructor(
    public resonate: ResonateBase,
    public invocation: Invocation<T>,
  ) {}

  execute(): ResonatePromise<T> {
    if (this.promise) {
      return this.promise;
    }

    // each execution has a fork phase and a join phase

    // fork phase is responsible for spawning the invocation,
    // which in general means creating a durable promise (if necessary)
    const forkPromise = this.fork();

    // join phase is responsible for awaiting the invocation,
    // if the invocation is backed by a durable promise the durable
    // promise is completed before the invocation is completed
    const joinPromise = forkPromise.then((f) => this.join(f));

    this.promise = new ResonatePromise(this.invocation.id, forkPromise, joinPromise);
    return this.promise;
  }

  protected abstract fork(): Promise<Future<T>>;
  protected abstract join(future: Future<T>): Promise<T>;

  get killed() {
    return this.invocation.root.killed;
  }

  kill(error: unknown) {
    // this will mark the entire invocation tree as killed
    this.invocation.root.killed = true;

    // reject only the root invocation
    this.invocation.root.reject(new ResonateError("Resonate function killed", ErrorCodes.KILLED, error, true));
  }

  protected async acquireLock() {
    try {
      return await this.resonate.store.locks.tryAcquire(this.invocation.id, this.invocation.eid);
    } catch (e: unknown) {
      // if lock is already acquired, return false so we can poll
      if (e instanceof ResonateError && e.code === ErrorCodes.STORE_FORBIDDEN) {
        return false;
      }

      throw e;
    }
  }

  protected async releaseLock() {
    try {
      await this.resonate.store.locks.release(this.invocation.id, this.invocation.eid);
    } catch (e) {
      this.resonate.logger.warn("Failed to release lock", e);
    }
  }
}

export class OrdinaryExecution<T> extends Execution<T> {
  constructor(
    resonate: ResonateBase,
    invocation: Invocation<T>,
    private func: () => T,
    private durablePromise?: DurablePromise<T>,
  ) {
    super(resonate, invocation);
  }

  protected async fork() {
    if (this.invocation.opts.durable) {
      // if durable, create a durable promise
      try {
        this.durablePromise =
          this.durablePromise ??
          (await DurablePromise.create<T>(
            this.resonate.store.promises,
            this.invocation.opts.encoder,
            this.invocation.id,
            this.invocation.timeout,
            {
              idempotencyKey: this.invocation.idempotencyKey,
              headers: this.invocation.headers,
              param: this.invocation.param,
              tags: this.invocation.opts.tags,
            },
          ));

        // override the invocation timeout
        this.invocation.timeout = this.durablePromise.timeout;
      } catch (e) {
        // if an error occurs, kill the execution
        this.kill(e);
      }
    }

    return this.invocation.future;
  }

  protected async join(future: Future<T>) {
    try {
      // acquire lock if necessary
      while (this.invocation.opts.lock && !(await this.acquireLock())) {
        await new Promise((resolve) => setTimeout(resolve, this.invocation.opts.poll));
      }

      if (this.durablePromise) {
        if (this.durablePromise.pending) {
          // if durable and pending, invoke the function and resolve/reject the durable promise
          let value!: T;
          let error: any;

          // we need to hold on to a boolean to determine if the function was successful,
          // we cannot rely on the value or error as these values could be undefined
          let success = true;

          try {
            value = await this.run();
          } catch (e) {
            error = e;
            success = false;
          }

          if (success) {
            await this.durablePromise.resolve(value, { idempotencyKey: this.invocation.idempotencyKey });
          } else {
            await this.durablePromise.reject(error, { idempotencyKey: this.invocation.idempotencyKey });
          }
        }

        // if durable resolve/reject the invocation
        if (this.durablePromise.resolved) {
          this.invocation.resolve(this.durablePromise.value());
        } else if (this.durablePromise.rejected || this.durablePromise.canceled || this.durablePromise.timedout) {
          this.invocation.reject(this.durablePromise.error());
        }
      } else {
        // if not durable, invoke the function and resolve/reject the invocation
        try {
          this.invocation.resolve(await this.run());
        } catch (e) {
          this.invocation.reject(e);
        }
      }

      // release lock if necessary
      if (this.invocation.opts.lock) {
        await this.releaseLock();
      }
    } catch (e) {
      // if an error occurs, kill the execution
      this.kill(e);
    }

    return await future.promise;
  }

  private async run(): Promise<T> {
    let error;

    // invoke the function according to the retry policy
    for (const delay of this.invocation.opts.retry.iterator(this.invocation)) {
      await new Promise((resolve) => setTimeout(resolve, delay));

      try {
        return await this.func();
      } catch (e) {
        error = e;

        // bump the attempt count
        this.invocation.attempt++;
      }
    }

    // if all attempts fail throw the last error
    throw error;
  }
}

export class DeferredExecution<T> extends Execution<T> {
  private durablePromise: DurablePromise<T> | null = null;

  protected async fork() {
    try {
      // create a durable promise
      this.durablePromise = await DurablePromise.create<T>(
        this.resonate.store.promises,
        this.invocation.opts.encoder,
        this.invocation.id,
        this.invocation.timeout,
        {
          idempotencyKey: this.invocation.idempotencyKey,
          headers: this.invocation.headers,
          param: this.invocation.param,
          tags: this.invocation.opts.tags,
          poll: this.invocation.opts.poll,
        },
      );

      // override the invocation timeout
      this.invocation.timeout = this.durablePromise.timeout;
    } catch (e) {
      // if an error occurs, kill the execution
      this.kill(e);
    }

    return this.invocation.future;
  }

  protected async join(future: Future<T>) {
    if (this.durablePromise) {
      // poll the completion of the durable promise
      await this.durablePromise.completed;

      if (this.durablePromise.resolved) {
        this.invocation.resolve(this.durablePromise.value());
      } else {
        this.invocation.reject(this.durablePromise.error());
      }
    }

    return await future.promise;
  }
}

export class GeneratorExecution<T> extends Execution<T> {
  constructor(
    resonate: ResonateBase,
    invocation: Invocation<T>,
    public generator: Generator<any, T>,
    private durablePromise?: DurablePromise<T> | undefined,
  ) {
    super(resonate, invocation);
  }

  async create() {
    try {
      if (this.invocation.opts.durable) {
        // create a durable promise
        this.durablePromise =
          this.durablePromise ??
          (await DurablePromise.create<T>(
            this.resonate.store.promises,
            this.invocation.opts.encoder,
            this.invocation.id,
            this.invocation.timeout,
            {
              idempotencyKey: this.invocation.idempotencyKey,
              headers: this.invocation.headers,
              param: this.invocation.param,
              tags: this.invocation.opts.tags,
            },
          ));

        // override the invocation timeout
        this.invocation.timeout = this.durablePromise.timeout;

        // resolve/reject the invocation if already completed
        if (this.durablePromise.resolved) {
          this.invocation.resolve(this.durablePromise.value());
        } else if (this.durablePromise.rejected || this.durablePromise.canceled || this.durablePromise.timedout) {
          this.invocation.reject(this.durablePromise.error());
        }
      }
    } catch (e) {
      // if an error occurs, kill the execution
      this.kill(e);
    }
  }

  async resolve(value: T) {
    try {
      if (this.durablePromise) {
        // resolve the durable promise if the invocation is durable
        await this.durablePromise.resolve(value, { idempotencyKey: this.invocation.idempotencyKey });

        // resolve/reject the invocation
        if (this.durablePromise.resolved) {
          this.invocation.resolve(this.durablePromise.value());
        } else if (this.durablePromise.rejected || this.durablePromise.canceled || this.durablePromise.timedout) {
          this.invocation.reject(this.durablePromise.error());
        }
      } else {
        // if not durable, just resolve the invocation
        this.invocation.resolve(value);
      }
    } catch (e) {
      // if an error occurs, kill the execution
      this.kill(e);
    }
  }

  async reject(error: any) {
    try {
      if (this.durablePromise) {
        // reject the durable promise if the invocation is durable
        await this.durablePromise.reject(error, { idempotencyKey: this.invocation.idempotencyKey });

        // resolve/reject the invocation
        if (this.durablePromise.resolved) {
          this.invocation.resolve(this.durablePromise.value());
        } else if (this.durablePromise.rejected || this.durablePromise.canceled || this.durablePromise.timedout) {
          this.invocation.reject(this.durablePromise.error());
        }
      } else {
        // if not durable, just reject the invocation
        this.invocation.reject(error);
      }
    } catch (e) {
      // if an error occurs, kill the execution
      this.kill(e);
    }
  }

  protected async fork() {
    return this.invocation.future;
  }

  protected async join(future: Future<T>) {
    return await future.promise;
  }
}
