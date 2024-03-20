import { Future, ResonatePromise } from "./future";
import { Invocation } from "./invocation";
import { DurablePromise } from "./promises/promises";
import { IRetry } from "./retry";

/////////////////////////////////////////////////////////////////////
// Execution
/////////////////////////////////////////////////////////////////////

export abstract class Execution<T> {
  /**
   * Represents an execution of a Resonate function invocation.
   *
   * @constructor
   * @param invocation - An invocation correpsonding to the Resonate function.
   */
  constructor(public invocation: Invocation<T>) {}

  execute() {
    const forkPromise = this.fork();
    const joinPromise = forkPromise.then((f) => this.join(f));

    return new ResonatePromise(this.invocation.id, forkPromise, joinPromise);
  }

  protected abstract fork(): Promise<Future<T>>;
  protected abstract join(future: Future<T>): Promise<T>;
}

export class OrdinaryExecution<T> extends Execution<T> {
  constructor(
    invocation: Invocation<T>,
    private func: () => T,
    private retry: IRetry = invocation.opts.retry,
  ) {
    super(invocation);
  }

  private async invoke(): Promise<T> {
    let error;

    // invoke the function according to the retry policy
    for (const delay of this.retry.iterator(this.invocation)) {
      try {
        await new Promise((resolve) => setTimeout(resolve, delay));
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

  protected async fork() {
    try {
      // create a durable promise
      const promise = await DurablePromise.create<T>(
        this.invocation.opts.store.promises,
        this.invocation.opts.encoder,
        this.invocation.id,
        this.invocation.timeout,
        { idempotencyKey: this.invocation.idempotencyKey },
      );

      if (promise.pending) {
        // if pending, invoke the function and resolve/reject the durable promise
        await this.invoke().then(
          (v) => promise.resolve(v, { idempotencyKey: this.invocation.idempotencyKey }),
          (e) => promise.reject(e, { idempotencyKey: this.invocation.idempotencyKey }),
        );
      }

      // resolve/reject the invocation
      if (promise.resolved) {
        this.invocation.resolve(promise.value);
      } else if (promise.rejected || promise.canceled || promise.timedout) {
        this.invocation.reject(promise.error);
      }
    } catch (e) {
      // if an error occurs, kill the invocation
      this.invocation.kill(e);
    }

    return this.invocation.future;
  }

  protected async join(future: Future<T>) {
    return await future.promise;
  }
}

export class DeferredExecution<T> extends Execution<T> {
  constructor(invocation: Invocation<T>) {
    super(invocation);
  }

  protected async fork() {
    try {
      // create a durable promise
      const promise = await DurablePromise.create<T>(
        this.invocation.opts.store.promises,
        this.invocation.opts.encoder,
        this.invocation.id,
        this.invocation.timeout,
        { idempotencyKey: this.invocation.idempotencyKey, poll: true },
      );

      // poll the completion of the durable promise
      promise.completed.then((p) => (p.resolved ? this.invocation.resolve(p.value) : this.invocation.reject(p.error)));
    } catch (e) {
      // if an error occurs, kill the invocation
      this.invocation.kill(e);
    }

    return this.invocation.future;
  }

  protected async join(future: Future<T>) {
    return await future.promise;
  }
}

export class GeneratorExecution<T> extends Execution<T> {
  constructor(
    invocation: Invocation<T>,
    public generator: Generator<any, T>,
  ) {
    super(invocation);
  }

  async create() {
    try {
      // create a durable promise
      const promise = await DurablePromise.create<T>(
        this.invocation.opts.store.promises,
        this.invocation.opts.encoder,
        this.invocation.id,
        this.invocation.timeout,
        { idempotencyKey: this.invocation.idempotencyKey },
      );

      // resolve/reject the invocation if already completed
      if (promise.resolved) {
        this.invocation.resolve(promise.value);
      } else if (promise.rejected || promise.canceled || promise.timedout) {
        this.invocation.reject(promise.error);
      }
      return promise;
    } catch (e) {
      // if an error occurs, kill the invocation
      this.invocation.kill(e);
    }
  }

  async resolve(promise: DurablePromise<T>, value: T) {
    try {
      // resolve the durable promise
      await promise.resolve(value, { idempotencyKey: this.invocation.idempotencyKey });

      // resolve/reject the invocation
      if (promise.resolved) {
        this.invocation.resolve(promise.value);
      } else if (promise.rejected || promise.canceled || promise.timedout) {
        this.invocation.reject(promise.error);
      }
    } catch (e) {
      // if an error occurs, kill the invocation
      this.invocation.kill(e);
    }

    return promise;
  }

  async reject(promise: DurablePromise<T>, error: any) {
    try {
      // reject the durable promise
      await promise.resolve(error, { idempotencyKey: this.invocation.idempotencyKey });

      // resolve/reject the invocation
      if (promise.resolved) {
        this.invocation.resolve(promise.value);
      } else if (promise.rejected || promise.canceled || promise.timedout) {
        this.invocation.reject(promise.error);
      }
    } catch (e) {
      // if an error occurs, kill the invocation
      this.invocation.kill(e);
    }

    return promise;
  }

  protected async fork() {
    return this.invocation.future;
  }

  protected async join(future: Future<T>) {
    return await future.promise;
  }
}
