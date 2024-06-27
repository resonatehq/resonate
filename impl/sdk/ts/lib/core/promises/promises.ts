import { IEncoder } from "../encoder";
import { ErrorCodes, ResonateError } from "../errors";
import { IPromiseStore } from "../store";
import { DurablePromiseRecord } from "./types";

/**
 * Durable Promise create options.
 */
export type CreateOptions = {
  /**
   * Durable Promise idempotency key.
   */
  idempotencyKey: string | undefined;

  /**
   * Durable Promise headers.
   */
  headers: Record<string, string>;

  /**
   * Durable Promise param, will be encoded with the provided encoder.
   */
  param: unknown;

  /**
   * Durable Promise tags.
   */
  tags: Record<string, string>;

  /**
   * Create the Durable Promise in strict mode.
   */
  strict: boolean;
};

/**
 * Durable Promise complete options.
 */
export type CompleteOptions = {
  /**
   * Durable Promise idempotency key.
   */
  idempotencyKey: string | undefined;

  /**
   * Durable Promise headers.
   */
  headers: Record<string, string>;

  /**
   * Create the Durable Promise in strict mode.
   */
  strict: boolean;
};

export class DurablePromise<T> {
  private readonly completed: Promise<DurablePromise<T>>;
  private complete!: (value: DurablePromise<T>) => void;
  private interval: NodeJS.Timeout | undefined;

  /**
   * Creates a Durable Promise instance. This is provided as a lower level API, used by the Resonate class internally.
   *
   * @constructor
   * @param store - A reference to a promise store.
   * @param encoder - An encoder instance used for encode and decode promise data.
   * @param promise - The raw Durable Promise.
   */
  constructor(
    private store: IPromiseStore,
    private encoder: IEncoder<unknown, string | undefined>,
    private promise: DurablePromiseRecord,
  ) {
    this.completed = new Promise((resolve) => {
      this.complete = resolve;
    });
  }

  /**
   * The Durable Promise id.
   */
  get id() {
    return this.promise.id;
  }

  /**
   * The Durable Promise create idempotency key.
   */
  get idempotencyKeyForCreate() {
    return this.promise.idempotencyKeyForCreate;
  }

  /**
   * The Durable Promise complete idempotency key.
   */
  get idempotencyKeyForComplete() {
    return this.promise.idempotencyKeyForComplete;
  }

  /**
   * The Durable Promise created on time.
   */
  get createdOn() {
    return this.promise.createdOn;
  }

  /**
   * The Durable Promise timeout time.
   */
  get timeout() {
    return this.promise.timeout;
  }

  /**
   * Returns true when the Durable Promise is pending.
   */
  get pending() {
    return this.promise.state === "PENDING";
  }

  /**
   * Returns true when the Durable Promise is resolved.
   */
  get resolved() {
    return this.promise.state === "RESOLVED";
  }

  /**
   * Returns true when the Durable Promise is rejected.
   */
  get rejected() {
    return this.promise.state === "REJECTED";
  }

  /**
   * Returns true when the Durable Promise is canceled.
   */
  get canceled() {
    return this.promise.state === "REJECTED_CANCELED";
  }

  /**
   * Returns true when the Durable Promise is timedout.
   */
  get timedout() {
    return this.promise.state === "REJECTED_TIMEDOUT";
  }

  /**
   * Returns the decoded promise param data.
   */
  param() {
    return this.encoder.decode(this.promise.param.data);
  }

  /**
   * Returns the decoded promise value data.
   */
  value() {
    if (!this.resolved) {
      throw new Error("Promise is not resolved");
    }

    return this.encoder.decode(this.promise.value.data) as T;
  }

  /**
   * Returns the decoded promise value data as an error.
   */
  error() {
    if (this.rejected) {
      return this.encoder.decode(this.promise.value.data);
    } else if (this.canceled) {
      return new ResonateError(
        "Resonate function canceled",
        ErrorCodes.CANCELED,
        this.encoder.decode(this.promise.value.data),
      );
    } else if (this.timedout) {
      return new ResonateError(
        `Resonate function timedout at ${new Date(this.promise.timeout).toISOString()}`,
        ErrorCodes.TIMEDOUT,
      );
    } else {
      throw new Error("Promise is not rejected, canceled, or timedout");
    }
  }

  /**
   * Creates a Durable Promise.
   * @param store - A reference to a promise store.
   * @param encoder - An encoder instance used for encode and decode promise data.
   * @param id - The Durable Promise id.
   * @param timeout - The Durable Promise timeout in milliseconds.
   * @param opts - A partial Durable Promise create options.
   * @returns A Durable Promise instance.
   */
  static async create<T>(
    store: IPromiseStore,
    encoder: IEncoder<unknown, string | undefined>,
    id: string,
    timeout: number,
    opts: Partial<CreateOptions> = {},
  ) {
    const storedPromise = await store.create(
      id,
      opts.idempotencyKey,
      opts.strict ?? false,
      opts.headers,
      encoder.encode(opts.param),
      timeout,
      opts.tags,
    );
    return new DurablePromise<T>(store, encoder, storedPromise);
  }

  /**
   * Resolves a Durable Promise.
   * @param store - A reference to a promise store.
   * @param encoder - An encoder instance used for encode and decode promise data.
   * @param id - The Durable Promise id.
   * @param value - The Durable Promise value, will be encoded with the provided encoder.
   * @param opts - A partial Durable Promise create options.
   * @returns A Durable Promise instance.
   */
  static async resolve<T>(
    store: IPromiseStore,
    encoder: IEncoder<unknown, string | undefined>,
    id: string,
    value: T,
    opts: Partial<CompleteOptions> = {},
  ) {
    return new DurablePromise<T>(
      store,
      encoder,
      await store.resolve(id, opts.idempotencyKey, opts.strict ?? false, opts.headers, encoder.encode(value)),
    );
  }

  /**
   * Rejects a Durable Promise.
   * @param store - A reference to a promise store.
   * @param encoder - An encoder instance used for encode and decode promise data.
   * @param id - The Durable Promise id.
   * @param error - The Durable Promise error value, will be encoded with the provided encoder.
   * @param opts - A partial Durable Promise create options.
   * @returns A Durable Promise instance.
   */
  static async reject<T>(
    store: IPromiseStore,
    encoder: IEncoder<unknown, string | undefined>,
    id: string,
    error: any,
    opts: Partial<CompleteOptions> = {},
  ) {
    return new DurablePromise<T>(
      store,
      encoder,
      await store.reject(id, opts.idempotencyKey, opts.strict ?? false, opts.headers, encoder.encode(error)),
    );
  }

  /**
   * Cancels a Durable Promise.
   * @param store - A reference to a promise store.
   * @param encoder - An encoder instance used for encode and decode promise data.
   * @param id - The Durable Promise id.
   * @param error - The Durable Promise error value, will be encoded with the provided encoder.
   * @param opts - A partial Durable Promise create options.
   * @returns A Durable Promise instance.
   */
  static async cancel<T>(
    store: IPromiseStore,
    encoder: IEncoder<unknown, string | undefined>,
    id: string,
    error: any,
    opts: Partial<CompleteOptions> = {},
  ) {
    return new DurablePromise<T>(
      store,
      encoder,
      await store.cancel(id, opts.idempotencyKey, opts.strict ?? false, opts.headers, encoder.encode(error)),
    );
  }

  /**
   * Gets a Durable Promise.
   * @param store - A reference to a promise store.
   * @param encoder - An encoder instance used for encode and decode promise data.
   * @param id - The Durable Promise id.
   * @returns A Durable Promise instance.
   */
  static async get<T>(store: IPromiseStore, encoder: IEncoder<unknown, string | undefined>, id: string) {
    return new DurablePromise<T>(store, encoder, await store.get(id));
  }

  /**
   * Search for Durable Promises.
   * @param store - A reference to a promise store.
   * @param encoder - An encoder instance used for encode and decode promise data.
   * @param id - An id to match against Durable Promise ids, can include wilcards.
   * @param state - A state to search for, can be one of {pending, resolved, rejected}, matches all states if undefined.
   * @param tags - Tags to search against.
   * @param limit - The maximum number of Durable Promises to return per page.
   * @returns An async generator that yields Durable Promise instances.
   */
  static async *search(
    store: IPromiseStore,
    encoder: IEncoder<unknown, string | undefined>,
    id: string,
    state?: string,
    tags?: Record<string, string>,
    limit?: number,
  ): AsyncGenerator<DurablePromise<any>[], void> {
    for await (const promises of store.search(id, state, tags, limit)) {
      yield promises.map((p) => new DurablePromise(store, encoder, p));
    }
  }

  /**
   * Resolves the Durable Promise.
   * @param value - The Durable Promise value, will be encoded with the provided encoder.
   * @param opts - A partial Durable Promise create options.
   * @returns this instance.
   */
  async resolve(value: T, opts: Partial<CompleteOptions> = {}) {
    this.promise = !this.pending
      ? this.promise
      : await this.store.resolve(
          this.id,
          opts.idempotencyKey,
          opts.strict ?? false,
          opts.headers,
          this.encoder.encode(value),
        );

    if (!this.pending) {
      this.complete(this);
    }

    return this;
  }

  /**
   * Rejects the Durable Promise.
   * @param error - The Durable Promise error value, will be encoded with the provided encoder.
   * @param opts - A partial Durable Promise create options.
   * @returns this instance.
   */
  async reject(error: any, opts: Partial<CompleteOptions> = {}) {
    this.promise = !this.pending
      ? this.promise
      : await this.store.reject(
          this.id,
          opts.idempotencyKey,
          opts.strict ?? false,
          opts.headers,
          this.encoder.encode(error),
        );

    if (!this.pending) {
      this.complete(this);
    }

    return this;
  }

  /**
   * Cancels the Durable Promise.
   * @param error - The Durable Promise error value, will be encoded with the provided encoder.
   * @param opts - A partial Durable Promise create options.
   * @returns this instance.
   */
  async cancel(error: any, opts: Partial<CompleteOptions> = {}) {
    this.promise = !this.pending
      ? this.promise
      : await this.store.cancel(
          this.id,
          opts.idempotencyKey,
          opts.strict ?? false,
          opts.headers,
          this.encoder.encode(error),
        );

    if (!this.pending) {
      this.complete(this);
    }

    return this;
  }

  /**
   * Polls the Durable Promise store to sychronize the state, stops when the promise is complete.
   * @param timeout - The time at which to stop polling if the promise is still pending.
   * @param frequency - The frequency in ms to poll.
   * @returns A Promise that resolves when the Durable Promise is complete.
   */
  async sync(timeout: number = Infinity, frequency: number = 5000): Promise<void> {
    if (!this.pending) return;

    // reset polling interval
    clearInterval(this.interval);
    this.interval = setInterval(() => this.poll(), frequency);

    // poll immediately for now
    // we can revisit when we implement a cache subsystem
    await this.poll();

    // set timeout promise
    let timeoutId: NodeJS.Timeout | undefined;
    const timeoutPromise =
      timeout === Infinity
        ? new Promise(() => {}) // wait forever
        : new Promise((resolve) => (timeoutId = setTimeout(resolve, timeout)));

    // await either:
    // - completion of the promise
    // - timeout
    await Promise.race([this.completed, timeoutPromise]);

    // clear polling interval
    clearInterval(this.interval);

    // clear timeout
    clearTimeout(timeoutId);

    // throw error if timeout occcured
    if (this.pending) {
      throw new Error("Timeout occured while waiting for promise to complete");
    }
  }

  /**
   * Polls the Durable Promise store, and returns the value when the Durable Promise is complete.
   * @param timeout - The time at which to stop polling if the promise is still pending.
   * @param frequency - The frequency in ms to poll.
   * @returns The promise value, or throws an error.
   */
  async wait(timeout: number = Infinity, frequency: number = 5000): Promise<T> {
    await this.sync(timeout, frequency);

    if (this.resolved) {
      return this.value();
    } else {
      throw this.error();
    }
  }

  private async poll() {
    try {
      this.promise = await this.store.get(this.id);

      if (!this.pending) {
        this.complete(this);
      }
    } catch (e) {
      // TODO: log
    }
  }
}
