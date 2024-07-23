import { IEncoder } from "../encoder";
import { Base64Encoder } from "../encoders/base64";
import { ErrorCodes, ResonateError } from "../errors";
import { ILogger } from "../logger";
import { Logger } from "../loggers/logger";
import { StoreOptions } from "../options";
import { DurablePromiseRecord, isDurablePromiseRecord, isCompletedPromise } from "../promises/types";
import { Schedule, isSchedule } from "../schedules/types";
import { IStore, IPromiseStore, IScheduleStore, ILockStore } from "../store";
import * as utils from "../utils";

export class RemoteStore implements IStore {
  private readonly headers: Record<string, string> = {
    Accept: "application/json",
    "Content-Type": "application/json",
  };

  public readonly promises: RemotePromiseStore;
  public readonly schedules: RemoteScheduleStore;
  public readonly locks: RemoteLockStore;

  public readonly encoder: IEncoder<string, string>;
  public readonly heartbeat: number;
  public readonly logger: ILogger;
  public readonly pid: string;
  public readonly retries: number;

  constructor(
    public readonly url: string,
    opts: Partial<StoreOptions> = {},
  ) {
    // store components
    this.promises = new RemotePromiseStore(this);
    this.schedules = new RemoteScheduleStore(this);
    this.locks = new RemoteLockStore(this);

    // store options
    this.encoder = opts.encoder ?? new Base64Encoder();
    this.logger = opts.logger ?? new Logger();
    this.heartbeat = opts.heartbeat ?? 15000;
    this.pid = opts.pid ?? utils.randomId();
    this.retries = opts.retries ?? 2;

    // auth
    if (opts.auth?.basic) {
      this.headers["Authorization"] = `Basic ${btoa(`${opts.auth.basic.username}:${opts.auth.basic.password}`)}`;
    }
  }

  async call<T>(path: string, guard: (b: unknown) => b is T, options: RequestInit): Promise<T> {
    let error: unknown;

    // add auth headers
    options.headers = { ...this.headers, ...options.headers };

    for (let i = 0; i < this.retries + 1; i++) {
      try {
        this.logger.debug("store:req", {
          url: this.url,
          method: options.method,
          headers: options.headers,
          body: options.body,
        });

        let r!: Response;
        try {
          r = await fetch(`${this.url}/${path}`, options);
        } catch (err) {
          // We need to differentiate between errors in this fetch and possible fetching
          // errors in the user code, that is why we wrap the fetch error in a resonate
          // error.
          // Some fetch errors are caused by malformed urls or wrong use of headers,
          // thoses shouldn't be retriable, but fetch throws a `TypeError` for those
          // aswell as errors when the server is unreachble which is a retriable error.
          // Since it is hard to identify which error was thrown, we mark all of them as retriable.
          throw new ResonateError("Fetch Error", ErrorCodes.FETCH, err, true);
        }

        const body: unknown = r.status !== 204 ? await r.json() : undefined;

        this.logger.debug("store:res", {
          status: r.status,
          body: body,
        });

        if (!r.ok) {
          switch (r.status) {
            case 400:
              throw new ResonateError("Invalid request", ErrorCodes.STORE_PAYLOAD, body);
            case 401:
              throw new ResonateError("Unauthorized request", ErrorCodes.STORE_UNAUTHORIZED, body);
            case 403:
              throw new ResonateError("Forbidden request", ErrorCodes.STORE_FORBIDDEN, body);
            case 404:
              throw new ResonateError("Not found", ErrorCodes.STORE_NOT_FOUND, body);
            case 409:
              throw new ResonateError("Already exists", ErrorCodes.STORE_ALREADY_EXISTS, body);
            default:
              throw new ResonateError("Server error", ErrorCodes.STORE, body, true);
          }
        }

        if (!guard(body)) {
          throw new ResonateError("Invalid response", ErrorCodes.STORE_PAYLOAD, body);
        }

        return body;
      } catch (e: unknown) {
        if (e instanceof ResonateError && !e.retriable) {
          throw e;
        } else {
          error = e;
        }
      }
    }

    throw ResonateError.fromError(error);
  }
}

export class RemotePromiseStore implements IPromiseStore {
  constructor(private store: RemoteStore) {}

  async create(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
    timeout: number,
    tags: Record<string, string> | undefined,
  ): Promise<DurablePromiseRecord> {
    const reqHeaders: Record<string, string> = {
      Strict: JSON.stringify(strict),
    };

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const promise = await this.store.call("promises", isDurablePromiseRecord, {
      method: "POST",
      headers: reqHeaders,
      body: JSON.stringify({
        id: id,
        param: {
          headers: headers,
          data: data ? encode(data, this.store.encoder) : undefined,
        },
        timeout: timeout,
        tags: tags,
      }),
    });

    return decode(promise, this.store.encoder);
  }

  async cancel(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<DurablePromiseRecord> {
    const reqHeaders: Record<string, string> = {
      Strict: JSON.stringify(strict),
    };

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const promise = await this.store.call(`promises/${id}`, isDurablePromiseRecord, {
      method: "PATCH",
      headers: reqHeaders,
      body: JSON.stringify({
        state: "REJECTED_CANCELED",
        value: {
          headers: headers,
          data: data ? encode(data, this.store.encoder) : undefined,
        },
      }),
    });

    if (!isCompletedPromise(promise)) {
      throw new ResonateError("Invalid response", ErrorCodes.STORE_PAYLOAD, promise);
    }

    return decode(promise, this.store.encoder);
  }

  async resolve(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<DurablePromiseRecord> {
    const reqHeaders: Record<string, string> = {
      Strict: JSON.stringify(strict),
    };

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const promise = await this.store.call(`promises/${id}`, isDurablePromiseRecord, {
      method: "PATCH",
      headers: reqHeaders,
      body: JSON.stringify({
        state: "RESOLVED",
        value: {
          headers: headers,
          data: data ? encode(data, this.store.encoder) : undefined,
        },
      }),
    });

    if (!isCompletedPromise(promise)) {
      throw new ResonateError("Invalid response", ErrorCodes.STORE_PAYLOAD, promise);
    }

    return decode(promise, this.store.encoder);
  }

  async reject(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<DurablePromiseRecord> {
    const reqHeaders: Record<string, string> = {
      Strict: JSON.stringify(strict),
    };

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const promise = await this.store.call(`promises/${id}`, isDurablePromiseRecord, {
      method: "PATCH",
      headers: reqHeaders,
      body: JSON.stringify({
        state: "REJECTED",
        value: {
          headers: headers,
          data: data ? encode(data, this.store.encoder) : undefined,
        },
      }),
    });

    if (!isCompletedPromise(promise)) {
      throw new ResonateError("Invalid response", ErrorCodes.STORE_PAYLOAD, promise);
    }

    return decode(promise, this.store.encoder);
  }

  async get(id: string): Promise<DurablePromiseRecord> {
    const promise = await this.store.call(`promises/${id}`, isDurablePromiseRecord, {
      method: "GET",
    });

    return decode(promise, this.store.encoder);
  }

  async *search(
    id: string,
    state: string | undefined,
    tags: Record<string, string> | undefined,
    limit: number | undefined,
  ): AsyncGenerator<DurablePromiseRecord[], void> {
    let cursor: string | null | undefined = undefined;

    while (cursor !== null) {
      const params = new URLSearchParams({ id });

      if (state !== undefined) {
        params.append("state", state);
      }

      for (const [k, v] of Object.entries(tags ?? {})) {
        params.append(`tags[${k}]`, v);
      }

      if (limit !== undefined) {
        params.append("limit", limit.toString());
      }

      if (cursor !== undefined) {
        params.append("cursor", cursor);
      }

      const res = await this.store.call(`promises?${params.toString()}`, isSearchPromiseResult, {
        method: "GET",
      });

      cursor = res.cursor;
      yield res.promises.map((p) => decode(p, this.store.encoder));
    }
  }
}

export class RemoteScheduleStore implements IScheduleStore {
  constructor(private store: RemoteStore) {}

  async create(
    id: string,
    ikey: string | undefined,
    description: string | undefined,
    cron: string,
    tags: Record<string, string> | undefined,
    promiseId: string,
    promiseTimeout: number,
    promiseHeaders: Record<string, string> | undefined,
    promiseData: string | undefined,
    promiseTags: Record<string, string> | undefined,
  ): Promise<Schedule> {
    const reqHeaders: Record<string, string> = {};

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const schedule = this.store.call<Schedule>(`schedules`, isSchedule, {
      method: "POST",
      headers: reqHeaders,
      body: JSON.stringify({
        id,
        description,
        cron,
        tags,
        promiseId,
        promiseTimeout,
        promiseParam: {
          headers: promiseHeaders,
          data: promiseData ? encode(promiseData, this.store.encoder) : undefined,
        },
        promiseTags,
      }),
    });

    return schedule;
  }

  async get(id: string): Promise<Schedule> {
    const schedule = this.store.call(`schedules/${id}`, isSchedule, {
      method: "GET",
    });

    return schedule;
  }

  async delete(id: string): Promise<void> {
    await this.store.call(`schedules/${id}`, (b: unknown): b is any => true, {
      method: "DELETE",
    });
  }

  async *search(
    id: string,
    tags: Record<string, string> | undefined,
    limit: number | undefined,
  ): AsyncGenerator<Schedule[], void> {
    let cursor: string | null | undefined = undefined;

    while (cursor !== null) {
      const params = new URLSearchParams({ id });

      for (const [k, v] of Object.entries(tags ?? {})) {
        params.append(`tags[${k}]`, v);
      }

      if (limit !== undefined) {
        params.append("limit", limit.toString());
      }

      if (cursor !== undefined) {
        params.append("cursor", cursor);
      }

      const res = await this.store.call(`schedules?${params.toString()}`, isSearchSchedulesResult, {
        method: "GET",
      });

      cursor = res.cursor;
      yield res.schedules;
    }
  }
}

export class RemoteLockStore implements ILockStore {
  private heartbeatInterval: number | null = null;
  private locksHeld: number = 0;

  constructor(private store: RemoteStore) {}

  async tryAcquire(
    resourceId: string,
    executionId: string,
    expiry: number = this.store.heartbeat * 4,
  ): Promise<boolean> {
    // lock expiry cannot be less than heartbeat frequency
    expiry = Math.max(expiry, this.store.heartbeat);

    await this.store.call<boolean>(`locks/acquire`, (b: unknown): b is any => true, {
      method: "POST",
      body: JSON.stringify({
        resourceId: resourceId,
        processId: this.store.pid,
        executionId: executionId,
        expiryInMilliseconds: expiry,
      }),
    });

    // increment the number of locks held
    this.locksHeld++;

    // lazily start the heartbeat
    this.startHeartbeat();

    return true;
  }

  async release(resourceId: string, executionId: string): Promise<boolean> {
    await this.store.call<void>(`locks/release`, (b: unknown): b is void => b === undefined, {
      method: "POST",
      body: JSON.stringify({
        resourceId,
        executionId,
      }),
    });

    // decrement the number of locks held
    this.locksHeld = Math.max(this.locksHeld - 1, 0);

    if (this.locksHeld === 0) {
      this.stopHeartbeat();
    }

    return true;
  }

  private startHeartbeat(): void {
    if (this.heartbeatInterval === null) {
      // the + converts to a number
      this.heartbeatInterval = +setInterval(() => this.heartbeat(), this.store.heartbeat);
    }
  }

  private stopHeartbeat(): void {
    if (this.heartbeatInterval !== null) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  private async heartbeat() {
    const res = await this.store.call<{ locksAffected: number }>(
      `locks/heartbeat`,
      (b: unknown): b is { locksAffected: number } =>
        typeof b === "object" && b !== null && "locksAffected" in b && typeof b.locksAffected === "number",
      {
        method: "POST",
        body: JSON.stringify({
          processId: this.store.pid,
        }),
      },
    );

    // set the number of locks held
    this.locksHeld = res.locksAffected;

    if (this.locksHeld === 0) {
      this.stopHeartbeat();
    }
  }
}

// Utils

function encode(value: string, encoder: IEncoder<string, string>): string {
  try {
    return encoder.encode(value);
  } catch (e: unknown) {
    throw new ResonateError("Encoder error", ErrorCodes.STORE_ENCODER, e);
  }
}

function decode<P extends DurablePromiseRecord>(promise: P, encoder: IEncoder<string, string>): P {
  try {
    if (promise.param?.data) {
      promise.param.data = encoder.decode(promise.param.data);
    }

    if (promise.value?.data) {
      promise.value.data = encoder.decode(promise.value.data);
    }

    return promise;
  } catch (e: unknown) {
    throw new ResonateError("Decoder error", ErrorCodes.STORE_ENCODER, e);
  }
}

// Type guards

function isSearchPromiseResult(obj: unknown): obj is { cursor: string; promises: DurablePromiseRecord[] } {
  return (
    typeof obj === "object" &&
    obj !== null &&
    "cursor" in obj &&
    obj.cursor !== undefined &&
    (obj.cursor === null || typeof obj.cursor === "string") &&
    "promises" in obj &&
    obj.promises !== undefined &&
    Array.isArray(obj.promises) &&
    obj.promises.every(isDurablePromiseRecord)
  );
}

function isSearchSchedulesResult(obj: unknown): obj is { cursor: string; schedules: Schedule[] } {
  return (
    typeof obj === "object" &&
    obj !== null &&
    "cursor" in obj &&
    obj.cursor !== undefined &&
    (obj.cursor === null || typeof obj.cursor === "string") &&
    "schedules" in obj &&
    obj.schedules !== undefined &&
    Array.isArray(obj.schedules) &&
    obj.schedules.every(isSchedule)
  );
}
