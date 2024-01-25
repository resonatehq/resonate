import {
  DurablePromise,
  PendingPromise,
  ResolvedPromise,
  RejectedPromise,
  CanceledPromise,
  TimedoutPromise,
  isDurablePromise,
  isCompletedPromise,
} from "../promise";
import { IStore, IPromiseStore, IScheduleStore } from "../store";
import { IEncoder } from "../encoder";
import { Base64Encoder } from "../encoders/base64";
import { ErrorCodes, ResonateError } from "../error";
import { ILockStore } from "../store";
import { ILogger } from "../logger";
import { Schedule, isSchedule } from "../schedule";
import { Logger } from "../loggers/logger";

export class RemoteStore implements IStore {
  public promises: RemotePromiseStore;
  public schedules: RemoteScheduleStore;
  public locks: RemoteLockStore;

  constructor(url: string, logger: ILogger, encoder: IEncoder<string, string> = new Base64Encoder()) {
    this.promises = new RemotePromiseStore(url, logger, encoder);
    this.schedules = new RemoteScheduleStore(url, logger, encoder);
    this.locks = new RemoteLockStore(url, logger);
  }
}

export class RemotePromiseStore implements IPromiseStore {
  constructor(
    private url: string,
    private logger: ILogger = new Logger(),
    private encoder: IEncoder<string, string> = new Base64Encoder(),
  ) {}

  async create(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
    timeout: number,
    tags: Record<string, string> | undefined,
  ): Promise<PendingPromise | CanceledPromise | ResolvedPromise | RejectedPromise | TimedoutPromise> {
    const reqHeaders: Record<string, string> = {
      Accept: "application/json",
      "Content-Type": "application/json",
      Strict: JSON.stringify(strict),
    };

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const promise = await call(
      `${this.url}/promises`,
      isDurablePromise,
      {
        method: "POST",
        headers: reqHeaders,
        body: JSON.stringify({
          id: id,
          param: {
            headers: headers,
            data: data ? encode(data, this.encoder) : undefined,
          },
          timeout: timeout,
          tags: tags,
        }),
      },
      this.logger,
    );

    return decode(promise, this.encoder);
  }

  async cancel(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    const reqHeaders: Record<string, string> = {
      Accept: "application/json",
      "Content-Type": "application/json",
      Strict: JSON.stringify(strict),
    };

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const promise = await call(
      `${this.url}/promises/${id}`,
      isCompletedPromise,
      {
        method: "PATCH",
        headers: reqHeaders,
        body: JSON.stringify({
          state: "REJECTED_CANCELED",
          value: {
            headers: headers,
            data: data ? encode(data, this.encoder) : undefined,
          },
        }),
      },
      this.logger,
    );

    return decode(promise, this.encoder);
  }

  async resolve(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    const reqHeaders: Record<string, string> = {
      Accept: "application/json",
      "Content-Type": "application/json",
      Strict: JSON.stringify(strict),
    };

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const promise = await call(
      `${this.url}/promises/${id}`,
      isCompletedPromise,
      {
        method: "PATCH",
        headers: reqHeaders,
        body: JSON.stringify({
          state: "RESOLVED",
          value: {
            headers: headers,
            data: data ? encode(data, this.encoder) : undefined,
          },
        }),
      },
      this.logger,
    );

    return decode(promise, this.encoder);
  }

  async reject(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    const reqHeaders: Record<string, string> = {
      Accept: "application/json",
      "Content-Type": "application/json",
      Strict: JSON.stringify(strict),
    };

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const promise = await call(
      `${this.url}/promises/${id}`,
      isCompletedPromise,
      {
        method: "PATCH",
        headers: reqHeaders,
        body: JSON.stringify({
          state: "REJECTED",
          value: {
            headers: headers,
            data: data ? encode(data, this.encoder) : undefined,
          },
        }),
      },
      this.logger,
    );

    return decode(promise, this.encoder);
  }

  async get(id: string): Promise<DurablePromise> {
    const promise = await call(
      `${this.url}/promises/${id}`,
      isDurablePromise,
      {
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        method: "GET",
      },
      this.logger,
    );

    return decode(promise, this.encoder);
  }

  async *search(
    id: string,
    state: string | undefined,
    tags: Record<string, string> | undefined,
    limit: number | undefined,
  ): AsyncGenerator<DurablePromise[], void> {
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

      const url = new URL(`${this.url}/promises`);
      url.search = params.toString();

      const res = await call(
        url.toString(),
        isSearchPromiseResult,
        {
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
          },
          method: "GET",
        },
        this.logger,
      );

      cursor = res.cursor;
      yield res.promises.map((p) => decode(p, this.encoder));
    }
  }
}

export class RemoteScheduleStore implements IScheduleStore {
  constructor(
    private url: string,
    private logger: ILogger,
    private encoder: IEncoder<string, string> = new Base64Encoder(),
  ) {}

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
    const reqHeaders: Record<string, string> = {
      Accept: "application/json",
      "Content-Type": "application/json",
    };

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const schedule = call(
      `${this.url}/schedules`,
      isSchedule,
      {
        method: "POST",
        headers: reqHeaders,
        body: JSON.stringify({
          id,
          description,
          cron,
          tags,
          promiseId,
          promiseTimeout,
          promiseHeaders,
          promiseData: promiseData ? encode(promiseData, this.encoder) : undefined,
          promiseTags,
        }),
      },
      this.logger,
    );

    return schedule;
  }

  async get(id: string): Promise<Schedule> {
    const schedule = call(
      `${this.url}/schedules/${id}`,
      isSchedule,
      {
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        method: "GET",
      },
      this.logger,
    );

    return schedule;
  }

  async delete(id: string): Promise<void> {
    await call(
      `${this.url}/schedules/${id}`,
      isDeleted,
      {
        method: "DELETE",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
      },
      this.logger,
    );
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

      const url = new URL(`${this.url}/schedules`);
      url.search = params.toString();

      const res = await call(
        url.toString(),
        isSearchSchedulesResult,
        {
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
          },
          method: "GET",
        },
        this.logger,
      );

      cursor = res.cursor;
      yield res.schedules;
    }
  }
}

// utils

async function call<T>(
  url: string,
  guard: (b: unknown) => b is T,
  options: RequestInit,
  logger: ILogger,
  retries: number = 3,
): Promise<T> {
  let error: unknown;

  for (let i = 0; i < retries; i++) {
    try {
      const r = await fetch(url, options);
      const body: unknown = await r.json();

      logger.debug("store", {
        req: {
          method: options.method,
          url: url,
          headers: options.headers,
          body: options.body,
        },
        res: {
          status: r.status,
          body: body,
        },
      });

      if (!r.ok) {
        switch (r.status) {
          case 400:
            throw new ResonateError(ErrorCodes.PAYLOAD, "Invalid request", body);
          case 403:
            throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request", body);
          case 404:
            throw new ResonateError(ErrorCodes.NOT_FOUND, "Not found", body);
          case 409:
            throw new ResonateError(ErrorCodes.ALREADY_EXISTS, "Already exists", body);
          default:
            throw new ResonateError(ErrorCodes.SERVER, "Server error", body, true);
        }
      }

      if (!guard(body)) {
        throw new ResonateError(ErrorCodes.PAYLOAD, "Invalid response", body);
      }

      return body;
    } catch (e: unknown) {
      if (e instanceof ResonateError && !e.retryable) {
        throw e;
      } else {
        error = e;
      }
    }
  }

  throw ResonateError.fromError(error);
}

function encode(value: string, encoder: IEncoder<string, string>): string {
  try {
    return encoder.encode(value);
  } catch (e: unknown) {
    throw new ResonateError(ErrorCodes.ENCODER, "Encode error", e);
  }
}

function decode<P extends DurablePromise>(promise: P, encoder: IEncoder<string, string>): P {
  try {
    if (promise.param?.data) {
      promise.param.data = encoder.decode(promise.param.data);
    }

    if (promise.value?.data) {
      promise.value.data = encoder.decode(promise.value.data);
    }

    return promise;
  } catch (e: unknown) {
    throw new ResonateError(ErrorCodes.ENCODER, "Decode error", e);
  }
}

// Type guards

function isDeleted(_: any): _ is any {
  return true;
}

function isSearchPromiseResult(obj: any): obj is { cursor: string; promises: DurablePromise[] } {
  return (
    obj !== undefined &&
    obj.cursor !== undefined &&
    (obj.cursor === null || typeof obj.cursor === "string") &&
    obj.promises !== undefined &&
    Array.isArray(obj.promises) &&
    obj.promises.every(isDurablePromise)
  );
}

function isSearchSchedulesResult(obj: any): obj is { cursor: string; schedules: Schedule[] } {
  return (
    obj !== undefined &&
    obj.cursor !== undefined &&
    (obj.cursor === null || typeof obj.cursor === "string") &&
    obj.schedules !== undefined &&
    Array.isArray(obj.schedules) &&
    obj.schedules.every(isSchedule)
  );
}

export class RemoteLockStore implements ILockStore {
  private url: string;
  private heartbeatInterval: number | null = null;

  constructor(
    url: string,
    private logger: ILogger = new Logger(),
  ) {
    this.url = url;
  }

  async tryAcquire(resourceId: string, processId: string, executionId: string): Promise<boolean> {
    const lockRequest = {
      resourceId: resourceId,
      processId: processId,
      executionId: executionId,
    };

    const acquired = call<boolean>(
      `${this.url}/locks/acquire`,
      (b: unknown): b is boolean => typeof b === "boolean",
      {
        method: "post",
        body: JSON.stringify(lockRequest),
      },
      this.logger,
    );

    if (await acquired) {
      if (this.heartbeatInterval === null) {
        this.startHeartbeat(processId);
      }
      return true;
    }
    return false;
  }

  async release(resourceId: string, executionId: string): Promise<void> {
    const releaseLockRequest = {
      resource_id: resourceId,
      execution_id: executionId,
    };

    call<void>(
      `${this.url}/locks/release`,
      (response: unknown): response is void => response === undefined,
      {
        method: "post",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(releaseLockRequest),
      },
      this.logger,
    );
  }

  private startHeartbeat(processId: string): void {
    this.heartbeatInterval = +setInterval(async () => {
      const locksAffected = await this.heartbeat(processId);
      if (locksAffected === 0) {
        this.stopHeartbeat();
      }
    }, 5000); // desired heartbeat interval (in ms)
  }

  private stopHeartbeat(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  private async heartbeat(processId: string): Promise<number> {
    const heartbeatRequest = {
      process_id: processId,
      timeout: 0,
    };

    return call<number>(
      `${this.url}/locks/heartbeat`,
      (locksAffected: unknown): locksAffected is number => typeof locksAffected === "number",
      {
        method: "post",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(heartbeatRequest),
      },
      this.logger,
    );
  }
}
