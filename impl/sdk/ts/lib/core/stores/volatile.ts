import {
  DurablePromise,
  PendingPromise,
  ResolvedPromise,
  RejectedPromise,
  CanceledPromise,
  TimedoutPromise,
  isDurablePromise,
  isPendingPromise,
  isResolvedPromise,
  isRejectedPromise,
  isCanceledPromise,
} from "../promise";
import { IPromiseStore } from "../store";
import { IEncoder } from "../encoder";
import { Base64Encoder } from "../encoders/base64";
import { ErrorCodes, ResonateError } from "../error";

export class VolatilePromiseStore implements IPromiseStore {
  private readonly promises: Record<string, string> = {};

  constructor(private encoder: IEncoder<string, string> = new Base64Encoder()) {}

  async create(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
    timeout: number,
    tags: Record<string, string> | undefined,
  ): Promise<PendingPromise | ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    const tick = Date.now();
    this.transition(tick);

    if (id in this.promises) {
      const promise = this.promiseDecoder(isDurablePromise)(JSON.parse(this.promises[id]));

      if (strict && !isPendingPromise(promise)) {
        throw new ResonateError("Forbidden request", ErrorCodes.FORBIDDEN);
      }

      if (promise.idempotencyKeyForCreate === undefined || ikey != promise.idempotencyKeyForCreate) {
        throw new ResonateError("Forbidden request", ErrorCodes.FORBIDDEN);
      }

      return promise;
    } else {
      this.promises[id] = JSON.stringify({
        state: "PENDING",
        id: id,
        timeout: timeout,
        param: {
          headers: headers,
          data: data ? this.encode(data) : undefined,
        },
        value: undefined,
        createdOn: tick,
        completedOn: undefined,
        idempotencyKeyForCreate: ikey,
        idempotencyKeyForComplete: undefined,
        tags: tags,
      });

      return this.promiseDecoder(isPendingPromise)(JSON.parse(this.promises[id]));
    }
  }

  async resolve(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    const tick = Date.now();
    this.transition(tick);

    if (id in this.promises) {
      const promise = this.promiseDecoder(isDurablePromise)(JSON.parse(this.promises[id]));

      if (strict && !isPendingPromise(promise) && !isResolvedPromise(promise)) {
        throw new ResonateError("Forbidden request", ErrorCodes.FORBIDDEN);
      }

      if (isPendingPromise(promise)) {
        this.promises[id] = JSON.stringify({
          state: "RESOLVED",
          id: promise.id,
          timeout: promise.timeout,
          param: {
            headers: promise.param.headers,
            data: promise.param.data ? this.encode(promise.param.data) : undefined,
          },
          value: {
            headers: headers,
            data: data ? this.encode(data) : undefined,
          },
          createdOn: promise.createdOn,
          completedOn: tick,
          idempotencyKeyForCreate: promise.idempotencyKeyForCreate,
          idempotencyKeyForComplete: ikey,
          tags: promise.tags,
        });

        return this.promiseDecoder(isResolvedPromise)(JSON.parse(this.promises[id]));
      }

      if (promise.idempotencyKeyForComplete === undefined || ikey != promise.idempotencyKeyForComplete) {
        throw new ResonateError("Forbidden request", ErrorCodes.FORBIDDEN);
      }

      return promise;
    } else {
      throw new ResonateError("Not found", ErrorCodes.NOT_FOUND);
    }
  }

  async reject(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    const tick = Date.now();
    this.transition(tick);

    if (id in this.promises) {
      const promise = this.promiseDecoder(isDurablePromise)(JSON.parse(this.promises[id]));

      if (strict && !isPendingPromise(promise) && !isRejectedPromise(promise)) {
        throw new ResonateError("Forbidden request", ErrorCodes.FORBIDDEN);
      }

      if (isPendingPromise(promise)) {
        this.promises[id] = JSON.stringify({
          state: "REJECTED",
          id: promise.id,
          timeout: promise.timeout,
          param: {
            headers: promise.param.headers,
            data: promise.param.data ? this.encode(promise.param.data) : undefined,
          },
          value: {
            headers: headers,
            data: data ? this.encode(data) : undefined,
          },
          createdOn: promise.createdOn,
          completedOn: tick,
          idempotencyKeyForCreate: promise.idempotencyKeyForCreate,
          idempotencyKeyForComplete: ikey,
          tags: promise.tags,
        });

        return this.promiseDecoder(isRejectedPromise)(JSON.parse(this.promises[id]));
      }

      if (promise.idempotencyKeyForComplete === undefined || ikey != promise.idempotencyKeyForComplete) {
        throw new ResonateError("Forbidden request", ErrorCodes.FORBIDDEN);
      }

      return promise;
    } else {
      throw new ResonateError("Not found", ErrorCodes.NOT_FOUND);
    }
  }

  async cancel(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    const tick = Date.now();
    this.transition(tick);

    if (id in this.promises) {
      const promise = this.promiseDecoder(isDurablePromise)(JSON.parse(this.promises[id]));

      if (strict && !isPendingPromise(promise) && !isCanceledPromise(promise)) {
        throw new ResonateError("Forbidden request", ErrorCodes.FORBIDDEN);
      }

      if (isPendingPromise(promise)) {
        this.promises[id] = JSON.stringify({
          state: "REJECTED_CANCELED",
          id: promise.id,
          timeout: promise.timeout,
          param: {
            headers: promise.param.headers,
            data: promise.param.data ? this.encode(promise.param.data) : undefined,
          },
          value: {
            headers: headers,
            data: data ? this.encode(data) : undefined,
          },
          createdOn: promise.createdOn,
          completedOn: tick,
          idempotencyKeyForCreate: promise.idempotencyKeyForCreate,
          idempotencyKeyForComplete: ikey,
          tags: promise.tags,
        });

        return this.promiseDecoder(isCanceledPromise)(JSON.parse(this.promises[id]));
      }

      if (promise.idempotencyKeyForComplete === undefined || ikey != promise.idempotencyKeyForComplete) {
        throw new ResonateError("Forbidden request", ErrorCodes.FORBIDDEN);
      }

      return promise;
    } else {
      throw new ResonateError("Not found", ErrorCodes.NOT_FOUND);
    }
  }

  async get(id: string): Promise<DurablePromise> {
    const tick = Date.now();
    this.transition(tick);

    if (id in this.promises) {
      return this.promiseDecoder(isDurablePromise)(JSON.parse(this.promises[id]));
    }

    throw new ResonateError("Not found", ErrorCodes.NOT_FOUND);
  }

  transition(tick: number) {
    for (const id in this.promises) {
      const promise = this.promiseDecoder(isDurablePromise)(JSON.parse(this.promises[id]));

      if (isPendingPromise(promise) && promise.timeout <= tick) {
        this.promises[id] = JSON.stringify({
          state: "REJECTED_TIMEDOUT",
          id: promise.id,
          timeout: promise.timeout,
          param: {
            headers: promise.param.headers,
            data: promise.param.data ? this.encode(promise.param.data) : undefined,
          },
          value: undefined,
          createdOn: promise.createdOn,
          completedOn: promise.timeout,
          idempotencyKeyForCreate: promise.idempotencyKeyForCreate,
          idempotencyKeyForComplete: promise.idempotencyKeyForComplete,
          tags: promise.tags,
        });
      }
    }
  }

  private encode(value: string): string {
    try {
      return this.encoder.encode(value);
    } catch (e: unknown) {
      throw new ResonateError("Encode error", ErrorCodes.ENCODER, e);
    }
  }

  private decode(value: string): string {
    try {
      return this.encoder.decode(value);
    } catch (e: unknown) {
      throw new ResonateError("Decode error", ErrorCodes.ENCODER, e);
    }
  }

  private promiseDecoder<T extends DurablePromise>(guard: (p: unknown) => p is T): (p: unknown) => T {
    return (p: unknown) => {
      if (!guard(p)) {
        throw new ResonateError("Invalid response", ErrorCodes.PAYLOAD, p);
      }

      if (p.param?.data) {
        p.param.data = this.decode(p.param.data);
      }

      if (p.value?.data) {
        p.value.data = this.decode(p.value.data);
      }

      return p;
    };
  }
}
