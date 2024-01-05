import {
  DurablePromise,
  PendingPromise,
  ResolvedPromise,
  RejectedPromise,
  CanceledPromise,
  TimedoutPromise,
  isPendingPromise,
  isResolvedPromise,
  isRejectedPromise,
  isCanceledPromise,
} from "../promise";
import { IPromiseStore } from "../store";
import { ErrorCodes, ResonateError } from "../error";
import { IStorage, WithTimeout } from "../storage";
import { MemoryStorage } from "../storages/memory";

export class LocalPromiseStore implements IPromiseStore {
  private storage: IStorage;

  constructor(storage: IStorage = new MemoryStorage()) {
    this.storage = new WithTimeout(storage);
  }

  async create(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
    timeout: number,
    tags: Record<string, string> | undefined,
  ): Promise<PendingPromise | ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    return this.storage.rmw(id, (promise) => {
      if (promise) {
        if (strict && !isPendingPromise(promise)) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        if (promise.idempotencyKeyForCreate === undefined || ikey != promise.idempotencyKeyForCreate) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        return promise;
      } else {
        return {
          state: "PENDING",
          id: id,
          timeout: timeout,
          param: {
            headers: headers,
            data: data,
          },
          value: {
            headers: undefined,
            data: undefined,
          },
          createdOn: Date.now(),
          completedOn: undefined,
          idempotencyKeyForCreate: ikey,
          idempotencyKeyForComplete: undefined,
          tags: tags,
        };
      }
    });
  }

  async resolve(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    return this.storage.rmw(id, (promise) => {
      if (promise) {
        if (strict && !isPendingPromise(promise) && !isResolvedPromise(promise)) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        if (isPendingPromise(promise)) {
          return {
            state: "RESOLVED",
            id: promise.id,
            timeout: promise.timeout,
            param: promise.param,
            value: {
              headers: headers,
              data: data,
            },
            createdOn: promise.createdOn,
            completedOn: Date.now(),
            idempotencyKeyForCreate: promise.idempotencyKeyForCreate,
            idempotencyKeyForComplete: ikey,
            tags: promise.tags,
          };
        }

        if (promise.idempotencyKeyForComplete === undefined || ikey != promise.idempotencyKeyForComplete) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        return promise;
      } else {
        throw new ResonateError(ErrorCodes.NOT_FOUND, "Not found");
      }
    });
  }

  async reject(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    return this.storage.rmw(id, (promise) => {
      if (promise) {
        if (strict && !isPendingPromise(promise) && !isRejectedPromise(promise)) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        if (isPendingPromise(promise)) {
          return {
            state: "REJECTED",
            id: promise.id,
            timeout: promise.timeout,
            param: promise.param,
            value: {
              headers: headers,
              data: data,
            },
            createdOn: promise.createdOn,
            completedOn: Date.now(),
            idempotencyKeyForCreate: promise.idempotencyKeyForCreate,
            idempotencyKeyForComplete: ikey,
            tags: promise.tags,
          };
        }

        if (promise.idempotencyKeyForComplete === undefined || ikey != promise.idempotencyKeyForComplete) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        return promise;
      } else {
        throw new ResonateError(ErrorCodes.NOT_FOUND, "Not found");
      }
    });
  }

  async cancel(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    return this.storage.rmw(id, (promise) => {
      if (promise) {
        if (strict && !isPendingPromise(promise) && !isCanceledPromise(promise)) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        if (isPendingPromise(promise)) {
          return {
            state: "REJECTED_CANCELED",
            id: promise.id,
            timeout: promise.timeout,
            param: promise.param,
            value: {
              headers: headers,
              data: data,
            },
            createdOn: promise.createdOn,
            completedOn: Date.now(),
            idempotencyKeyForCreate: promise.idempotencyKeyForCreate,
            idempotencyKeyForComplete: ikey,
            tags: promise.tags,
          };
        }

        if (promise.idempotencyKeyForComplete === undefined || ikey != promise.idempotencyKeyForComplete) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        return promise;
      } else {
        throw new ResonateError(ErrorCodes.NOT_FOUND, "Not found");
      }
    });
  }

  async get(id: string): Promise<DurablePromise> {
    const promise = await this.storage.rmw(id, (p) => p);

    if (promise) {
      return promise;
    }

    throw new ResonateError(ErrorCodes.NOT_FOUND, "Not found");
  }

  search(
    id: string,
    state: string | undefined,
    tags: Record<string, string> | undefined,
    limit: number | undefined,
  ): AsyncGenerator<DurablePromise[], void> {
    return this.storage.search(id, state, tags, limit);
  }
}
