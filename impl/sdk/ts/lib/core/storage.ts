import { DurablePromise, isPendingPromise } from "./promise";

export interface IStorage {
  rmw<P extends DurablePromise | undefined>(id: string, f: (promise: DurablePromise | undefined) => P): Promise<P>;
}

export class WithTimeout implements IStorage {
  constructor(private storage: IStorage) {}

  rmw<P extends DurablePromise | undefined>(id: string, f: (promise: DurablePromise | undefined) => P): Promise<P> {
    return this.storage.rmw(id, (promise) => {
      if (promise && isPendingPromise(promise) && Date.now() >= promise.timeout) {
        promise = {
          state: "REJECTED_TIMEDOUT",
          id: promise.id,
          timeout: promise.timeout,
          param: promise.param,
          value: {
            headers: undefined,
            data: undefined,
          },
          createdOn: promise.createdOn,
          completedOn: promise.timeout,
          idempotencyKeyForCreate: promise.idempotencyKeyForCreate,
          idempotencyKeyForComplete: undefined,
          tags: promise.tags,
        };
      }

      return f(promise);
    });
  }
}
