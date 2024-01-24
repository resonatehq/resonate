import { DurablePromise, TimedoutPromise, isPendingPromise } from "../promise";
import { IStorage } from "../storage";

export class WithTimeout implements IStorage<DurablePromise> {
  constructor(private storage: IStorage<DurablePromise>) {}

  rmw<T extends DurablePromise | undefined>(id: string, func: (item: DurablePromise | undefined) => T): Promise<T> {
    return this.storage.rmw(id, (p) => func(p ? timeout(p) : undefined));
  }

  rmd(id: string, func: (item: DurablePromise) => boolean): Promise<void> {
    return this.storage.rmd(id, (p) => func(timeout(p)));
  }

  async *all(): AsyncGenerator<DurablePromise[], void> {
    for await (const promises of this.storage.all()) {
      yield promises.map(timeout);
    }
  }
}

function timeout<T extends DurablePromise>(promise: T): T | TimedoutPromise {
  if (isPendingPromise(promise) && Date.now() >= promise.timeout) {
    return {
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

  return promise;
}
