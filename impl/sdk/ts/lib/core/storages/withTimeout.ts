import { DurablePromiseRecord, isPendingPromise } from "../promises/types";
import { IStorage } from "../storage";
import { MemoryStorage } from "./memory";

export class WithTimeout implements IStorage<DurablePromiseRecord> {
  constructor(private storage: IStorage<DurablePromiseRecord> = new MemoryStorage<DurablePromiseRecord>()) {}

  rmw<T extends DurablePromiseRecord | undefined>(
    id: string,
    func: (item: DurablePromiseRecord | undefined) => T,
  ): Promise<T> {
    return this.storage.rmw(id, (p) => func(p ? timeout(p) : undefined));
  }

  rmd(id: string, func: (item: DurablePromiseRecord) => boolean): Promise<boolean> {
    return this.storage.rmd(id, (p) => func(timeout(p)));
  }

  async *all(): AsyncGenerator<DurablePromiseRecord[], void> {
    for await (const promises of this.storage.all()) {
      yield promises.map(timeout);
    }
  }
}

function timeout<T extends DurablePromiseRecord>(promise: T): DurablePromiseRecord {
  if (isPendingPromise(promise) && Date.now() >= promise.timeout) {
    return {
      state: promise.tags?.["resonate:timeout"] === "true" ? "RESOLVED" : "REJECTED_TIMEDOUT",
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
