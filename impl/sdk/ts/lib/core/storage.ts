import { DurablePromise, TimedoutPromise, isPendingPromise } from "./promise";

export interface IStorage {
  rmw<P extends DurablePromise | undefined>(id: string, f: (promise: DurablePromise | undefined) => P): Promise<P>;
  search(
    id: string,
    state: string | undefined,
    tags: Record<string, string> | undefined,
    limit: number | undefined,
  ): AsyncGenerator<DurablePromise[], void>;
}

export class WithTimeout implements IStorage {
  constructor(private storage: IStorage) {}

  rmw<P extends DurablePromise | undefined>(id: string, f: (promise: DurablePromise | undefined) => P): Promise<P> {
    return this.storage.rmw(id, (promise) => f(timeout(promise)));
  }

  async *search(
    id: string,
    state: string | undefined,
    tags: Record<string, string> | undefined,
    limit: number | undefined,
  ): AsyncGenerator<DurablePromise[], void, unknown> {
    const regex = new RegExp(id.replaceAll("*", ".*"));
    const tagEntries = Object.entries(tags ?? {});

    let states: string[] = [];
    if (state?.toLowerCase() == "pending") {
      states = ["PENDING"];
    } else if (state?.toLowerCase() == "resolved") {
      states = ["RESOLVED"];
    } else if (state?.toLowerCase() == "rejected") {
      states = ["REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"];
    } else {
      states = ["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"];
    }

    for await (const res of this.storage.search("*", undefined, undefined, limit)) {
      yield res
        .map(timeout)
        .filter((promise) => regex.test(promise.id))
        .filter((promise) => states.includes(promise.state))
        .filter((promise) => tagEntries.every(([k, v]) => promise.tags?.[k] == v));
    }
  }
}

function timeout<P extends DurablePromise | undefined>(promise: P): P | TimedoutPromise {
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
