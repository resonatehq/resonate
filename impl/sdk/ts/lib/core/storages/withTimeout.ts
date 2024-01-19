import { DurablePromise, TimedoutPromise, isPendingPromise, searchStates } from "../promise";
import { IPromiseStorage } from "../storage";

export class WithTimeout implements IPromiseStorage {
  constructor(private storage: IPromiseStorage) {}

  rmw<P extends DurablePromise | undefined>(id: string, f: (promise: DurablePromise | undefined) => P): Promise<P> {
    return this.storage.rmw(id, (promise) => f(promise ? timeout(promise) : undefined));
  }

  async *search(
    id: string,
    state: string | undefined,
    tags: Record<string, string> | undefined,
    limit: number | undefined,
  ): AsyncGenerator<DurablePromise[], void, unknown> {
    // search promises against all provided criteria except state,
    // then timeout any pending promises that have exceeded the
    // timout, and finally apply the state filter

    const states = searchStates(state);

    for await (const promises of this.storage.search(id, undefined, tags, limit)) {
      yield promises.map(timeout).filter((promise) => states.includes(promise.state));
    }
  }
}

function timeout<P extends DurablePromise>(promise: P): P | TimedoutPromise {
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
