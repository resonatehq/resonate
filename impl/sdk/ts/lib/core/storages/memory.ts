import { IStorage } from "../storage";
import { DurablePromise } from "../promise";

export class MemoryStorage implements IStorage {
  private promises: Record<string, DurablePromise> = {};

  constructor() {}

  async rmw<P extends DurablePromise | undefined>(
    id: string,
    f: (promise: DurablePromise | undefined) => P,
  ): Promise<P> {
    const promise = f(this.promises[id]);
    if (promise) {
      this.promises[id] = promise;
    }

    return promise;
  }

  async *search(
    id: string,
    state: string | undefined,
    tags: Record<string, string> | undefined,
    limit: number | undefined,
  ): AsyncGenerator<DurablePromise[], void> {
    // for now WithTimeout will implement
    // search logic
    yield Object.values(this.promises);
  }
}
