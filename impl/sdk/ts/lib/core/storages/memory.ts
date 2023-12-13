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
}
