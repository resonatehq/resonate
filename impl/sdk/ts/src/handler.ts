interface DurablePromise<T> {
  uuid: string;
  state: "pending" | "completed";
  value?: T;
}

export class Handler {
  private promises: Record<string, DurablePromise<any>> = {};

  constructor(initialPromises?: DurablePromise<any>[]) {
    if (initialPromises) {
      for (const p of initialPromises) {
        this.promises[p.uuid] = p;
      }
    }
  }

  public eCreatePromise<T>(uuid: string, callback: (promise: DurablePromise<T>) => void): void {
    if (!this.promises[uuid]) {
      this.promises[uuid] = {
        uuid,
        state: "pending",
      };
    }

    callback(this.promises[uuid]);
  }

  public createPromise<T>(uuid: string): DurablePromise<T> {
    if (!this.promises[uuid]) {
      this.promises[uuid] = {
        uuid,
        state: "pending",
      };
    }

    return this.promises[uuid];
  }

  public resolvePromise<T>(uuid: string, value: T): DurablePromise<T> {
    const promise = this.promises[uuid];

    if (promise.state === "pending") {
      promise.value = value;
      promise.state = "completed";
    }

    return promise;
  }
}
