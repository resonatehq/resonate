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

  public createPromise<T>(uuid: string, callback: (res: DurablePromise<T>) => void): void {
    if (!this.promises[uuid]) {
      this.promises[uuid] = {
        uuid,
        state: "pending",
      };
    }

    callback(this.promises[uuid]);
  }

  public resolvePromise<T>(uuid: string, value: T, callback: (res: DurablePromise<T>) => void): void {
    const promise = this.promises[uuid];

    if (promise.state === "pending") {
      promise.value = value;
      promise.state = "completed";
    }

    callback(promise);
  }
}
