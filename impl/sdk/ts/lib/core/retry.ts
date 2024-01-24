export interface IRetry {
  next<T extends { attempt: number; timeout: number }>(ctx: T): { done: boolean; delay?: number };
  iterator<T extends { attempt: number; timeout: number }>(ctx: T): IterableIterator<number>;
}

export class IterableRetry implements IRetry {
  next<T extends { attempt: number; timeout: number }>(ctx: T): { done: boolean; delay?: number } {
    throw new Error("Method not implemented");
  }

  iterator<T extends { attempt: number; timeout: number }>(ctx: T): IterableIterator<number> {
    const self = this; // eslint-disable-line @typescript-eslint/no-this-alias

    return {
      next() {
        const { done, delay } = self.next(ctx);
        return { done, value: delay || 0 };
      },
      [Symbol.iterator]() {
        return this;
      },
    };
  }
}
