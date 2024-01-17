import { Context } from "../resonate";

export interface IRetry {
  next(ctx: Context): { done: boolean; delay?: number };
  iterator(ctx: Context): IterableIterator<number>;
}

export class IterableRetry implements IRetry {
  next(ctx: Context): { done: boolean; delay?: number } {
    throw new Error("Method not implemented");
  }

  iterator(ctx: Context): IterableIterator<number> {
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
