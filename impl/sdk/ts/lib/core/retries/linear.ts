import { Context } from "../../resonate";
import { IRetry, IterableRetry } from "../retry";

export class LinearRetry extends IterableRetry implements IRetry {
  constructor(
    private delay: number,
    private maxAttempts: number = Infinity,
  ) {
    super();
  }

  next(ctx: Context): { done: boolean; delay?: number } {
    // attempt 0: 0ms delay
    // attampt n: {delay}ms delay
    const delay = Math.min(ctx.attempt, 1) * this.delay;

    if (Date.now() + delay >= ctx.timeout || ctx.attempt >= this.maxAttempts) {
      return { done: true };
    }

    return {
      done: false,
      delay: delay,
    };
  }
}
