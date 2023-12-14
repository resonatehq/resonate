import { Context } from "../../resonate";
import { IRetry } from "../retry";

export class LinearRetry implements IRetry {
  constructor(
    private delay: number,
    private maxAttempts: number = Infinity,
  ) {}

  next(context: Context): { done: boolean; delay?: number } {
    if (Date.now() >= context.timeout || context.attempt >= this.maxAttempts) {
      return { done: true };
    }

    return {
      done: false,
      delay: this.delay,
    };
  }
}
