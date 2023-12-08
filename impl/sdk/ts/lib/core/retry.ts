import { Context } from "../resonate";

export interface IRetry {
  next(context: Context): { done: boolean; delay?: number };
}
