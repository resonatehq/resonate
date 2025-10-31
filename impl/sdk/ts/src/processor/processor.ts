import type { InnerContext } from "../context";
import type { Result } from "../types";

type F = () => Promise<unknown>;

export interface Processor {
  process(
    id: string,
    ctx: InnerContext,
    name: string,
    func: F,
    done: (result: Result<unknown>) => void,
    verbose: boolean,
  ): void;
}

export class AsyncProcessor implements Processor {
  process<T>(
    id: string,
    ctx: InnerContext,
    name: string,
    func: () => Promise<T>,
    done: (result: Result<T>) => void,
    verbose: boolean,
  ): void {
    this.run(id, ctx, name, func, done, verbose);
  }

  private async run<T>(
    id: string,
    ctx: InnerContext,
    name: string,
    func: () => Promise<T>,
    done: (result: Result<T>) => void,
    verbose: boolean,
  ) {
    while (true) {
      try {
        const data = await func();
        done({ success: true, value: data });
        return;
      } catch (error) {
        const retryIn = ctx.retryPolicy.next(ctx.info.attempt);
        if (retryIn === null) {
          done({ success: false, error });
          return;
        }

        if (Date.now() + retryIn >= ctx.info.timeout) {
          done({ success: false, error });
          return;
        }

        console.warn(`Runtime. Function '${name}' failed with '${String(error)}' (retrying in ${retryIn / 1000} secs)`);
        if (verbose) {
          console.warn(error);
        }

        await new Promise((resolve) => setTimeout(resolve, retryIn));
        ctx.info.attempt++;
      }
    }
  }
}
