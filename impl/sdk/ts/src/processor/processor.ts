import type { InnerContext } from "../context";
import type { Span } from "../tracer";
import type { Result } from "../types";

type F = () => Promise<unknown>;

export interface Processor {
  process(
    id: string,
    ctx: InnerContext,
    func: F,
    done: (result: Result<unknown>) => void,
    verbose: boolean,
    span: Span,
  ): void;
}

export class AsyncProcessor implements Processor {
  async process<T>(
    id: string,
    ctx: InnerContext,
    func: () => Promise<T>,
    done: (result: Result<T>) => void,
    verbose: boolean,
    span: Span,
  ) {
    while (true) {
      let retryIn: number | null = null;
      const childSpan = span.startSpan(`${id}::${ctx.info.attempt}`, ctx.clock.now());
      childSpan.setAttribute("attempt", ctx.info.attempt);

      try {
        const data = await func();
        childSpan.setStatus(true);
        done({ success: true, value: data });
        return;
      } catch (error) {
        childSpan.setStatus(false, String(error));

        retryIn = ctx.retryPolicy.next(ctx.info.attempt);
        if (retryIn === null) {
          done({ success: false, error });
          return;
        }

        // Use the same clock sourced from ctx for consistency
        if (ctx.clock.now() + retryIn >= ctx.info.timeout) {
          done({ success: false, error });
          return;
        }

        console.warn(
          `Runtime. Function '${ctx.func}' failed with '${String(error)}' (retrying in ${retryIn / 1000} secs)`,
        );
        if (verbose) {
          console.warn(error);
        }
      } finally {
        childSpan.end(ctx.clock.now());
      }

      // Ensure a numeric delay for setTimeout; guard against null just in case
      await new Promise((resolve) => setTimeout(resolve, retryIn ?? 0));
      ctx.info.attempt++;
    }
  }
}
