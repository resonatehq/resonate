import type { RetryPolicy } from "../retries";
import type { Result } from "../types";

type F = () => Promise<unknown>;

export interface Processor {
  process(
    id: string,
    name: string,
    func: F,
    cb: (result: Result<unknown>) => void,
    retryPolicy: RetryPolicy,
    timeout: number,
  ): void;
}

export class AsyncProcessor implements Processor {
  process<T>(
    id: string,
    name: string,
    func: () => Promise<T>,
    cb: (result: Result<T>) => void,
    retryPolicy: RetryPolicy,
    timeout: number,
  ): void {
    void this.run(id, name, func, cb, retryPolicy, timeout);
  }

  private async run<T>(
    id: string,
    name: string,
    func: () => Promise<T>,
    cb: (result: Result<T>) => void,
    retryPolicy: RetryPolicy,
    timeout: number,
  ) {
    let attempt = 1;

    while (true) {
      try {
        const data = await func();
        cb({ success: true, value: data });
        return;
      } catch (error) {
        const retryIn = retryPolicy.next(attempt);
        if (retryIn === null) {
          cb({ success: false, error });
          return;
        }

        const now = Date.now();

        if (now + retryIn >= timeout) {
          cb({ success: false, error });
          return;
        }

        console.log(
          `RuntimeError. Function ${name} failed with '${String(error)}' (retrying in ${retryIn / 1000} secs)`,
        );

        await new Promise((resolve) => setTimeout(resolve, retryIn));
        attempt++;
      }
    }
  }
}
