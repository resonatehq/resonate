type F = () => Promise<unknown>;

type Ok<T> = {
  success: true;
  data: T;
};

type Ko = {
  success: false;
  error: any;
};

export type Result<T> = Ok<T> | Ko;

export interface Processor {
  process(id: string, fn: F, cb: (result: Result<unknown>) => void): void;
}

export class AsyncProcessor implements Processor {
  private seen = new Set<string>();

  process(id: string, fn: F, cb: (result: Result<unknown>) => void): void {
    // If already seen, ignore, either we are already working on it, or it was already completed
    if (this.seen.has(id)) {
      return;
    }

    this.seen.add(id);

    fn()
      .then((data) => {
        const result: Result<unknown> = { success: true, data };
        cb(result);
      })
      .catch((error) => {
        const result: Result<unknown> = { success: false, error };
        cb(result);
      });
  }
}
