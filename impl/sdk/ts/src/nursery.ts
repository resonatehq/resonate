import type { Result } from "./types";
import * as util from "./util";
export class Nursery<T> {
  // event queue, these functions are ensured to execute sequentially
  private q: Array<() => void> = [];

  // the function the nursery is instantiated with, added to the
  // queue when holds are released
  private f: () => void;

  // the callback the nursery is instantiated with, called once when
  // the nursery is done and all holds are released
  private c: (res: Result<T, any>) => void;
  private res?: Result<T, any>;

  private holds = 0;
  private running = false;
  private completed = false;

  constructor(f: (n: Nursery<T>) => void, c: (res: Result<T, any>) => void) {
    this.f = () => f(this);
    this.c = c;

    // kick off the nursery
    this.enqueue(this.f);
  }

  hold(f: (f: () => void) => void) {
    if (!this.running || this.completed) return;
    this.holds++;

    f(() => {
      this.holds--;

      if (this.completed) {
        this.complete();
      } else {
        this.enqueue(this.f);
      }
    });
  }

  cont() {
    this.running = false;

    if (this.completed) {
      this.complete();
    } else {
      this.execute();
    }
  }

  done(res: Result<T, any>) {
    if (this.completed) return;
    this.res = res;
    this.running = false;
    this.completed = true;
    this.complete();
  }

  all<T, U>(
    list: T[],
    func: (item: T, done: (res: Result<U, any>) => void) => void,
    done: (res: Result<U[], any>) => void,
  ) {
    const results: U[] = new Array(list.length);

    let remaining = list.length;
    let completed = false;

    const finalize = (err?: any) => {
      if (completed) return;
      completed = true;
      if (err) {
        done({ kind: "error", error: err });
      } else {
        done({ kind: "value", value: results });
      }
    };

    list.forEach((item, index) => {
      func(item, (res) => {
        if (completed) return;
        if (res.kind === "error") return finalize(res.error);

        results[index] = res.value;
        remaining--;

        if (remaining === 0) {
          finalize();
        }
      });
    });
  }

  private enqueue(f: () => void) {
    this.q.push(f);
    this.execute();
  }

  private execute() {
    if (this.running) return;

    const f = this.q.shift();
    if (f) {
      this.running = true;
      f();
    }
  }

  private complete() {
    if (!this.completed || this.holds > 0) return;
    util.assertDefined(this.res);
    this.c(this.res);
  }
}
