export class Nursery<E, R> {
  private q: Array<() => void> = [];
  private f: () => void;
  private c: (e: E, r: R) => void;
  private e?: E;
  private r?: R;

  private holds = 0;
  private running = false;
  private completed = false;

  constructor(f: (n: Nursery<E, R>) => void, c: (e: any, r: any) => void) {
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
  }

  done(err: E, res?: R) {
    if (!this.running || this.completed) return;

    this.e = err;
    this.r = res;
    this.running = false;
    this.completed = true;
    this.complete();
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
    this.c(this.e!, this.r!);
  }
}
