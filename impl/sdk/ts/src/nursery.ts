export class Nursery {
  private q: Array<() => void> = [];
  private f: () => void;
  private c: (e: any, r: any) => void;
  private e?: any;
  private r?: any;

  private running = false;
  private holds = 0;

  constructor(f: (n: Nursery) => void, c: (e: any, r: any) => void) {
    this.f = () => f(this);
    this.c = c;

    // kick off the nursery
    this.enqueue(this.f);
  }

  get completed() {
    return this.e || this.r;
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

  done(err?: any, res?: any) {
    if (!this.running || this.completed) return;
    this.running = false;

    if (err || res) {
      this.e = err;
      this.r = res;
      this.complete();
    }
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
    this.c(this.e, this.r);
  }
}
