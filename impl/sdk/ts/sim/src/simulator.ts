declare const document: any;

export class Random {
  // Internal state of the random number generator (RNG)
  private state: number;

  constructor(seed: number) {
    // Ensure the seed is treated as an unsigned 32-bit integer
    this.state = seed >>> 0;
  }

  random(bound: number): number {
    return this.next() * bound;
  }

  next(): number {
    // Update the internal state using the LCG formula
    this.state = (1664525 * this.state + 1013904223) >>> 0;

    // Convert the 32-bit integer to a float in the range [0, 1)
    return this.state / 0x100000000; // Equivalent to 2^32
  }

  pick<T>(list: T[]): T | undefined {
    return list[Math.floor(this.next() * list.length)];
  }

  randint(min: number, max: number): number {
    return Math.floor(this.next() * (max - min + 1)) + min;
  }
}

export type Address = { kind: "unicast"; iaddr: string } | { kind: "anycast"; gaddr: string; iaddr?: string };
export function unicast(iaddr: string): Address {
  return { kind: "unicast", iaddr };
}
export function anycast(gaddr: string, iaddr?: string): Address {
  return { kind: "anycast", gaddr, iaddr };
}

export class Message<T> {
  constructor(
    public source: Address,
    public target: Address,
    public data: T,
    public head: Record<string, any> = {},
  ) {}
  isRequest(): boolean {
    return this.head.requ;
  }

  isResponse(): boolean {
    return this.head.resp;
  }
  resp<U>(data: U) {
    return new Message(this.target, this.source, data, {
      resp: this.head.requ,
      correlationId: this.head.correlationId,
    });
  }
}

export class Process {
  public active = true;

  constructor(
    public readonly iaddr: string,
    public readonly gaddr?: string,
  ) {}

  tick(time: number, messages: Message<any>[]): Message<any>[] {
    throw new Error("not implemented");
  }

  log(time: number, ...args: any[]): void {
    const message = `[tick: ${time}] [proc: ${this.iaddr}] ${args.map(JSON.stringify as any)}`;

    // Always log to console
    console.log(message);

    // Only append to the DOM if running in the browser
    if (typeof document !== "undefined") {
      const logEl = document.createElement("div");
      logEl.textContent = message;
      document.body.appendChild(logEl);
    }
  }
}

export interface DeliveryOptions {
  dropProb?: number;
  randomDelay?: number;
  duplProb?: number;
  deactivateProb?: number;
  activateProb?: number;
}

export class Simulator {
  private prng: Random;
  public time = 0;
  private init = false;
  private process: Process[] = [];
  private network: Message<any>[] = [];
  public outbox: Message<any>[] = [];
  public deliveryOptions: Required<DeliveryOptions>;
  private scheduledRepeat: { interval: number; fn: () => void }[] = [];
  private scheduledDelay: { runAt: number; fn: () => void }[] = [];

  constructor(
    prng: Random,
    { dropProb = 0, randomDelay = 0, duplProb = 0, deactivateProb = 0, activateProb = 0 }: DeliveryOptions = {},
  ) {
    this.prng = prng;
    this.deliveryOptions = { dropProb, randomDelay, duplProb, deactivateProb, activateProb };
  }

  addMessage(message: Message<any>): void {
    this.network.push(message);
  }

  assertAlways(cond: boolean, msg: string): void {
    if (cond) return; // Early return if assertion passes

    console.assert(cond, "Assertion Failed: %s", msg);
    console.trace();

    if (typeof process !== "undefined" && process.versions.node) {
      process.exit(1);
    }
  }

  register(process: Process): void {
    this.process.push(process);
  }

  more(): boolean {
    // Simulator can make progress if it is not initialized or if there are messages in the network
    return !this.init || this.network.length > 0;
  }

  send(message: Message<any>): void {
    this.network.push(message);
  }

  tick(): void {
    if (!this.init) {
      this.init = true;
    }

    this.time += 1;

    const { deactivateProb, activateProb } = this.deliveryOptions;
    if ((deactivateProb > 0 || activateProb > 0) && this.process.length > 0) {
      for (const proc of this.process) {
        if (proc.active) {
          if (this.prng.next() < deactivateProb) {
            proc.active = false;
          }
        } else {
          if (this.prng.next() < activateProb) {
            proc.active = true;
          }
        }
      }
    }

    const retained: Message<any>[] = [];
    const consumed: Message<any>[] = [];

    for (const message of this.network) {
      // Drop?
      if (this.prng.next() < this.deliveryOptions.dropProb) {
        continue;
      }
      // Delay?
      if (this.prng.next() < this.deliveryOptions.randomDelay) {
        retained.push(message);
        continue;
      }
      // Deliver now
      consumed.push(message);
      // Duplicate?
      if (this.prng.next() < this.deliveryOptions.duplProb) {
        retained.push(message);
      }
    }

    const inboxes: Record<string, Message<any>[]> = {};
    for (const process of this.process) {
      inboxes[process.iaddr] = [];
    }
    for (const message of consumed) {
      const target = message.target;
      if (target.kind === "unicast") {
        if (target.iaddr in inboxes) {
          inboxes[target.iaddr].push(message);
        }
      } else if (target.kind === "anycast") {
        const preference = this.process.find((p) => p.active && p.iaddr === target.iaddr);
        if (preference) {
          inboxes[preference.iaddr].push(message);
        } else {
          const process = this.prng.pick(this.process.filter((p) => p.active && target.gaddr === p.gaddr));
          if (process) {
            inboxes[process.iaddr].push(message);
          }
        }
      } else {
        throw new Error("unknown target.kind");
      }
    }
    const newMessages: Message<any>[] = [];
    for (const process of this.process) {
      if (!process.active) {
        continue;
      }
      for (const msg of process.tick(this.time, inboxes[process.iaddr])) {
        if (msg.target.kind === "unicast" && msg.target.iaddr === "environment") {
          this.outbox.push(msg);
        } else {
          newMessages.push(msg);
        }
      }
    }

    this.network = retained.concat(newMessages);
  }

  exec(steps: number): void {
    let i = 0;
    while (i < steps) {
      for (const task of this.scheduledRepeat) {
        if (this.time % task.interval === 0) {
          task.fn();
        }
      }

      if (this.scheduledDelay.length > 0) {
        const remaining: { runAt: number; fn: () => void }[] = [];
        for (const task of this.scheduledDelay) {
          if (task.runAt === this.time) {
            task.fn();
          } else if (task.runAt > this.time) {
            remaining.push(task);
          }
        }
        this.scheduledDelay = remaining;
      }

      this.tick();

      i++;
    }
  }

  repeat(interval: number, fn: () => void): void {
    this.scheduledRepeat.push({ interval, fn });
  }
  delay(runAt: number, fn: () => void): void {
    this.scheduledDelay.push({ runAt, fn });
  }
}
