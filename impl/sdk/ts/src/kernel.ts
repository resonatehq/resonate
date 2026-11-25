// The synchronous kernel.
//
// Core is written as fibers: plain generators that never await. A fiber
// yields an Op describing what it needs from the outside world and is resumed
// with the answer. The kernel steps fibers synchronously and turns every
// external need into a Submission; the answer comes back later as a
// Completion:
//
//   Kernel : State × Completion → State × Submission*
//
// Nothing in here performs I/O, reads a clock or schedules a microtask. Who
// performs the submissions, and in which order their completions are fed
// back, is decided by a driver: `drive` below for production, the
// deterministic simulator (sim/dst) for testing.

import type { InnerContext } from "./context.js";
import type { Request, Response } from "./network/types.js";
import type { Func, Send } from "./types.js";

/** What a fiber can ask for. */
export type Op =
  /** Send a request to the server; resumes with the response. */
  | { op: "send"; req: Request }
  /** Run a leaf function (one attempt); resumes with its return value or throws its error. */
  | { op: "invoke"; func: Func; ctx: InnerContext; args: any[] }
  /** Wait `ms` milliseconds; resumes with undefined. */
  | { op: "sleep"; ms: number }
  /** Start a child fiber eagerly; resumes immediately with a handle. */
  | { op: "fork"; fiber: Fiber<any> }
  /** Wait for a forked child; resumes with its return value or throws its error. */
  | { op: "join"; handle: number };

export type Fiber<T> = Generator<Op, T, any>;

/** An external need, identified so its completion can find the fiber that waits for it. */
export type Submission =
  | { id: number; op: "send"; req: Request }
  | { id: number; op: "invoke"; func: Func; ctx: InnerContext; args: any[] }
  | { id: number; op: "sleep"; ms: number };

export type Outcome<T = any> = { ok: true; value: T } | { ok: false; error: unknown };

export type Completion = { id: number } & Outcome;

class Node {
  outcome?: Outcome;
  joiner?: Node;
  handle?: number;

  constructor(
    readonly gen: Fiber<any>,
    readonly onDone?: (o: Outcome) => void,
  ) {}
}

export class Kernel {
  private next = 1;
  private readonly waiting = new Map<number, Node>();
  private readonly handles = new Map<number, Node>();
  private out: Submission[] = [];

  /** Start a fiber and run it until it blocks. Returns what it (and anything it forked) submitted. */
  spawn<T>(fiber: Fiber<T>, onDone?: (o: Outcome<T>) => void): Submission[] {
    this.resume(new Node(fiber, onDone as (o: Outcome) => void), { ok: true, value: undefined });
    return this.flush();
  }

  /** Feed one completion back and run until every fiber blocks again. Unknown ids are ignored. */
  complete(c: Completion): Submission[] {
    const node = this.waiting.get(c.id);
    if (!node) return [];
    this.waiting.delete(c.id);
    this.resume(node, c.ok ? { ok: true, value: c.value } : { ok: false, error: c.error });
    return this.flush();
  }

  /** Submissions that have not been completed yet. */
  get outstanding(): number {
    return this.waiting.size;
  }

  private flush(): Submission[] {
    const out = this.out;
    this.out = [];
    return out;
  }

  private resume(node: Node, input: Outcome): void {
    let next = input;
    while (true) {
      let r: IteratorResult<Op, any>;
      try {
        r = next.ok ? node.gen.next(next.value) : node.gen.throw(next.error);
      } catch (error) {
        this.finish(node, { ok: false, error });
        return;
      }
      if (r.done) {
        this.finish(node, { ok: true, value: r.value });
        return;
      }
      const op = r.value;
      switch (op.op) {
        case "fork": {
          const child = new Node(op.fiber);
          const handle = this.next++;
          child.handle = handle;
          this.handles.set(handle, child);
          this.resume(child, { ok: true, value: undefined });
          next = { ok: true, value: handle };
          continue;
        }
        case "join": {
          const child = this.handles.get(op.handle);
          if (!child) {
            next = { ok: false, error: new Error(`join on unknown fiber ${op.handle}`) };
            continue;
          }
          if (child.outcome) {
            this.handles.delete(op.handle);
            next = child.outcome;
            continue;
          }
          child.joiner = node;
          return;
        }
        default: {
          const id = this.next++;
          this.waiting.set(id, node);
          this.out.push({ ...op, id } as Submission);
          return;
        }
      }
    }
  }

  private finish(node: Node, outcome: Outcome): void {
    node.outcome = outcome;
    node.onDone?.(outcome);
    const joiner = node.joiner;
    if (joiner) {
      node.joiner = undefined;
      if (node.handle !== undefined) this.handles.delete(node.handle);
      this.resume(joiner, outcome);
    }
  }
}

export type IO = { send: Send };

/**
 * The production driver: runs a fiber to completion by performing its
 * submissions for real and feeding each completion back as it arrives.
 * Submissions run concurrently; the kernel itself only ever sees one
 * completion at a time.
 */
export function drive<T>(fiber: Fiber<T>, io: IO): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const kernel = new Kernel();
    const feed = (c: Completion) => perform(kernel.complete(c));
    const settle = (id: number, p: Promise<unknown>) =>
      p.then(
        (value) => feed({ id, ok: true, value }),
        (error) => feed({ id, ok: false, error }),
      );
    const perform = (subs: Submission[]) => {
      for (const s of subs) {
        switch (s.op) {
          case "send":
            settle(s.id, io.send(s.req as any) as Promise<Response>);
            break;
          case "invoke":
            settle(s.id, (async () => s.func(s.ctx, ...s.args))());
            break;
          case "sleep":
            setTimeout(() => feed({ id: s.id, ok: true, value: undefined }), s.ms);
            break;
        }
      }
    };
    // Start on a microtask, like the first await of an async function would.
    Promise.resolve().then(() => perform(kernel.spawn(fiber, (o) => (o.ok ? resolve(o.value) : reject(o.error)))));
  });
}
