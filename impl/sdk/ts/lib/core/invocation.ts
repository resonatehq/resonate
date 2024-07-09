import { Future } from "./future";
import { Options } from "./options";
import { RetryPolicy, exponential } from "./retry";

/////////////////////////////////////////////////////////////////////
// Invocation
/////////////////////////////////////////////////////////////////////

export class Invocation<T> {
  future: Future<T>;
  resolve: (v: T) => void;
  reject: (e: unknown) => void;

  eid: string;
  idempotencyKey: string;
  timeout: number;

  killed: boolean = false;

  createdOn: number = Date.now();
  counter: number = 0;
  attempt: number = 0;
  retryPolicy: RetryPolicy = exponential();

  awaited: Future<any>[] = [];
  blocked: Future<any> | null = null;

  readonly root: Invocation<any>;
  readonly parent: Invocation<any> | null;
  readonly children: Invocation<any>[] = [];

  /**
   * Represents a Resonate function invocation.
   *
   * @constructor
   * @param id - A unique id for this invocation.
   * @param idempotencyKey - An idempotency key used to deduplicate invocations.
   * @param opts - The invocation options.
   * @param parent - The parent invocation.
   */
  constructor(
    public readonly name: string,
    public readonly id: string,
    public readonly headers: Record<string, string> | undefined,
    public readonly param: unknown,
    public readonly opts: Options,
    parent?: Invocation<any>,
  ) {
    // create a future and hold on to its resolvers
    const { future, resolve, reject } = Future.deferred<T>(this);
    this.future = future;
    this.resolve = resolve;
    this.reject = reject;

    this.root = parent?.root ?? this;
    this.parent = parent ?? null;

    // get the execution id from either:
    // - a hard coded string
    // - a function that returns a string given the invocation id
    this.eid = this.opts.eidFn(this.id);

    // get the idempotency key from either:
    // - a hard coded string
    // - a function that returns a string given the invocation id
    this.idempotencyKey = this.opts.idempotencyKeyFn(this.id);

    // the timeout is the minimum of:
    // - the current time plus the user provided relative time
    // - the parent timeout
    this.timeout = Math.min(this.createdOn + this.opts.timeout, this.parent?.timeout ?? Infinity);
    this.retryPolicy = this.opts.retryPolicy;
  }

  addChild(child: Invocation<any>) {
    this.children.push(child);
  }

  await(future: Future<any>) {
    this.awaited.push(future);
  }

  block(future: Future<any>) {
    this.blocked = future;
  }

  unblock() {
    this.blocked = null;
  }
}
