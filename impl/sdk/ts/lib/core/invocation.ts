import { Future } from "./future";
import { Options, PartialOptions, isOptions } from "./options";

/////////////////////////////////////////////////////////////////////
// Invocation
/////////////////////////////////////////////////////////////////////

export class Invocation<T> {
  future: Future<T>;
  resolve: (v: T) => void;
  reject: (e: unknown) => void;

  killed: boolean = false;

  createdAt: number = Date.now();
  counter: number = 0;
  attempt: number = 0;

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
    public readonly idempotencyKey: string | undefined,
    public readonly headers: Record<string, string> | undefined,
    public readonly param: unknown,
    public readonly opts: Options,
    parent?: Invocation<any>,
  ) {
    const { future, resolve, reject } = Future.deferred<T>(this);
    this.future = future;
    this.resolve = resolve;
    this.reject = reject;

    this.root = parent?.root ?? this;
    this.parent = parent ?? null;
  }

  get timeout(): number {
    return Math.min(this.createdAt + this.opts.timeout, this.parent?.timeout ?? Infinity);
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

  split(args: [...any, PartialOptions?]): { args: any[]; opts: Options } {
    const opts = args[args.length - 1];

    // defaults are specified on the root invocation
    // this means that overrides only apply to the current invocation
    // and do no propagate to children
    const defaults = this.root.opts;

    return isOptions(opts)
      ? { args: args.slice(0, -1), opts: { ...defaults, ...opts, tags: { ...defaults.tags, ...opts.tags } } }
      : { args, opts: defaults };
  }
}
