import { Invocation } from "./invocation";

/////////////////////////////////////////////////////////////////////
// Promise
/////////////////////////////////////////////////////////////////////

export class ResonatePromise<T> extends Promise<T> {
  // resolves to the id of the durable promise
  // can be used to await durable promise creation
  created: Promise<string>;

  // You are not expected to understand this (we don't either)
  // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/@@species
  static [Symbol.species] = Promise;

  /**
   * Represents the eventual return value of a Resonate function. This is achieved by wrapping a Javscript promise.
   *
   * @constructor
   * @param id - A unique identifier for this promise.
   * @param created - A promise that resolves when the durable promise has been created.
   * @param completed - A promise that resolves when the durable promise has been fulfilled.
   */
  constructor(
    public id: string,
    created: Promise<any>,
    completed: Promise<T>,
  ) {
    // bind the promise to the completed promise
    super((resolve, reject) => {
      completed.then(resolve, reject);
    });

    // expose the id when the durable promise has been created
    this.created = created.then(() => this.id);
  }
}

/////////////////////////////////////////////////////////////////////
// Future
/////////////////////////////////////////////////////////////////////

export class Future<T> {
  // a discriminate property
  readonly kind = "future";

  // a unique identifier
  id: string;

  // used to deduplicate futures
  idempotencyKey: string | undefined;

  /**
   * Represents the eventual return value of a Resonate function.
   *
   * @constructor
   * @param invocation - An invocation correpsonding to the Resonate function.
   * @param promise - A promise that resolves to the return value of the Resonate function.
   */
  constructor(
    private invocation: Invocation<T>,
    public promise: Promise<T>,
  ) {
    this.id = invocation.id;
    this.idempotencyKey = invocation.idempotencyKey;
  }

  static deferred<T>(executor: Invocation<T>) {
    let resolve!: (v: T) => void;
    let reject!: (v?: unknown) => void;

    // construct a javascript promise
    const promise = new Promise<T>((_resolve, _reject) => {
      resolve = _resolve;
      reject = _reject;
    });

    // return a new future and resolvers for the future
    return { future: new Future(executor, promise), resolve, reject };
  }

  get root() {
    return this.invocation.root.future;
  }

  get parent() {
    return this.invocation.parent?.future ?? null;
  }

  get children() {
    return this.invocation.children.map((i) => i.future);
  }
}
