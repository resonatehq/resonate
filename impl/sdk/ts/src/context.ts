export type AnyGen<T> = (...args: any[]) => Generator<any, T, any>;
export type AnyFun<T> = (...args: any[]) => T;

export type Invocable<T> = AnyGen<T> | AnyFun<T>;
export type Yieldable = Invoke<any> | Future<any>;

export class Invoke<T> implements Iterable<Invoke<T>> {
  public type: "lfi" | "rfi";
  public func: Invocable<T>;
  public args: any[];

  constructor(type: "lfi" | "rfi", func: Invocable<T>, ...args: any[]) {
    this.type = type;
    this.func = func;
    this.args = args;
  }

  *[Symbol.iterator](): Generator<Invoke<T>, Future<T>, any> {
    const future = yield this;
    return future as Future<T>;
  }
}

export class Future<T> implements Iterable<Future<T>> {
  public readonly value?: T;
  private state: "pending" | "completed";

  constructor(
    public uuid: string,
    state: "pending" | "completed",
    value?: T,
  ) {
    this.value = value;
    this.state = state;
  }

  isCompleted(): boolean {
    return this.state === "completed";
  }

  getValue(): T | undefined {
    return this.value;
  }

  *[Symbol.iterator](): Generator<Future<T>, T, undefined> {
    yield this;
    return this.value!;
  }
}

export function invoke<R>(func: Invocable<R>, ...args: any[]): Invoke<R> {
  return new Invoke<R>("lfi", func, ...args);
}

export function rpc<R>(func: Invocable<R>, ...args: any[]): Invoke<R> {
  return new Invoke<R>("rfi", func, ...args);
}
