// Expression

export type InternalExpr<T> = InternalAsync<T> | InternalAwait<T> | InternalReturn<T>;

type F<T> = ((...args: any[]) => T) | ((...args: any[]) => Generator<any, T, any>);

export type InternalAsync<T> = {
  type: "internal.async";
  id: string;
  kind: "lfi" | "rfi";
  mode: "eager" | "defer";
  func: F<T>;
  args: Value<any>[];
};

export type InternalAwait<T> = {
  type: "internal.await";
  id: string;
  promise: Promise<T>;
};

export type InternalReturn<T> = {
  type: "internal.return";
  id: string;
  value: Value<T>;
};

// Values

export type Value<T> = Nothing | Literal<T> | Promise<T>;

export type Nothing = {
  type: "internal.nothing";
  id: string;
};

export type Literal<T> = {
  type: "internal.literal";
  id: string;
  value: T;
};

export type Promise<T> = PromisePending | PromiseCompleted<T>;

export type PromisePending = {
  type: "internal.promise";
  state: "pending";
  id: string;
};

export type PromiseCompleted<T> = {
  type: "internal.promise";
  state: "completed";
  id: string;
  value: Literal<T>;
};
