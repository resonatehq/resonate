// Expression

export type InternalExpr<T> = InternalAsync<T> | InternalAwait<T> | InternalReturn<T>;

type F<T> = ((...args: any[]) => T) | ((...args: any[]) => Generator<any, T, any>);

export type InternalAsync<T> = {
  type: "internal.async";
  uuid: string;
  kind: "lfi" | "rfi";
  mode: "eager" | "defer";
  func: F<T>;
  args: Value<any>[];
};

export type InternalAwait<T> = {
  type: "internal.await";
  uuid: string;
  promise: Promise<T>;
};

export type InternalReturn<T> = {
  type: "internal.return";
  uuid: string;
  value: Value<T>;
};

// Values

export type Value<T> = Nothing | Literal<T> | Promise<T>;

export type Nothing = {
  type: "internal.nothing";
  uuid: string;
};

export type Literal<T> = {
  type: "internal.literal";
  uuid: string;
  value: T;
};

export type Promise<T> = PromisePending | PromiseCompleted<T>;

export type PromisePending = {
  type: "internal.promise";
  state: "pending";
  uuid: string;
};

export type PromiseCompleted<T> = {
  type: "internal.promise";
  state: "completed";
  uuid: string;
  value: Literal<T>;
};
