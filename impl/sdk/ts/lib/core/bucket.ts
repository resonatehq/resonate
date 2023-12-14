export interface IBucket {
  schedule<R>(thunk: () => R, delay?: number): Promise<R>;
}
