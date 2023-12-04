export interface IEncoder<I, O> {
  readonly key?: string;
  match(data: unknown): data is I;
  encode(data: I): O;
  decode(data: O): I;
}
