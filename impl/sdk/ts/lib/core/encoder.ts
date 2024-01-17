export interface IEncoder<I, O> {
  encode(data: I): O;
  decode(data: O): I;
}
