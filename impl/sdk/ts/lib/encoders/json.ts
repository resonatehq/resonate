import { IEncoder } from "../encoder";

export class JSONEncoder implements IEncoder<unknown, string> {
  match(data: unknown): data is unknown {
    return true;
  }

  encode(data: unknown): string {
    return JSON.stringify(data);
  }

  decode(data: string): unknown {
    return JSON.parse(data);
  }
}
