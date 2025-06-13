import { Future, Invoke, lfi } from "../src/context";

describe("User", () => {
  it("", () => {
    function* foo() {
      // This type checks to Future<number> and number
      const future1 = yield* lfi((x: number) => x + x, 666);
      const result1 = yield* future1;

      // This type checks to Future<string> and string
      const future2 = yield* lfi((x: string) => x + x, "a");
      const result2 = yield* future2;

      return result1;
    }

    // const c = foo();

    // console.log(c);

    // const r1 = c.next();

    // const i1 : Invoke<any> = r1.value as Invoke<any>;

    // const f1 = new Future<any>();

    // i1.resolve(f1);

    // console.log(r1);
  });
});
