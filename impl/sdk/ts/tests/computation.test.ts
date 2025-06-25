import { Computation, type Suspended } from "../src/computation";
import { lfc, lfi, rfc, rfi } from "../src/context";
import { Coroutine } from "../src/coroutine";
import { Handler } from "../src/handler";

describe("Computation", () => {
  test('basic computation completes with { type: "completed", value: 42 }', () => {
    function* bar() {
      return 42;
    }

    function* foo() {
      const p = yield* lfi(bar);
      const v = yield* p;
      return v;
    }
    const h = new Handler();
    const c = new Computation(new Coroutine("foo.1", foo()), h);

    const r = c.exec();
    expect(r).toEqual({ type: "completed", value: 42 });
  });

  test("basic computation with function suspends after first await", () => {
    function bar() {
      return 42;
    }
    function baz() {
      return 31416;
    }

    function* foo() {
      const p = yield* lfi(bar);
      const p2 = yield* lfi(baz);
      const v = yield* p;
      const v2 = yield* p2;
      return v + v2;
    }
    const h = new Handler();
    let r = Computation.exec("foo.1", foo, [], h);

    expect(r).toMatchObject({ type: "suspended" });
    r = r as Suspended;
    expect(r.todos).toHaveLength(2);

    h.resolvePromise("foo.1.0", 42);
    h.resolvePromise("foo.1.1", 31416);

    r = Computation.exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "completed", value: 42 + 31416 });
  });

  test("basic computation with function suspends after lfc", () => {
    function bar() {
      return 42;
    }
    function baz() {
      return 31416;
    }

    function* foo() {
      const v = yield* lfc(bar);
      const p2 = yield* lfi(baz);
      const v2 = yield* p2;
      return v + v2;
    }
    const h = new Handler();
    let r = Computation.exec("foo.1", foo, [], h);

    expect(r).toMatchObject({ type: "suspended" });
    r = r as Suspended;
    expect(r.todos).toHaveLength(1);
    h.resolvePromise("foo.1.0", 42);

    r = Computation.exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "suspended" });
    r = r as Suspended;
    expect(r.todos).toHaveLength(1);

    h.resolvePromise("foo.1.1", 31416);

    r = Computation.exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "completed", value: 42 + 31416 });
  });

  test("computation with a suspension point suspends if can not make more progress", () => {
    function* bar() {
      return 42;
    }

    function* foo() {
      const p1 = yield* lfi(bar);
      const p2 = yield* rfi(bar);
      const v1 = yield* p1;
      const vx = yield* p1;
      const v2 = yield* p2;
      return v1;
    }

    const h = new Handler();

    let r = Computation.exec("foo.1", foo, [], h);
    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todos).toHaveLength(1);

    h.resolvePromise("foo.1.1", 42);
    r = Computation.exec("foo.1", foo, [], h);
    expect(r).toEqual({ type: "completed", value: 42 });
  });

  test("Structured concurrency", () => {
    function* bar() {
      return 42;
    }

    function* foo() {
      yield* rfi(bar);
      yield* rfi(bar);
      return 99;
    }

    const h = new Handler();
    let r = Computation.exec("foo.1", foo, [], h);
    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todos).toHaveLength(2);

    // If we resolve only one of the promises, in the second call to exec there must be 1 await still left
    h.resolvePromise("foo.1.1", 42);
    r = Computation.exec("foo.1", foo, [], h);
    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todos).toHaveLength(1);

    h.resolvePromise("foo.1.0", 42);
    r = Computation.exec("foo.1", foo, [], h);
    expect(r).toEqual({ type: "completed", value: 99 });
  });

  test("lfc/rfc", () => {
    function* bar() {
      return 42;
    }

    function* foo() {
      const v1: number = yield* lfc(bar);
      const v2: number = yield* rfc(bar);
      return v1 + v2;
    }

    const h = new Handler();
    let r = Computation.exec("foo.1", foo, [], h);
    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todos).toHaveLength(1);

    h.resolvePromise("foo.1.1", 42);
    r = Computation.exec("foo.1", foo, [], h);
    expect(r).toEqual({ type: "completed", value: 84 });
  });
});
