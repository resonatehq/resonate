import { Computation, type Suspended } from "../src/computation";
import { invoke, rpc } from "../src/context";
import { Coroutine } from "../src/coroutine";
import { Handler } from "../src/handler";

describe("Computation", () => {
  test('basic computation completes with { type: "completed", value: 42 }', () => {
    function* bar() {
      return 42;
    }

    function* foo() {
      const p = yield* invoke(bar);
      const v = yield* p;
      return v;
    }
    const h = new Handler();
    const c = new Computation(new Coroutine("foo.1", foo()), h);

    const r = c.exec();
    expect(r).toEqual({ type: "completed", value: 42 });
  });

  test("computation with a suspension point suspends if can not make more progress", () => {
    function* bar() {
      return 42;
    }

    function* foo() {
      const p1 = yield* invoke(bar);
      const p2 = yield* rpc(bar);
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
      yield* rpc(bar);
      yield* rpc(bar);
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
});
