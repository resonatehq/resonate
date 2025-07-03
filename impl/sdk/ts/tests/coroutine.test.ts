import { lfc, lfi, rfc, rfi } from "../src/context";
import { Coroutine, type Suspended } from "../src/coroutine";
import { Handler } from "../src/handler";

describe("Coroutine", () => {
  // Helper functions to write test easily
  const exec = (uuid: string, func: any, args: any[], handler: Handler) => {
    return new Promise<any>((resolve) => {
      Coroutine.exec(uuid, func, args, handler, resolve);
    });
  };

  const resolvePromise = (handler: Handler, uuid: string, value: any) => {
    return new Promise<any>((resolve) => {
      handler.resolvePromise(uuid, value, resolve);
    });
  };

  test('basic coroutine completes with { type: "completed", value: 42 }', async () => {
    function* bar() {
      return 42;
    }

    function* foo() {
      const p = yield* lfi(bar);
      const v = yield* p;
      return v;
    }
    const h = new Handler();
    const r = await exec("foo.1", foo, [], h);
    expect(r).toEqual({ type: "completed", value: 42 });
  });

  test("basic coroutine with function suspends after first await", async () => {
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

    // First execution - should suspend
    let r = await exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "suspended" });
    const suspended = r as Suspended;
    expect(suspended.todos).toHaveLength(2);

    await resolvePromise(h, "foo.1.0", 42);
    await resolvePromise(h, "foo.1.1", 31416);

    // Second execution - should complete
    r = await exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "completed", value: 42 + 31416 });
  });

  test("coroutine with a suspension point suspends if can not make more progress", async () => {
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

    let r = await exec("foo.1", foo, [], h);
    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todos).toHaveLength(1);

    await resolvePromise(h, "foo.1.1", 42);
    r = await exec("foo.1", foo, [], h);
    expect(r).toEqual({ type: "completed", value: 42 });
  });

  test("Structured concurrency", async () => {
    function* bar() {
      return 42;
    }

    function* foo() {
      yield* rfi(bar);
      yield* rfi(bar);
      return 99;
    }

    const h = new Handler();
    let r = await exec("foo.1", foo, [], h);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todos).toHaveLength(2);

    await resolvePromise(h, "foo.1.1", 42);
    r = await exec("foo.1", foo, [], h);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todos).toHaveLength(1);

    await resolvePromise(h, "foo.1.0", 42);
    r = await exec("foo.1", foo, [], h);

    expect(r).toEqual({ type: "completed", value: 99 });
  });

  test("lfc/rfc", async () => {
    function* bar() {
      return 42;
    }

    function* foo() {
      const v1: number = yield* lfc(bar);
      const v2: number = yield* rfc(bar);
      return v1 + v2;
    }

    const h = new Handler();

    let r = await exec("foo.1", foo, [], h);
    expect(r.type).toBe("suspended");
    let suspended = r as Suspended;
    expect(suspended.todos).toHaveLength(1);

    await resolvePromise(h, "foo.1.1", 42);

    r = await exec("foo.1", foo, [], h);
    expect(r).toEqual({ type: "completed", value: 84 });
  });
});
