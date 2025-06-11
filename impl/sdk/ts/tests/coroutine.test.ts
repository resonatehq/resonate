import { Coroutine } from "../src/coroutine";

import { Future, Invoke } from "../src/context";

describe("Coroutine", () => {
  it("returns internal.return when generator is done", () => {
    function* foo(): Generator<Invoke<any> | Future<any>, any, number> {
      return 42;
    }

    const d = new Coroutine("abc", foo());
    const r = d.next({ type: "internal.nothing", uuid: "abc" });

    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: 42,
      },
    });
  });

  it("handles internal.async correctly", () => {
    function* foo(): Generator<Invoke<any>, any, any> {
      yield new Invoke("lfi", () => 42);
    }

    const d = new Coroutine("abc", foo());
    const r = d.next({ type: "internal.nothing", uuid: "abc" });

    expect(r).toMatchObject({
      type: "internal.async",
      kind: "lfi",
      uuid: "abc.0",
    });
  });

  it("handles internal.await correctly", () => {
    function* foo(): Generator<Future<any>, any, any> {
      yield new Future("future-1", "pending");
    }

    const d = new Coroutine("abc", foo());
    const r = d.next({ type: "internal.nothing", uuid: "abc" });

    expect(r).toMatchObject({
      type: "internal.await",
      promise: {
        uuid: "future-1",
        state: "pending",
      },
    });
  });

  it("returns final value after multiple yields", () => {
    function* foo(): Generator<Invoke<any> | Future<any>, any, number> {
      yield new Future("future-1", "completed", 10);
      yield new Invoke("lfi", () => 42);
      return 30;
    }

    const d = new Coroutine("abc", foo());

    d.next({ type: "internal.nothing", uuid: "abc" }); // First yield
    d.next({ type: "internal.literal", uuid: "abc.1", value: 10 }); // yield a future, get a literal back
    const r = d.next({
      type: "internal.promise",
      state: "completed",
      uuid: "abc.2",
      value: {
        type: "internal.literal",
        uuid: "abc.2.lit",
        value: 42,
      },
    }); // yield an invoke, get a completed promise back

    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: 30,
      },
    });
  });

  it("awaits if there are pending invokes - Structured Concurrency", () => {
    function* foo(): Generator<Invoke<any> | Future<any>, any, number> {
      yield new Future("future-1", "completed", 10); // A
      yield new Invoke("lfi", () => 20); // B
      yield new Invoke("lfi", () => 30); // C
      return 30; // D
    }

    const d = new Coroutine("abc", foo());

    d.next({ type: "internal.nothing", uuid: "abc" }); // First yield
    d.next({ type: "internal.literal", uuid: "abc.1", value: 10 }); // A -> yield a future, get a literal back
    d.next({
      type: "internal.promise",
      state: "pending",
      uuid: "abc.1",
    }); // B -> yield an invoke, get a pending promise back
    const r = d.next({
      type: "internal.promise",
      state: "completed",
      uuid: "abc.2",
      value: {
        type: "internal.literal",
        uuid: "abc.1.lit",
        value: 30,
      },
    }); // C -> yield an invoke, get a completed promise back

    // D -> Must not return, instead tell the caller to await given that there is a pending invoke
    expect(r).toMatchObject({
      type: "internal.await",
      promise: {
        type: "internal.promise",
        state: "pending",
      },
    });
  });

  it("returns if there are no pending invokes even if not explecityly awaited", () => {
    function* foo(): Generator<Invoke<any> | Future<any>, any, number> {
      yield new Future("future-1", "completed", 10); // A
      yield new Invoke("lfi", () => 20); // B
      yield new Invoke("lfi", () => 30); // C
      return 42; // D
    }

    const d = new Coroutine("abc", foo());

    d.next({ type: "internal.nothing", uuid: "abc" }); // First yield
    d.next({ type: "internal.literal", uuid: "abc.1", value: 10 }); // A -> yield a future, get a literal back
    d.next({
      type: "internal.promise",
      state: "completed",
      uuid: "abc.1",
      value: {
        type: "internal.literal",
        uuid: "abc.1.lit",
        value: 20,
      },
    }); // B -> yield an invoke, get a completed promise back
    const r = d.next({
      type: "internal.promise",
      state: "completed",
      uuid: "abc.2",
      value: {
        type: "internal.literal",
        uuid: "abc.1.lit",
        value: 30,
      },
    }); // C -> yield an invoke, get a completed promise back

    // D -> Must return given that the previous invokes were completed
    // even if they were not explicitly awaited.
    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: 42,
      },
    });
  });
});
