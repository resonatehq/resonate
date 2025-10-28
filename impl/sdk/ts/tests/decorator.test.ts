import { WallClock } from "../src/clock";
import { type Context, Future, InnerContext, type LFI } from "../src/context";
import { Decorator } from "../src/decorator";
import { Never } from "../src/retries";
import { ok, type Yieldable } from "../src/types";

describe("Decorator", () => {
  it("returns internal.return when generator is done", () => {
    function* foo(): Generator<LFI<any> | Future<any>, any, number> {
      return 42;
    }

    const d = new Decorator(foo());
    const r = d.next({ type: "internal.nothing" });

    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: ok(42),
      },
    });
  });

  it("handles internal.async correctly", () => {
    function* foo(ctx: Context): Generator<Yieldable, any, any> {
      yield* ctx.beginRun((_ctx: Context) => 42);
    }

    const d = new Decorator(
      foo(InnerContext.root("foo", "poll://any@default", 0, new Never(), new WallClock(), new Map())),
    );
    const r = d.next({ type: "internal.nothing" });

    expect(r).toMatchObject({
      type: "internal.async.l",
      id: "foo.0",
    });
  });

  it("handles internal.await correctly", () => {
    function* foo(): Generator<Future<any>, any, any> {
      yield new Future("future-1", "pending");
    }

    const d = new Decorator(foo());
    const r = d.next({ type: "internal.nothing" });

    expect(r).toMatchObject({
      type: "internal.await",
      promise: {
        id: "future-1",
        state: "pending",
      },
    });
  });

  it("handles lfc/rfc correctly", () => {
    function* foo(ctx: Context): Generator<any, any, any> {
      const v1 = yield* ctx.run((ctx: Context, x: number) => x + 1, 1);
      const v2 = yield* ctx.rpc<number>("foo");
      return v1 + v2;
    }

    const d = new Decorator(
      foo(InnerContext.root("foo", "poll://any@default", 0, new Never(), new WallClock(), new Map())),
    );
    let r = d.next({ type: "internal.nothing" });
    expect(r).toMatchObject({
      type: "internal.async.l",
    });

    r = d.next({
      type: "internal.promise",
      state: "completed",
      id: "abc",
      value: {
        type: "internal.literal",
        value: ok(2),
      },
    });

    expect(r).toMatchObject({
      type: "internal.await",
      promise: {
        type: "internal.promise",
        state: "completed",
        value: {
          type: "internal.literal",
          value: ok(2),
        },
      },
    });

    r = d.next({
      type: "internal.literal",
      value: ok(2),
    });

    expect(r).toMatchObject({
      type: "internal.async.r",
    });

    r = d.next({
      type: "internal.promise",
      state: "pending",
      mode: "attached",
      id: "abc",
    });

    expect(r).toMatchObject({
      type: "internal.await",
      promise: {
        type: "internal.promise",
        state: "pending",
      },
    });
  });

  it("returns final value after multiple yields", () => {
    function* foo(ctx: Context): Generator<LFI<any> | Future<any>, any, number> {
      yield new Future("future-1", "completed", ok(10));
      yield* ctx.beginRun(() => 42);
      return 30;
    }

    const d = new Decorator(
      foo(InnerContext.root("foo", "poll://any@default", 0, new Never(), new WallClock(), new Map())),
    );

    d.next({ type: "internal.nothing" }); // First yield
    d.next({ type: "internal.literal", value: ok(10) }); // yield a future, get a literal back
    const r = d.next({
      type: "internal.promise",
      state: "completed",
      id: "abc.2",
      value: {
        type: "internal.literal",
        value: ok(42),
      },
    }); // yield an invoke, get a completed promise back

    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: ok(30),
      },
    });
  });

  it("awaits if there are pending invokes - Structured Concurrency", () => {
    function* foo(ctx: Context): Generator<Yieldable, any, number> {
      yield new Future("future-1", "completed", ok(10)); // A
      yield* ctx.beginRun((_ctx: Context) => 20); // B
      yield* ctx.beginRun((_ctx: Context) => 30); // C
      return 30; // D
    }

    const d = new Decorator(
      foo(InnerContext.root("foo", "poll://any@default", 0, new Never(), new WallClock(), new Map())),
    );

    d.next({ type: "internal.nothing" }); // First yield
    d.next({ type: "internal.literal", value: ok(10) }); // A -> yield a future, get a literal back
    d.next({
      type: "internal.promise",
      state: "pending",
      mode: "attached",
      id: "abc.1",
    }); // B -> yield an invoke, get a pending promise back
    const r = d.next({
      type: "internal.promise",
      state: "completed",
      id: "abc.2",
      value: {
        type: "internal.literal",
        value: ok(30),
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
    function* foo(ctx: Context): Generator<Yieldable, any, number> {
      yield new Future("future-1", "completed", ok(10)); // A
      yield* ctx.beginRun((_ctx: Context) => 20); // B
      yield* ctx.beginRun((_ctx: Context) => 30); // C
      return 42; // D
    }

    const d = new Decorator(
      foo(InnerContext.root("foo", "poll://any@default", 0, new Never(), new WallClock(), new Map())),
    );

    d.next({ type: "internal.nothing" }); // First yield
    d.next({ type: "internal.literal", value: ok(10) }); // A -> yield a future, get a literal back
    d.next({
      type: "internal.promise",
      state: "completed",
      id: "abc.1",
      value: {
        type: "internal.literal",
        value: ok(20),
      },
    }); // B -> yield an invoke, get a completed promise back
    const r = d.next({
      type: "internal.promise",
      state: "completed",
      id: "abc.2",
      value: {
        type: "internal.literal",
        value: ok(30),
      },
    }); // C -> yield an invoke, get a completed promise back

    // D -> Must return given that the previous invokes were completed
    // even if they were not explicitly awaited.
    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: ok(42),
      },
    });
  });

  it("handles internal.die when condition is true", () => {
    function* foo(ctx: Context): Generator<any, any, any> {
      yield* ctx.panic(true, "Should panic");
      return "should not reach here";
    }

    const d = new Decorator(
      foo(InnerContext.root("foo", "poll://any@default", 0, new Never(), new WallClock(), new Map())),
    );
    const r = d.next({ type: "internal.nothing" });

    expect(r).toMatchObject({
      type: "internal.die",
      condition: true,
      error: {
        code: "98",
        type: "Panic",
      },
    });
  });

  it("handles internal.die when condition is false", () => {
    function* foo(ctx: Context): Generator<any, any, any> {
      yield* ctx.panic(false, "Should not die");
      return 42;
    }

    const d = new Decorator(
      foo(InnerContext.root("foo", "poll://any@default", 0, new Never(), new WallClock(), new Map())),
    );
    d.next({ type: "internal.nothing" }); // Process the die with condition=false
    const r = d.next({ type: "internal.nothing" }); // Continue to return

    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: ok(42),
      },
    });
  });
});
