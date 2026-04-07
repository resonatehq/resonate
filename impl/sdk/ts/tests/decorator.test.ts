import { WallClock } from "../src/clock.js";
import { type Context, Future, InnerContext, type LFI } from "../src/context.js";
import { Decorator } from "../src/decorator.js";
import { LocalNetwork } from "../src/network/local.js";
import { OptionsBuilder } from "../src/options.js";
import { Registry } from "../src/registry.js";
import { Never } from "../src/retries.js";
import type { Yieldable } from "../src/types.js";

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
        value: { kind: "value", value: 42 },
      },
    });
  });

  it("handles internal.async correctly", () => {
    function* foo(ctx: Context): Generator<Yieldable, any, any> {
      yield* ctx.beginRun((_ctx: Context) => 42);
    }

    const m = new LocalNetwork();
    const d = new Decorator(
      foo(
        new InnerContext({
          id: "foo",
          func: foo.name,
          clock: new WallClock(),
          registry: new Registry(),
          dependencies: new Map(),
          timeout: 0,
          version: 1,
          retryPolicy: new Never(),
          optsBuilder: new OptionsBuilder({ match: (target: string) => `local://any@${target}`, idPrefix: "" }),
        }),
      ),
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

    const m = new LocalNetwork();
    const d = new Decorator(
      foo(
        new InnerContext({
          id: "foo",
          func: foo.name,
          clock: new WallClock(),
          registry: new Registry(),
          dependencies: new Map(),
          timeout: 0,
          version: 1,
          retryPolicy: new Never(),
          optsBuilder: new OptionsBuilder({ match: (target: string) => `local://any@${target}`, idPrefix: "" }),
        }),
      ),
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
        value: { kind: "value", value: 2 },
      },
    });

    expect(r).toMatchObject({
      type: "internal.await",
      promise: {
        type: "internal.promise",
        state: "completed",
        value: {
          type: "internal.literal",
          value: { kind: "value", value: 2 },
        },
      },
    });

    r = d.next({
      type: "internal.literal",
      value: { kind: "value", value: 2 },
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
      yield* ctx.beginRun(() => 42);
      yield* ctx.beginRun(() => 10);
      return 30;
    }

    const m = new LocalNetwork();
    const d = new Decorator(
      foo(
        new InnerContext({
          id: "foo",
          func: foo.name,
          clock: new WallClock(),
          registry: new Registry(),
          dependencies: new Map(),
          timeout: 0,
          version: 1,
          retryPolicy: new Never(),
          optsBuilder: new OptionsBuilder({ match: (target: string) => `local://any@${target}`, idPrefix: "" }),
        }),
      ),
    );

    let r = d.next({ type: "internal.nothing" }); // First LFI
    expect(r).toMatchObject({ type: "internal.async.l" });

    r = d.next({
      type: "internal.promise",
      state: "pending",
      mode: "attached",
      id: "foo.0",
    }); // pending promise → generator gets Future(pending), advances to second LFI
    expect(r).toMatchObject({ type: "internal.async.l" });

    r = d.next({
      type: "internal.promise",
      state: "completed",
      id: "foo.1",
      value: {
        type: "internal.literal",
        value: { kind: "value", value: 10 },
      },
    }); // second LFI completed → generator gets Future(completed), which short-circuits → return 30

    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: { kind: "value", value: 30 },
      },
    });
  });

  it("returns even if there are pending invokes (structured concurrency is enforced by coroutine)", () => {
    function* foo(ctx: Context): Generator<Yieldable, any, number> {
      yield* ctx.beginRun((_ctx: Context) => 10); // A
      yield* ctx.beginRun((_ctx: Context) => 20); // B
      yield* ctx.beginRun((_ctx: Context) => 30); // C
      return 30; // D
    }

    const m = new LocalNetwork();
    const d = new Decorator(
      foo(
        new InnerContext({
          id: "foo",
          func: foo.name,
          clock: new WallClock(),
          registry: new Registry(),
          dependencies: new Map(),
          timeout: 0,
          version: 1,
          retryPolicy: new Never(),
          optsBuilder: new OptionsBuilder({ match: (target: string) => `local://any@${target}`, idPrefix: "" }),
        }),
      ),
    );

    let r = d.next({ type: "internal.nothing" }); // A -> LFI
    expect(r).toMatchObject({ type: "internal.async.l" });

    r = d.next({
      type: "internal.promise",
      state: "completed",
      id: "foo.0",
      value: {
        type: "internal.literal",
        value: { kind: "value", value: 10 },
      },
    }); // A -> completed, short-circuits → B -> LFI
    expect(r).toMatchObject({ type: "internal.async.l" });

    r = d.next({
      type: "internal.promise",
      state: "pending",
      mode: "attached",
      id: "foo.1",
    }); // B -> pending promise → generator gets Future(pending), advances to C -> LFI
    expect(r).toMatchObject({ type: "internal.async.l" });

    r = d.next({
      type: "internal.promise",
      state: "completed",
      id: "foo.2",
      value: {
        type: "internal.literal",
        value: { kind: "value", value: 30 },
      },
    }); // C -> completed, short-circuits → return 30

    // D -> Decorator returns the value; structured concurrency is enforced by the coroutine
    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: { kind: "value", value: 30 },
      },
    });
  });

  it("returns if there are no pending invokes even if not explicitly awaited", () => {
    function* foo(ctx: Context): Generator<Yieldable, any, number> {
      yield* ctx.beginRun((_ctx: Context) => 10); // A
      yield* ctx.beginRun((_ctx: Context) => 20); // B
      yield* ctx.beginRun((_ctx: Context) => 30); // C
      return 42; // D
    }

    const m = new LocalNetwork();
    const d = new Decorator(
      foo(
        new InnerContext({
          id: "foo",
          func: foo.name,
          clock: new WallClock(),
          registry: new Registry(),
          dependencies: new Map(),
          timeout: 0,
          version: 1,
          retryPolicy: new Never(),
          optsBuilder: new OptionsBuilder({ match: (target: string) => `local://any@${target}`, idPrefix: "" }),
        }),
      ),
    );

    let r = d.next({ type: "internal.nothing" }); // A -> LFI
    expect(r).toMatchObject({ type: "internal.async.l" });

    r = d.next({
      type: "internal.promise",
      state: "completed",
      id: "foo.0",
      value: {
        type: "internal.literal",
        value: { kind: "value", value: 10 },
      },
    }); // A -> completed, short-circuits → B -> LFI
    expect(r).toMatchObject({ type: "internal.async.l" });

    r = d.next({
      type: "internal.promise",
      state: "completed",
      id: "foo.1",
      value: {
        type: "internal.literal",
        value: { kind: "value", value: 20 },
      },
    }); // B -> completed, short-circuits → C -> LFI
    expect(r).toMatchObject({ type: "internal.async.l" });

    r = d.next({
      type: "internal.promise",
      state: "completed",
      id: "foo.2",
      value: {
        type: "internal.literal",
        value: { kind: "value", value: 30 },
      },
    }); // C -> completed, short-circuits → return 42

    // D -> Must return given that the previous invokes were completed
    // even if they were not explicitly awaited.
    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: { kind: "value", value: 42 },
      },
    });
  });

  it("handles internal.die when condition is true", () => {
    function* foo(ctx: Context): Generator<any, any, any> {
      yield* ctx.panic(true, "Should panic");
      return "should not reach here";
    }

    const m = new LocalNetwork();
    const d = new Decorator(
      foo(
        new InnerContext({
          id: "foo",
          func: foo.name,
          clock: new WallClock(),
          registry: new Registry(),
          dependencies: new Map(),
          timeout: 0,
          version: 1,
          retryPolicy: new Never(),
          optsBuilder: new OptionsBuilder({ match: (target: string) => `local://any@${target}`, idPrefix: "" }),
        }),
      ),
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

    const m = new LocalNetwork();
    const d = new Decorator(
      foo(
        new InnerContext({
          id: "foo",
          func: foo.name,
          clock: new WallClock(),
          registry: new Registry(),
          dependencies: new Map(),
          timeout: 0,
          version: 1,
          retryPolicy: new Never(),
          optsBuilder: new OptionsBuilder({ match: (target: string) => `local://any@${target}`, idPrefix: "" }),
        }),
      ),
    );
    d.next({ type: "internal.nothing" }); // Process the die with condition=false
    const r = d.next({ type: "internal.nothing" }); // Continue to return

    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: { kind: "value", value: 42 },
      },
    });
  });

  // --- New tests for inline literal resolution after pending await ---

  it("handles lfc/rfc correctly — feeds literal after pending RFC await", () => {
    function* foo(ctx: Context): Generator<any, any, any> {
      const v1 = yield* ctx.run((ctx: Context, x: number) => x + 1, 1);
      const v2 = yield* ctx.rpc<number>("foo");
      return v1 + v2;
    }

    const m = new LocalNetwork();
    const d = new Decorator(
      foo(
        new InnerContext({
          id: "foo",
          func: foo.name,
          clock: new WallClock(),
          registry: new Registry(),
          dependencies: new Map(),
          timeout: 0,
          version: 1,
          retryPolicy: new Never(),
          optsBuilder: new OptionsBuilder({ match: (target: string) => `local://any@${target}`, idPrefix: "" }),
        }),
      ),
    );

    // LFC: internal.async.l
    let r = d.next({ type: "internal.nothing" });
    expect(r).toMatchObject({ type: "internal.async.l" });

    // Feed completed promise for LFC → auto-await → internal.await completed
    r = d.next({
      type: "internal.promise",
      state: "completed",
      id: "abc",
      value: { type: "internal.literal", value: { kind: "value", value: 2 } },
    });
    expect(r).toMatchObject({ type: "internal.await", promise: { state: "completed" } });

    // Feed literal for the LFC result → advances to RFC
    r = d.next({ type: "internal.literal", value: { kind: "value", value: 2 } });
    expect(r).toMatchObject({ type: "internal.async.r" });

    // Feed pending promise for RFC → auto-await → internal.await pending
    r = d.next({
      type: "internal.promise",
      state: "pending",
      mode: "attached",
      id: "abc",
    });
    expect(r).toMatchObject({ type: "internal.await", promise: { state: "pending" } });

    // Feed literal after pending await (inline resolution by coroutine)
    r = d.next({
      type: "internal.literal",
      value: { kind: "value", value: 99 },
    });

    // Generator receives 99 and returns v1 + v2 = 2 + 99 = 101
    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: { kind: "value", value: 101 },
      },
    });
  });

  it("pending LFI await followed by literal resolution", () => {
    function* foo(ctx: Context): Generator<any, any, any> {
      const f = yield* ctx.beginRun((_ctx: Context) => 42);
      const v = yield* f;
      return v + 1;
    }

    const m = new LocalNetwork();
    const d = new Decorator(
      foo(
        new InnerContext({
          id: "foo",
          func: foo.name,
          clock: new WallClock(),
          registry: new Registry(),
          dependencies: new Map(),
          timeout: 0,
          version: 1,
          retryPolicy: new Never(),
          optsBuilder: new OptionsBuilder({ match: (target: string) => `local://any@${target}`, idPrefix: "" }),
        }),
      ),
    );

    // LFI: internal.async.l
    let r = d.next({ type: "internal.nothing" });
    expect(r).toMatchObject({ type: "internal.async.l" });

    // Feed pending promise → generator gets Future(pending)
    r = d.next({
      type: "internal.promise",
      state: "pending",
      mode: "attached",
      id: "foo.0",
    });
    // Generator advances to yield* f (Future iterator: yield this)
    expect(r).toMatchObject({ type: "internal.await", promise: { state: "pending", id: "foo.0" } });

    // Coroutine resolves inline and feeds a literal back
    r = d.next({
      type: "internal.literal",
      value: { kind: "value", value: 42 },
    });

    // Generator receives 42, returns 42 + 1 = 43
    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: { kind: "value", value: 43 },
      },
    });
  });

  it("error literal after pending await propagates to generator", () => {
    function* foo(ctx: Context): Generator<any, any, any> {
      const f = yield* ctx.beginRun((_ctx: Context) => 0);
      try {
        yield* f;
        return "no error";
      } catch (e) {
        return `caught: ${(e as Error).message}`;
      }
    }

    const m = new LocalNetwork();
    const d = new Decorator(
      foo(
        new InnerContext({
          id: "foo",
          func: foo.name,
          clock: new WallClock(),
          registry: new Registry(),
          dependencies: new Map(),
          timeout: 0,
          version: 1,
          retryPolicy: new Never(),
          optsBuilder: new OptionsBuilder({ match: (target: string) => `local://any@${target}`, idPrefix: "" }),
        }),
      ),
    );

    // LFI: internal.async.l
    let r = d.next({ type: "internal.nothing" });
    expect(r).toMatchObject({ type: "internal.async.l" });

    // Feed pending promise → generator gets Future(pending)
    r = d.next({
      type: "internal.promise",
      state: "pending",
      mode: "attached",
      id: "foo.0",
    });
    expect(r).toMatchObject({ type: "internal.await", promise: { state: "pending" } });

    // Feed error literal (coroutine resolved with failure)
    r = d.next({
      type: "internal.literal",
      value: { kind: "error", error: new Error("boom") },
    });

    // Generator's catch block caught it and returned the message
    expect(r).toMatchObject({
      type: "internal.return",
      value: {
        type: "internal.literal",
        value: { kind: "value", value: "caught: boom" },
      },
    });
  });
});
