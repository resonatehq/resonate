import { describe, test, expect, jest } from "@jest/globals";
import * as a from "../lib/async";
import * as g from "../lib/generator";

jest.setTimeout(10000);

describe("Functions: versions", () => {
  const rA = new a.Resonate();
  rA.register("default", async (ctx: a.Context, val: string) => {
    return await ctx.run(() => val);
  });
  rA.register(
    "durable",
    async (ctx: a.Context, val: string) => {
      return await ctx.run(() => val, ctx.options({ durable: true }));
    },
    { durable: false },
  );
  rA.register(
    "volatile",
    async (ctx: a.Context, val: string) => {
      return await ctx.run(() => val, ctx.options({ durable: false }));
    },
    { durable: false },
  );

  const rG = new g.Resonate();
  rG.register("default", function* (ctx: g.Context, val: string) {
    return yield ctx.run(function* () {
      return val;
    });
  });
  rG.register(
    "durable",
    function* (ctx: g.Context, val: string) {
      return yield ctx.run(
        function* () {
          return val;
        },
        ctx.options({ durable: true }),
      );
    },
    { durable: false },
  );
  rG.register(
    "volatile",
    function* (ctx: g.Context, val: string) {
      return yield ctx.run(
        function* () {
          return val;
        },
        ctx.options({ durable: false }),
      );
    },
    { durable: false },
  );

  for (const { name, resonate } of [
    { name: "async", resonate: rA },
    { name: "generator", resonate: rG },
  ]) {
    describe(name, () => {
      test("default should be durable", async () => {
        expect(await resonate.run("default", "default.a", "a")).toBe("a");
        expect(await resonate.run("default", "default.a", "b")).toBe("a");
        expect(await resonate.run("default", "default.b", "b")).toBe("b");
      });

      test("durable should be durable", async () => {
        expect(await resonate.run("durable", "durable.a", "a")).toBe("a");
        expect(await resonate.run("durable", "durable.a", "b")).toBe("a");
        expect(await resonate.run("durable", "durable.b", "b")).toBe("b");
      });

      test("volatile should not be durable", async () => {
        expect(await resonate.run("volatile", "volatile.a", "a")).toBe("a");
        expect(await resonate.run("volatile", "volatile.a", "b")).toBe("b");
        expect(await resonate.run("volatile", "volatile.b", "b")).toBe("b");
      });
    });
  }
});
