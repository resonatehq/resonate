import { describe, test, expect, jest } from "@jest/globals";
import * as a from "../lib/async";
import * as g from "../lib/generator";

jest.setTimeout(10000);

describe("Functions: versions", () => {
  const rA = new a.Resonate();
  rA.register("test", (ctx: a.Context) => ({ v: "v1", c: ctx.version }));
  rA.register("test", (ctx: a.Context) => ({ v: "v2", c: ctx.version }), { version: 2 });
  rA.register("test", (ctx: a.Context) => ({ v: "v3", c: ctx.version }), { version: 3 });

  const rG = new g.Resonate();
  rG.register("test", function* (ctx: g.Context) {
    return { v: "v1", c: ctx.version };
  });
  rG.register(
    "test",
    function* (ctx: g.Context) {
      return { v: "v2", c: ctx.version };
    },
    { version: 2 },
  );
  rG.register(
    "test",
    function* (ctx: g.Context) {
      return { v: "v3", c: ctx.version };
    },
    { version: 3 },
  );

  for (const { name, resonate } of [
    { name: "async", resonate: rA },
    { name: "generator", resonate: rG },
  ]) {
    describe(name, () => {
      test("should return v1", async () => {
        const result = await resonate.run("test", "a", resonate.options({ version: 1 }));
        expect(result).toMatchObject({ v: "v1", c: 1 });
      });

      test("should return v2", async () => {
        const result = await resonate.run("test", "b", resonate.options({ version: 2 }));
        expect(result).toMatchObject({ v: "v2", c: 2 });
      });

      test("should return v3", async () => {
        const r1 = await resonate.run("test", "c", resonate.options({ version: 3 }));
        expect(r1).toMatchObject({ v: "v3", c: 3 });

        const r2 = await resonate.run("test", "d");
        expect(r2).toMatchObject({ v: "v3", c: 3 });
      });
    });
  }
});
