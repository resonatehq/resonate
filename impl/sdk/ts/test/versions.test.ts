import { describe, test, expect, jest } from "@jest/globals";
import * as async from "../lib/async";
import * as generator from "../lib/generator";

jest.setTimeout(10000);

describe("Functions: versions", () => {
  const a = new async.Resonate();
  a.register("test", (ctx: async.Context) => ({ v: "v1", c: ctx.version }));
  a.register("test", 2, (ctx: async.Context) => ({ v: "v2", c: ctx.version }));
  a.register("test", 3, (ctx: async.Context) => ({ v: "v3", c: ctx.version }));

  const g = new generator.Resonate();
  g.register("test", function* (ctx: generator.Context) {
    return { v: "v1", c: ctx.version };
  });
  g.register("test", 2, function* (ctx: generator.Context) {
    return { v: "v2", c: ctx.version };
  });
  g.register("test", 3, function* (ctx: generator.Context) {
    return { v: "v3", c: ctx.version };
  });

  for (const resonate of [a, g]) {
    test("should return v1", async () => {
      const result = await resonate.run("test", 1, "a");
      expect(result).toMatchObject({ v: "v1", c: 1 });
    });

    test("should return v2", async () => {
      const result = await resonate.run("test", 2, "b");
      expect(result).toMatchObject({ v: "v2", c: 2 });
    });

    test("should return v3", async () => {
      const r1 = await resonate.run("test", 3, "c");
      expect(r1).toMatchObject({ v: "v3", c: 3 });

      const r2 = await resonate.run("test", "d");
      expect(r2).toMatchObject({ v: "v3", c: 3 });
    });
  }
});
