import { describe, test, expect, jest } from "@jest/globals";
import { Resonate, Context } from "../lib/async";

jest.setTimeout(10000);

describe("Sleep", () => {
  const resonate = new Resonate();

  resonate.register("sleep", async (ctx: Context, ms: number) => {
    const t1 = Date.now();
    await ctx.sleep(ms);
    const t2 = Date.now();

    return t2 - t1;
  });

  for (const ms of [1, 10, 100]) {
    test(`${ms}ms`, async () => {
      const t = await resonate.run("sleep", `sleep.${ms}`, ms);
      expect(t).toBeGreaterThanOrEqual(ms);
    });
  }
});
