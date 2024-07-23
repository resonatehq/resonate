import { describe, test, expect, jest } from "@jest/globals";
import { IStore } from "../lib/core/store";
import { LocalStore } from "../lib/core/stores/local";
import { RemoteStore } from "../lib/core/stores/remote";
import { Resonate, Context } from "../lib/resonate";

jest.setTimeout(10000);

describe("Sleep", () => {
  const stores: IStore[] = [new LocalStore()];

  if (process.env.RESONATE_STORE_URL) {
    stores.push(new RemoteStore(process.env.RESONATE_STORE_URL));
  }

  for (const store of stores) {
    describe(store.constructor.name, () => {
      const resonate = new Resonate({ store });
      const timestamp = Date.now();
      const funcId = `sleep-${timestamp}`;
      resonate.register(funcId, async (ctx: Context, ms: number) => {
        const t1 = Date.now();
        await ctx.sleep(ms);
        const t2 = Date.now();

        return t2 - t1;
      });

      for (const ms of [1, 10, 100, 500]) {
        test(`${ms}ms`, async () => {
          const t = await resonate.run(funcId, `${funcId}.${ms}`, ms);
          expect(t).toBeGreaterThanOrEqual(ms);
          expect(t).toBeLessThan(ms + 200); // Allow 200ms tolerance because of the server round trip
        });
      }
    });
  }
});
