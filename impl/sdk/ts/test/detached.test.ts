import { describe, expect, test } from "@jest/globals";
import { options } from "../lib/core/options";
import { never } from "../lib/core/retry";
import { sleep } from "../lib/core/utils";
import { Resonate, Context } from "../lib/resonate";

describe("Detached tests", () => {
  test("A detached function does not get implicitly awaited at the end of a context", async () => {
    const resonate = new Resonate({
      // url: "http://127.0.0.1:8001",
    });

    const arr = [];
    resonate.register("detached", async (ctx: Context) => {
      const handle = await ctx.invokeLocal(async () => {
        await sleep(15);
        arr.push(4);
      });
      await handle.result();
    });

    resonate.register("foo", async (ctx: Context) => {
      await ctx.invokeLocal(
        async (ctx: Context) => {
          await sleep(10);
          arr.push(2);
        },
        options({ retryPolicy: never() }),
      );

      await ctx.detached("detached", "d.1");

      arr.push(1);
    });

    const tophandle = await resonate.invokeLocal("foo", "foo.0");
    await tophandle.result();
    arr.push(3);

    // Since we are not awaiting the detached invocation we should not see its effect over arr
    expect(arr).toEqual([1, 2, 3]);
  });
});
