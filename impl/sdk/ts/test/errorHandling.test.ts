import { fail } from "assert";
import { describe, test, expect, jest } from "@jest/globals";
import { ErrorCodes, never, options, ResonateError } from "../lib";
import { sleep } from "../lib/core/utils";
import { Context, Resonate } from "../lib/resonate";

jest.setTimeout(10000);

describe("Errors", () => {
  test("Errors in user functions propagate back to top level", async () => {
    const resonate = new Resonate();

    resonate.register(
      "err",
      async (ctx: Context, val: string) => {
        await ctx.run(async (ctx: Context) => {
          await ctx.run(async (ctx: Context) => {
            await ctx.run(async (ctx: Context) => {
              throw new Error(val);
            });
            throw new Error("should not reach this point");
          });
        });
      },
      options({ retryPolicy: never() }),
    );

    const errToReturn = "This is the error";
    const handle = await resonate.invokeLocal<void>("err", "err.0", errToReturn);

    await expect(handle.result()).rejects.toThrow(errToReturn);
    const durablePromiseRecord = await resonate.store.promises.get(handle.invocationId);
    expect(durablePromiseRecord.state).toBe("REJECTED");
  });

  test("Unrecoverable errors deep in the stack causes top level to abort", async () => {
    const resonate = new Resonate();

    resonate.register(
      "err",
      async (ctx: Context, val: string) => {
        await ctx.run(async (ctx: Context) => {
          await ctx.run(async (ctx: Context) => {
            // Make the create fail from this point forward
            jest
              .spyOn(resonate.store.promises, "create")
              .mockRejectedValue(new ResonateError("Fetch Error", ErrorCodes.FETCH, "mock", true));
            await ctx.run(async (ctx: Context) => {
              return "Should not reach this point";
            });
          });
        });
      },
      options({ retryPolicy: never() }),
    );
    // TODO(avillega): Should it not retry when there was an unrecoverable error?

    const handle = await resonate.invokeLocal<void>("err", "err.0", "nil");
    try {
      await handle.result();
    } catch (err) {
      if (err instanceof ResonateError) {
        expect(err.code).toBe(ErrorCodes.ABORT);
      } else {
        fail("Error should be a Resonate Error");
      }
    }

    // The local promise must be rejected after aborting, but the durablePromise must be pending
    await expect(handle.state()).resolves.toBe("rejected");
    const durablePromiseRecord = await resonate.store.promises.get(handle.invocationId);
    expect(durablePromiseRecord.state).toBe("PENDING");
  });

  test("Unrecoverable errors deep in the stack causes top level to abort even when the user catch the error", async () => {
    const resonate = new Resonate();

    resonate.register(
      "err",
      async (ctx: Context, val: string) => {
        await ctx.run(async (ctx: Context) => {
          try {
            await ctx.run(async (ctx: Context) => {
              // Make the create fail from this point forward
              jest
                .spyOn(resonate.store.promises, "create")
                .mockRejectedValue(new ResonateError("Fetch Error", ErrorCodes.FETCH, "mock", true));
              await ctx.run(async (ctx: Context) => {
                return "Should not reach this point";
              });
            });
          } catch (err) {
            // ignore the error
          }
        });
      },
      options({ retryPolicy: never() }),
    );
    // TODO(avillega): Should it not retry when there was an unrecoverable error?

    const handle = await resonate.invokeLocal<void>("err", "err.0", "nil");
    try {
      await handle.result();
    } catch (err) {
      if (err instanceof ResonateError) {
        expect(err.code).toBe(ErrorCodes.ABORT);
      } else {
        fail("Error should be a Resonate Error");
      }
    }

    // The local promise must be rejected after aborting, but the durablePromise must be pending
    await expect(handle.state()).resolves.toBe("rejected");
    const durablePromiseRecord = await resonate.store.promises.get(handle.invocationId);
    expect(durablePromiseRecord.state).toBe("PENDING");
  });

  test("Unrecoverable errors in the top level cause to abort", async () => {
    const resonate = new Resonate();

    resonate.register(
      "err",
      async (ctx: Context, val: string) => {
        await ctx.run((ctx: Context) => "all good here");

        await ctx.run(async (ctx: Context) => {
          return "all good here too";
        });

        jest
          .spyOn(resonate.store.promises, "resolve")
          .mockRejectedValue(new ResonateError("Fetch Error", ErrorCodes.FETCH, "mock", true));
      },
      options({ retryPolicy: never() }),
    );

    const handle = await resonate.invokeLocal<void>("err", "err.0", "nil");
    try {
      await handle.result();
    } catch (err) {
      if (err instanceof ResonateError) {
        expect(err.code).toBe(ErrorCodes.ABORT);
      } else {
        fail("Error should be a Resonate Error");
      }
    }

    // The local promise must be rejected after aborting, but the durablePromise must be pending
    await expect(handle.state()).resolves.toBe("rejected");
    const durablePromiseRecord = await resonate.store.promises.get(handle.invocationId);
    expect(durablePromiseRecord.state).toBe("PENDING");
  });

  test("Unrecoverable errors in the top level cause to abort even when ignored by the user", async () => {
    const resonate = new Resonate();

    resonate.register(
      "err",
      async (ctx: Context, val: string) => {
        try {
          await ctx.run((ctx: Context) => "all good here");

          await ctx.run(async (ctx: Context) => {
            return "all good here too";
          });

          jest
            .spyOn(resonate.store.promises, "resolve")
            .mockRejectedValue(new ResonateError("Fetch Error", ErrorCodes.FETCH, "mock", true));
        } catch {
          // ignore error
        }
      },
      options({ retryPolicy: never() }),
    );

    const handle = await resonate.invokeLocal<void>("err", "err.0", "nil");
    try {
      await handle.result();
    } catch (err) {
      if (err instanceof ResonateError) {
        expect(err.code).toBe(ErrorCodes.ABORT);
      } else {
        fail("Error should be a Resonate Error");
      }
    }

    // The local promise must be rejected after aborting, but the durablePromise must be pending
    await expect(handle.state()).resolves.toBe("rejected");
    const durablePromiseRecord = await resonate.store.promises.get(handle.invocationId);
    expect(durablePromiseRecord.state).toBe("PENDING");
  });

  test("Timeout errors should propagate to top level execution", async () => {
    const resonate = new Resonate();

    resonate.register(
      "err",
      async (ctx: Context, val: string) => {
        await ctx.run((ctx: Context) => "all good here");
        await ctx.run(
          async (ctx: Context) => {
            await sleep(12);
            return "all good here too";
          },
          options({ timeout: 10 }),
        );
      },
      options({ retryPolicy: never() }),
    );

    const handle = await resonate.invokeLocal<void>("err", "err.0", "nil");
    try {
      await handle.result();
    } catch (err) {
      if (err instanceof ResonateError) {
        expect(err.code).toBe(ErrorCodes.TIMEDOUT);
      } else {
        fail("Error should be a Resonate Error");
      }
    }

    await expect(handle.state()).resolves.toBe("rejected");
    const durablePromiseRecord = await resonate.store.promises.get(handle.invocationId);
    expect(durablePromiseRecord.state).toBe("REJECTED");
  });
});
