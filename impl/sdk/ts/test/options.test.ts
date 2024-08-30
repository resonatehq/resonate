import { describe, test, expect, jest } from "@jest/globals";
import { Options, options } from "../lib/core/options";
import * as retry from "../lib/core/retry";
import * as a from "../lib/resonate";

jest.setTimeout(10000);

async function aTest(ctx: a.Context, opts: Partial<Options> = {}) {
  console.log({ optsOverrides: opts });
  return [
    ctx.invocationData.opts,
    ...(await ctx.run(
      async (ctx: a.Context) => [ctx.invocationData.opts, await ctx.run((ctx: a.Context) => ctx.invocationData.opts)],
      options(opts),
    )),
  ];
}

describe("Options", () => {
  const resonateOpts = {
    pollFrequency: 1000,
    retryPolicy: retry.exponential(),
    tags: { a: "a", b: "b", c: "c" },
    timeout: 1000,
  };

  const overrides: Partial<Options> = {
    durable: false,
    eidFn: () => "eid",
    idempotencyKeyFn: (_: string) => "idempotencyKey",
    shouldLock: false,
    pollFrequency: 2000,
    retryPolicy: retry.linear(),
    tags: { c: "x", d: "d", e: "e" },
    timeout: 2000,
    version: 2,
  };
  // Note: eidFn, encoder and idempotencyKeyFn are not serializable, and are note checked in the tests

  // Note: we are disabling durable for all tests here
  // so that value returned from the run is not serialized.

  const resonate = new a.Resonate(resonateOpts);
  resonate.register("test.1", aTest, { durable: false });
  resonate.register("test.1", aTest, { durable: false, version: 2 });
  resonate.register("test.2", aTest, overrides);

  test("resonate default options propagate down", async () => {
    const [top, middle, bottom] = await resonate.run<[Options, Options, Options]>(
      "test.1",
      `test.1.1`,
      options({ version: 1 }),
    );

    // Most options defaults are set when created a resonate instance
    for (const opts of [top, middle, bottom]) {
      expect(opts.durable).toBe(false);
      expect(opts.pollFrequency).toBe(resonateOpts.pollFrequency);
      expect(opts.retryPolicy).toEqual(resonateOpts.retryPolicy);
      expect(opts.timeout).toBe(resonateOpts.timeout);
      expect(opts.version).toBe(1);
    }

    expect(top.shouldLock).toBe(false);
    expect(middle.shouldLock).toBe(false);
    expect(bottom.shouldLock).toBe(false);

    expect(top.tags).toEqual({ ...resonateOpts.tags, "resonate:invocation": "true" });
    expect(middle.tags).toEqual(resonateOpts.tags);
    expect(bottom.tags).toEqual(resonateOpts.tags);
  });

  test("registered options propagate down", async () => {
    const a = await resonate.run<[Options, Options, Options]>("test.2", `test.2.1`);
    console.log({ a });

    const [top, middle, bottom] = a;
    for (const opts of [top, middle, bottom]) {
      console.log({ opts });
      expect(opts.durable).toBe(overrides.durable);
      expect(opts.shouldLock).toBe(overrides.shouldLock);
      expect(opts.pollFrequency).toBe(overrides.pollFrequency);
      expect(opts.retryPolicy).toEqual(overrides.retryPolicy);
      expect(opts.timeout).toBe(overrides.timeout);
      expect(opts.version).toBe(overrides.version);
    }

    expect(top.tags).toEqual({ ...resonateOpts.tags, ...overrides.tags, "resonate:invocation": "true" });
    expect(middle.tags).toEqual({ ...resonateOpts.tags, ...overrides.tags });
    expect(bottom.tags).toEqual({ ...resonateOpts.tags, ...overrides.tags });
  });

  test("options passed to resonate run affect top level only", async () => {
    const [top, ...bottom] = await resonate.run<[Options, Options, Options]>("test.1", `test.1.2`, options(overrides));

    // Note: only some options are valid at the top level
    // this is because we would lose this information on the recovery path.

    // top level options
    expect(top.durable).toBe(false);
    expect(top.shouldLock).toBe(false);
    expect(top.pollFrequency).toBe(resonateOpts.pollFrequency);
    expect(top.retryPolicy).toEqual(overrides.retryPolicy);
    expect(top.tags).toEqual({ ...resonateOpts.tags, ...overrides.tags, "resonate:invocation": "true" });
    expect(top.timeout).toBe(overrides.timeout);
    expect(top.version).toBe(overrides.version);

    // bottom level options
    for (const opts of bottom) {
      expect(opts.durable).toBe(false);
      expect(opts.shouldLock).toBe(false);
      expect(opts.pollFrequency).toBe(resonateOpts.pollFrequency);
      expect(opts.retryPolicy).toEqual(resonateOpts.retryPolicy);
      expect(opts.tags).toEqual(resonateOpts.tags);
      expect(opts.timeout).toBe(resonateOpts.timeout);
      expect(opts.version).toBe(overrides.version);
    }
  });

  test("options passed to context run affect current level only", async () => {
    const [top, middle, bottom] = await resonate.run<[Options, Options, Options]>(
      "test.1",
      `test.1.3`,
      overrides,
      options({ version: 1 }),
    );

    // middle options (overriden)
    expect(middle.durable).toBe(overrides.durable);
    expect(middle.shouldLock).toBe(overrides.shouldLock);
    expect(middle.pollFrequency).toBe(overrides.pollFrequency);
    expect(middle.retryPolicy).toEqual(overrides.retryPolicy);
    expect(middle.tags).toEqual({ ...resonateOpts.tags, ...overrides.tags });
    expect(middle.timeout).toBe(overrides.timeout);

    // top and bottom options
    for (const opts of [top, bottom]) {
      expect(opts.durable).toBe(false);
      expect(opts.pollFrequency).toBe(resonateOpts.pollFrequency);
      expect(opts.retryPolicy).toEqual(resonateOpts.retryPolicy);
      expect(opts.timeout).toBe(resonateOpts.timeout);
      expect(opts.shouldLock).toBe(false);
    }

    expect(top.version).toBe(1);
    expect(middle.version).toBeDefined();
    expect(bottom.version).toBeDefined();

    expect(top.tags).toEqual({ ...resonateOpts.tags, "resonate:invocation": "true" });
    expect(bottom.tags).toEqual(resonateOpts.tags);
  });
});
