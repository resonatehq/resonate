import { describe, test, expect, jest } from "@jest/globals";
import * as a from "../lib/async";
import { Base64Encoder } from "../lib/core/encoders/base64";
import { JSONEncoder } from "../lib/core/encoders/json";
import { Options } from "../lib/core/options";
import * as retry from "../lib/core/retry";
import * as utils from "../lib/core/utils";

jest.setTimeout(10000);

async function aTest(ctx: a.Context, opts: Partial<Options> = {}) {
  return [
    ctx.opts,
    ...(await ctx.run(
      async (ctx: a.Context) => [ctx.opts, await ctx.run((ctx: a.Context) => ctx.opts)],
      ctx.options(opts),
    )),
  ];
}

describe("Options", () => {
  const resonateOpts = {
    encoder: new JSONEncoder(),
    pollFrequency: 1000,
    retryPolicy: retry.exponential(),
    tags: { a: "a", b: "b", c: "c" },
    timeout: 1000,
  };

  const overrides: Partial<Options> = {
    durable: false,
    eidFn: () => "eid",
    encoder: new Base64Encoder(),
    idempotencyKeyFn: (_: string) => "idempotencyKey",
    shouldLock: false,
    pollFrequency: 2000,
    retryPolicy: retry.linear(),
    tags: { c: "x", d: "d", e: "e" },
    timeout: 2000,
    version: 2,
  };

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
      resonate.options({ version: 1 }),
    );

    // Most options defaults are set when created a resonate instance
    for (const opts of [top, middle, bottom]) {
      expect(opts.durable).toBe(false);
      expect(opts.eidFn).toBe(utils.randomId);
      expect(opts.encoder).toBe(resonateOpts.encoder);
      expect(opts.idempotencyKeyFn).toBe(utils.hash);
      expect(opts.pollFrequency).toBe(resonateOpts.pollFrequency);
      expect(opts.retryPolicy).toBe(resonateOpts.retryPolicy);
      expect(opts.timeout).toBe(resonateOpts.timeout);
      expect(opts.version).toBe(1);
    }

    expect(top.shouldLock).toBe(true);
    expect(middle.shouldLock).toBe(false);
    expect(bottom.shouldLock).toBe(false);

    expect(top.tags).toEqual({ ...resonateOpts.tags, "resonate:invocation": "true" });
    expect(middle.tags).toEqual(resonateOpts.tags);
    expect(bottom.tags).toEqual(resonateOpts.tags);
  });

  test("registered options propagate down", async () => {
    const [top, middle, bottom] = await resonate.run<[Options, Options, Options]>("test.2", `test.2.1`);

    for (const opts of [top, middle, bottom]) {
      expect(opts.durable).toBe(overrides.durable);
      expect(opts.eidFn).toBe(overrides.eidFn);
      expect(opts.encoder).toBe(overrides.encoder);
      expect(opts.idempotencyKeyFn).toBe(overrides.idempotencyKeyFn);
      expect(opts.shouldLock).toBe(overrides.shouldLock);
      expect(opts.pollFrequency).toBe(overrides.pollFrequency);
      expect(opts.retryPolicy).toBe(overrides.retryPolicy);
      expect(opts.timeout).toBe(overrides.timeout);
      expect(opts.version).toBe(overrides.version);
    }

    expect(top.tags).toEqual({ ...resonateOpts.tags, ...overrides.tags, "resonate:invocation": "true" });
    expect(middle.tags).toEqual({ ...resonateOpts.tags, ...overrides.tags });
    expect(bottom.tags).toEqual({ ...resonateOpts.tags, ...overrides.tags });
  });

  test("options passed to resonate run affect top level only", async () => {
    const [top, ...bottom] = await resonate.run<[Options, Options, Options]>(
      "test.1",
      `test.1.2`,
      resonate.options(overrides),
    );

    // Note: only some options are valid at the top level
    // this is because we would lose this information on the recovery path.

    // top level options
    expect(top.durable).toBe(false);
    expect(top.eidFn).toBe(overrides.eidFn);
    expect(top.encoder).toBe(resonateOpts.encoder);
    expect(top.idempotencyKeyFn).toBe(overrides.idempotencyKeyFn);
    expect(top.shouldLock).toBe(true);
    expect(top.pollFrequency).toBe(resonateOpts.pollFrequency);
    expect(top.retryPolicy).toBe(overrides.retryPolicy);
    expect(top.tags).toEqual({ ...resonateOpts.tags, ...overrides.tags, "resonate:invocation": "true" });
    expect(top.timeout).toBe(overrides.timeout);
    expect(top.version).toBe(overrides.version);

    // bottom level options
    for (const opts of bottom) {
      expect(opts.durable).toBe(false);
      expect(opts.eidFn).toBe(utils.randomId);
      expect(opts.encoder).toBe(resonateOpts.encoder);
      expect(opts.idempotencyKeyFn).toBe(utils.hash);
      expect(opts.shouldLock).toBe(false);
      expect(opts.pollFrequency).toBe(resonateOpts.pollFrequency);
      expect(opts.retryPolicy).toBe(resonateOpts.retryPolicy);
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
      resonate.options({ version: 1 }),
    );

    // middle options (overriden)
    expect(middle.durable).toBe(overrides.durable);
    expect(middle.eidFn).toBe(overrides.eidFn);
    expect(middle.encoder).toBe(overrides.encoder);
    expect(middle.idempotencyKeyFn).toBe(overrides.idempotencyKeyFn);
    expect(middle.shouldLock).toBe(overrides.shouldLock);
    expect(middle.pollFrequency).toBe(overrides.pollFrequency);
    expect(middle.retryPolicy).toBe(overrides.retryPolicy);
    expect(middle.tags).toEqual({ ...resonateOpts.tags, ...overrides.tags });
    expect(middle.timeout).toBe(overrides.timeout);

    // top and bottom options
    for (const opts of [top, bottom]) {
      expect(opts.durable).toBe(false);
      expect(opts.eidFn).toBe(utils.randomId);
      expect(opts.encoder).toBe(resonateOpts.encoder);
      expect(opts.idempotencyKeyFn).toBe(utils.hash);
      expect(opts.pollFrequency).toBe(resonateOpts.pollFrequency);
      expect(opts.retryPolicy).toBe(resonateOpts.retryPolicy);
      expect(opts.timeout).toBe(resonateOpts.timeout);
    }

    expect(top.version).toBe(1);
    expect(middle.version).toBeDefined();
    expect(bottom.version).toBeDefined();

    expect(top.shouldLock).toBe(true);
    expect(bottom.shouldLock).toBe(false);

    expect(top.tags).toEqual({ ...resonateOpts.tags, "resonate:invocation": "true" });
    expect(bottom.tags).toEqual(resonateOpts.tags);
  });
});
