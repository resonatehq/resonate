import { describe, test, expect, jest } from "@jest/globals";
import * as a from "../lib/async";
import { Base64Encoder } from "../lib/core/encoders/base64";
import { JSONEncoder } from "../lib/core/encoders/json";
import { Options } from "../lib/core/options";
import { Retry } from "../lib/core/retries/retry";
import * as utils from "../lib/core/utils";
import * as g from "../lib/generator";

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

function* gTest(ctx: g.Context, opts: Partial<Options> = {}) {
  const o: [Options, Options] = yield ctx.run(function* (ctx: g.Context) {
    return [
      ctx.opts,
      yield ctx.run(function* (ctx: g.Context) {
        return ctx.opts;
      }),
    ];
  }, ctx.options(opts));

  return [ctx.opts, ...o];
}

describe("Options", () => {
  const resonateOpts = {
    encoder: new JSONEncoder(),
    poll: 1000,
    retry: Retry.exponential(),
    tags: { a: "a", b: "b", c: "c" },
    timeout: 1000,
  };

  const overrides = {
    durable: false,
    eid: "eid",
    encoder: new Base64Encoder(),
    idempotencyKey: "idempotencyKey",
    lock: false,
    poll: 2000,
    retry: Retry.linear(),
    tags: { c: "x", d: "d", e: "e" },
    timeout: 2000,
    version: 2,
  };

  // Note: we are disabling durable for all tests here
  // so that value returned from the run is not serialized.

  const rA = new a.Resonate(resonateOpts);
  rA.register("test.1", aTest, { durable: false });
  rA.register("test.1", aTest, { durable: false, version: 2 });
  rA.register("test.2", aTest, overrides);

  const rG = new g.Resonate(resonateOpts);
  rG.register("test.1", gTest, { durable: false });
  rG.register("test.1", gTest, { durable: false, version: 2 });
  rG.register("test.2", gTest, overrides);

  for (const { name, resonate } of [
    { name: "async", resonate: rA },
    // { name: "generator", resonate: rG },
  ]) {
    describe(name, () => {
      test("resonate default options propagate down", async () => {
        const [top, middle, bottom] = await resonate.run<[Options, Options, Options]>(
          "test.1",
          `test.1.${name}.1`,
          resonate.options({ version: 1 }),
        );

        for (const opts of [top, middle, bottom]) {
          expect(opts.durable).toBe(false);
          expect(opts.eid).toBe(utils.randomId);
          expect(opts.encoder).toBe(resonateOpts.encoder);
          expect(opts.idempotencyKey).toBe(utils.hash);
          expect(opts.poll).toBe(resonateOpts.poll);
          expect(opts.retry).toBe(resonateOpts.retry);
          expect(opts.timeout).toBe(resonateOpts.timeout);
          expect(opts.version).toBe(1);
        }

        expect(top.lock).toBe(true);
        expect(middle.lock).toBe(false);
        expect(bottom.lock).toBe(false);

        expect(top.tags).toEqual({ ...resonateOpts.tags, "resonate:invocation": "true" });
        expect(middle.tags).toEqual(resonateOpts.tags);
        expect(bottom.tags).toEqual(resonateOpts.tags);
      });

      test("registered options propagate down", async () => {
        const [top, middle, bottom] = await resonate.run<[Options, Options, Options]>("test.2", `test.2.${name}.1`);

        for (const opts of [top, middle, bottom]) {
          expect(opts.durable).toBe(overrides.durable);
          expect(opts.eid).toBe(overrides.eid);
          expect(opts.encoder).toBe(overrides.encoder);
          expect(opts.idempotencyKey).toBe(overrides.idempotencyKey);
          expect(opts.lock).toBe(overrides.lock);
          expect(opts.poll).toBe(overrides.poll);
          expect(opts.retry).toBe(overrides.retry);
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
          `test.1.${name}.2`,
          resonate.options(overrides),
        );

        // Note: only some options are valid at the top level
        // this is because we would lose this information on the recovery path.

        // top level options
        expect(top.durable).toBe(false);
        expect(top.eid).toBe(utils.randomId);
        expect(top.encoder).toBe(resonateOpts.encoder);
        expect(top.idempotencyKey).toBe(overrides.idempotencyKey);
        expect(top.lock).toBe(true);
        expect(top.poll).toBe(resonateOpts.poll);
        expect(top.retry).toBe(resonateOpts.retry);
        expect(top.tags).toEqual({ ...resonateOpts.tags, ...overrides.tags, "resonate:invocation": "true" });
        expect(top.timeout).toBe(overrides.timeout);
        expect(top.version).toBe(overrides.version);

        // bottom level options
        for (const opts of bottom) {
          expect(opts.durable).toBe(false);
          expect(opts.eid).toBe(utils.randomId);
          expect(opts.encoder).toBe(resonateOpts.encoder);
          expect(opts.idempotencyKey).toBe(utils.hash);
          expect(opts.lock).toBe(false);
          expect(opts.poll).toBe(resonateOpts.poll);
          expect(opts.retry).toBe(resonateOpts.retry);
          expect(opts.tags).toEqual(resonateOpts.tags);
          expect(opts.timeout).toBe(resonateOpts.timeout);
          expect(opts.version).toBe(overrides.version);
        }
      });

      test("options passed to context run affect current level only", async () => {
        const [top, middle, bottom] = await resonate.run<[Options, Options, Options]>(
          "test.1",
          `test.1.${name}.3`,
          overrides,
          resonate.options({ version: 1 }),
        );

        // middle options (overriden)
        expect(middle.durable).toBe(overrides.durable);
        expect(middle.eid).toBe(overrides.eid);
        expect(middle.encoder).toBe(overrides.encoder);
        expect(middle.idempotencyKey).toBe(overrides.idempotencyKey);
        expect(middle.lock).toBe(overrides.lock);
        expect(middle.poll).toBe(overrides.poll);
        expect(middle.retry).toBe(overrides.retry);
        expect(middle.tags).toEqual({ ...resonateOpts.tags, ...overrides.tags });
        expect(middle.timeout).toBe(overrides.timeout);
        expect(middle.version).toBe(1);

        // top and bottom options
        for (const opts of [top, bottom]) {
          expect(opts.durable).toBe(false);
          expect(opts.eid).toBe(utils.randomId);
          expect(opts.encoder).toBe(resonateOpts.encoder);
          expect(opts.idempotencyKey).toBe(utils.hash);
          expect(opts.poll).toBe(resonateOpts.poll);
          expect(opts.retry).toBe(resonateOpts.retry);
          expect(opts.timeout).toBe(resonateOpts.timeout);
          expect(opts.version).toBe(1);
        }

        expect(top.lock).toBe(true);
        expect(bottom.lock).toBe(false);

        expect(top.tags).toEqual({ ...resonateOpts.tags, "resonate:invocation": "true" });
        expect(bottom.tags).toEqual(resonateOpts.tags);
      });
    });
  }
});
