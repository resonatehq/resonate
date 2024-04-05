import { describe, test, expect, jest } from "@jest/globals";
import { Resonate, Context } from "../lib/async";
import { Retry } from "../lib/core/retries/retry";

jest.setTimeout(10000);

async function throwOrReturn(v: any) {
  if (v instanceof Error) {
    throw v;
  }

  return v;
}

describe("Combinators", () => {
  const resonate = new Resonate({
    retry: Retry.never(),
  });

  describe("all", () => {
    resonate.register("all", (c: Context, v: any[]) => c.all(v.map((v) => c.run(() => throwOrReturn(v)))));

    for (const { name, args } of [
      { name: "empty", args: [] },
      { name: "numbers", args: [1, 2, 3] },
      { name: "strings", args: ["a", "b", "c"] },
      { name: "mixed", args: [1, "b", {}] },
    ]) {
      test(`resolved: ${name}`, async () => {
        const r = await resonate.run("all", `all.resolved.${name}`, args);
        expect(r).toEqual(await Promise.all(args.map(throwOrReturn)));
        expect(r).toEqual(args);
      });
    }

    for (const { name, args } of [
      { name: "first", args: [new Error("1"), 2, 3] },
      { name: "middle", args: [1, new Error("2"), 3] },
      { name: "last", args: [1, 2, new Error("3")] },
    ]) {
      test(`rejected: ${name}`, async () => {
        const r = resonate.run("all", `all.rejected.${name}`, args);
        const e = await Promise.all(args.map(throwOrReturn)).catch((e) => e);
        expect(r).rejects.toThrow(e);
      });
    }
  });

  describe("any", () => {
    resonate.register("any", (c: Context, v: any[]) => c.any(v.map((v) => c.run(() => throwOrReturn(v)))));

    for (const { name, args } of [
      { name: "first", args: [new Error("1"), 2, 3] },
      { name: "middle", args: [1, new Error("2"), 3] },
      { name: "last", args: [1, 2, new Error("3")] },
    ]) {
      test(`resolved: ${name}`, async () => {
        const r = await resonate.run("any", `any.resolved.${name}`, args);
        expect(r).toEqual(await Promise.any(args.map(throwOrReturn)));
        expect(r).toEqual(args.find((v) => !(v instanceof Error)));
      });
    }

    for (const { name, args } of [
      { name: "empty", args: [] },
      { name: "one", args: [new Error("1")] },
      { name: "two", args: [new Error("1"), new Error("2")] },
    ]) {
      test(`rejected: ${name}`, async () => {
        const r = resonate.run("any", `any.rejected.${name}`, args);
        const e = await Promise.any(args.map(throwOrReturn)).catch((e) => e);
        expect(r).rejects.toThrow(e);
        expect(r).rejects.toBeInstanceOf(AggregateError);
      });
    }
  });

  describe("race", () => {
    resonate.register("race", (c: Context, v: any[]) => c.race(v.map((v) => c.run(() => throwOrReturn(v)))));

    for (const { name, args } of [
      { name: "one", args: [1] },
      { name: "two", args: [1, new Error("2")] },
      { name: "three", args: [1, 2, new Error("3")] },
    ]) {
      test(`resolved: ${name}`, async () => {
        const r = await resonate.run("race", `race.resolved.${name}`, args);
        expect(r).toEqual(await Promise.race(args.map(throwOrReturn)));
        expect(r).toEqual(args[0]);
      });
    }

    for (const { name, args } of [
      { name: "one", args: [new Error("1")] },
      { name: "two", args: [new Error("1"), 2] },
      { name: "three", args: [new Error("1"), 2, 3] },
    ]) {
      test(`rejected: ${name}`, async () => {
        const r = resonate.run("race", `race.rejected.${name}`, args);
        const e = await Promise.race(args.map(throwOrReturn)).catch((e) => e);
        expect(r).rejects.toThrow(e);
        expect(r).rejects.toThrow(args[0]);
      });
    }
  });

  describe("allSettled", () => {
    resonate.register("allSettled", (c: Context, v: any[]) =>
      c.allSettled(v.map((v) => c.run(() => throwOrReturn(v)))),
    );

    for (const { name, args } of [
      { name: "empty", args: [] },
      { name: "one", args: [1] },
      { name: "two", args: [1, new Error("2")] },
      { name: "three", args: [1, 2, new Error("3")] },
    ]) {
      test(`resolved: ${name}`, async () => {
        const r = await resonate.run("allSettled", `allSettled.resolved.${name}`, args);
        expect(r).toEqual(await Promise.allSettled(args.map(throwOrReturn)));
        expect(r).toEqual(
          args.map((a) =>
            a instanceof Error
              ? {
                  status: "rejected",
                  reason: a,
                }
              : {
                  status: "fulfilled",
                  value: a,
                },
          ),
        );
      });
    }
  });
});
