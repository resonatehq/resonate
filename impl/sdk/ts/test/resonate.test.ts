import { describe, test, expect, jest } from "@jest/globals";

import * as async from "../lib/async";
import { Options } from "../lib/core/options";
import * as generator from "../lib/generator";

jest.setTimeout(10000);

function register(r: async.Resonate | generator.Resonate, n: string, f: () => unknown, o: Partial<Options> = {}) {
  if (r instanceof async.Resonate) {
    return r.register(n, f, o);
  } else {
    return r.register(
      n,
      function* () {
        return f();
      },
      o,
    );
  }
}

function schedule(
  r: async.Resonate | generator.Resonate,
  n: string,
  c: string,
  f: (...a: any[]) => unknown,
  ...a: any[]
) {
  if (r instanceof async.Resonate) {
    return r.schedule(n, c, f, ...a);
  } else {
    return r.schedule(
      n,
      c,
      function* (...a: any[]) {
        return f(...a);
      },
      ...a,
    );
  }
}

describe("Resonate", () => {
  for (const { name, module } of [
    { name: "async", module: async },
    { name: "generator", module: generator },
  ]) {
    describe(name, () => {
      test("Register without options", async () => {
        const resonate = new module.Resonate();

        register(resonate, "foo", () => "foo.1");
        register(resonate, "foo", () => "foo.2", { version: 2 });
        register(resonate, "bar", () => "bar.1", { version: 1 });
        register(resonate, "bar", () => "bar.2", { version: 2 });

        expect(await resonate.run("foo", "foo.0")).toBe("foo.2");
        expect(await resonate.run("foo", "foo.1", resonate.options({ version: 1 }))).toBe("foo.1");
        expect(await resonate.run("foo", "foo.2", resonate.options({ version: 2 }))).toBe("foo.2");

        expect(await resonate.run("bar", "bar.0")).toBe("bar.2");
        expect(await resonate.run("bar", "bar.1", resonate.options({ version: 1 }))).toBe("bar.1");
        expect(await resonate.run("bar", "bar.2", resonate.options({ version: 2 }))).toBe("bar.2");
      });

      test("Register with options", async () => {
        const resonate = new module.Resonate();

        register(resonate, "foo", () => "foo.1", resonate.options({ timeout: 1000 }));
        register(resonate, "foo", () => "foo.2", { version: 2 });
        register(resonate, "bar", () => "bar.1", { version: 1 });
        register(resonate, "bar", () => "bar.2", { version: 2 });

        expect(await resonate.run("foo", "foo.0")).toBe("foo.2");
        expect(await resonate.run("foo", "foo.1", resonate.options({ version: 1 }))).toBe("foo.1");
        expect(await resonate.run("foo", "foo.2", resonate.options({ version: 2 }))).toBe("foo.2");

        expect(await resonate.run("bar", "bar.0")).toBe("bar.2");
        expect(await resonate.run("bar", "bar.1", resonate.options({ version: 1 }))).toBe("bar.1");
        expect(await resonate.run("bar", "bar.2", resonate.options({ version: 2 }))).toBe("bar.2");
      });

      test("Register throws error", () => {
        const resonate = new module.Resonate();
        register(resonate, "foo", () => {});
        register(resonate, "foo", () => {}, { version: 2 });

        expect(() => register(resonate, "foo", () => {})).toThrow("Function foo version 1 already registered");
        expect(() => register(resonate, "foo", () => {}, { version: 1 })).toThrow(
          "Function foo version 1 already registered",
        );
        expect(() => register(resonate, "foo", () => {}, { version: 2 })).toThrow(
          "Function foo version 2 already registered",
        );
      });

      test("Register module", async () => {
        const resonate = new module.Resonate();

        if (resonate instanceof async.Resonate) {
          resonate.registerModule({
            foo() {
              return "foo";
            },
            bar() {
              return "bar";
            },
          });
        } else {
          resonate.registerModule({
            foo: function* () {
              return "foo";
            },
            bar: function* () {
              return "bar";
            },
          });
        }

        expect(await resonate.run("foo", "foo.0")).toBe("foo");
        expect(await resonate.run("bar", "bar.0")).toBe("bar");
      });

      test("Schedule", async () => {
        const resonate = new module.Resonate();

        const fooPromise = new Promise((resolve) => {
          schedule(resonate, "foo", "* * * * * *", () => resolve("foo"));
        });

        const barPromise = new Promise((resolve) => {
          schedule(resonate, "bar", "* * * * * *", (c: any, v: string) => resolve(v), "bar");
        });

        const bazPromise = new Promise((resolve) => {
          register(resonate, "baz", () => resolve("baz"));
          resonate.schedule("baz", "* * * * * *", "baz");
        });

        const quxPromise = new Promise((resolve) => {
          register(resonate, "qux", () => resolve("qux"), { version: 1 });
          resonate.schedule("qux", "* * * * * *", "qux", resonate.options({ version: 1 }));
        });

        const foo = await resonate.schedules.get("foo");
        const bar = await resonate.schedules.get("bar");
        const baz = await resonate.schedules.get("baz");
        const qux = await resonate.schedules.get("qux");

        resonate.start(0); // no delay for tests

        expect(await fooPromise).toBe("foo");
        expect(await barPromise).toBe("bar");
        expect(await bazPromise).toBe("baz");
        expect(await quxPromise).toBe("qux");

        resonate.stop();

        // delete the schedules in order to stop the local
        // store interval that creates promises
        await foo.delete();
        await bar.delete();
        await baz.delete();
        await qux.delete();
      });

      test("Schedule throws error", async () => {
        const resonate = new module.Resonate();

        expect(() => resonate.schedule("foo", "", "foo")).toThrow("Function foo version 0 not registered");
        expect(() => resonate.schedule("foo", "", "foo", resonate.options({ version: 1 }))).toThrow(
          "Function foo version 1 not registered",
        );
        expect(() => resonate.schedule("foo", "", "foo", resonate.options({ version: 2 }))).toThrow(
          "Function foo version 2 not registered",
        );

        register(resonate, "bar", () => {});
        expect(() => schedule(resonate, "bar", "", () => {})).toThrow("Function bar version 1 already registered");

        const baz = await schedule(resonate, "baz", "", () => {});
        expect(() => schedule(resonate, "baz", "", () => {})).toThrow("Function baz version 1 already registered");

        register(resonate, "qux", () => {});
        expect(() => resonate.schedule("qux", "x", "qux")).rejects.toThrow();
        expect(() => resonate.schedule("qux", "* * * * * * *", "qux")).rejects.toThrow();

        // delete the schedules in order to stop the local
        // store interval that creates promises
        await baz.delete();
      });
    });
  }
});
