import { describe, test, expect, jest } from "@jest/globals";
import { options } from "../lib/core/options";
import { Context, Resonate } from "../lib/resonate";

jest.setTimeout(10000);

describe("Resonate", () => {
  test("Register without options", async () => {
    const resonate = new Resonate();

    resonate.register("foo", () => "foo.1");
    resonate.register("foo", () => "foo.2", { version: 2 });
    resonate.register("bar", () => "bar.1", { version: 1 });
    resonate.register("bar", () => "bar.2", { version: 2 });

    expect(await resonate.run("foo", "foo.0")).toBe("foo.2");
    expect(await resonate.run("foo", "foo.1", options({ version: 1 }))).toBe("foo.1");
    expect(await resonate.run("foo", "foo.2", options({ version: 2 }))).toBe("foo.2");

    expect(await resonate.run("bar", "bar.0")).toBe("bar.2");
    expect(await resonate.run("bar", "bar.1", options({ version: 1 }))).toBe("bar.1");
    expect(await resonate.run("bar", "bar.2", options({ version: 2 }))).toBe("bar.2");
  });

  test("Register with options", async () => {
    const resonate = new Resonate();

    resonate.register("foo", () => "foo.1", options({ timeout: 1000 }));
    resonate.register("foo", () => "foo.2", { version: 2 });
    resonate.register("bar", () => "bar.1", { version: 1 });
    resonate.register("bar", () => "bar.2", { version: 2 });

    expect(await resonate.run("foo", "foo.0")).toBe("foo.2");
    expect(await resonate.run("foo", "foo.1", options({ version: 1 }))).toBe("foo.1");
    expect(await resonate.run("foo", "foo.2", options({ version: 2 }))).toBe("foo.2");

    expect(await resonate.run("bar", "bar.0")).toBe("bar.2");
    expect(await resonate.run("bar", "bar.1", options({ version: 1 }))).toBe("bar.1");
    expect(await resonate.run("bar", "bar.2", options({ version: 2 }))).toBe("bar.2");
  });

  test("Register throws error", () => {
    const resonate = new Resonate();
    resonate.register("foo", () => {});
    resonate.register("foo", () => {}, { version: 2 });

    expect(() => resonate.register("foo", () => {})).toThrow("Function foo version 1 already registered");
    expect(() => resonate.register("foo", () => {}, { version: 1 })).toThrow(
      "Function foo version 1 already registered",
    );
    expect(() => resonate.register("foo", () => {}, { version: 2 })).toThrow(
      "Function foo version 2 already registered",
    );
  });

  test("Register module", async () => {
    const resonate = new Resonate();

    resonate.registerModule({
      foo() {
        return "foo";
      },
      bar() {
        return "bar";
      },
    });

    expect(await resonate.run("foo", "foo.0")).toBe("foo");
    expect(await resonate.run("bar", "bar.0")).toBe("bar");
  });

  test("Schedule", async () => {
    const resonate = new Resonate();

    const fooPromise = new Promise((resolve) => {
      resonate.schedule("foo", "* * * * * *", () => resolve("foo"));
    });

    const barPromise = new Promise((resolve) => {
      resonate.schedule("bar", "* * * * * *", (c: Context, v: string) => resolve(v), "bar");
    });

    const bazPromise = new Promise((resolve) => {
      resonate.register("baz", () => resolve("baz"));
      resonate.schedule("baz", "* * * * * *", "baz");
    });

    const quxPromise = new Promise((resolve) => {
      resonate.register("qux", () => resolve("qux"), { version: 1 });
      resonate.schedule("qux", "* * * * * *", "qux", options({ version: 1 }));
    });

    const foo = await resonate.schedules.get("foo");
    const bar = await resonate.schedules.get("bar");
    const baz = await resonate.schedules.get("baz");
    const qux = await resonate.schedules.get("qux");

    resonate.start(1000); // no delay for tests

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
    const resonate = new Resonate();

    expect(resonate.schedule("foo", "", "foo")).rejects.toThrow("Function foo version 0 not registered");
    expect(resonate.schedule("foo", "", "foo", options({ version: 1 }))).rejects.toThrow(
      "Function foo version 1 not registered",
    );
    expect(resonate.schedule("foo", "", "foo", options({ version: 2 }))).rejects.toThrow(
      "Function foo version 2 not registered",
    );

    resonate.register("bar", () => {});
    expect(resonate.schedule("bar", "", () => {})).rejects.toThrow("Function bar version 1 already registered");

    const baz = await resonate.schedule("baz", "", () => {});
    expect(resonate.schedule("baz", "", () => {})).rejects.toThrow("Function baz version 1 already registered");

    resonate.register("qux", () => {});
    expect(resonate.schedule("qux", "x", "qux")).rejects.toThrow();
    expect(resonate.schedule("qux", "* * * * * * *", "qux")).rejects.toThrow();

    // delete the schedules in order to stop the local
    // store interval that creates promises
    await baz.delete();
  });
});
