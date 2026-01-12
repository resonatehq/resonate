import { Registry } from "../src/registry";

describe("Registry", () => {
  let registry: Registry;

  beforeEach(() => {
    registry = new Registry();
  });

  test("register a function by name", () => {
    function foo() {}
    registry.add(foo);

    expect(registry.get(foo)).toMatchObject({ name: "foo", func: foo, version: 1 });
    expect(registry.get("foo")).toMatchObject({ name: "foo", func: foo, version: 1 });
  });

  test("register a function by reference", () => {
    function bar() {}
    registry.add(bar, "bar*", 2);

    expect(registry.get(bar)).toMatchObject({ name: "bar*", func: bar, version: 2 });
    expect(registry.get("bar*")).toMatchObject({ name: "bar*", func: bar, version: 2 });
  });

  test("throws when registering same name and version", () => {
    function baz() {}
    registry.add(baz, "baz*", 1);

    expect(() => registry.add(baz)).toThrow("Function 'baz' (version 1) is already registered");
    expect(() => registry.add(baz, "baz*")).toThrow("Function 'baz*' (version 1) is already registered");
  });

  test("register different functions under same name, different versions", () => {
    function qux1() {}
    function qux2() {}

    registry.add(qux1, "qux", 1);
    registry.add(qux2, "qux", 2);

    expect(registry.get("qux")).toMatchObject({ name: "qux", func: qux2, version: 2 });
    expect(registry.get(qux1)).toMatchObject({ name: "qux", func: qux1, version: 1 });
    expect(registry.get(qux2)).toMatchObject({ name: "qux", func: qux2, version: 2 });
  });

  test("register function under different names", () => {
    function foo() {}
    registry.add(foo, "foo1", 1);
    registry.add(foo, "foo2", 2);
    registry.add(foo, "foo3", 3);

    expect(registry.get(foo)).toMatchObject({ name: "foo3", func: foo, version: 3 });
    expect(registry.get("foo1")).toMatchObject({ name: "foo1", func: foo, version: 1 });
    expect(registry.get("foo2")).toMatchObject({ name: "foo2", func: foo, version: 2 });
    expect(registry.get("foo3")).toMatchObject({ name: "foo3", func: foo, version: 3 });
  });

  test("register function under different versions", () => {
    function foo() {}
    registry.add(foo, "", 1);
    registry.add(foo, "", 2);
    registry.add(foo, "", 3);

    expect(registry.get(foo)).toMatchObject({ name: "foo", func: foo, version: 3 });
    expect(registry.get(foo, 1)).toMatchObject({ name: "foo", func: foo, version: 1 });
    expect(registry.get(foo, 2)).toMatchObject({ name: "foo", func: foo, version: 2 });
    expect(registry.get(foo, 3)).toMatchObject({ name: "foo", func: foo, version: 3 });

    expect(registry.get("foo")).toMatchObject({ name: "foo", func: foo, version: 3 });
    expect(registry.get("foo", 1)).toMatchObject({ name: "foo", func: foo, version: 1 });
    expect(registry.get("foo", 2)).toMatchObject({ name: "foo", func: foo, version: 2 });
    expect(registry.get("foo", 3)).toMatchObject({ name: "foo", func: foo, version: 3 });
  });

  test("returns undefined if function not registered", () => {
    expect(registry.get("doesNotExist")).toBeUndefined();
  });

  test("throws if function registered with invalid version", () => {
    function foo() {}
    expect(() => registry.add(foo, "foo", 0)).toThrow("Function version must be greater than zero (0 provided)");
    expect(() => registry.add(foo, "foo", -1)).toThrow("Function version must be greater than zero (-1 provided)");
  });

  test("throws if function registered without name", () => {
    expect(() => registry.add(() => {})).toThrow("Function name is required");
  });
});
