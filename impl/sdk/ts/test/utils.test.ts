import { describe, test, expect, jest } from "@jest/globals";
import { mergeObjects } from "../lib/core/utils";

jest.setTimeout(1000);

describe("mergeObjects", () => {
  test("merges two objects with non-overlapping keys", () => {
    const obj1 = { a: 1, b: 2 };
    const obj2 = { c: 3, d: 4 };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: 1, b: 2, c: 3, d: 4 });
  });

  test("prefers values from obj1 when keys overlap and neither is undefined", () => {
    const obj1 = { a: 1, b: 2 };
    const obj2 = { b: 3, c: 4 };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: 1, b: 2, c: 4 });
  });

  test("uses obj2 value when obj1 value is undefined", () => {
    const obj1 = { a: 1, b: undefined as number | undefined };
    const obj2 = { b: 2, c: 3 };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: 1, b: 2, c: 3 });
  });

  test("handles nested objects", () => {
    const obj1 = { a: { x: 1 }, b: 2 };
    const obj2 = { a: { y: 2 }, c: 3 };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: { x: 1 }, b: 2, c: 3 });
  });

  test("handles arrays", () => {
    const obj1 = { a: [1, 2], b: 2 };
    const obj2 = { a: [3, 4], c: 3 };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: [1, 2], b: 2, c: 3 });
  });

  test("handles empty objects", () => {
    const obj1 = {};
    const obj2 = { a: 1 };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: 1 });
  });

  test("handles objects with null values", () => {
    const obj1 = { a: null, b: 2 };
    const obj2 = { a: 1, c: null };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: null, b: 2, c: null });
  });
});
