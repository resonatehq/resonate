import { base64Decode, base64Encode, isGeneratorFunction } from "../src/util.js";

describe("isGeneratorFunction", () => {
  // Basic generator functions
  test("should return true for basic generator function", () => {
    function* basicGenerator() {
      yield 1;
      yield 2;
    }
    expect(isGeneratorFunction(basicGenerator)).toBe(true);
  });

  test("should return true for anonymous generator function", () => {
    const anonymousGenerator = function* () {
      yield "hello";
    };
    expect(isGeneratorFunction(anonymousGenerator)).toBe(true);
  });

  test("should return true for arrow generator function", () => {
    // Note: Arrow functions can't be generators, but testing the concept
    const generatorExpression = function* () {
      yield 42;
    };
    expect(isGeneratorFunction(generatorExpression)).toBe(true);
  });

  // Async generator functions
  test("should return true for async generator function", () => {
    async function* asyncGenerator() {
      yield Promise.resolve(1);
      yield Promise.resolve(2);
    }
    expect(isGeneratorFunction(asyncGenerator)).toBe(true);
  });

  test("should return true for anonymous async generator", () => {
    const asyncAnonymous = async function* () {
      yield await Promise.resolve("async");
    };
    expect(isGeneratorFunction(asyncAnonymous)).toBe(true);
  });

  // Regular functions
  test("should return false for regular function", () => {
    function regularFunction() {
      return "not a generator";
    }
    expect(isGeneratorFunction(regularFunction)).toBe(false);
  });

  test("should return false for arrow function", () => {
    const arrowFunction = () => "arrow";
    expect(isGeneratorFunction(arrowFunction)).toBe(false);
  });

  test("should return false for async function", () => {
    async function asyncFunction() {
      return await Promise.resolve("async");
    }
    expect(isGeneratorFunction(asyncFunction)).toBe(false);
  });

  test("should return false for anonymous function", () => {
    const anonymous = () => "anonymous";
    expect(isGeneratorFunction(anonymous)).toBe(false);
  });

  // Built-in functions
  test("should return false for built-in functions", () => {
    expect(isGeneratorFunction(console.log)).toBe(false);
    expect(isGeneratorFunction(Math.max)).toBe(false);
    expect(isGeneratorFunction(Array.prototype.map)).toBe(false);
    expect(isGeneratorFunction(Object.keys)).toBe(false);
  });

  // Class methods
  test("should handle class methods correctly", () => {
    class TestClass {
      regularMethod() {
        return "regular";
      }

      *generatorMethod() {
        yield "generator";
      }

      async asyncMethod() {
        return "async";
      }

      async *asyncGeneratorMethod() {
        yield "async generator";
      }
    }

    const instance = new TestClass();
    expect(isGeneratorFunction(instance.regularMethod)).toBe(false);
    expect(isGeneratorFunction(instance.generatorMethod)).toBe(true);
    expect(isGeneratorFunction(instance.asyncMethod)).toBe(false);
    expect(isGeneratorFunction(instance.asyncGeneratorMethod)).toBe(true);
  });

  test("should handle functions created with Function constructor", () => {
    const dynamicFunction = new Function("return 42");
    const dynamicGenerator = new (Object.getPrototypeOf(function* () {}).constructor)("yield 42");

    expect(isGeneratorFunction(dynamicFunction)).toBe(false);
    expect(isGeneratorFunction(dynamicGenerator)).toBe(true);
  });
});

describe("base64 encoder", () => {
  const cases = [
    "【NEW LAUNCH】BUNDLING Scarlett Fragrance Brightening Body Serum 170ml & Scarlett Whitening Extrait De Parfum 30ml ( Velvet Rouge / Purple Kiss )  | Melembapkan mencerahkan meratakan warna kulit, Kulit cerah wangi mewah",
    "",

    // Emojis
    "Summer vibes 🌞🏖️🍹",
    "Best Seller 🚀🔥 #1",
    "Happy Birthday 🎉🎂🎁",

    // Non-ASCII (accents, umlauts, tildes)
    "Crème brûlée délicieuse",
    "¡Oferta increíble! Sólo hoy",
    "Übermäßig schön & großartig",

    // Asian characters
    "日本の化粧品 - 高品質スキンケア",
    "韩国产品 - 保湿美白精华液",
    "منتج جديد للعناية بالبشرة 🌙✨",

    // Mixed: emojis + multilingual
    "Glow Up ✨ | Belleza natural 🌸 | 피부 미백 🌿",
  ];

  test.each(cases.map((str) => [str]))("encodes and decodes correctly: %s", (string) => {
    expect(base64Decode(base64Encode(string))).toEqual(string);
  });
});
