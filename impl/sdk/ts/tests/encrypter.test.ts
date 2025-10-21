import type { Value } from "types";
import { AES256GCMEncryptor, type Encryptor, NoopEncryptor } from "../src/encryptor";

describe("Encryptors", () => {
  const encryptors: Encryptor[] = [new AES256GCMEncryptor("foo"), new NoopEncryptor()];
  const plaintexts: Value<string>[] = [
    // Basic cases
    { headers: { foo: "bar" }, data: "Hello, world!" },
    { headers: { foo: "bar" }, data: "" },
    { headers: { foo: "bar" } },
    { data: "No headers here" },

    // Unicode and emoji
    { data: "😊" },
    { headers: { lang: "jp" }, data: "こんにちは世界" }, // Japanese
    { headers: { lang: "cn" }, data: "你好，世界" }, // Chinese
    { headers: { lang: "ar" }, data: "مرحبا بالعالم" }, // Arabic
    { headers: { lang: "emoji" }, data: "🔥💯🚀" },

    // Whitespace and edge formatting
    { data: "   " },
    { data: "\n\t\r" },
    { headers: {}, data: " leading and trailing " },

    // Long and random text
    {
      headers: { type: "long" },
      data: "A".repeat(10_000),
    },
    {
      headers: { type: "json" },
      data: JSON.stringify({ user: "Alice", role: "admin", active: true }),
    },

    // Complex headers
    {
      headers: { "x-custom-1": "αβγ", "x-custom-2": "©2025" },
      data: "Custom header data",
    },

    // Potential edge/binary-like content
    { data: "\u0000\u0001\u0002\u0003" },
    { headers: { encoding: "base64" }, data: Buffer.from("binarydata").toString("base64") },
  ];

  // Generate all combinations
  const cases = encryptors.flatMap((encryptor) => plaintexts.map((plaintext) => [encryptor, plaintext] as const));

  test.each(cases)("encrypts and decrypts back to original", (encryptor, plaintext) => {
    const encrypted = encryptor.encrypt(plaintext);
    const decrypted = encryptor.decrypt(encrypted);
    expect(decrypted?.data).toBe(plaintext.data);
    expect(decrypted?.headers).toEqual(plaintext.headers);
  });
});
