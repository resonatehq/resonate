import crypto from "node:crypto";

import type { Value } from "types";
import { type Encryptor, NoopEncryptor } from "../src/encryptor";

export class DummyEncryptor implements Encryptor {
  private key: Buffer;

  constructor(secret: string) {
    this.key = crypto.createHash("sha256").update(secret).digest();
  }

  encrypt(plaintext: Value<string>): Value<string> {
    if (plaintext.data === undefined) return plaintext;

    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv("aes-256-gcm", this.key, iv);

    const encrypted = Buffer.concat([cipher.update(plaintext.data, "utf8"), cipher.final()]);
    const authTag = cipher.getAuthTag();
    const combined = Buffer.concat([iv, authTag, encrypted]);

    return { headers: plaintext.headers, data: combined.toString("base64") };
  }

  decrypt(ciphertext: Value<string> | undefined): Value<string> | undefined {
    if (ciphertext === undefined) return ciphertext;
    if (ciphertext.data === undefined) return ciphertext;

    const data = Buffer.from(ciphertext.data, "base64");
    const iv = data.subarray(0, 12);
    const authTag = data.subarray(12, 28);
    const encrypted = data.subarray(28);

    const decipher = crypto.createDecipheriv("aes-256-gcm", this.key, iv);
    decipher.setAuthTag(authTag);

    const decrypted = Buffer.concat([decipher.update(encrypted), decipher.final()]);

    return { headers: ciphertext.headers, data: decrypted.toString("utf8") };
  }
}

describe("Encryptors", () => {
  const encryptors: Encryptor[] = [new NoopEncryptor(), new DummyEncryptor("foo")];
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
