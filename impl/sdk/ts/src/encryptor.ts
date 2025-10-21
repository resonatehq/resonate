import crypto from "node:crypto";
import type { Value } from "types";

export interface Encryptor {
  encrypt(plaintext: Value<string>): Value<string>;
  decrypt(ciphertext: Value<string> | undefined): Value<string> | undefined;
}

export class NoopEncryptor implements Encryptor {
  encrypt(plaintext: Value<string>): Value<string> {
    return plaintext;
  }

  decrypt(ciphertext: Value<string> | undefined): Value<string> | undefined {
    return ciphertext;
  }
}

export class AES256GCMEncryptor implements Encryptor {
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
