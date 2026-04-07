import type { Value } from "./network/types.js";

export interface Encryptor {
  encrypt(plaintext: Value): Value;
  decrypt(ciphertext: Value): Value;
}

export class NoopEncryptor implements Encryptor {
  encrypt(plaintext: Value): Value {
    return plaintext;
  }

  decrypt(ciphertext: Value): Value {
    return ciphertext;
  }
}
