// mulberry32: a small, fast, seedable 32-bit generator. Same algorithm JoJo's
// simulator uses, so a seed means the same thing in both places.
export class Prng {
  private state: number;

  constructor(seed: number) {
    if (!Number.isInteger(seed) || seed < 0 || seed > 0xffffffff) {
      throw new RangeError("seed must be an unsigned 32-bit integer");
    }
    this.state = seed >>> 0;
  }

  /** A float in [0, 1). */
  next(): number {
    this.state = (this.state + 0x6d2b79f5) >>> 0;
    let value = Math.imul(this.state ^ (this.state >>> 15), this.state | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 0x100000000;
  }

  /** An integer in [0, bound). */
  int(bound: number): number {
    return Math.floor(this.next() * bound);
  }

  pick<T>(items: readonly T[]): T {
    return items[this.int(items.length)];
  }
}
