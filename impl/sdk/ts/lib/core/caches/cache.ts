export class Cache<T> {
  cache: Record<string, T> = {};

  get(key: string): T {
    return this.cache[key];
  }

  set(key: string, value: T): void {
    this.cache[key] = value;
  }

  has(key: string): boolean {
    return key in this.cache;
  }
}
