export interface ICache<T> {
  get(key: string): T;
  set(key: string, value: T): void;
  has(key: string): boolean;
}
