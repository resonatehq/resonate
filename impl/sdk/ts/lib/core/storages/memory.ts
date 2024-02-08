import { IStorage } from "../storage";

export class MemoryStorage<T> implements IStorage<T> {
  private items: Record<string, T> = {};

  async rmw<X extends T | undefined>(id: string, func: (item: T | undefined) => X): Promise<X> {
    const item = func(this.items[id]);
    if (item) {
      this.items[id] = item;
    }

    return item;
  }

  async rmd(id: string, func: (item: T) => boolean): Promise<boolean> {
    const item = this.items[id];
    let result = false;

    if (item && func(item)) {
      delete this.items[id];
      result = true;
    }
    return result;
  }

  async *all(): AsyncGenerator<T[], void> {
    yield Object.values(this.items);
  }
}
