import type { Func } from "./types";

type Item = {
  name: string;
  func: Func;
};

export class Registry {
  public forward_registry: Map<string, Item> = new Map();
  public reverse_registry: Map<Func, Item> = new Map();

  get(nameOrFunc: string | Func): Item | undefined {
    return typeof nameOrFunc === "string"
      ? this.forward_registry.get(nameOrFunc)
      : this.reverse_registry.get(nameOrFunc);
  }

  has(nameOrFunc: string | Func): boolean {
    return typeof nameOrFunc === "string"
      ? this.forward_registry.has(nameOrFunc)
      : this.reverse_registry.has(nameOrFunc);
  }

  set(name: string, func: Func): void {
    this.forward_registry.set(name, { name, func });
    this.reverse_registry.set(func, { name, func });
  }

  keys(): IterableIterator<string> {
    return this.forward_registry.keys();
  }
}
