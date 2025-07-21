import type { Func } from "./types";

export class Registry {
  private funcs: Map<string, Func> = new Map();

  get(id: string): Func | undefined {
    return this.funcs.get(id);
  }

  set(id: string, func: Func): void {
    this.funcs.set(id, func);
  }

  has(id: string): boolean {
    return this.funcs.has(id);
  }
}
