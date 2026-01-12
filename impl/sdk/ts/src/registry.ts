import exceptions from "./exceptions";
import type { Func } from "./types";

export type RegistryItem = {
  name: string;
  func: Func;
  version: number;
};

export class Registry {
  private forward: Map<string, Record<number, RegistryItem>> = new Map();
  private reverse: Map<Func, Record<number, RegistryItem>> = new Map();

  add(func: Func, name = "", version = 1): void {
    // version must be greater than zero
    if (!(version > 0)) {
      throw exceptions.REGISTRY_VERSION_INVALID(version);
    }

    // function must have a name
    if (name === "" && func.name === "") {
      throw exceptions.REGISTRY_NAME_REQUIRED();
    }

    const funcName = name || func.name;

    // function must not already be registered
    if (this.get(funcName, version)) {
      throw exceptions.REGISTRY_FUNCTION_ALREADY_REGISTERED(funcName, version);
    }
    if (this.get(func, version)) {
      throw exceptions.REGISTRY_FUNCTION_ALREADY_REGISTERED(func.name, version, this.get(func, version)?.name);
    }

    const forward = this.forward.get(funcName) ?? {};
    const reverse = this.reverse.get(func) ?? {};
    forward[version] = reverse[version] = { name: funcName, func, version };

    this.forward.set(funcName, forward);
    this.reverse.set(func, reverse);
  }

  get(func: string | Func, version = 0): RegistryItem | undefined {
    const registry = typeof func === "string" ? this.forward.get(func) : this.reverse.get(func);
    return registry?.[version > 0 ? version : this.latest(registry)];
  }

  private latest(registry: Record<number, RegistryItem>): number {
    return Math.max(...(Object.keys(registry ?? {}).map(Number) || [1]));
  }
}
