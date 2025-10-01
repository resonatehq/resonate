import type { Func } from "./types";

export type RegistryItem = {
  name: string;
  func: Func;
  version: number;
};

export class Registry {
  private forward: Map<string, Record<number, RegistryItem>> = new Map();
  private reverse: Map<Func, RegistryItem> = new Map();

  add(func: Func, name?: string, version = 1): void {
    if (!(version > 0)) {
      throw new Error("provided version must be greater than zero");
    }

    if (name === undefined && func.name === "") {
      throw new Error("name required when registering an anonymous function");
    }
    const funcName = name ?? func.name;

    const existingByName = this.forward.get(funcName);
    if ((existingByName && existingByName[version] !== undefined) || this.reverse.has(func)) {
      throw new Error(`function ${funcName} already registered`);
    }

    const item: RegistryItem = { name: funcName, func, version };

    if (!existingByName) {
      this.forward.set(funcName, {});
    }
    this.forward.get(funcName)![version] = item;

    this.reverse.set(func, item);
  }

  get(func: string | Func, version = 0): RegistryItem {
    if (typeof func === "string") {
      const versions = this.forward.get(func);
      if (!versions) {
        throw new Error(`function ${func} not found in registry`);
      }

      // pick latest if version = 0
      const chosenVersion = version === 0 ? Math.max(...Object.keys(versions).map(Number)) : version;

      if (!(chosenVersion in versions)) {
        throw new Error(`function ${func} version ${version} not found in registry`);
      }

      return versions[chosenVersion];
    }
    const entry = this.reverse.get(func);
    if (!entry) {
      const fnName = func.name || "unknown";
      throw new Error(`function ${fnName} not found in registry`);
    }

    if (version !== 0 && entry.version !== version) {
      const fnName = func.name || "unknown";
      throw new Error(`function ${fnName} version ${version} not found in registry`);
    }

    return entry;
  }

  latest(func: string | Func, defaultVersion = 1): number {
    if (typeof func === "string") {
      const versions = this.forward.get(func);
      if (!versions) {
        return defaultVersion;
      }
      const versionNumbers = Object.keys(versions).map(Number);
      return versionNumbers.length > 0 ? Math.max(...versionNumbers) : defaultVersion;
    }
    const entry = this.reverse.get(func);
    if (!entry) {
      return defaultVersion;
    }
    return entry.version;
  }
}
