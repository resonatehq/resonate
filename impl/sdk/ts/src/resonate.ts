import { InnerRegisteredFunc, type InvocationHandler, ResonateInner } from "./resonate-inner";
import type { Func, Params, Ret } from "./types";

export interface RegisteredFunc<F extends Func> {
  options: () => void;
  beginRun: (id: string, ...args: Params<F>) => Promise<InvocationHandler<Ret<F>>>;
}

export class Resonate {
  private inner: ResonateInner;

  constructor(inner: ResonateInner) {
    this.inner = inner;
  }

  /**
   * Create a local Resonate instance
   */
  static local(): Resonate {
    return new Resonate(ResonateInner.local());
  }

  /**
   * Create a remote Resonate instance
   */
  static remote(config: {
    host?: string;
    storePort?: string;
    messageSourcePort?: string;
    group?: string;
    pid?: string;
    ttl?: number;
  }): Resonate {
    return new Resonate(ResonateInner.remote(config));
  }

  /**
   * Invoke a function and return a Promise
   */
  public async beginRun<T>(id: string, funcName: string, ...args: any[]): Promise<InvocationHandler<T>> {
    return new Promise<InvocationHandler<T>>((resolve, reject) => {
      try {
        this.inner.beginRun<T>(id, funcName, args, (invocationHandler: InvocationHandler<T>) => {
          resolve(invocationHandler);
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Register a function and return a Promise-based interface
   */
  public register<T extends Func>(name: string, func: T): RegisteredFunc<T> {
    const registeredFunc = this.inner.register(name, func);

    return {
      beginRun: (id: string, ...args: Params<T>): Promise<InvocationHandler<Ret<T>>> => {
        return new Promise<InvocationHandler<Ret<T>>>((resolve, reject) => {
          try {
            registeredFunc.beginRun(id, args, (invocationHandler: InvocationHandler<Ret<T>>) => {
              resolve(invocationHandler);
            });
          } catch (error) {
            reject(error);
          }
        });
      },
      options: registeredFunc.options,
    };
  }
}
