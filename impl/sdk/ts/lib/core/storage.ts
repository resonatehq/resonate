import { DurablePromise } from "./promise";
import { Schedule } from "./schedule";

export interface IPromiseStorage {
  rmw<P extends DurablePromise | undefined>(id: string, f: (promise: DurablePromise | undefined) => P): Promise<P>;
  search(
    id: string,
    state: string | undefined,
    tags: Record<string, string> | undefined,
    limit: number | undefined,
  ): AsyncGenerator<DurablePromise[], void>;
}

export interface IScheduleStorage {
  rmw<S extends Schedule | undefined>(id: string, f: (schedule: Schedule | undefined) => S): Promise<S>;
  search(
    id: string,
    tags: Record<string, string> | undefined,
    limit: number | undefined,
  ): AsyncGenerator<Schedule[], void>;

  delete(id: string): Promise<boolean>;
}
