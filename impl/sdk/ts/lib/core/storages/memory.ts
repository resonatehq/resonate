import { IPromiseStorage, IScheduleStorage } from "../storage";
import { DurablePromise, searchStates } from "../promise";
import { Schedule } from "../schedule";

export class MemoryPromiseStorage implements IPromiseStorage {
  private promises: Record<string, DurablePromise> = {};
  constructor() {}

  async rmw<P extends DurablePromise | undefined>(id: string, f: (item: DurablePromise | undefined) => P): Promise<P> {
    const item = f(this.promises[id]);
    if (item) {
      this.promises[id] = item;
    }

    return item;
  }

  async *search(
    id: string,
    state: string | undefined,
    tags: Record<string, string> | undefined,
    limit: number | undefined,
  ): AsyncGenerator<DurablePromise[], void> {
    // for the memory storage, we will ignore limit and return all
    // promises that match the search criteria
    const regex = new RegExp(id.replaceAll("*", ".*"));
    const states = searchStates(state);
    const tagEntries = Object.entries(tags ?? {});

    yield Object.values(this.promises)
      .filter((promise) => states.includes(promise.state))
      .filter((promise) => regex.test(promise.id))
      .filter((promise) => tagEntries.every(([k, v]) => promise.tags?.[k] == v));
  }
}

export class MemoryScheduleStorage implements IScheduleStorage {
  private schedules: Record<string, Schedule> = {};

  constructor() {}

  async rmw<S extends Schedule | undefined>(id: string, f: (item: Schedule | undefined) => S): Promise<S> {
    const item = f(this.schedules[id]);
    if (item) {
      this.schedules[id] = item;
    }

    return item;
  }

  async *search(
    id: string,
    tags: Record<string, string> | undefined,
    limit: number | undefined,
  ): AsyncGenerator<Schedule[], void> {
    // for the memory storage, we will ignore limit and return all
    // schedules that match the search criteria
    const regex = new RegExp(id.replaceAll("*", ".*"));
    const tagEntries = Object.entries(tags ?? {});

    yield Object.values(this.schedules)
      .filter((schedule) => regex.test(schedule.id))
      .filter((schedule) => tagEntries.every(([k, v]) => schedule.tags?.[k] == v));
  }

  async delete(id: string): Promise<boolean> {
    if (this.schedules[id]) {
      delete this.schedules[id];
      return true;
    }
    return false;
  }
}
