import { IEncoder } from "../encoder";
import { IScheduleStore } from "../store";
import { Schedule as ScheduleRecord } from "./types";

export type Options = {
  description: string | undefined;
  idempotencyKey: string | undefined;
  tags: Record<string, string> | undefined;
  promiseHeaders: Record<string, string>;
  promiseParam: unknown;
  promiseTags: Record<string, string> | undefined;
};

export class Schedule {
  constructor(
    private store: IScheduleStore,
    private encoder: IEncoder<unknown, string | undefined>,
    public schedule: ScheduleRecord,
  ) {}

  static async create(
    store: IScheduleStore,
    encoder: IEncoder<unknown, string | undefined>,
    id: string,
    cron: string,
    promiseId: string,
    promiseTimeout: number,
    opts: Partial<Options> = {},
  ): Promise<Schedule> {
    return new Schedule(
      store,
      encoder,
      await store.create(
        id,
        opts.idempotencyKey,
        opts.description,
        cron,
        opts.tags,
        promiseId,
        promiseTimeout,
        opts.promiseHeaders,
        encoder.encode(opts.promiseParam),
        opts.promiseTags,
      ),
    );
  }

  static async get(store: IScheduleStore, encoder: IEncoder<unknown, string | undefined>, id: string) {
    return new Schedule(store, encoder, await store.get(id));
  }

  static async *search(
    store: IScheduleStore,
    encoder: IEncoder<unknown, string | undefined>,
    id: string,
    tags?: Record<string, string>,
    limit?: number,
  ): AsyncGenerator<Schedule[], void> {
    for await (const schedules of store.search(id, tags, limit)) {
      yield schedules.map((s) => new Schedule(store, encoder, s));
    }
  }

  async delete() {
    await this.store.delete(this.schedule.id);
  }
}
