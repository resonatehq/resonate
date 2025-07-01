import { DurablePromiseRecord } from "./network/network";

export class Store {
  public promises: PromiseStore;

  constructor() {
    this.promises = new PromiseStore(this);
  }
}

export class PromiseStore {
  private promises: Map<string, DurablePromiseRecord>;
  private store: Store;

  constructor(store: Store) {
    this.promises = new Map();
    this.store = store;
  }

  async create(
    id: string,
    timeout: number,
    ikey: string | undefined,
    strict: boolean = false,
    headers: Record<string, string> | undefined,
    data: any | undefined,
    tags: Record<string, string>,
  ): Promise<DurablePromiseRecord> {
    var record = this.promises.get(id);

    if (!record) {
      record = {
        id: id,
        state: "pending",
        timeout: timeout,
        param: {
          headers: headers,
          data: data,
        },
        value: {
          headers: undefined,
          data: undefined,
        },
        tags: tags,
        iKeyForCreate: ikey,
        createdOn: Date.now(),
      };
      this.promises.set(id, record);
      return record;
    }

    record = this.timeout(record);

    if (strict && !(record.state === "pending")) {
      throw new Error("Durable promise previously created");
    }
    if (record.iKeyForCreate === undefined || ikey !== record.iKeyForCreate) {
      throw new Error("missing idempotency key for create");
    }

    return record;
  }

  private timeout(record: DurablePromiseRecord): DurablePromiseRecord {
    if (record.state === "pending" && Date.now() >= record.timeout) {
      record = {
        id: record.id,
        state:
          record.tags?.["resonate:timeout"] === "true"
            ? "resolved"
            : "rejected_timedout",
        timeout: record.timeout,
        param: record.param,
        value: {
          headers: undefined,
          value: undefined,
        },
        createdOn: record.createdOn,
        completedOn: record.timeout,
        iKeyForCreate: record.iKeyForCreate,
        iKeyForComplete: undefined,
        tags: record.tags,
      };

      this.promises.set(record.id, record);
      return record;
    }
    return record;
  }

  private async complete(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: any | undefined,
    to: "resolved" | "rejected" | "rejected_canceled",
  ): Promise<DurablePromiseRecord> {
    var record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    record = this.timeout(record);

    if (record.state === "pending") {
      record = {
        id: id,
        state: to,
        timeout: record.timeout,
        param: record.param,
        value: {
          headers: headers,
          data: data,
        },
        createdOn: record.createdOn,
        completedOn: Date.now(),
        iKeyForCreate: record.iKeyForCreate,
        iKeyForComplete: ikey,
        tags: record.tags,
      };
      this.promises.set(id, record);
      return record;
    }

    if (strict && !(record.state === to)) {
      throw new Error("forbidden");
    }

    if (
      !(record.state === "rejected_timedout") &&
      (record.iKeyForComplete === undefined || ikey !== record.iKeyForComplete)
    ) {
      throw new Error("forbidden");
    }

    return record;
  }

  async resolve(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: any | undefined,
  ): Promise<DurablePromiseRecord> {
    return this.complete(id, ikey, strict, headers, data, "resolved");
  }

  async reject(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: any | undefined,
  ): Promise<DurablePromiseRecord> {
    return this.complete(id, ikey, strict, headers, data, "rejected");
  }

  async cancel(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: any | undefined,
  ): Promise<DurablePromiseRecord> {
    return this.complete(id, ikey, strict, headers, data, "rejected_canceled");
  }

  async get(id: string): Promise<DurablePromiseRecord> {
    const record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    return record;
  }
}
