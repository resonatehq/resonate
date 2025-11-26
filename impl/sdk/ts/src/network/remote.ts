import { EventSource } from "eventsource";
import exceptions, { ResonateError, type ResonateServerError } from "../exceptions";
import type { Value } from "../types";
import * as util from "../util";
import type {
  CallbackRecord,
  ClaimTaskReq,
  ClaimTaskRes,
  CompletePromiseReq,
  CompletePromiseRes,
  CompleteTaskReq,
  CompleteTaskRes,
  CreateCallbackReq,
  CreateCallbackRes,
  CreatePromiseAndTaskReq,
  CreatePromiseAndTaskRes,
  CreatePromiseReq,
  CreatePromiseRes,
  CreateScheduleReq,
  CreateScheduleRes,
  CreateSubscriptionReq,
  CreateSubscriptionRes,
  DeleteScheduleReq,
  DeleteScheduleRes,
  DropTaskReq,
  DropTaskRes,
  DurablePromiseRecord,
  HeartbeatTasksReq,
  HeartbeatTasksRes,
  Message,
  MessageSource,
  Network,
  ReadPromiseReq,
  ReadPromiseRes,
  ReadScheduleReq,
  ReadScheduleRes,
  Request,
  Response,
  ResponseFor,
  ScheduleRecord,
  SearchPromisesReq,
  SearchPromisesRes,
  SearchSchedulesReq,
  SearchSchedulesRes,
  TaskRecord,
} from "./network";

// API Response Types
interface PromiseDto {
  id: string;
  state: string;
  timeout: number;
  param?: Value<string>;
  value?: Value<string>;
  tags?: Record<string, string>;
  idempotencyKeyForCreate?: string;
  idempotencyKeyForComplete?: string;
  createdOn?: number;
  completedOn?: number;
}

interface TaskDto {
  id: string;
  rootPromiseId: string;
  counter: number;
  timeout: number;
  processId: string;
  createdOn?: number;
  completedOn?: number;
}

interface CallbackDto {
  id: string;
  promiseId: string;
  timeout: number;
  createdOn?: number;
}

interface ScheduleDto {
  id: string;
  description?: string;
  cron: string;
  tags?: Record<string, string>;
  promiseId: string;
  promiseTimeout: number;
  promiseParam?: Value<string>;
  promiseTags?: Record<string, string>;
  idempotencyKey?: string;
  lastRunTime?: number;
  nextRunTime?: number;
  createdOn?: number;
}

interface CreatePromiseAndTaskResponseDto {
  promise: PromiseDto;
  task?: TaskDto;
}

interface CallbackResponseDto {
  callback?: CallbackDto;
  promise: PromiseDto;
}

interface ClaimTaskResponseDto {
  type: string;
  promises: {
    root?: {
      id: string;
      href: string;
      data: PromiseDto;
    };
    leaf?: {
      id: string;
      href: string;
      data: PromiseDto;
    };
  };
}

interface HeartbeatResponseDto {
  tasksAffected: number;
}
interface SearchPromisesResponseDto {
  promises: PromiseDto[];
  cursor: string;
}
interface SearchSchedulesResponseDto {
  schedules: ScheduleDto[];
  cursor: string;
}

export interface HttpNetworkConfig {
  verbose: boolean;
  url: string;
  auth?: { username: string; password: string };
  timeout?: number;
  headers?: Record<string, string>;
}

export interface HttpMessageSourceConfig {
  url: string;
  pid?: string;
  group: string;
  auth?: { username: string; password: string };
}

export type RetryPolicy = {
  retries?: number;
  delay?: number;
};

export class HttpNetwork implements Network {
  private EXCPECTED_RESONATE_VERSION = "0.7.15";

  private verbose: boolean;
  private url: string;
  private timeout: number;
  private headers: Record<string, string>;

  constructor({ verbose, url, auth, timeout = 30 * util.SEC, headers = {} }: HttpNetworkConfig) {
    this.verbose = verbose;
    this.url = url;
    this.timeout = timeout;

    this.headers = { "Content-Type": "application/json", ...headers };
    if (auth) {
      this.headers.Authorization = `Basic ${util.base64Encode(`${auth.username}:${auth.password}`)}`;
    }
  }

  send<T extends Request>(
    req: T,
    callback: (err?: ResonateError, res?: ResponseFor<T>) => void,
    headers: Record<string, string> = {},
    retryForever = false,
  ): void {
    const retryPolicy = retryForever ? { retries: Number.MAX_SAFE_INTEGER, delay: 1000 } : { retries: 0 };

    this.handleRequest(req, headers, retryPolicy).then(
      (res) => {
        util.assert(res.kind === req.kind, "res kind must match req kind");
        callback(undefined, res as ResponseFor<T>);
      },
      (err) => {
        callback(err as ResonateError);
      },
    );
  }

  public stop(): void {
    // No-op for HttpNetwork, MessageSource handles connection cleanup
  }

  private async handleRequest(
    req: Request,
    headers: Record<string, string>,
    retryPolicy: RetryPolicy = {},
  ): Promise<Response> {
    switch (req.kind) {
      case "createPromise":
        return this.createPromise(req, headers, retryPolicy);
      case "createPromiseAndTask":
        return this.createPromiseAndTask(req, headers, retryPolicy);
      case "readPromise":
        return this.readPromise(req, retryPolicy);
      case "completePromise":
        return this.completePromise(req, retryPolicy);
      case "createCallback":
        return this.createCallback(req, headers, retryPolicy);
      case "createSubscription":
        return this.createSubscription(req, retryPolicy);
      case "createSchedule":
        return this.createSchedule(req, retryPolicy);
      case "readSchedule":
        return this.readSchedule(req, retryPolicy);
      case "deleteSchedule":
        return this.deleteSchedule(req, retryPolicy);
      case "claimTask":
        return this.claimTask(req, retryPolicy);
      case "completeTask":
        return this.completeTask(req, retryPolicy);
      case "dropTask":
        return this.dropTask(req, retryPolicy);
      case "heartbeatTasks":
        return this.heartbeatTasks(req, retryPolicy);
      case "searchPromises":
        return this.searchPromises(req, retryPolicy);
      case "searchSchedules":
        return this.searchSchedules(req, retryPolicy);
    }
  }

  private async createPromise(
    req: CreatePromiseReq,
    headers: Record<string, string>,
    retryPolicy: RetryPolicy = {},
  ): Promise<CreatePromiseRes> {
    if (req.iKey) headers["idempotency-key"] = req.iKey;
    if (req.strict !== undefined) headers.strict = req.strict.toString();

    const res = await this.fetch(
      "/promises",
      {
        method: "POST",
        headers,
        body: JSON.stringify({
          id: req.id,
          timeout: req.timeout,
          param: req.param,
          tags: req.tags,
        }),
      },
      retryPolicy,
    );

    const promise = mapPromiseDtoToRecord((await res.json()) as PromiseDto);
    return { kind: "createPromise", promise };
  }

  private async createPromiseAndTask(
    req: CreatePromiseAndTaskReq,
    headers: Record<string, string>,
    retryPolicy: RetryPolicy = {},
  ): Promise<CreatePromiseAndTaskRes> {
    if (req.iKey) headers["idempotency-key"] = req.iKey;
    if (req.strict !== undefined) headers.strict = req.strict.toString();

    const res = await this.fetch(
      "/promises/task",
      {
        method: "POST",
        headers,
        body: JSON.stringify({
          promise: {
            id: req.promise.id,
            timeout: req.promise.timeout,
            param: req.promise.param,
            tags: req.promise.tags,
          },
          task: {
            processId: req.task.processId,
            ttl: req.task.ttl,
          },
        }),
      },
      retryPolicy,
    );

    const data = (await res.json()) as CreatePromiseAndTaskResponseDto;
    return {
      kind: "createPromiseAndTask",
      promise: mapPromiseDtoToRecord(data.promise),
      task: data.task ? this.mapTaskDtoToRecord(data.task) : undefined,
    };
  }

  private async readPromise(req: ReadPromiseReq, retryPolicy: RetryPolicy = {}): Promise<ReadPromiseRes> {
    const res = await this.fetch(`/promises/${encodeURIComponent(req.id)}`, { method: "GET" }, retryPolicy);

    const promise = mapPromiseDtoToRecord((await res.json()) as PromiseDto);
    return { kind: "readPromise", promise };
  }

  private async completePromise(req: CompletePromiseReq, retryPolicy: RetryPolicy = {}): Promise<CompletePromiseRes> {
    const headers: Record<string, string> = {};
    if (req.iKey) headers["idempotency-key"] = req.iKey;
    if (req.strict !== undefined) headers.strict = req.strict.toString();

    const res = await this.fetch(
      `/promises/${encodeURIComponent(req.id)}`,
      {
        method: "PATCH",
        headers,
        body: JSON.stringify({
          state: req.state.toUpperCase(),
          value: req.value,
        }),
      },
      retryPolicy,
    );

    const promise = mapPromiseDtoToRecord((await res.json()) as PromiseDto);
    return { kind: "completePromise", promise };
  }

  private async createCallback(
    req: CreateCallbackReq,
    headers: Record<string, string>,
    retryPolicy: RetryPolicy = {},
  ): Promise<CreateCallbackRes> {
    const res = await this.fetch(
      `/promises/callback/${encodeURIComponent(req.promiseId)}`,
      {
        method: "POST",
        headers,
        body: JSON.stringify({
          rootPromiseId: req.rootPromiseId,
          timeout: req.timeout,
          recv: req.recv,
        }),
      },
      retryPolicy,
    );

    const data = (await res.json()) as CallbackResponseDto;
    return {
      kind: "createCallback",
      callback: data.callback ? this.mapCallbackDtoToRecord(data.callback) : undefined,
      promise: mapPromiseDtoToRecord(data.promise),
    };
  }

  private async createSubscription(
    req: CreateSubscriptionReq,
    retryPolicy: RetryPolicy = {},
  ): Promise<CreateSubscriptionRes> {
    const body = {
      id: req.id,
      timeout: req.timeout,
      recv: req.recv,
    };

    const res = await this.fetch(
      `/promises/subscribe/${encodeURIComponent(req.promiseId)}`,
      {
        method: "POST",
        body: JSON.stringify(body),
      },
      retryPolicy,
    );

    const data = (await res.json()) as CallbackResponseDto;
    return {
      kind: "createSubscription",
      callback: data.callback ? this.mapCallbackDtoToRecord(data.callback) : undefined,
      promise: mapPromiseDtoToRecord(data.promise),
    };
  }

  private async createSchedule(req: CreateScheduleReq, retryPolicy: RetryPolicy = {}): Promise<CreateScheduleRes> {
    const headers: Record<string, string> = {};
    if (req.iKey) headers["idempotency-key"] = req.iKey;

    const res = await this.fetch(
      "/schedules",
      {
        method: "POST",
        headers,
        body: JSON.stringify({
          id: req.id,
          description: req.description,
          cron: req.cron,
          tags: req.tags,
          promiseId: req.promiseId,
          promiseTimeout: req.promiseTimeout,
          promiseParam: req.promiseParam,
          promiseTags: req.promiseTags,
        }),
      },
      retryPolicy,
    );

    return {
      kind: "createSchedule",
      schedule: this.mapScheduleDtoToRecord((await res.json()) as ScheduleDto),
    };
  }

  private async readSchedule(req: ReadScheduleReq, retryPolicy: RetryPolicy = {}): Promise<ReadScheduleRes> {
    const res = await this.fetch(`/schedules/${encodeURIComponent(req.id)}`, { method: "GET" }, retryPolicy);

    return {
      kind: "readSchedule",
      schedule: this.mapScheduleDtoToRecord((await res.json()) as ScheduleDto),
    };
  }

  private async deleteSchedule(req: DeleteScheduleReq, retryPolicy: RetryPolicy = {}): Promise<DeleteScheduleRes> {
    await this.fetch(`/schedules/${encodeURIComponent(req.id)}`, { method: "DELETE" }, retryPolicy);

    return { kind: "deleteSchedule" };
  }

  private async claimTask(req: ClaimTaskReq, retryPolicy: RetryPolicy = {}): Promise<ClaimTaskRes> {
    const res = await this.fetch(
      "/tasks/claim",
      {
        method: "POST",
        body: JSON.stringify({
          id: req.id,
          counter: req.counter,
          processId: req.processId,
          ttl: req.ttl,
        }),
      },
      retryPolicy,
    );

    const message = (await res.json()) as ClaimTaskResponseDto;

    if (message.type !== "invoke" && message.type !== "resume") {
      throw new Error(`Unknown message type: ${message.type}`);
    }

    return {
      kind: "claimTask",
      message: {
        kind: message.type,
        promises: {
          root: message.promises.root
            ? {
                id: message.promises.root.id,
                data: mapPromiseDtoToRecord(message.promises.root.data),
              }
            : undefined,
          leaf: message.promises.leaf
            ? {
                id: message.promises.leaf.id,
                data: mapPromiseDtoToRecord(message.promises.leaf.data),
              }
            : undefined,
        },
      },
    };
  }

  private async completeTask(req: CompleteTaskReq, retryPolicy: RetryPolicy = {}): Promise<CompleteTaskRes> {
    const res = await this.fetch(
      "/tasks/complete",
      {
        method: "POST",
        body: JSON.stringify({
          id: req.id,
          counter: req.counter,
        }),
      },
      retryPolicy,
    );

    return {
      kind: "completeTask",
      task: this.mapTaskDtoToRecord((await res.json()) as TaskDto),
    };
  }

  private async dropTask(req: DropTaskReq, retryPolicy: RetryPolicy = {}): Promise<DropTaskRes> {
    await this.fetch(
      "/tasks/drop",
      {
        method: "POST",
        body: JSON.stringify({
          id: req.id,
          counter: req.counter,
        }),
      },
      retryPolicy,
    );

    return { kind: "dropTask" };
  }

  private async heartbeatTasks(req: HeartbeatTasksReq, retryPolicy: RetryPolicy = {}): Promise<HeartbeatTasksRes> {
    const res = await this.fetch(
      "/tasks/heartbeat",
      {
        method: "POST",
        body: JSON.stringify({
          processId: req.processId,
        }),
      },
      retryPolicy,
    );

    const data = (await res.json()) as HeartbeatResponseDto;
    return {
      kind: "heartbeatTasks",
      tasksAffected: data.tasksAffected,
    };
  }

  private async searchPromises(req: SearchPromisesReq, retryPolicy: RetryPolicy = {}): Promise<SearchPromisesRes> {
    const params = new URLSearchParams({ id: req.id });
    if (req.state) params.append("state", req.state);
    if (req.limit) params.append("limit", String(req.limit));
    if (req.cursor) params.append("cursor", req.cursor);

    const res = await this.fetch(`/promises?${params.toString()}`, { method: "GET" }, retryPolicy);

    const data: any = (await res.json()) as SearchPromisesResponseDto;

    return { kind: "searchPromises", promises: data.promises, cursor: data.cursor };
  }

  private async searchSchedules(req: SearchSchedulesReq, retryPolicy: RetryPolicy = {}): Promise<SearchSchedulesRes> {
    const params = new URLSearchParams({ id: req.id });
    if (req.limit) params.append("limit", String(req.limit));
    if (req.cursor) params.append("cursor", req.cursor);
    const res = await this.fetch(`/schedules?${params.toString()}`, { method: "GET" }, retryPolicy);
    const data: any = (await res.json()) as SearchSchedulesResponseDto;
    return { kind: "searchSchedules", schedules: data.schedules, cursor: data.cursor };
  }

  private async fetch(
    path: string,
    init: RequestInit,
    { retries = 0, delay = 1000 }: RetryPolicy = {},
  ): Promise<globalThis.Response> {
    const url = `${this.url}${path}`;

    // add default headers
    init.headers = { ...this.headers, ...init.headers };

    for (let attempt = 0; attempt <= retries; attempt++) {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.timeout);

      try {
        const res = await fetch(url, { ...init, signal: controller.signal });
        const ver = res.headers.get("Resonate-Version") ?? "0.0.0";

        if (util.semverLessThan(ver, this.EXCPECTED_RESONATE_VERSION)) {
          console.warn(
            `Networking. Resonate server ${this.EXCPECTED_RESONATE_VERSION} or newer required (provided ${ver}). Will continue.`,
          );
        }

        if (!res.ok) {
          const err = (await res
            .json()
            .then((r: any) => r.error)
            .catch(() => undefined)) as ResonateServerError | undefined;
          throw exceptions.SERVER_ERROR(err ? err.message : res.statusText, res.status >= 500 && res.status < 600, err);
        }

        return res;
      } catch (err) {
        if (err instanceof ResonateError && !err.retriable) {
          throw err;
        }
        if (attempt >= retries) {
          throw exceptions.SERVER_ERROR(String(err));
        }

        console.warn(`Networking. Cannot connect to [${this.url}]. Retrying in ${delay / 1000}s.`);
        if (this.verbose) {
          console.warn(err);
        }

        // sleep before retrying
        await new Promise((resolve) => setTimeout(resolve, delay));
      } finally {
        clearTimeout(timeoutId);
      }
    }

    throw new Error("Fetch error");
  }

  private mapScheduleDtoToRecord(schedule: ScheduleDto): ScheduleRecord {
    return {
      id: schedule.id,
      description: schedule.description,
      cron: schedule.cron,
      tags: schedule.tags || {},
      promiseId: schedule.promiseId,
      promiseTimeout: schedule.promiseTimeout,
      promiseParam: schedule.promiseParam,
      promiseTags: schedule.promiseTags || {},
      iKey: schedule.idempotencyKey,
      lastRunTime: schedule.lastRunTime,
      nextRunTime: schedule.nextRunTime,
      createdOn: schedule.createdOn,
    };
  }

  private mapCallbackDtoToRecord(apiCallback: CallbackDto): CallbackRecord {
    return {
      id: apiCallback.id,
      promiseId: apiCallback.promiseId,
      timeout: apiCallback.timeout,
      createdOn: apiCallback.createdOn,
    };
  }

  private mapTaskDtoToRecord(apiTask: TaskDto): TaskRecord {
    return {
      id: apiTask.id,
      rootPromiseId: apiTask.rootPromiseId,
      counter: apiTask.counter,
      timeout: apiTask.timeout,
      processId: apiTask.processId,
      createdOn: apiTask.createdOn,
      completedOn: apiTask.completedOn,
    };
  }
}

export class HttpMessageSource implements MessageSource {
  readonly pid: string;
  readonly group: string;
  readonly unicast: string;
  readonly anycast: string;

  private url: string;
  private headers: Record<string, string>;
  private eventSource: EventSource;
  private subscriptions: {
    invoke: Array<(msg: Message) => void>;
    resume: Array<(msg: Message) => void>;
    notify: Array<(msg: Message) => void>;
  } = { invoke: [], resume: [], notify: [] };

  constructor({ url, pid = crypto.randomUUID().replace(/-/g, ""), group, auth }: HttpMessageSourceConfig) {
    this.pid = pid;
    this.group = group;
    this.unicast = `poll://uni@${group}/${pid}`;
    this.anycast = `poll://any@${group}/${pid}`;
    this.url = url;
    this.group = group;
    this.pid = pid;
    this.headers = {};
    if (auth) {
      this.headers.Authorization = `Basic ${util.base64Encode(`${auth.username}:${auth.password}`)}`;
    }

    this.eventSource = this.connect();
  }

  private connect() {
    const url = new URL(`/poll/${encodeURIComponent(this.group)}/${encodeURIComponent(this.pid)}`, `${this.url}`);
    this.eventSource = new EventSource(url, {
      fetch: (url, init) =>
        fetch(url, {
          ...init,
          headers: {
            ...init.headers,
            ...this.headers,
          },
        }),
    });

    this.eventSource.addEventListener("message", (event) => {
      let msg: Message;

      try {
        const data = JSON.parse(event.data);

        if ((data?.type === "invoke" || data?.type === "resume") && util.isTaskRecord(data?.task)) {
          msg = { type: data.type, task: data.task, headers: data.head ?? {} };
        } else if (data?.type === "notify" && util.isDurablePromiseRecord(data?.promise)) {
          msg = { type: data.type, promise: mapPromiseDtoToRecord(data.promise), headers: data.head ?? {} };
        } else {
          throw new Error("invalid message");
        }
      } catch (e) {
        console.warn("Networking. Received invalid message. Will continue.");
        return;
      }

      this.recv(msg);
    });

    this.eventSource.addEventListener("error", () => {
      // some browsers/runtimes may handle automatic reconnect
      // differently, so to ensure consistency close the eventsource
      // and recreate
      this.eventSource.close();

      console.warn(`Networking. Cannot connect to [${this.url}/poll]. Retrying in 5s.`);
      setTimeout(() => this.connect(), 5000);
    });

    return this.eventSource;
  }

  recv(msg: Message): void {
    for (const callback of this.subscriptions[msg.type]) {
      callback(msg);
    }
  }

  public stop(): void {
    this.eventSource.close();
  }

  public subscribe(type: "invoke" | "resume" | "notify", callback: (msg: Message) => void): void {
    this.subscriptions[type].push(callback);
  }
  match(target: string): string {
    return `poll://any@${target}`;
  }
}

function mapApiStateToInternal(
  state: string,
): "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout" {
  switch (state) {
    case "PENDING":
      return "pending";
    case "RESOLVED":
      return "resolved";
    case "REJECTED":
      return "rejected";
    case "REJECTED_CANCELED":
      return "rejected_canceled";
    case "REJECTED_TIMEDOUT":
      return "rejected_timedout";
    default:
      throw new Error(`Unknown API state: ${state}`);
  }
}

function mapPromiseDtoToRecord(promise: PromiseDto): DurablePromiseRecord {
  return {
    id: promise.id,
    state: mapApiStateToInternal(promise.state),
    timeout: promise.timeout,
    param: promise.param,
    value: promise.value,
    tags: promise.tags || {},
    iKeyForCreate: promise.idempotencyKeyForCreate,
    iKeyForComplete: promise.idempotencyKeyForComplete,
    createdOn: promise.createdOn,
    completedOn: promise.completedOn,
  };
}
