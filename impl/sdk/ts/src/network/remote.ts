import type {
  CallbackRecord,
  ClaimTaskReq,
  CompletePromiseReq,
  CompleteTaskReq,
  CreateCallbackReq,
  CreatePromiseAndTaskReq,
  CreatePromiseReq,
  CreateScheduleReq,
  CreateSubscriptionReq,
  DeleteScheduleReq,
  DropTaskReq,
  DurablePromiseRecord,
  ErrorRes,
  HeartbeatTasksReq,
  Network,
  ReadPromiseReq,
  ReadScheduleReq,
  RecvMsg,
  RequestMsg,
  ResponseMsg,
  ScheduleRecord,
  TaskRecord,
} from "./network"; // Assuming types are in a separate file

import { EventSource } from "eventsource";
import * as util from "../util";

// API Value format from OpenAPI spec
interface ApiValue {
  headers?: Record<string, string>;
  data?: string;
}

// API Response Types
interface PromiseDto {
  id: string;
  state: string;
  timeout: number;
  param?: ApiValue;
  value?: ApiValue;
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
  promiseParam?: ApiValue;
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

export interface Encoder {
  encode(value: any): ApiValue;
  decode(apiValue: ApiValue | undefined): any;
}

export class JsonEncoder implements Encoder {
  inf = "__INF__";
  negInf = "__NEG_INF__";
  encode(value: any): ApiValue {
    // note about undefined:
    // undefined is not json serializable, so immediately return undefined
    if (value === undefined) {
      return {
        data: undefined,
      };
    }

    const jsonVal = JSON.stringify(value, (_, value) => {
      if (value === Number.POSITIVE_INFINITY) {
        return this.inf;
      }

      if (value === Number.NEGATIVE_INFINITY) {
        return this.negInf;
      }

      if (value instanceof AggregateError) {
        return {
          __type: "aggregate_error",
          message: value.message,
          stack: value.stack,
          name: value.name,
          errors: value.errors,
        };
      }

      if (value instanceof Error) {
        return {
          __type: "error",
          message: value.message,
          stack: value.stack,
          name: value.name,
        };
      }

      return value;
    });

    return {
      headers: {},
      data: btoa(jsonVal),
    };
  }

  decode(apiValue: ApiValue | undefined): any {
    if (!apiValue?.data) {
      return undefined;
    }

    const data = atob(apiValue.data);

    return JSON.parse(data, (_, value) => {
      if (value === this.inf) {
        return Number.POSITIVE_INFINITY;
      }

      if (value === this.negInf) {
        return Number.NEGATIVE_INFINITY;
      }

      if (value?.__type === "aggregate_error") {
        return Object.assign(new AggregateError(value.errors, value.message), value);
      }

      if (value?.__type === "error") {
        return Object.assign(new Error(value.message), value);
      }

      return value;
    });
  }
}

export interface HttpNetworkConfig {
  host: string;
  storePort: string;
  msgSrcPort: string;
  pid: string;
  group: string;
  timeout?: number;
  headers?: Record<string, string>;
  encoder?: Encoder;
}

export type Msg = { type: "invoke" | "resume"; task: TaskRecord } | { type: "notify"; promise: DurablePromiseRecord };

export class HttpNetwork implements Network {
  private url: string;
  private msgUrl: string;
  private timeout: number;
  private baseHeaders: Record<string, string>;
  private encoder: Encoder;
  private eventSource: EventSource;

  public onMessage?: (msg: Msg) => void;

  constructor(config: HttpNetworkConfig) {
    const { host, storePort, msgSrcPort, pid, group } = config;
    this.url = `${host}:${storePort}/`;
    this.msgUrl = new URL(`/${encodeURIComponent(group)}/${encodeURIComponent(pid)}`, `${host}:${msgSrcPort}`).href;
    console.log("poller:", this.msgUrl);
    this.timeout = config.timeout || 30 * util.SEC;

    this.baseHeaders = {
      "Content-Type": "application/json",
      ...config.headers,
    };
    this.encoder = config.encoder ?? new JsonEncoder();

    this.eventSource = new EventSource(this.msgUrl);
    this.eventSource.addEventListener("message", (event) => this.recv(event)); // TODO(avillega): Handle errors on the event source
  }

  send(request: RequestMsg, callback: (timeout: boolean, response: ResponseMsg) => void): void {
    this.handleRequest(request)
      .then((response) => callback(false, response))
      .catch((error) => {
        if (error.name === "TimeoutError") {
          callback(true, this.createErrorResponse("invalid_request", "Request timeout"));
        } else {
          callback(false, this.createErrorResponse("invalid_request", error.message));
        }
      });
  }

  recv(event: any): void {
    const e = event as MessageEvent;
    const data = JSON.parse(e.data);

    if ((data?.type === "invoke" || data?.type === "resume") && util.isTaskRecord(data?.task)) {
      this.onMessage?.({ type: data.type, task: data.task });
      return;
    }

    console.warn("couldn't parse", data, "as a message");
  }

  private async handleRequest(request: RequestMsg): Promise<ResponseMsg> {
    switch (request.kind) {
      case "createPromise":
        return this.createPromise(request);
      case "createPromiseAndTask":
        return this.createPromiseAndTask(request);
      case "readPromise":
        return this.readPromise(request);
      case "completePromise":
        return this.completePromise(request);
      case "createCallback":
        return this.createCallback(request);
      case "createSubscription":
        return this.createSubscription(request);
      case "createSchedule":
        return this.createSchedule(request);
      case "readSchedule":
        return this.readSchedule(request);
      case "deleteSchedule":
        return this.deleteSchedule(request);
      case "claimTask":
        return this.claimTask(request);
      case "completeTask":
        return this.completeTask(request);
      case "dropTask":
        return this.dropTask(request);
      case "heartbeatTasks":
        return this.heartbeatTasks(request);
      default:
        throw new Error(`Unsupported request kind: ${(request as any).kind}`);
    }
  }

  private async createPromise(req: CreatePromiseReq): Promise<ResponseMsg> {
    const headers: Record<string, string> = { ...this.baseHeaders };
    if (req.iKey) headers["idempotency-key"] = req.iKey;
    if (req.strict !== undefined) headers.strict = req.strict.toString();

    const body = {
      id: req.id,
      timeout: req.timeout,
      param: this.encoder.encode(req.param),
      tags: req.tags,
    };

    const response = await this.fetch("/promises", {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });

    const apiPromise = (await response.json()) as PromiseDto;
    const promise = this.mapPromiseDtoToRecord(apiPromise);
    return { kind: "createPromise", promise };
  }

  private async createPromiseAndTask(req: CreatePromiseAndTaskReq): Promise<ResponseMsg> {
    const headers: Record<string, string> = { ...this.baseHeaders };
    if (req.iKey) headers["idempotency-key"] = req.iKey;
    if (req.strict !== undefined) headers.strict = req.strict.toString();

    const body = {
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
    };

    const response = await this.fetch("/promises/task", {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });

    const data = (await response.json()) as CreatePromiseAndTaskResponseDto;
    return {
      kind: "createPromiseAndTask",
      promise: this.mapPromiseDtoToRecord(data.promise),
      task: data.task ? this.mapTaskDtoToRecord(data.task) : undefined,
    };
  }

  private async readPromise(req: ReadPromiseReq): Promise<ResponseMsg> {
    const response = await this.fetch(`/promises/${encodeURIComponent(req.id)}`, {
      method: "GET",
      headers: this.baseHeaders,
    });

    const apiPromise = (await response.json()) as PromiseDto;
    const promise = this.mapPromiseDtoToRecord(apiPromise);
    return { kind: "readPromise", promise };
  }

  private async completePromise(req: CompletePromiseReq): Promise<ResponseMsg> {
    const headers: Record<string, string> = { ...this.baseHeaders };
    if (req.iKey) headers["idempotency-key"] = req.iKey;
    if (req.strict !== undefined) headers.strict = req.strict.toString();

    const body = {
      state: req.state.toUpperCase(),
      value: this.encoder.encode(req.value),
    };

    const response = await this.fetch(`/promises/${encodeURIComponent(req.id)}`, {
      method: "PATCH",
      headers,
      body: JSON.stringify(body),
    });

    const apiPromise = (await response.json()) as PromiseDto;
    const promise = this.mapPromiseDtoToRecord(apiPromise);
    return { kind: "completePromise", promise };
  }

  private async createCallback(req: CreateCallbackReq): Promise<ResponseMsg> {
    const body = {
      rootPromiseId: req.rootPromiseId,
      timeout: req.timeout,
      recv: req.recv,
    };

    const response = await this.fetch(`/promises/callback/${encodeURIComponent(req.id)}`, {
      method: "POST",
      headers: this.baseHeaders,
      body: JSON.stringify(body),
    });

    const data = (await response.json()) as CallbackResponseDto;
    return {
      kind: "createCallback",
      callback: data.callback ? this.mapCallbackDtoToRecord(data.callback) : undefined,
      promise: this.mapPromiseDtoToRecord(data.promise),
    };
  }

  private async createSubscription(req: CreateSubscriptionReq): Promise<ResponseMsg> {
    const body = {
      id: req.id,
      timeout: req.timeout,
      recv: req.recv,
    };

    const response = await this.fetch(`/promises/subscribe/${encodeURIComponent(req.id)}`, {
      method: "POST",
      headers: this.baseHeaders,
      body: JSON.stringify(body),
    });

    const data = (await response.json()) as CallbackResponseDto;
    return {
      kind: "createSubscription",
      callback: data.callback ? this.mapCallbackDtoToRecord(data.callback) : undefined,
      promise: this.mapPromiseDtoToRecord(data.promise),
    };
  }

  private async createSchedule(req: CreateScheduleReq): Promise<ResponseMsg> {
    const headers: Record<string, string> = { ...this.baseHeaders };
    if (req.iKey) headers["idempotency-key"] = req.iKey;

    const body = {
      id: req.id,
      description: req.description,
      cron: req.cron,
      tags: req.tags,
      promiseId: req.promiseId,
      promiseTimeout: req.promiseTimeout,
      promiseParam: req.promiseParam,
      promiseTags: req.promiseTags,
    };

    const response = await this.fetch("/schedules", {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });

    const schedule = (await response.json()) as ScheduleDto;
    return {
      kind: "createSchedule",
      schedule: this.mapScheduleDtoToRecord(schedule),
    };
  }

  private async readSchedule(req: ReadScheduleReq): Promise<ResponseMsg> {
    const response = await this.fetch(`/schedules/${encodeURIComponent(req.id)}`, {
      method: "GET",
      headers: this.baseHeaders,
    });

    const schedule = (await response.json()) as ScheduleDto;
    return {
      kind: "readSchedule",
      schedule: this.mapScheduleDtoToRecord(schedule),
    };
  }

  private async deleteSchedule(req: DeleteScheduleReq): Promise<ResponseMsg> {
    await this.fetch(`/schedules/${encodeURIComponent(req.id)}`, {
      method: "DELETE",
      headers: this.baseHeaders,
    });

    return { kind: "deleteSchedule" };
  }

  private async claimTask(req: ClaimTaskReq): Promise<ResponseMsg> {
    const body = {
      id: req.id,
      counter: req.counter,
      processId: req.processId,
      ttl: req.ttl,
    };

    const response = await this.fetch("/tasks/claim", {
      method: "POST",
      headers: this.baseHeaders,
      body: JSON.stringify(body),
    });

    const message = (await response.json()) as ClaimTaskResponseDto;
    if (message.type !== "invoke" && message.type !== "resume") {
      throw new Error(`Unknown message type: ${message.type}`);
    }

    return {
      kind: "claimedtask",
      message: {
        kind: message.type,
        promises: {
          root: message.promises.root
            ? {
                id: message.promises.root.id,
                data: this.mapPromiseDtoToRecord(message.promises.root.data),
              }
            : undefined,
          leaf: message.promises.leaf
            ? {
                id: message.promises.leaf.id,
                data: this.mapPromiseDtoToRecord(message.promises.leaf.data),
              }
            : undefined,
        },
      },
    };
  }

  private async completeTask(req: CompleteTaskReq): Promise<ResponseMsg> {
    const body = {
      id: req.id,
      counter: req.counter,
    };

    const response = await this.fetch("/tasks/complete", {
      method: "POST",
      headers: this.baseHeaders,
      body: JSON.stringify(body),
    });

    const task = (await response.json()) as TaskDto;
    return {
      kind: "completedtask",
      task: this.mapTaskDtoToRecord(task),
    };
  }

  private async dropTask(req: DropTaskReq): Promise<ResponseMsg> {
    const body = {
      id: req.id,
      counter: req.counter,
    };

    await this.fetch("/tasks/drop", {
      method: "POST",
      headers: this.baseHeaders,
      body: JSON.stringify(body),
    });

    return { kind: "droppedtask" };
  }

  private async heartbeatTasks(req: HeartbeatTasksReq): Promise<ResponseMsg> {
    const body = {
      processId: req.processId,
    };

    const response = await this.fetch("/tasks/heartbeat", {
      method: "POST",
      headers: this.baseHeaders,
      body: JSON.stringify(body),
    });

    const data = (await response.json()) as HeartbeatResponseDto;
    return {
      kind: "heartbeatTasks",
      tasksAffected: data.tasksAffected,
    };
  }

  private async fetch(path: string, options: RequestInit): Promise<Response> {
    const url = `${this.url}${path}`;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);

    try {
      const response = await fetch(url, {
        ...options,
        signal: controller.signal,
      });

      if (!response.ok) {
        const errorData = (await response.json().catch(() => ({}))) as {
          message?: string;
        };
        throw new Error(errorData.message || `HTTP ${response.status}: ${response.statusText}`);
      }

      return response;
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") {
        const timeoutError = new Error("Request timeout");
        timeoutError.name = "TimeoutError";
        throw timeoutError;
      }
      throw error;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  private mapPromiseDtoToRecord(apiPromise: PromiseDto): DurablePromiseRecord {
    return {
      id: apiPromise.id,
      state: this.mapApiStateToInternal(apiPromise.state),
      timeout: apiPromise.timeout,
      param: this.encoder.decode(apiPromise.param),
      value: this.encoder.decode(apiPromise.value),
      tags: apiPromise.tags || {},
      iKeyForCreate: apiPromise.idempotencyKeyForCreate,
      iKeyForComplete: apiPromise.idempotencyKeyForComplete,
      createdOn: apiPromise.createdOn,
      completedOn: apiPromise.completedOn,
    };
  }

  private mapScheduleDtoToRecord(apiSchedule: ScheduleDto): ScheduleRecord {
    return {
      id: apiSchedule.id,
      description: apiSchedule.description,
      cron: apiSchedule.cron,
      tags: apiSchedule.tags || {},
      promiseId: apiSchedule.promiseId,
      promiseTimeout: apiSchedule.promiseTimeout,
      promiseParam: apiSchedule.promiseParam,
      promiseTags: apiSchedule.promiseTags || {},
      iKey: apiSchedule.idempotencyKey,
      lastRunTime: apiSchedule.lastRunTime,
      nextRunTime: apiSchedule.nextRunTime,
      createdOn: apiSchedule.createdOn,
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

  private mapApiStateToInternal(
    apiState: string,
  ): "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout" {
    switch (apiState) {
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
        throw new Error(`Unknown API state: ${apiState}`);
    }
  }

  private createErrorResponse(code: ErrorRes["code"], message: string): ErrorRes {
    return {
      kind: "error",
      code,
      message,
    };
  }
}
