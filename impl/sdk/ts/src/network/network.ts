// Records

import type { ResonateError } from "../exceptions";
import type { Value } from "../types";

export interface DurablePromiseRecord<T = string> {
  id: string;
  state: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout";
  timeout: number;
  param?: Value<T>;
  value?: Value<T>;
  tags: Record<string, string>;
  iKeyForCreate?: string;
  iKeyForComplete?: string;
  createdOn?: number;
  completedOn?: number;
}

export interface ScheduleRecord {
  id: string;
  description?: string;
  cron: string;
  tags: Record<string, string>;
  promiseId: string;
  promiseTimeout: number;
  promiseParam?: Value<string>;
  promiseTags: Record<string, string>;
  iKey?: string;
  lastRunTime?: number;
  nextRunTime?: number;
  createdOn?: number;
}

export interface TaskRecord {
  id: string;
  rootPromiseId: string;
  counter: number;
  timeout: number;
  processId?: string;
  createdOn?: number;
  completedOn?: number;
}

export interface CallbackRecord {
  id: string;
  promiseId: string;
  timeout: number;
  createdOn?: number;
}

// Request

export type Request =
  | CreatePromiseReq
  | CreatePromiseAndTaskReq
  | ReadPromiseReq
  | CompletePromiseReq
  | CreateCallbackReq
  | CreateSubscriptionReq
  | CreateScheduleReq
  | ReadScheduleReq
  | DeleteScheduleReq
  | ClaimTaskReq
  | CompleteTaskReq
  | DropTaskReq
  | HeartbeatTasksReq
  | SearchPromisesReq;

export type CreatePromiseReq<T = string> = {
  kind: "createPromise";
  id: string;
  timeout: number;
  param?: Value<T>;
  tags?: Record<string, string>;
  iKey?: string;
  strict?: boolean;
};

export type CreatePromiseAndTaskReq<T = string> = {
  kind: "createPromiseAndTask";
  promise: {
    id: string;
    timeout: number;
    param?: Value<T>;
    tags?: Record<string, string>;
  };
  task: {
    processId: string;
    ttl: number;
  };
  iKey?: string;
  strict?: boolean;
};

export type ReadPromiseReq = {
  kind: "readPromise";
  id: string;
};

export type CompletePromiseReq<T = string> = {
  kind: "completePromise";
  id: string;
  state: "resolved" | "rejected" | "rejected_canceled";
  value?: Value<T>;
  iKey?: string;
  strict?: boolean;
};

export type CreateCallbackReq = {
  kind: "createCallback";
  promiseId: string;
  rootPromiseId: string;
  timeout: number;
  recv: string;
};

export type CreateSubscriptionReq = {
  kind: "createSubscription";
  id: string;
  promiseId: string;
  timeout: number;
  recv: string;
};

export type CreateScheduleReq = {
  kind: "createSchedule";
  id?: string;
  description?: string;
  cron?: string;
  tags?: Record<string, string>;
  promiseId?: string;
  promiseTimeout?: number;
  promiseParam?: Value<string>;
  promiseTags?: Record<string, string>;
  iKey?: string;
};

export type ReadScheduleReq = {
  kind: "readSchedule";
  id: string;
};

export type DeleteScheduleReq = {
  kind: "deleteSchedule";
  id: string;
};

export type ClaimTaskReq = {
  kind: "claimTask";
  id: string;
  counter: number;
  processId: string;
  ttl: number;
};

export type CompleteTaskReq = {
  kind: "completeTask";
  id: string;
  counter: number;
};

export type DropTaskReq = {
  kind: "dropTask";
  id: string;
  counter: number;
};

export type HeartbeatTasksReq = {
  kind: "heartbeatTasks";
  processId: string;
};

export type SearchPromisesReq = {
  kind: "searchPromises";
  id: string;
  state?: "pending" | "resolved" | "rejected";
  limit?: number;
  cursor?: string;
};

// Response

export type Response =
  | CreatePromiseRes
  | CreatePromiseAndTaskRes
  | ReadPromiseRes
  | CompletePromiseRes
  | CreateCallbackRes
  | CreateSubscriptionRes
  | CreateScheduleRes
  | ReadScheduleRes
  | DeleteScheduleRes
  | ClaimTaskRes
  | CompleteTaskRes
  | DropTaskRes
  | HeartbeatTasksRes
  | SearchPromisesRes;

export type CreatePromiseRes = {
  kind: "createPromise";
  promise: DurablePromiseRecord;
};

export type CreatePromiseAndTaskRes = {
  kind: "createPromiseAndTask";
  promise: DurablePromiseRecord;
  task?: TaskRecord;
};

export type ReadPromiseRes = {
  kind: "readPromise";
  promise: DurablePromiseRecord;
};

export type CompletePromiseRes = {
  kind: "completePromise";
  promise: DurablePromiseRecord;
};

export type CreateCallbackRes = {
  kind: "createCallback";
  callback?: CallbackRecord;
  promise: DurablePromiseRecord;
};

export type CreateSubscriptionRes = {
  kind: "createSubscription";
  callback?: CallbackRecord;
  promise: DurablePromiseRecord;
};

export type CreateScheduleRes = {
  kind: "createSchedule";
  schedule: ScheduleRecord;
};

export type ReadScheduleRes = {
  kind: "readSchedule";
  schedule: ScheduleRecord;
};

export type DeleteScheduleRes = {
  kind: "deleteSchedule";
};

export type ClaimTaskRes = {
  kind: "claimTask";
  message: {
    kind: "invoke" | "resume";
    promises: {
      root?: {
        id: string;
        data: DurablePromiseRecord;
      };
      leaf?: {
        id: string;
        data: DurablePromiseRecord;
      };
    };
  };
};

export type CompleteTaskRes = {
  kind: "completeTask";
  task: TaskRecord;
};

export type DropTaskRes = {
  kind: "dropTask";
};

export type HeartbeatTasksRes = {
  kind: "heartbeatTasks";
  tasksAffected: number;
};

export type SearchPromisesRes = {
  kind: "searchPromises";
  promises: DurablePromiseRecord[];
  cursor?: string;
};

// Message

export type Message =
  | { type: "invoke" | "resume"; task: TaskRecord }
  | { type: "notify"; promise: DurablePromiseRecord };

// Network

export type ResponseFor<T extends Request> = Extract<Response, { kind: T["kind"] }>;

export interface Network {
  send<T extends Request>(
    req: T,
    callback: (err?: ResonateError, res?: ResponseFor<T>) => void,
    retryForever?: boolean,
  ): void;
  stop(): void;
}

export interface MessageSource {
  recv(msg: Message): void;
  subscribe(type: "invoke" | "resume" | "notify", callback: (msg: Message) => void): void;
  stop(): void;
}
