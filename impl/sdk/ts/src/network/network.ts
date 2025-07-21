export interface DurablePromiseRecord {
  id: string;
  state: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout";
  timeout: number;
  param: any;
  value: any;
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
  promiseParam: any;
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

export interface Mesg {
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
}

// Request message types
export type CreatePromiseReq = {
  kind: "createPromise";
  id: string;
  timeout: number;
  param?: any;
  tags?: Record<string, string>;
  iKey?: string;
  strict?: boolean;
};

export type CreatePromiseAndTaskReq = {
  kind: "createPromiseAndTask";
  promise: {
    id: string;
    timeout: number;
    param?: any;
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

export type CompletePromiseReq = {
  kind: "completePromise";
  id: string;
  state: "resolved" | "rejected" | "rejected_canceled";
  value?: any;
  iKey?: string;
  strict?: boolean;
};

export type CreateCallbackReq = {
  kind: "createCallback";
  id: string;
  rootPromiseId: string;
  timeout: number;
  recv: string;
};

export type CreateSubscriptionReq = {
  kind: "createSubscription";
  id: string;
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
  promiseParam?: any;
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

// Union of all request types
export type RequestMsg =
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
  | HeartbeatTasksReq;

// Response message types
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
  kind: "claimedtask";
  message: Mesg;
};

export type CompleteTaskRes = {
  kind: "completedtask";
  task: TaskRecord;
};

export type DropTaskRes = {
  kind: "droppedtask";
};

export type HeartbeatTasksRes = {
  kind: "heartbeatTasks";
  tasksAffected: number;
};

// Error response types
export type ErrorRes = {
  kind: "error";
  code: "invalid_request" | "forbidden" | "not_found" | "conflict";
  message: string;
};

// Union of all response types
export type ResponseMsg =
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
  | ErrorRes;

export type RecvMsg =
  | { type: "invoke" | "resume"; task: TaskRecord }
  | { type: "notify"; promise: DurablePromiseRecord };

export interface Network {
  send(request: RequestMsg, callback: (timeout: boolean, response: ResponseMsg) => void): void;
  recv(msg: any): void;

  // Provided by the user of the network, interface
  onMessage?: (msg: RecvMsg) => void;
}
