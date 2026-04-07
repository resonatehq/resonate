// =============================================================================
// SHARED TYPES
// =============================================================================

export type Value = {
  headers?: Record<string, string>;
  data?: any;
};

// =============================================================================
// RECORDS
// =============================================================================

export type PromiseRecord = {
  id: string;
  state: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout";
  param: Value;
  value: Value;
  tags: Record<string, string>;
  timeoutAt: number;
  createdAt: number;
  settledAt?: number;
};

export type TaskRecord = {
  id: string;
  state: "pending" | "acquired" | "suspended" | "halted" | "fulfilled";
  version: number;
  resumes: string[] | number | boolean;
  ttl?: number;
  pid?: string;
};

export type ScheduleRecord = {
  id: string;
  cron: string;
  promiseId: string;
  promiseTimeout: number;
  promiseParam: Value;
  promiseTags: Record<string, string>;
  createdAt: number;
  nextRunAt: number;
  lastRunAt?: number;
};

// =============================================================================
// MESSAGES
// =============================================================================

export type MessageHead = { serverUrl?: string };

export type ExecuteMsg = {
  kind: "execute";
  head: MessageHead;
  data: { task: { id: string; version: number } };
};

export type UnblockMsg = {
  kind: "unblock";
  head: MessageHead;
  data: { promise: PromiseRecord };
};

export type Message = ExecuteMsg | UnblockMsg;

// =============================================================================
// REQUEST HEAD
// =============================================================================

export type RequestHead = {
  auth?: string;
  corrId: string;
  version: string;
  "resonate:debug_time"?: number;
};

// =============================================================================
// REQUESTS - PROMISE
// =============================================================================

export type PromiseGetReq = {
  kind: "promise.get";
  head: RequestHead;
  data: { id: string };
};

export type PromiseCreateReq = {
  kind: "promise.create";
  head: RequestHead;
  data: {
    id: string;
    timeoutAt: number;
    param: Value;
    tags: Record<string, string>;
  };
};

export type PromiseSettleReq = {
  kind: "promise.settle";
  head: RequestHead;
  data: {
    id: string;
    state: "resolved" | "rejected" | "rejected_canceled";
    value: Value;
  };
};

export type PromiseRegisterCallbackReq = {
  kind: "promise.register_callback";
  head: RequestHead;
  data: {
    awaited: string;
    awaiter: string;
  };
};

export type PromiseRegisterListenerReq = {
  kind: "promise.register_listener";
  head: RequestHead;
  data: {
    awaited: string;
    address: string;
  };
};

export type PromiseSearchReq = {
  kind: "promise.search";
  head: RequestHead;
  data: {
    state?: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout";
    tags?: Record<string, string>;
    limit?: number;
    cursor?: string;
  };
};

// =============================================================================
// REQUESTS - TASK
// =============================================================================

export type TaskGetReq = {
  kind: "task.get";
  head: RequestHead;
  data: { id: string };
};

export type TaskCreateReq = {
  kind: "task.create";
  head: RequestHead;
  data: {
    pid: string;
    ttl: number;
    action: PromiseCreateReq;
  };
};

export type TaskAcquireReq = {
  kind: "task.acquire";
  head: RequestHead;
  data: {
    id: string;
    version: number;
    pid: string;
    ttl: number;
  };
};

export type TaskReleaseReq = {
  kind: "task.release";
  head: RequestHead;
  data: {
    id: string;
    version: number;
  };
};

export type TaskSuspendReq = {
  kind: "task.suspend";
  head: RequestHead;
  data: {
    id: string;
    version: number;
    actions: PromiseRegisterCallbackReq[];
  };
};

export type TaskHaltReq = {
  kind: "task.halt";
  head: RequestHead;
  data: { id: string };
};

export type TaskContinueReq = {
  kind: "task.continue";
  head: RequestHead;
  data: { id: string };
};

export type TaskFulfillReq = {
  kind: "task.fulfill";
  head: RequestHead;
  data: {
    id: string;
    version: number;
    action: PromiseSettleReq;
  };
};

export type TaskFenceReq = {
  kind: "task.fence";
  head: RequestHead;
  data: {
    id: string;
    version: number;
    action: PromiseCreateReq | PromiseSettleReq;
  };
};

export type TaskHeartbeatReq = {
  kind: "task.heartbeat";
  head: RequestHead;
  data: {
    pid: string;
    tasks: { id: string; version: number }[];
  };
};

export type TaskSearchReq = {
  kind: "task.search";
  head: RequestHead;
  data: {
    state?: "pending" | "acquired" | "suspended" | "halted" | "fulfilled";
    limit?: number;
    cursor?: string;
  };
};

// =============================================================================
// REQUESTS - SCHEDULE
// =============================================================================

export type ScheduleGetReq = {
  kind: "schedule.get";
  head: RequestHead;
  data: { id: string };
};

export type ScheduleCreateReq = {
  kind: "schedule.create";
  head: RequestHead;
  data: {
    id: string;
    cron: string;
    promiseId: string;
    promiseTimeout: number;
    promiseParam: Value;
    promiseTags: Record<string, string>;
  };
};

export type ScheduleDeleteReq = {
  kind: "schedule.delete";
  head: RequestHead;
  data: { id: string };
};

export type ScheduleSearchReq = {
  kind: "schedule.search";
  head: RequestHead;
  data: {
    tags?: Record<string, string>;
    limit?: number;
    cursor?: string;
  };
};

// =============================================================================
// REQUESTS - DEBUG
// =============================================================================

export type DebugStartReq = {
  kind: "debug.start";
  head: RequestHead;
  data: Record<string, never>;
};

export type DebugResetReq = {
  kind: "debug.reset";
  head: RequestHead;
  data: Record<string, never>;
};

export type DebugTickReq = {
  kind: "debug.tick";
  head: RequestHead;
  data: { time: number };
};

export type DebugSnapReq = {
  kind: "debug.snap";
  head: RequestHead;
  data: Record<string, never>;
};

export type DebugStopReq = {
  kind: "debug.stop";
  head: RequestHead;
  data: Record<string, never>;
};

// =============================================================================
// REQUEST UNION
// =============================================================================

export type Request =
  | PromiseGetReq
  | PromiseCreateReq
  | PromiseSettleReq
  | PromiseRegisterCallbackReq
  | PromiseRegisterListenerReq
  | PromiseSearchReq
  | TaskGetReq
  | TaskCreateReq
  | TaskAcquireReq
  | TaskReleaseReq
  | TaskSuspendReq
  | TaskHaltReq
  | TaskContinueReq
  | TaskFulfillReq
  | TaskFenceReq
  | TaskHeartbeatReq
  | TaskSearchReq
  | ScheduleGetReq
  | ScheduleCreateReq
  | ScheduleDeleteReq
  | ScheduleSearchReq
  | DebugStartReq
  | DebugResetReq
  | DebugTickReq
  | DebugSnapReq
  | DebugStopReq;

// =============================================================================
// RESPONSE HEAD
// =============================================================================

export type ResponseHead<S extends number> = {
  corrId: string;
  status: S;
  version: string;
};

// =============================================================================
// RESPONSES - PROMISE
// =============================================================================

export type PromiseGetRes =
  | {
      kind: "promise.get";
      head: ResponseHead<200>;
      data: { promise: PromiseRecord };
    }
  | { kind: "promise.get"; head: ResponseHead<400>; data: string }
  | { kind: "promise.get"; head: ResponseHead<401>; data: string }
  | { kind: "promise.get"; head: ResponseHead<403>; data: string }
  | { kind: "promise.get"; head: ResponseHead<404>; data: string }
  | { kind: "promise.get"; head: ResponseHead<429>; data: string }
  | { kind: "promise.get"; head: ResponseHead<500>; data: string };

export type PromiseCreateRes =
  | {
      kind: "promise.create";
      head: ResponseHead<200>;
      data: { promise: PromiseRecord };
    }
  | { kind: "promise.create"; head: ResponseHead<400>; data: string }
  | { kind: "promise.create"; head: ResponseHead<401>; data: string }
  | { kind: "promise.create"; head: ResponseHead<403>; data: string }
  | { kind: "promise.create"; head: ResponseHead<429>; data: string }
  | { kind: "promise.create"; head: ResponseHead<500>; data: string };

export type PromiseSettleRes =
  | {
      kind: "promise.settle";
      head: ResponseHead<200>;
      data: { promise: PromiseRecord };
    }
  | { kind: "promise.settle"; head: ResponseHead<400>; data: string }
  | { kind: "promise.settle"; head: ResponseHead<401>; data: string }
  | { kind: "promise.settle"; head: ResponseHead<403>; data: string }
  | { kind: "promise.settle"; head: ResponseHead<404>; data: string }
  | { kind: "promise.settle"; head: ResponseHead<429>; data: string }
  | { kind: "promise.settle"; head: ResponseHead<500>; data: string };

export type PromiseRegisterCallbackRes =
  | {
      kind: "promise.register_callback";
      head: ResponseHead<200>;
      data: { promise: PromiseRecord };
    }
  | { kind: "promise.register_callback"; head: ResponseHead<400>; data: string }
  | { kind: "promise.register_callback"; head: ResponseHead<401>; data: string }
  | { kind: "promise.register_callback"; head: ResponseHead<403>; data: string }
  | { kind: "promise.register_callback"; head: ResponseHead<404>; data: string }
  | { kind: "promise.register_callback"; head: ResponseHead<422>; data: string }
  | { kind: "promise.register_callback"; head: ResponseHead<429>; data: string }
  | { kind: "promise.register_callback"; head: ResponseHead<500>; data: string };

export type PromiseRegisterListenerRes =
  | {
      kind: "promise.register_listener";
      head: ResponseHead<200>;
      data: { promise: PromiseRecord };
    }
  | { kind: "promise.register_listener"; head: ResponseHead<400>; data: string }
  | { kind: "promise.register_listener"; head: ResponseHead<401>; data: string }
  | { kind: "promise.register_listener"; head: ResponseHead<403>; data: string }
  | { kind: "promise.register_listener"; head: ResponseHead<404>; data: string }
  | { kind: "promise.register_listener"; head: ResponseHead<429>; data: string }
  | { kind: "promise.register_listener"; head: ResponseHead<500>; data: string }
  | { kind: "promise.register_listener"; head: ResponseHead<501>; data: string };

export type PromiseSearchRes =
  | {
      kind: "promise.search";
      head: ResponseHead<200>;
      data: { promises: PromiseRecord[]; cursor?: string };
    }
  | { kind: "promise.search"; head: ResponseHead<400>; data: string }
  | { kind: "promise.search"; head: ResponseHead<401>; data: string }
  | { kind: "promise.search"; head: ResponseHead<403>; data: string }
  | { kind: "promise.search"; head: ResponseHead<429>; data: string }
  | { kind: "promise.search"; head: ResponseHead<500>; data: string }
  | { kind: "promise.search"; head: ResponseHead<501>; data: string };

// =============================================================================
// RESPONSES - TASK
// =============================================================================

export type TaskGetRes =
  | { kind: "task.get"; head: ResponseHead<200>; data: { task: TaskRecord } }
  | { kind: "task.get"; head: ResponseHead<400>; data: string }
  | { kind: "task.get"; head: ResponseHead<401>; data: string }
  | { kind: "task.get"; head: ResponseHead<403>; data: string }
  | { kind: "task.get"; head: ResponseHead<404>; data: string }
  | { kind: "task.get"; head: ResponseHead<429>; data: string }
  | { kind: "task.get"; head: ResponseHead<500>; data: string };

export type TaskCreateRes =
  | {
      kind: "task.create";
      head: ResponseHead<200>;
      data: { task?: TaskRecord; promise: PromiseRecord; preload: PromiseRecord[] };
    }
  | { kind: "task.create"; head: ResponseHead<400>; data: string }
  | { kind: "task.create"; head: ResponseHead<401>; data: string }
  | { kind: "task.create"; head: ResponseHead<403>; data: string }
  | { kind: "task.create"; head: ResponseHead<409>; data: string }
  | { kind: "task.create"; head: ResponseHead<429>; data: string }
  | { kind: "task.create"; head: ResponseHead<500>; data: string }
  | { kind: "task.create"; head: ResponseHead<501>; data: string };

export type TaskAcquireRes =
  | {
      kind: "task.acquire";
      head: ResponseHead<200>;
      data: { task: TaskRecord; promise: PromiseRecord; preload: PromiseRecord[] };
    }
  | { kind: "task.acquire"; head: ResponseHead<400>; data: string }
  | { kind: "task.acquire"; head: ResponseHead<401>; data: string }
  | { kind: "task.acquire"; head: ResponseHead<403>; data: string }
  | { kind: "task.acquire"; head: ResponseHead<404>; data: string }
  | { kind: "task.acquire"; head: ResponseHead<409>; data: string }
  | { kind: "task.acquire"; head: ResponseHead<429>; data: string }
  | { kind: "task.acquire"; head: ResponseHead<500>; data: string };

export type TaskReleaseRes =
  | {
      kind: "task.release";
      head: ResponseHead<200>;
      data: Record<string, never>;
    }
  | { kind: "task.release"; head: ResponseHead<400>; data: string }
  | { kind: "task.release"; head: ResponseHead<401>; data: string }
  | { kind: "task.release"; head: ResponseHead<403>; data: string }
  | { kind: "task.release"; head: ResponseHead<404>; data: string }
  | { kind: "task.release"; head: ResponseHead<409>; data: string }
  | { kind: "task.release"; head: ResponseHead<429>; data: string }
  | { kind: "task.release"; head: ResponseHead<500>; data: string };

export type TaskSuspendRes =
  | {
      kind: "task.suspend";
      head: ResponseHead<200>;
      data: Record<string, never>;
    }
  | {
      kind: "task.suspend";
      head: ResponseHead<300>;
      data: { preload: PromiseRecord[] };
    }
  | { kind: "task.suspend"; head: ResponseHead<400>; data: string }
  | { kind: "task.suspend"; head: ResponseHead<401>; data: string }
  | { kind: "task.suspend"; head: ResponseHead<403>; data: string }
  | { kind: "task.suspend"; head: ResponseHead<404>; data: string }
  | { kind: "task.suspend"; head: ResponseHead<409>; data: string }
  | { kind: "task.suspend"; head: ResponseHead<422>; data: string }
  | { kind: "task.suspend"; head: ResponseHead<429>; data: string }
  | { kind: "task.suspend"; head: ResponseHead<500>; data: string };

export type TaskHaltRes =
  | {
      kind: "task.halt";
      head: ResponseHead<200>;
      data: Record<string, never>;
    }
  | { kind: "task.halt"; head: ResponseHead<400>; data: string }
  | { kind: "task.halt"; head: ResponseHead<401>; data: string }
  | { kind: "task.halt"; head: ResponseHead<403>; data: string }
  | { kind: "task.halt"; head: ResponseHead<404>; data: string }
  | { kind: "task.halt"; head: ResponseHead<409>; data: string }
  | { kind: "task.halt"; head: ResponseHead<429>; data: string }
  | { kind: "task.halt"; head: ResponseHead<500>; data: string };

export type TaskContinueRes =
  | {
      kind: "task.continue";
      head: ResponseHead<200>;
      data: Record<string, never>;
    }
  | { kind: "task.continue"; head: ResponseHead<400>; data: string }
  | { kind: "task.continue"; head: ResponseHead<401>; data: string }
  | { kind: "task.continue"; head: ResponseHead<403>; data: string }
  | { kind: "task.continue"; head: ResponseHead<404>; data: string }
  | { kind: "task.continue"; head: ResponseHead<409>; data: string }
  | { kind: "task.continue"; head: ResponseHead<429>; data: string }
  | { kind: "task.continue"; head: ResponseHead<500>; data: string };

export type TaskFulfillRes =
  | {
      kind: "task.fulfill";
      head: ResponseHead<200>;
      data: { promise: PromiseRecord };
    }
  | { kind: "task.fulfill"; head: ResponseHead<400>; data: string }
  | { kind: "task.fulfill"; head: ResponseHead<401>; data: string }
  | { kind: "task.fulfill"; head: ResponseHead<403>; data: string }
  | { kind: "task.fulfill"; head: ResponseHead<404>; data: string }
  | { kind: "task.fulfill"; head: ResponseHead<409>; data: string }
  | { kind: "task.fulfill"; head: ResponseHead<429>; data: string }
  | { kind: "task.fulfill"; head: ResponseHead<500>; data: string };

export type TaskFenceRes =
  | {
      kind: "task.fence";
      head: ResponseHead<200>;
      data: { action: PromiseCreateRes | PromiseSettleRes; preload: PromiseRecord[] };
    }
  | { kind: "task.fence"; head: ResponseHead<400>; data: string }
  | { kind: "task.fence"; head: ResponseHead<401>; data: string }
  | { kind: "task.fence"; head: ResponseHead<403>; data: string }
  | { kind: "task.fence"; head: ResponseHead<404>; data: string }
  | { kind: "task.fence"; head: ResponseHead<409>; data: string }
  | { kind: "task.fence"; head: ResponseHead<429>; data: string }
  | { kind: "task.fence"; head: ResponseHead<500>; data: string };

export type TaskHeartbeatRes =
  | {
      kind: "task.heartbeat";
      head: ResponseHead<200>;
      data: Record<string, never>;
    }
  | { kind: "task.heartbeat"; head: ResponseHead<400>; data: string }
  | { kind: "task.heartbeat"; head: ResponseHead<401>; data: string }
  | { kind: "task.heartbeat"; head: ResponseHead<403>; data: string }
  | { kind: "task.heartbeat"; head: ResponseHead<429>; data: string }
  | { kind: "task.heartbeat"; head: ResponseHead<500>; data: string };

export type TaskSearchRes =
  | {
      kind: "task.search";
      head: ResponseHead<200>;
      data: { tasks: TaskRecord[]; cursor?: string };
    }
  | { kind: "task.search"; head: ResponseHead<400>; data: string }
  | { kind: "task.search"; head: ResponseHead<401>; data: string }
  | { kind: "task.search"; head: ResponseHead<403>; data: string }
  | { kind: "task.search"; head: ResponseHead<429>; data: string }
  | { kind: "task.search"; head: ResponseHead<500>; data: string }
  | { kind: "task.search"; head: ResponseHead<501>; data: string };

// =============================================================================
// RESPONSES - SCHEDULE
// =============================================================================

export type ScheduleGetRes =
  | {
      kind: "schedule.get";
      head: ResponseHead<200>;
      data: { schedule: ScheduleRecord };
    }
  | { kind: "schedule.get"; head: ResponseHead<400>; data: string }
  | { kind: "schedule.get"; head: ResponseHead<401>; data: string }
  | { kind: "schedule.get"; head: ResponseHead<403>; data: string }
  | { kind: "schedule.get"; head: ResponseHead<404>; data: string }
  | { kind: "schedule.get"; head: ResponseHead<429>; data: string }
  | { kind: "schedule.get"; head: ResponseHead<500>; data: string }
  | { kind: "schedule.get"; head: ResponseHead<501>; data: string };

export type ScheduleCreateRes =
  | {
      kind: "schedule.create";
      head: ResponseHead<200>;
      data: { schedule: ScheduleRecord };
    }
  | { kind: "schedule.create"; head: ResponseHead<400>; data: string }
  | { kind: "schedule.create"; head: ResponseHead<401>; data: string }
  | { kind: "schedule.create"; head: ResponseHead<403>; data: string }
  | { kind: "schedule.create"; head: ResponseHead<429>; data: string }
  | { kind: "schedule.create"; head: ResponseHead<500>; data: string }
  | { kind: "schedule.create"; head: ResponseHead<501>; data: string };

export type ScheduleDeleteRes =
  | {
      kind: "schedule.delete";
      head: ResponseHead<200>;
      data: Record<string, never>;
    }
  | { kind: "schedule.delete"; head: ResponseHead<400>; data: string }
  | { kind: "schedule.delete"; head: ResponseHead<401>; data: string }
  | { kind: "schedule.delete"; head: ResponseHead<403>; data: string }
  | { kind: "schedule.delete"; head: ResponseHead<404>; data: string }
  | { kind: "schedule.delete"; head: ResponseHead<429>; data: string }
  | { kind: "schedule.delete"; head: ResponseHead<500>; data: string }
  | { kind: "schedule.delete"; head: ResponseHead<501>; data: string };

export type ScheduleSearchRes =
  | {
      kind: "schedule.search";
      head: ResponseHead<200>;
      data: { schedules: ScheduleRecord[]; cursor?: string };
    }
  | { kind: "schedule.search"; head: ResponseHead<400>; data: string }
  | { kind: "schedule.search"; head: ResponseHead<401>; data: string }
  | { kind: "schedule.search"; head: ResponseHead<403>; data: string }
  | { kind: "schedule.search"; head: ResponseHead<429>; data: string }
  | { kind: "schedule.search"; head: ResponseHead<500>; data: string }
  | { kind: "schedule.search"; head: ResponseHead<501>; data: string };

// =============================================================================
// RESPONSES - DEBUG
// =============================================================================

export type DebugStartRes =
  | {
      kind: "debug.start";
      head: ResponseHead<200>;
      data: Record<string, never>;
    }
  | { kind: "debug.start"; head: ResponseHead<400>; data: string }
  | { kind: "debug.start"; head: ResponseHead<401>; data: string }
  | { kind: "debug.start"; head: ResponseHead<403>; data: string }
  | { kind: "debug.start"; head: ResponseHead<429>; data: string }
  | { kind: "debug.start"; head: ResponseHead<500>; data: string }
  | { kind: "debug.start"; head: ResponseHead<501>; data: string };

export type DebugResetRes =
  | {
      kind: "debug.reset";
      head: ResponseHead<200>;
      data: Record<string, never>;
    }
  | { kind: "debug.reset"; head: ResponseHead<400>; data: string }
  | { kind: "debug.reset"; head: ResponseHead<401>; data: string }
  | { kind: "debug.reset"; head: ResponseHead<403>; data: string }
  | { kind: "debug.reset"; head: ResponseHead<429>; data: string }
  | { kind: "debug.reset"; head: ResponseHead<500>; data: string }
  | { kind: "debug.reset"; head: ResponseHead<501>; data: string };

export type DebugTickAction =
  | {
      kind: "promise.settle";
      data: { id: string; state: "rejected_timedout" | "resolved" };
    }
  | { kind: "task.release"; data: { id: string; version: number } }
  | { kind: "task.retry"; data: { id: string; version: number } };

export type DebugTickRes =
  | { kind: "debug.tick"; head: ResponseHead<200>; data: DebugTickAction[] }
  | { kind: "debug.tick"; head: ResponseHead<400>; data: string }
  | { kind: "debug.tick"; head: ResponseHead<401>; data: string }
  | { kind: "debug.tick"; head: ResponseHead<403>; data: string }
  | { kind: "debug.tick"; head: ResponseHead<429>; data: string }
  | { kind: "debug.tick"; head: ResponseHead<500>; data: string }
  | { kind: "debug.tick"; head: ResponseHead<501>; data: string };

export type DebugSnapRes =
  | {
      kind: "debug.snap";
      head: ResponseHead<200>;
      data: {
        promises: PromiseRecord[];
        promiseTimeouts: { id: string; timeout: number }[];
        callbacks: { awaiter: string; awaited: string }[];
        listeners?: { id: string; address: string }[];
        tasks: TaskRecord[];
        taskTimeouts: { id: string; type: number; timeout: number }[];
        messages: { address: string; message: Message }[];
      };
    }
  | { kind: "debug.snap"; head: ResponseHead<400>; data: string }
  | { kind: "debug.snap"; head: ResponseHead<401>; data: string }
  | { kind: "debug.snap"; head: ResponseHead<403>; data: string }
  | { kind: "debug.snap"; head: ResponseHead<429>; data: string }
  | { kind: "debug.snap"; head: ResponseHead<500>; data: string }
  | { kind: "debug.snap"; head: ResponseHead<501>; data: string };

export type DebugStopRes =
  | { kind: "debug.stop"; head: ResponseHead<200>; data: Record<string, never> }
  | { kind: "debug.stop"; head: ResponseHead<400>; data: string }
  | { kind: "debug.stop"; head: ResponseHead<401>; data: string }
  | { kind: "debug.stop"; head: ResponseHead<403>; data: string }
  | { kind: "debug.stop"; head: ResponseHead<429>; data: string }
  | { kind: "debug.stop"; head: ResponseHead<500>; data: string }
  | { kind: "debug.stop"; head: ResponseHead<501>; data: string };

// =============================================================================
// RESPONSE UNION
// =============================================================================

export type Response =
  | PromiseGetRes
  | PromiseCreateRes
  | PromiseSettleRes
  | PromiseRegisterCallbackRes
  | PromiseRegisterListenerRes
  | PromiseSearchRes
  | TaskGetRes
  | TaskCreateRes
  | TaskAcquireRes
  | TaskReleaseRes
  | TaskSuspendRes
  | TaskHaltRes
  | TaskContinueRes
  | TaskFulfillRes
  | TaskFenceRes
  | TaskHeartbeatRes
  | TaskSearchRes
  | ScheduleGetRes
  | ScheduleCreateRes
  | ScheduleDeleteRes
  | ScheduleSearchRes
  | DebugStartRes
  | DebugResetRes
  | DebugTickRes
  | DebugSnapRes
  | DebugStopRes;

// =============================================================================
// TYPE GUARDS
// =============================================================================

export function isSuccess<T extends Response>(res: T): res is Extract<T, { head: { status: 200 } }> {
  return res.head.status === 200;
}

export function isRedirect<T extends Response>(res: T): res is Extract<T, { head: { status: 300 } }> {
  return res.head.status === 300;
}

export function isBadRequest<T extends Response>(res: T): res is Extract<T, { head: { status: 400 } }> {
  return res.head.status === 400;
}

export function isUnauthorized<T extends Response>(res: T): res is Extract<T, { head: { status: 401 } }> {
  return res.head.status === 401;
}

export function isForbidden<T extends Response>(res: T): res is Extract<T, { head: { status: 403 } }> {
  return res.head.status === 403;
}

export function isNotFound<T extends Response>(res: T): res is Extract<T, { head: { status: 404 } }> {
  return res.head.status === 404;
}

export function isConflict<T extends Response>(res: T): res is Extract<T, { head: { status: 409 } }> {
  return res.head.status === 409;
}

export function isUnprocessable<T extends Response>(res: T): res is Extract<T, { head: { status: 422 } }> {
  return res.head.status === 422;
}

export function isRateLimited<T extends Response>(res: T): res is Extract<T, { head: { status: 429 } }> {
  return res.head.status === 429;
}

export function isError<T extends Response>(res: T): res is Extract<T, { head: { status: 500 } }> {
  return res.head.status === 500;
}

export function isNotImplemented<T extends Response>(res: T): res is Extract<T, { head: { status: 501 } }> {
  return res.head.status === 501;
}

// =============================================================================
// TYPE GUARDS - DEBUG TICK ACTION
// =============================================================================

export function isDebugTickAction(val: unknown): val is DebugTickAction {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  if (v.kind === "promise.settle") {
    return typeof d.id === "string" && (d.state === "rejected_timedout" || d.state === "resolved");
  }
  if (v.kind === "task.release" || v.kind === "task.retry") {
    return typeof d.id === "string" && typeof d.version === "number";
  }
  return false;
}

// =============================================================================
// TYPE GUARDS - REQUESTS
// =============================================================================

function isRequestHead(val: unknown): val is RequestHead {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  return (
    typeof v.corrId === "string" &&
    typeof v.version === "string" &&
    (v.auth === undefined || typeof v.auth === "string") &&
    (v["resonate:debug_time"] === undefined || typeof v["resonate:debug_time"] === "number")
  );
}

export function isPromiseGetReq(val: unknown): val is PromiseGetReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "promise.get" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  return typeof (v.data as Record<string, unknown>).id === "string";
}

export function isPromiseCreateReq(val: unknown): val is PromiseCreateReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "promise.create" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return (
    typeof d.id === "string" &&
    typeof d.timeoutAt === "number" &&
    isValue(d.param) &&
    typeof d.tags === "object" &&
    d.tags !== null
  );
}

export function isPromiseSettleReq(val: unknown): val is PromiseSettleReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "promise.settle" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return (
    typeof d.id === "string" &&
    (d.state === "resolved" || d.state === "rejected" || d.state === "rejected_canceled") &&
    isValue(d.value)
  );
}

export function isPromiseRegisterCallbackReq(val: unknown): val is PromiseRegisterCallbackReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "promise.register_callback" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return typeof d.awaited === "string" && typeof d.awaiter === "string";
}

export function isPromiseRegisterListenerReq(val: unknown): val is PromiseRegisterListenerReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "promise.register_listener" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return typeof d.awaited === "string" && typeof d.address === "string";
}

export function isPromiseSearchReq(val: unknown): val is PromiseSearchReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "promise.search" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return (
    (d.state === undefined ||
      d.state === "pending" ||
      d.state === "resolved" ||
      d.state === "rejected" ||
      d.state === "rejected_canceled" ||
      d.state === "rejected_timedout") &&
    (d.tags === undefined || (typeof d.tags === "object" && d.tags !== null)) &&
    (d.limit === undefined || typeof d.limit === "number") &&
    (d.cursor === undefined || typeof d.cursor === "string")
  );
}

export function isTaskGetReq(val: unknown): val is TaskGetReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.get" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  return typeof (v.data as Record<string, unknown>).id === "string";
}

export function isTaskCreateReq(val: unknown): val is TaskCreateReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.create" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return typeof d.pid === "string" && typeof d.ttl === "number" && isPromiseCreateReq(d.action);
}

export function isTaskAcquireReq(val: unknown): val is TaskAcquireReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.acquire" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return (
    typeof d.id === "string" && typeof d.version === "number" && typeof d.pid === "string" && typeof d.ttl === "number"
  );
}

export function isTaskReleaseReq(val: unknown): val is TaskReleaseReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.release" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return typeof d.id === "string" && typeof d.version === "number";
}

export function isTaskSuspendReq(val: unknown): val is TaskSuspendReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.suspend" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return (
    typeof d.id === "string" &&
    typeof d.version === "number" &&
    Array.isArray(d.actions) &&
    (d.actions as unknown[]).every(isPromiseRegisterCallbackReq)
  );
}

export function isTaskHaltReq(val: unknown): val is TaskHaltReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.halt" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  return typeof (v.data as Record<string, unknown>).id === "string";
}

export function isTaskContinueReq(val: unknown): val is TaskContinueReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.continue" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  return typeof (v.data as Record<string, unknown>).id === "string";
}

export function isTaskFulfillReq(val: unknown): val is TaskFulfillReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.fulfill" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return typeof d.id === "string" && typeof d.version === "number" && isPromiseSettleReq(d.action);
}

export function isTaskFenceReq(val: unknown): val is TaskFenceReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.fence" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return (
    typeof d.id === "string" &&
    typeof d.version === "number" &&
    (isPromiseCreateReq(d.action) || isPromiseSettleReq(d.action))
  );
}

export function isTaskHeartbeatReq(val: unknown): val is TaskHeartbeatReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.heartbeat" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return (
    typeof d.pid === "string" &&
    Array.isArray(d.tasks) &&
    (d.tasks as unknown[]).every(
      (t) =>
        typeof t === "object" &&
        t !== null &&
        typeof (t as Record<string, unknown>).id === "string" &&
        typeof (t as Record<string, unknown>).version === "number",
    )
  );
}

export function isTaskSearchReq(val: unknown): val is TaskSearchReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.search" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return (
    (d.state === undefined ||
      d.state === "pending" ||
      d.state === "acquired" ||
      d.state === "suspended" ||
      d.state === "halted" ||
      d.state === "fulfilled") &&
    (d.limit === undefined || typeof d.limit === "number") &&
    (d.cursor === undefined || typeof d.cursor === "string")
  );
}

export function isScheduleGetReq(val: unknown): val is ScheduleGetReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "schedule.get" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  return typeof (v.data as Record<string, unknown>).id === "string";
}

export function isScheduleCreateReq(val: unknown): val is ScheduleCreateReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "schedule.create" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return (
    typeof d.id === "string" &&
    typeof d.cron === "string" &&
    typeof d.promiseId === "string" &&
    typeof d.promiseTimeout === "number" &&
    isValue(d.promiseParam) &&
    typeof d.promiseTags === "object" &&
    d.promiseTags !== null
  );
}

export function isScheduleDeleteReq(val: unknown): val is ScheduleDeleteReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "schedule.delete" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  return typeof (v.data as Record<string, unknown>).id === "string";
}

export function isScheduleSearchReq(val: unknown): val is ScheduleSearchReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "schedule.search" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  return (
    (d.tags === undefined || (typeof d.tags === "object" && d.tags !== null)) &&
    (d.limit === undefined || typeof d.limit === "number") &&
    (d.cursor === undefined || typeof d.cursor === "string")
  );
}

export function isDebugStartReq(val: unknown): val is DebugStartReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  return v.kind === "debug.start" && isRequestHead(v.head) && typeof v.data === "object" && v.data !== null;
}

export function isDebugResetReq(val: unknown): val is DebugResetReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  return v.kind === "debug.reset" && isRequestHead(v.head) && typeof v.data === "object" && v.data !== null;
}

export function isDebugTickReq(val: unknown): val is DebugTickReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "debug.tick" || !isRequestHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  return typeof (v.data as Record<string, unknown>).time === "number";
}

export function isDebugSnapReq(val: unknown): val is DebugSnapReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  return v.kind === "debug.snap" && isRequestHead(v.head) && typeof v.data === "object" && v.data !== null;
}

export function isDebugStopReq(val: unknown): val is DebugStopReq {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  return v.kind === "debug.stop" && isRequestHead(v.head) && typeof v.data === "object" && v.data !== null;
}

export function isRequest(val: unknown): val is Request {
  return (
    isPromiseGetReq(val) ||
    isPromiseCreateReq(val) ||
    isPromiseSettleReq(val) ||
    isPromiseRegisterCallbackReq(val) ||
    isPromiseRegisterListenerReq(val) ||
    isPromiseSearchReq(val) ||
    isTaskGetReq(val) ||
    isTaskCreateReq(val) ||
    isTaskAcquireReq(val) ||
    isTaskReleaseReq(val) ||
    isTaskSuspendReq(val) ||
    isTaskHaltReq(val) ||
    isTaskContinueReq(val) ||
    isTaskFulfillReq(val) ||
    isTaskFenceReq(val) ||
    isTaskHeartbeatReq(val) ||
    isTaskSearchReq(val) ||
    isScheduleGetReq(val) ||
    isScheduleCreateReq(val) ||
    isScheduleDeleteReq(val) ||
    isScheduleSearchReq(val) ||
    isDebugStartReq(val) ||
    isDebugResetReq(val) ||
    isDebugTickReq(val) ||
    isDebugSnapReq(val) ||
    isDebugStopReq(val)
  );
}

// =============================================================================
// TYPE GUARDS - RESPONSES
// =============================================================================

function isResponseHead(val: unknown): val is ResponseHead<number> {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  return typeof v.corrId === "string" && typeof v.status === "number" && typeof v.version === "string";
}

export function isPromiseGetRes(val: unknown): val is PromiseGetRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "promise.get" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    return isPromiseRecord((v.data as Record<string, unknown>).promise);
  }
  return typeof v.data === "string";
}

export function isPromiseCreateRes(val: unknown): val is PromiseCreateRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "promise.create" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    return isPromiseRecord((v.data as Record<string, unknown>).promise);
  }
  return typeof v.data === "string";
}

export function isPromiseSettleRes(val: unknown): val is PromiseSettleRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "promise.settle" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    return isPromiseRecord((v.data as Record<string, unknown>).promise);
  }
  return typeof v.data === "string";
}

export function isPromiseRegisterCallbackRes(val: unknown): val is PromiseRegisterCallbackRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "promise.register_callback" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    return isPromiseRecord((v.data as Record<string, unknown>).promise);
  }
  return typeof v.data === "string";
}

export function isPromiseRegisterListenerRes(val: unknown): val is PromiseRegisterListenerRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "promise.register_listener" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    return isPromiseRecord((v.data as Record<string, unknown>).promise);
  }
  return typeof v.data === "string";
}

export function isPromiseSearchRes(val: unknown): val is PromiseSearchRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "promise.search" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    const d = v.data as Record<string, unknown>;
    return (
      Array.isArray(d.promises) &&
      (d.promises as unknown[]).every(isPromiseRecord) &&
      (d.cursor === undefined || typeof d.cursor === "string")
    );
  }
  return typeof v.data === "string";
}

export function isTaskGetRes(val: unknown): val is TaskGetRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.get" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    return isTaskRecord((v.data as Record<string, unknown>).task);
  }
  return typeof v.data === "string";
}

export function isTaskCreateRes(val: unknown): val is TaskCreateRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.create" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    const d = v.data as Record<string, unknown>;
    return (
      (d.task === undefined || isTaskRecord(d.task)) &&
      isPromiseRecord(d.promise) &&
      Array.isArray(d.preload) &&
      (d.preload as unknown[]).every(isPromiseRecord)
    );
  }
  return typeof v.data === "string";
}

export function isTaskAcquireRes(val: unknown): val is TaskAcquireRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.acquire" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    const d = v.data as Record<string, unknown>;
    return isPromiseRecord(d.promise) && Array.isArray(d.preload) && (d.preload as unknown[]).every(isPromiseRecord);
  }
  return typeof v.data === "string";
}

export function isTaskReleaseRes(val: unknown): val is TaskReleaseRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.release" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) return typeof v.data === "object" && v.data !== null;
  return typeof v.data === "string";
}

export function isTaskSuspendRes(val: unknown): val is TaskSuspendRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.suspend" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) return typeof v.data === "object" && v.data !== null;
  if (status === 300) {
    if (typeof v.data !== "object" || v.data === null) return false;
    const d = v.data as Record<string, unknown>;
    return Array.isArray(d.preload) && (d.preload as unknown[]).every(isPromiseRecord);
  }
  return typeof v.data === "string";
}

export function isTaskHaltRes(val: unknown): val is TaskHaltRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.halt" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) return typeof v.data === "object" && v.data !== null;
  return typeof v.data === "string";
}

export function isTaskContinueRes(val: unknown): val is TaskContinueRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.continue" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) return typeof v.data === "object" && v.data !== null;
  return typeof v.data === "string";
}

export function isTaskFulfillRes(val: unknown): val is TaskFulfillRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.fulfill" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    return isPromiseRecord((v.data as Record<string, unknown>).promise);
  }
  return typeof v.data === "string";
}

export function isTaskFenceRes(val: unknown): val is TaskFenceRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.fence" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    const d = v.data as Record<string, unknown>;
    return (
      (isPromiseCreateRes(d.action) || isPromiseSettleRes(d.action)) &&
      Array.isArray(d.preload) &&
      (d.preload as unknown[]).every(isPromiseRecord)
    );
  }
  return typeof v.data === "string";
}

export function isTaskHeartbeatRes(val: unknown): val is TaskHeartbeatRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.heartbeat" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) return typeof v.data === "object" && v.data !== null;
  return typeof v.data === "string";
}

export function isTaskSearchRes(val: unknown): val is TaskSearchRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "task.search" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    const d = v.data as Record<string, unknown>;
    return (
      Array.isArray(d.tasks) &&
      (d.tasks as unknown[]).every(isTaskRecord) &&
      (d.cursor === undefined || typeof d.cursor === "string")
    );
  }
  return typeof v.data === "string";
}

export function isScheduleGetRes(val: unknown): val is ScheduleGetRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "schedule.get" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    return isScheduleRecord((v.data as Record<string, unknown>).schedule);
  }
  return typeof v.data === "string";
}

export function isScheduleCreateRes(val: unknown): val is ScheduleCreateRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "schedule.create" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    return isScheduleRecord((v.data as Record<string, unknown>).schedule);
  }
  return typeof v.data === "string";
}

export function isScheduleDeleteRes(val: unknown): val is ScheduleDeleteRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "schedule.delete" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) return typeof v.data === "object" && v.data !== null;
  return typeof v.data === "string";
}

export function isScheduleSearchRes(val: unknown): val is ScheduleSearchRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "schedule.search" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    const d = v.data as Record<string, unknown>;
    return (
      Array.isArray(d.schedules) &&
      (d.schedules as unknown[]).every(isScheduleRecord) &&
      (d.cursor === undefined || typeof d.cursor === "string")
    );
  }
  return typeof v.data === "string";
}

export function isDebugStartRes(val: unknown): val is DebugStartRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "debug.start" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) return typeof v.data === "object" && v.data !== null;
  return typeof v.data === "string";
}

export function isDebugResetRes(val: unknown): val is DebugResetRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "debug.reset" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) return typeof v.data === "object" && v.data !== null;
  return typeof v.data === "string";
}

export function isDebugTickRes(val: unknown): val is DebugTickRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "debug.tick" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    return Array.isArray(v.data) && (v.data as unknown[]).every(isDebugTickAction);
  }
  return typeof v.data === "string";
}

export function isDebugSnapRes(val: unknown): val is DebugSnapRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "debug.snap" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) {
    if (typeof v.data !== "object" || v.data === null) return false;
    const d = v.data as Record<string, unknown>;
    return (
      Array.isArray(d.promises) &&
      (d.promises as unknown[]).every(isPromiseRecord) &&
      Array.isArray(d.promiseTimeouts) &&
      (d.promiseTimeouts as unknown[]).every(
        (t) =>
          typeof t === "object" &&
          t !== null &&
          typeof (t as Record<string, unknown>).id === "string" &&
          typeof (t as Record<string, unknown>).timeout === "number",
      ) &&
      Array.isArray(d.callbacks) &&
      (d.callbacks as unknown[]).every(
        (c) =>
          typeof c === "object" &&
          c !== null &&
          typeof (c as Record<string, unknown>).awaiter === "string" &&
          typeof (c as Record<string, unknown>).awaited === "string",
      ) &&
      (d.listeners === undefined ||
        (Array.isArray(d.listeners) &&
          (d.listeners as unknown[]).every(
            (l) =>
              typeof l === "object" &&
              l !== null &&
              typeof (l as Record<string, unknown>).id === "string" &&
              typeof (l as Record<string, unknown>).address === "string",
          ))) &&
      Array.isArray(d.tasks) &&
      (d.tasks as unknown[]).every(isTaskRecord) &&
      Array.isArray(d.taskTimeouts) &&
      (d.taskTimeouts as unknown[]).every(
        (t) =>
          typeof t === "object" &&
          t !== null &&
          typeof (t as Record<string, unknown>).id === "string" &&
          typeof (t as Record<string, unknown>).type === "number" &&
          typeof (t as Record<string, unknown>).timeout === "number",
      ) &&
      Array.isArray(d.messages) &&
      (d.messages as unknown[]).every(
        (m) =>
          typeof m === "object" &&
          m !== null &&
          typeof (m as Record<string, unknown>).address === "string" &&
          isMessage((m as Record<string, unknown>).message),
      )
    );
  }
  return typeof v.data === "string";
}

export function isDebugStopRes(val: unknown): val is DebugStopRes {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "debug.stop" || !isResponseHead(v.head)) return false;
  const { status } = v.head as ResponseHead<number>;
  if (status === 200) return typeof v.data === "object" && v.data !== null;
  return typeof v.data === "string";
}

export function isResponse(val: unknown): val is Response {
  return (
    isPromiseGetRes(val) ||
    isPromiseCreateRes(val) ||
    isPromiseSettleRes(val) ||
    isPromiseRegisterCallbackRes(val) ||
    isPromiseRegisterListenerRes(val) ||
    isPromiseSearchRes(val) ||
    isTaskGetRes(val) ||
    isTaskCreateRes(val) ||
    isTaskAcquireRes(val) ||
    isTaskReleaseRes(val) ||
    isTaskSuspendRes(val) ||
    isTaskHaltRes(val) ||
    isTaskContinueRes(val) ||
    isTaskFulfillRes(val) ||
    isTaskFenceRes(val) ||
    isTaskHeartbeatRes(val) ||
    isTaskSearchRes(val) ||
    isScheduleGetRes(val) ||
    isScheduleCreateRes(val) ||
    isScheduleDeleteRes(val) ||
    isScheduleSearchRes(val) ||
    isDebugStartRes(val) ||
    isDebugResetRes(val) ||
    isDebugTickRes(val) ||
    isDebugSnapRes(val) ||
    isDebugStopRes(val)
  );
}

// =============================================================================
// TYPE GUARDS - RECORDS
// =============================================================================

export function isValue(val: unknown): val is Value {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.headers !== undefined && (typeof v.headers !== "object" || v.headers === null)) return false;
  if (v.data !== undefined && typeof v.data !== "string") return false;
  return true;
}

export function isPromiseRecord(val: unknown): val is PromiseRecord {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  return (
    typeof v.id === "string" &&
    (v.state === "pending" ||
      v.state === "resolved" ||
      v.state === "rejected" ||
      v.state === "rejected_canceled" ||
      v.state === "rejected_timedout") &&
    isValue(v.param) &&
    isValue(v.value) &&
    typeof v.tags === "object" &&
    v.tags !== null &&
    typeof v.timeoutAt === "number" &&
    typeof v.createdAt === "number" &&
    (v.settledAt === undefined || typeof v.settledAt === "number")
  );
}

export function isTaskRecord(val: unknown): val is TaskRecord {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  return (
    typeof v.id === "string" &&
    (v.state === "pending" ||
      v.state === "acquired" ||
      v.state === "suspended" ||
      v.state === "halted" ||
      v.state === "fulfilled") &&
    typeof v.version === "number" &&
    (v.ttl === undefined || typeof v.ttl === "number") &&
    (v.pid === undefined || typeof v.pid === "string") &&
    (Array.isArray(v.resumes) || typeof v.resumes === "number" || typeof v.resumes === "boolean")
  );
}

export function isScheduleRecord(val: unknown): val is ScheduleRecord {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  return (
    typeof v.id === "string" &&
    typeof v.cron === "string" &&
    typeof v.promiseId === "string" &&
    typeof v.promiseTimeout === "number" &&
    isValue(v.promiseParam) &&
    typeof v.promiseTags === "object" &&
    v.promiseTags !== null &&
    typeof v.createdAt === "number" &&
    typeof v.nextRunAt === "number" &&
    (v.lastRunAt === undefined || typeof v.lastRunAt === "number")
  );
}

// =============================================================================
// TYPE GUARDS - MESSAGES
// =============================================================================

function isMessageHead(val: unknown): val is MessageHead {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  return v.serverUrl === undefined || typeof v.serverUrl === "string";
}

export function isExecuteMsg(val: unknown): val is ExecuteMsg {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "execute" || !isMessageHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  const d = v.data as Record<string, unknown>;
  if (typeof d.task !== "object" || d.task === null) return false;
  const task = d.task as Record<string, unknown>;
  return typeof task.id === "string" && typeof task.version === "number";
}

export function isUnblockMsg(val: unknown): val is UnblockMsg {
  if (typeof val !== "object" || val === null) return false;
  const v = val as Record<string, unknown>;
  if (v.kind !== "unblock" || !isMessageHead(v.head)) return false;
  if (typeof v.data !== "object" || v.data === null) return false;
  return isPromiseRecord((v.data as Record<string, unknown>).promise);
}

export function isMessage(val: unknown): val is Message {
  return isExecuteMsg(val) || isUnblockMsg(val);
}
