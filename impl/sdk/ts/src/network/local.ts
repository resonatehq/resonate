import { randomUUID } from "node:crypto";
import { CronExpressionParser } from "cron-parser";
import { assert, VERSION } from "../util.js";
import type { Network } from "./network.js";
import { isResponse } from "./types.js";
import type {
  DebugResetRes,
  DebugSnapRes,
  DebugStartRes,
  DebugStopRes,
  DebugTickAction,
  DebugTickReq,
  DebugTickRes,
  Message,
  PromiseCreateReq,
  PromiseCreateRes,
  PromiseGetReq,
  PromiseGetRes,
  PromiseRecord,
  PromiseRegisterCallbackReq,
  PromiseRegisterCallbackRes,
  PromiseRegisterListenerReq,
  PromiseRegisterListenerRes,
  PromiseSettleReq,
  PromiseSettleRes,
  Request,
  Response,
  ScheduleCreateReq,
  ScheduleCreateRes,
  ScheduleDeleteReq,
  ScheduleDeleteRes,
  ScheduleGetReq,
  ScheduleGetRes,
  ScheduleRecord,
  TaskAcquireReq,
  TaskAcquireRes,
  TaskContinueReq,
  TaskContinueRes,
  TaskCreateReq,
  TaskCreateRes,
  TaskFenceReq,
  TaskFenceRes,
  TaskFulfillReq,
  TaskFulfillRes,
  TaskGetReq,
  TaskGetRes,
  TaskHaltReq,
  TaskHaltRes,
  TaskHeartbeatReq,
  TaskHeartbeatRes,
  TaskRecord,
  TaskReleaseReq,
  TaskReleaseRes,
  TaskSuspendReq,
  TaskSuspendRes,
  Value,
} from "./types.ts";

export interface PTimeout {
  id: string;
  timeout: number;
}

export interface TTimeout {
  id: string;
  type: 0 | 1; // 0 = pending retry, 1 = lease timeout
  timeout: number;
}

export interface STimeout {
  id: string;
  timeout: number;
}

export type PromiseState = "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout";

export interface Promise {
  id: string;
  state: PromiseState;
  param: Value;
  value: Value;
  tags: Record<string, string>;
  timeoutAt: number;
  createdAt: number;
  settledAt: number | null;
  callbacks: Set<string>; // IDs of promises waiting on this one
  listeners: Set<string>; // addresses listening for this promise's settlement
}

export type TaskState = "pending" | "acquired" | "suspended" | "halted" | "fulfilled";

export interface Task {
  id: string;
  state: TaskState;
  version: number;
  pid?: string;
  ttl?: number;
  resumes: Set<string>; // awaited IDs that resolved while task was pending/acquired (R in the spec)
}

export interface Schedule {
  id: string;
  cron: string; // A cron expression (standard 5-field format) specifying when to create promises.
  promiseId: string; // A template for the promise identifier. Supports {{.id}} and {{.timestamp}} substitutions
  promiseTimeout: number; // The timeout in milliseconds for created promises.
  promiseParam: Value; // The parameters for created promises. The data field is base64 encoded.
  promiseTags: Record<string, string>; // Tags for created promises.
  createdAt: number; // Unix timestamp in milliseconds when the schedule was created.
  lastRunAt?: number; // Unix timestamp in milliseconds of the last run. Only present if the schedule has run at least once.
}

// =============================================================================
// CHANGE TRACKING
// =============================================================================

export type Change =
  | { kind: "promise.set"; promise: PromiseRecord }
  | { kind: "task.set"; task: TaskRecord }
  | { kind: "schedule.set"; schedule: ScheduleRecord }
  | { kind: "schedule.del"; id: string }
  | { kind: "ptimeout.set"; timeout: { id: string; timeout: number } }
  | { kind: "ptimeout.del"; id: string }
  | { kind: "ttimeout.set"; timeout: { id: string; type: number; timeout: number } }
  | { kind: "ttimeout.del"; id: string }
  | { kind: "stimeout.set"; timeout: { id: string; timeout: number } }
  | { kind: "stimeout.del"; id: string }
  | { kind: "message.send"; address: string; message: Message };

// =============================================================================
// CONSTANTS
// =============================================================================

const PENDING_RETRY_TTL = 30000;

// =============================================================================
// SERVER
// =============================================================================

export class Server {
  promises = new Map<string, Promise>();
  tasks = new Map<string, Task>();
  schedules = new Map<string, Schedule>();
  pTimeouts: PTimeout[] = [];
  tTimeouts: TTimeout[] = [];
  sTimeouts: STimeout[] = [];
  outgoing: { address: string; message: Message }[] = [];

  apply(now: number, req: Request): { response: Response; changes: Change[] } {
    const changes: Change[] = [];

    const error = this.validate(req);
    if (error !== null) {
      return { response: { kind: req.kind, head: { status: 400 }, data: error } as Response, changes };
    }

    let result: { response: Response; changes: Change[] };
    switch (req.kind) {
      case "promise.get": {
        changes.push(...this.tryAutoTimeout(now, req.data.id));
        result = this.promiseGet(now, req);
        break;
      }
      case "promise.create": {
        changes.push(...this.tryAutoTimeout(now, req.data.id));
        result = this.promiseCreate(now, req);
        break;
      }
      case "promise.settle": {
        changes.push(...this.tryAutoTimeout(now, req.data.id));
        result = this.promiseSettle(now, req);
        break;
      }
      case "promise.register_callback": {
        changes.push(...this.tryAutoTimeout(now, req.data.awaited));
        changes.push(...this.tryAutoTimeout(now, req.data.awaiter));
        result = this.promiseRegisterCallback(now, req);
        break;
      }
      case "promise.register_listener": {
        changes.push(...this.tryAutoTimeout(now, req.data.awaited));
        result = this.promiseRegisterListener(now, req);
        break;
      }
      case "promise.search": {
        result = { response: this.response("promise.search", 501, "Not implemented"), changes: [] };
        break;
      }
      case "task.get": {
        changes.push(...this.tryAutoTimeout(now, req.data.id));
        result = this.taskGet(now, req);
        break;
      }
      case "task.create": {
        changes.push(...this.tryAutoTimeout(now, req.data.action.data.id));
        result = this.taskCreate(now, req);
        break;
      }
      case "task.acquire": {
        changes.push(...this.tryAutoTimeout(now, req.data.id));
        result = this.taskAcquire(now, req);
        break;
      }
      case "task.release": {
        changes.push(...this.tryAutoTimeout(now, req.data.id));
        result = this.taskRelease(now, req);
        break;
      }
      case "task.fulfill": {
        changes.push(...this.tryAutoTimeout(now, req.data.id));
        result = this.taskFulfill(now, req);
        break;
      }
      case "task.suspend": {
        changes.push(...this.tryAutoTimeout(now, req.data.id));
        for (const action of req.data.actions) {
          changes.push(...this.tryAutoTimeout(now, action.data.awaited));
        }
        result = this.taskSuspend(now, req);
        break;
      }
      case "task.halt": {
        changes.push(...this.tryAutoTimeout(now, req.data.id));
        result = this.taskHalt(now, req);
        break;
      }
      case "task.continue": {
        changes.push(...this.tryAutoTimeout(now, req.data.id));
        result = this.taskContinue(now, req);
        break;
      }
      case "task.fence": {
        changes.push(...this.tryAutoTimeout(now, req.data.id));
        changes.push(...this.tryAutoTimeout(now, req.data.action.data.id));
        result = this.taskFence(now, req);
        break;
      }
      case "task.heartbeat": {
        result = this.taskHeartbeat(now, req);
        break;
      }
      case "task.search": {
        result = { response: this.response("task.search", 501, "Not implemented"), changes: [] };
        break;
      }
      case "debug.start": {
        result = this.debugStart();
        break;
      }
      case "debug.reset": {
        result = this.debugReset();
        break;
      }
      case "debug.snap": {
        result = this.debugSnap();
        break;
      }
      case "debug.tick": {
        result = this.debugTick(req);
        break;
      }
      case "debug.stop": {
        result = this.debugStop();
        break;
      }
      case "schedule.get": {
        result = this.scheduleGet(req);
        break;
      }
      case "schedule.create": {
        result = this.scheduleCreate(now, req);
        break;
      }
      case "schedule.delete": {
        result = this.scheduleDelete(req);
        break;
      }
      case "schedule.search": {
        result = { response: this.response("schedule.search", 501, "Not implemented"), changes: [] };
        break;
      }
    }

    changes.push(...result.changes);
    return { response: result.response, changes };
  }

  private validate(req: Request): string | null {
    switch (req.kind) {
      case "promise.register_callback":
        if (req.data.awaited === req.data.awaiter) {
          return "Awaited and awaiter must be different";
        }
        return null;
      case "task.suspend":
        if (req.data.actions.length === 0) {
          return "Actions list must not be empty";
        }
        if (req.data.actions.some((a) => a.data.awaiter !== req.data.id)) {
          return "Awaiter must be the suspending task";
        }
        if (req.data.actions.some((a) => a.data.awaited === req.data.id)) {
          return "Task cannot await its own promise";
        }
        return null;
      case "task.create":
        if (!req.data.action.data.tags?.["resonate:target"]) {
          return "Action must have a resonate:target tag";
        }
        return null;
      case "task.fulfill":
        if (req.data.action.data.id !== req.data.id) {
          return "Promise ID must match task ID";
        }
        return null;
      default:
        return null;
    }
  }

  // ===========================================================================
  // PROMISE OPERATIONS
  // ===========================================================================

  private promiseGet(now: number, req: PromiseGetReq): { response: PromiseGetRes; changes: Change[] } {
    const promise = this.promises.get(req.data.id);
    if (!promise) {
      return { response: this.response("promise.get", 404, "Promise not found"), changes: [] };
    }
    return { response: this.response("promise.get", 200, { promise: this.toPromiseRecord(promise) }), changes: [] };
  }

  private promiseCreate(now: number, req: PromiseCreateReq): { response: PromiseCreateRes; changes: Change[] } {
    const existingPromise = this.promises.get(req.data.id);
    if (existingPromise) {
      return {
        response: this.response("promise.create", 200, { promise: this.toPromiseRecord(existingPromise) }),
        changes: [],
      };
    }

    const changes: Change[] = [];

    // Check if the promise is already timed out at creation
    if (now >= req.data.timeoutAt) {
      const promise: Promise = {
        id: req.data.id,
        state: this.timeoutState(req.data.tags),
        param: req.data.param ?? { data: undefined, headers: undefined },
        value: {},
        tags: req.data.tags,
        createdAt: req.data.timeoutAt,
        settledAt: req.data.timeoutAt,
        timeoutAt: req.data.timeoutAt,
        callbacks: new Set(),
        listeners: new Set(),
      };
      changes.push(this.setPromise(promise));

      // Create fulfilled task inline if promise has a target
      const address = req.data.tags["resonate:target"];
      if (address) {
        changes.push(
          this.setTask({
            id: req.data.id,
            state: "fulfilled",
            version: 0,
            pid: undefined,
            ttl: undefined,
            resumes: new Set(),
          }),
        );
      }

      changes.push(...this.triggerSettlement(req.data.id, now));

      return {
        response: this.response("promise.create", 200, { promise: this.toPromiseRecord(promise) }),
        changes,
      };
    }

    const promise: Promise = {
      id: req.data.id,
      state: "pending",
      param: req.data.param ?? { data: undefined, headers: undefined },
      value: {},
      tags: req.data.tags,
      createdAt: now,
      settledAt: null,
      timeoutAt: req.data.timeoutAt,
      callbacks: new Set(),
      listeners: new Set(),
    };
    changes.push(this.setPromise(promise));
    changes.push(this.setPTimeout({ id: req.data.id, timeout: req.data.timeoutAt }));

    const address = req.data.tags["resonate:target"];
    if (address) {
      const delay = Number(req.data.tags["resonate:delay"]);
      const deferred = Number.isFinite(delay) && now < delay;

      const task: Task = {
        id: req.data.id,
        state: "pending",
        version: 0,
        resumes: new Set(),
      };
      changes.push(this.setTask(task));
      changes.push(
        this.setTTimeout({
          id: req.data.id,
          type: 0,
          timeout: deferred ? delay : now + PENDING_RETRY_TTL,
        }),
      );
      if (!deferred) {
        changes.push(
          this.sendMessage(address, { kind: "execute", head: {}, data: { task: { id: req.data.id, version: 0 } } }),
        );
      }
    }

    return {
      response: this.response("promise.create", 200, { promise: this.toPromiseRecord(promise) }),
      changes,
    };
  }

  private promiseSettle(now: number, req: PromiseSettleReq): { response: PromiseSettleRes; changes: Change[] } {
    const promise = this.promises.get(req.data.id);
    if (!promise) {
      return { response: this.response("promise.settle", 404, "Promise not found"), changes: [] };
    }

    if (promise.state !== "pending") {
      return {
        response: this.response("promise.settle", 200, { promise: this.toPromiseRecord(promise) }),
        changes: [],
      };
    }

    const changes: Change[] = [];

    const settledPromise: Promise = {
      ...promise,
      state: req.data.state,
      value: req.data.value,
      settledAt: now,
    };
    changes.push(this.setPromise(settledPromise));
    changes.push(this.delPTimeout(req.data.id));
    changes.push(...this.triggerSettlement(req.data.id, now));

    return {
      response: this.response("promise.settle", 200, { promise: this.toPromiseRecord(settledPromise) }),
      changes,
    };
  }

  private promiseRegisterCallback(
    now: number,
    req: PromiseRegisterCallbackReq,
  ): { response: PromiseRegisterCallbackRes; changes: Change[] } {
    const awaitedPromise = this.promises.get(req.data.awaited);
    if (!awaitedPromise) {
      return { response: this.response("promise.register_callback", 404, "Awaited promise not found"), changes: [] };
    }

    const awaiterPromise = this.promises.get(req.data.awaiter);
    if (!awaiterPromise) {
      return { response: this.response("promise.register_callback", 422, "Awaiter promise not found"), changes: [] };
    }

    // HasAddress check: awaiter must have a resonate:target tag
    if (!awaiterPromise.tags["resonate:target"]) {
      return { response: this.response("promise.register_callback", 422, "Awaiter has no address"), changes: [] };
    }

    const changes: Change[] = [];

    // Add to callbacks if awaited is pending and awaiter is not settled
    if (awaitedPromise.state === "pending" && awaiterPromise.state === "pending") {
      awaitedPromise.callbacks.add(req.data.awaiter);
      changes.push(this.setPromise(awaitedPromise));
    }

    return {
      response: this.response("promise.register_callback", 200, { promise: this.toPromiseRecord(awaitedPromise) }),
      changes,
    };
  }

  private promiseRegisterListener(
    now: number,
    req: PromiseRegisterListenerReq,
  ): { response: PromiseRegisterListenerRes; changes: Change[] } {
    const promise = this.promises.get(req.data.awaited);
    if (!promise) {
      return { response: this.response("promise.register_listener", 404, "Promise not found"), changes: [] };
    }

    const changes: Change[] = [];

    // Add listener if promise is pending
    if (promise.state === "pending") {
      promise.listeners.add(req.data.address);
      changes.push(this.setPromise(promise));
    }

    return {
      response: this.response("promise.register_listener", 200, { promise: this.toPromiseRecord(promise) }),
      changes,
    };
  }

  // ===========================================================================
  // TASK OPERATIONS
  // ===========================================================================

  private taskGet(now: number, req: TaskGetReq): { response: TaskGetRes; changes: Change[] } {
    const task = this.tasks.get(req.data.id);
    if (!task) {
      return { response: this.response("task.get", 404, "Task not found"), changes: [] };
    }
    return { response: this.response("task.get", 200, { task: this.toTaskRecord(task) }), changes: [] };
  }

  private taskCreate(now: number, req: TaskCreateReq): { response: TaskCreateRes; changes: Change[] } {
    const existingTask = this.tasks.get(req.data.action.data.id);
    if (existingTask) {
      const promise = this.getPromiseOrThrow(existingTask.id);

      // Pending → Acquired (acquire and return 200)
      if (existingTask.state === "pending") {
        const newVersion = existingTask.version + 1;
        const changes: Change[] = [];
        changes.push(
          this.setTask({
            ...existingTask,
            state: "acquired",
            version: newVersion,
            pid: req.data.pid,
            ttl: req.data.ttl,
            resumes: new Set(),
          }),
        );
        changes.push(this.setTTimeout({ id: existingTask.id, type: 1, timeout: now + req.data.ttl }));
        return {
          response: this.response("task.create", 200, {
            task: this.toTaskRecord({
              ...existingTask,
              state: "acquired",
              version: newVersion,
              pid: req.data.pid,
              ttl: req.data.ttl,
              resumes: new Set(),
            }),
            promise: this.toPromiseRecord(promise),
            preload: this.preload(existingTask.id),
          }),
          changes,
        };
      }

      // Fulfilled → Fulfilled (idempotent, return 200)
      if (existingTask.state === "fulfilled") {
        return {
          response: this.response("task.create", 200, {
            task: this.toTaskRecord(existingTask),
            promise: this.toPromiseRecord(promise),
            preload: this.preload(existingTask.id),
          }),
          changes: [],
        };
      }

      return { response: this.response("task.create", 409, "Task already exists"), changes: [] };
    }

    const changes: Change[] = [];
    const actionData = req.data.action.data;

    // Guard: promise already exists
    const existingPromise = this.promises.get(actionData.id);
    if (existingPromise) {
      if (!existingPromise.tags["resonate:target"]) {
        return { response: this.response("task.create", 422, "Promise has no address"), changes: [] };
      }
      return { response: this.response("task.create", 409, "Promise already exists"), changes: [] };
    }

    // Guard: promise already timed out
    if (now >= actionData.timeoutAt) {
      const promise: Promise = {
        id: actionData.id,
        state: this.timeoutState(actionData.tags),
        param: actionData.param,
        value: {},
        tags: actionData.tags,
        createdAt: actionData.timeoutAt,
        settledAt: actionData.timeoutAt,
        timeoutAt: actionData.timeoutAt,
        callbacks: new Set(),
        listeners: new Set(),
      };
      changes.push(this.setPromise(promise));
      const task: Task = {
        id: actionData.id,
        state: "fulfilled",
        version: 0,
        pid: undefined,
        ttl: undefined,
        resumes: new Set(),
      };
      changes.push(this.setTask(task));
      return {
        response: this.response("task.create", 200, {
          task: this.toTaskRecord(task),
          promise: this.toPromiseRecord(promise),
          preload: [],
        }),
        changes,
      };
    }

    // Create promise in pending state
    const promise: Promise = {
      id: actionData.id,
      state: "pending",
      param: actionData.param,
      value: {},
      tags: actionData.tags,
      createdAt: now,
      settledAt: null,
      timeoutAt: actionData.timeoutAt,
      callbacks: new Set(),
      listeners: new Set(),
    };
    changes.push(this.setPromise(promise));
    changes.push(this.setPTimeout({ id: actionData.id, timeout: actionData.timeoutAt }));

    // Step 2: Create task in acquired state
    const task: Task = {
      id: actionData.id,
      state: "acquired",
      version: 1,
      pid: req.data.pid,
      ttl: req.data.ttl,
      resumes: new Set(),
    };
    changes.push(this.setTask(task));

    // Step 3: Set type=1 (lease) timeout
    changes.push(this.setTTimeout({ id: actionData.id, type: 1, timeout: now + req.data.ttl }));

    return {
      response: this.response("task.create", 200, {
        task: this.toTaskRecord(task),
        promise: this.toPromiseRecord(promise),
        preload: this.preload(actionData.id),
      }),
      changes,
    };
  }

  private taskAcquire(now: number, req: TaskAcquireReq): { response: TaskAcquireRes; changes: Change[] } {
    const task = this.tasks.get(req.data.id);
    if (!task) {
      return { response: this.response("task.acquire", 404, "Task not found"), changes: [] };
    }
    if (task.state !== "pending") {
      return { response: this.response("task.acquire", 409, "Task not in pending state"), changes: [] };
    }
    if (task.version !== req.data.version) {
      return { response: this.response("task.acquire", 409, "Version mismatch"), changes: [] };
    }

    const newVersion = task.version + 1;
    const changes: Change[] = [];
    const promise = this.getPromiseOrThrow(req.data.id);
    changes.push(
      this.setTask({
        ...task,
        state: "acquired",
        version: newVersion,
        pid: req.data.pid,
        ttl: req.data.ttl,
        resumes: new Set(),
      }),
    );
    changes.push(this.setTTimeout({ id: req.data.id, type: 1, timeout: now + req.data.ttl }));

    return {
      response: this.response("task.acquire", 200, {
        task: this.toTaskRecord({
          ...task,
          state: "acquired",
          version: newVersion,
          pid: req.data.pid,
          ttl: req.data.ttl,
          resumes: new Set(),
        }),
        promise: this.toPromiseRecord(promise),
        preload: this.preload(req.data.id),
      }),
      changes,
    };
  }

  private taskRelease(now: number, req: TaskReleaseReq): { response: TaskReleaseRes; changes: Change[] } {
    const task = this.tasks.get(req.data.id);
    if (!task) {
      return { response: this.response("task.release", 404, "Task not found"), changes: [] };
    }
    if (task.state !== "acquired") {
      return { response: this.response("task.release", 409, "Task not acquired"), changes: [] };
    }
    if (task.version !== req.data.version) {
      return { response: this.response("task.release", 409, "Version mismatch"), changes: [] };
    }

    const changes: Change[] = [];
    changes.push(this.setTask({ ...task, state: "pending", pid: undefined, ttl: undefined }));
    changes.push(this.setTTimeout({ id: req.data.id, type: 0, timeout: now + PENDING_RETRY_TTL }));

    const promise = this.getPromiseOrThrow(req.data.id);
    const address = this.getAddressOrThrow(promise);
    changes.push(
      this.sendMessage(address, {
        kind: "execute",
        head: {},
        data: { task: { id: req.data.id, version: task.version } },
      }),
    );

    return { response: this.response("task.release", 200, {}), changes };
  }

  private taskFulfill(now: number, req: TaskFulfillReq): { response: TaskFulfillRes; changes: Change[] } {
    const task = this.tasks.get(req.data.id);
    if (!task) {
      return { response: this.response("task.fulfill", 404, "Task not found"), changes: [] };
    }
    if (task.state !== "acquired") {
      return { response: this.response("task.fulfill", 409, "Task not acquired"), changes: [] };
    }
    if (task.version !== req.data.version) {
      return { response: this.response("task.fulfill", 409, "Version mismatch"), changes: [] };
    }

    const promise = this.getPromiseOrThrow(req.data.action.data.id);

    const changes: Change[] = [];

    // Check if promise is already settled (possibly by auto-timeout above)
    if (promise.state !== "pending") {
      // Still fulfill the task but indicate the promise was already settled
      changes.push(...this.triggerFulfilled(req.data.id));

      return { response: this.response("task.fulfill", 200, { promise: this.toPromiseRecord(promise) }), changes };
    }

    // Settle the promise
    const settledPromise: Promise = {
      ...promise,
      state: req.data.action.data.state,
      value: req.data.action.data.value ?? {},
      settledAt: now,
    };
    changes.push(this.setPromise(settledPromise));
    changes.push(this.delPTimeout(req.data.action.data.id));
    changes.push(...this.triggerSettlement(req.data.id, now));

    return { response: this.response("task.fulfill", 200, { promise: this.toPromiseRecord(settledPromise) }), changes };
  }

  private taskSuspend(now: number, req: TaskSuspendReq): { response: TaskSuspendRes; changes: Change[] } {
    const task = this.tasks.get(req.data.id);
    if (!task) {
      return { response: this.response("task.suspend", 404, "Task not found"), changes: [] };
    }
    if (task.state !== "acquired") {
      return { response: this.response("task.suspend", 409, "Task not acquired"), changes: [] };
    }
    if (task.version !== req.data.version) {
      return { response: this.response("task.suspend", 409, "Version mismatch"), changes: [] };
    }

    const changes: Change[] = [];

    // First pass: validate all promises exist and classify as pending or settled.
    // Callbacks are only registered when ALL promises are pending (spec transition #40).
    // If any promise is already settled, return 300 immediately with no callbacks (spec transition #39).
    const pendingPromises: Promise[] = [];
    let hasSettled = false;

    for (const action of req.data.actions) {
      const awaitedPromise = this.promises.get(action.data.awaited);
      if (!awaitedPromise) {
        return { response: this.response("task.suspend", 422, {}), changes: [] };
      }

      if (awaitedPromise.state === "pending") {
        pendingPromises.push(awaitedPromise);
      } else {
        hasSettled = true;
      }
    }

    // Immediate resume — stay acquired, clear all resumes, no callbacks registered
    if (hasSettled) {
      changes.push(this.setTask({ ...task, resumes: new Set() }));
      return { response: this.response("task.suspend", 300, { preload: this.preload(req.data.id) }), changes };
    }

    // All pending: register callbacks then suspend
    for (const awaitedPromise of pendingPromises) {
      awaitedPromise.callbacks.add(req.data.id);
      changes.push(this.setPromise(awaitedPromise));
    }

    changes.push(this.setTask({ ...task, state: "suspended", pid: undefined, ttl: undefined, resumes: new Set() }));
    changes.push(this.delTTimeout(req.data.id));

    return { response: this.response("task.suspend", 200, {}), changes };
  }

  private taskHalt(now: number, req: TaskHaltReq): { response: TaskHaltRes; changes: Change[] } {
    const task = this.tasks.get(req.data.id);
    if (!task) {
      return { response: this.response("task.halt", 404, "Task not found"), changes: [] };
    }
    if (task.state === "fulfilled") {
      return { response: this.response("task.halt", 409, "Task is fulfilled"), changes: [] };
    }

    const changes: Change[] = [];

    if (task.state !== "halted") {
      changes.push(this.setTask({ ...task, state: "halted", pid: undefined, ttl: undefined }));
      changes.push(this.delTTimeout(req.data.id));
    }

    return { response: this.response("task.halt", 200, {}), changes };
  }

  private taskContinue(now: number, req: TaskContinueReq): { response: TaskContinueRes; changes: Change[] } {
    const task = this.tasks.get(req.data.id);
    if (!task) {
      return { response: this.response("task.continue", 404, "Task not found"), changes: [] };
    }
    if (task.state !== "halted") {
      return { response: this.response("task.continue", 409, "Task is not halted"), changes: [] };
    }

    const changes: Change[] = [];

    changes.push(this.setTask({ ...task, state: "pending" }));
    changes.push(this.setTTimeout({ id: req.data.id, type: 0, timeout: now + PENDING_RETRY_TTL }));

    const promise = this.getPromiseOrThrow(req.data.id);
    const address = this.getAddressOrThrow(promise);
    changes.push(
      this.sendMessage(address, {
        kind: "execute",
        head: {},
        data: { task: { id: req.data.id, version: task.version } },
      }),
    );

    return { response: this.response("task.continue", 200, {}), changes };
  }

  private taskFence(now: number, req: TaskFenceReq): { response: TaskFenceRes; changes: Change[] } {
    const task = this.tasks.get(req.data.id);
    if (!task) {
      return { response: this.response("task.fence", 404, "Task not found"), changes: [] };
    }
    if (task.state !== "acquired") {
      return { response: this.response("task.fence", 409, "Fence check failed"), changes: [] };
    }
    if (task.version !== req.data.version) {
      return { response: this.response("task.fence", 409, "Fence check failed"), changes: [] };
    }

    const action = req.data.action;

    let inner: { response: PromiseCreateRes | PromiseSettleRes; changes: Change[] };
    if (action.kind === "promise.create") {
      inner = this.promiseCreate(now, action);
    } else {
      inner = this.promiseSettle(now, action);
    }

    return {
      response: this.response("task.fence", 200, { action: inner.response, preload: [] }),
      changes: inner.changes,
    };
  }

  private taskHeartbeat(now: number, req: TaskHeartbeatReq): { response: TaskHeartbeatRes; changes: Change[] } {
    const changes: Change[] = [];

    for (const ref of req.data.tasks) {
      const task = this.tasks.get(ref.id);
      if (!task || task.state !== "acquired" || task.version !== ref.version || task.pid !== req.data.pid) {
        continue;
      }

      const ttl = task.ttl ?? 30000;
      changes.push(this.setTTimeout({ id: ref.id, type: 1, timeout: now + ttl }));
    }

    return { response: this.response("task.heartbeat", 200, {}), changes };
  }

  // ===========================================================================
  // SCHEDULE OPERATIONS
  // ===========================================================================

  private scheduleGet(req: ScheduleGetReq): { response: ScheduleGetRes; changes: Change[] } {
    const schedule = this.schedules.get(req.data.id);
    if (!schedule) {
      return { response: this.response("schedule.get", 404, "Schedule not found"), changes: [] };
    }
    return { response: this.response("schedule.get", 200, { schedule: this.toScheduleRecord(schedule) }), changes: [] };
  }

  private scheduleCreate(now: number, req: ScheduleCreateReq): { response: ScheduleCreateRes; changes: Change[] } {
    const existingSchedule = this.schedules.get(req.data.id);
    if (existingSchedule) {
      return {
        response: this.response("schedule.create", 200, { schedule: this.toScheduleRecord(existingSchedule) }),
        changes: [],
      };
    }

    let nextRunAt: number;
    try {
      const interval = CronExpressionParser.parse(req.data.cron, { currentDate: new Date(now) });
      nextRunAt = interval.next().getTime();
    } catch {
      return { response: this.response("schedule.create", 400, "Invalid cron expression"), changes: [] };
    }

    const changes: Change[] = [];

    const schedule: Schedule = {
      id: req.data.id,
      cron: req.data.cron,
      promiseId: req.data.promiseId,
      promiseTimeout: req.data.promiseTimeout,
      promiseParam: req.data.promiseParam,
      promiseTags: req.data.promiseTags,
      createdAt: now,
    };
    changes.push(this.setSTimeout({ id: schedule.id, timeout: nextRunAt }));
    changes.push(this.setSchedule(schedule));

    return { response: this.response("schedule.create", 200, { schedule: this.toScheduleRecord(schedule) }), changes };
  }

  private scheduleDelete(req: ScheduleDeleteReq): { response: ScheduleDeleteRes; changes: Change[] } {
    const schedule = this.schedules.get(req.data.id);
    if (!schedule) {
      return { response: this.response("schedule.delete", 404, "Schedule not found"), changes: [] };
    }

    const changes: Change[] = [];
    changes.push(this.delSTimeout(req.data.id));
    changes.push(this.delSchedule(req.data.id));

    return { response: this.response("schedule.delete", 200, {}), changes };
  }

  // ===========================================================================
  // DEBUG OPERATIONS
  // ===========================================================================

  private debugStart(): { response: DebugStartRes; changes: Change[] } {
    return { response: this.response("debug.start", 200, {}), changes: [] };
  }

  private debugReset(): { response: DebugResetRes; changes: Change[] } {
    this.promises.clear();
    this.tasks.clear();
    this.schedules.clear();
    this.outgoing = [];
    this.pTimeouts = [];
    this.tTimeouts = [];
    this.sTimeouts = [];
    return { response: this.response("debug.reset", 200, {}), changes: [] };
  }

  private debugSnap(): { response: DebugSnapRes; changes: Change[] } {
    return {
      response: this.response("debug.snap", 200, {
        promises: Array.from(this.promises.values()).map((p) => this.toPromiseRecord(p)),
        promiseTimeouts: this.pTimeouts,
        callbacks: Array.from(this.promises.values()).flatMap((p) =>
          [...p.callbacks].map((awaiter) => ({ awaiter, awaited: p.id })),
        ),
        listeners: Array.from(this.promises.values()).flatMap((p) =>
          [...p.listeners].map((address) => ({ id: p.id, address })),
        ),
        tasks: Array.from(this.tasks.values()).map((t) => this.toTaskRecord(t)),
        taskTimeouts: this.tTimeouts,
        messages: this.outgoing,
      }),
      changes: [],
    };
  }

  private debugTick(req: DebugTickReq): { response: DebugTickRes; changes: Change[] } {
    const now = req.data.time;
    const changes: Change[] = [];
    const actions: DebugTickAction[] = [];

    // Promise timeouts -> settle as rejected_timedout (or resolved for timer promises)
    for (const pt of this.pTimeouts) {
      if (now >= pt.timeout) {
        const promise = this.getPromiseOrThrow(pt.id);
        if (promise.state === "pending") {
          const state = this.timeoutState(promise.tags);
          actions.push({
            kind: "promise.settle",
            data: { id: pt.id, state },
          });
        }
      }
    }

    // Task timeouts -> release (lease) or retry (pending)
    for (const tt of this.tTimeouts) {
      if (now < tt.timeout) continue;
      if (tt.type === 1) {
        const task = this.getTaskOrThrow(tt.id);
        if (task.state === "acquired") {
          actions.push({
            kind: "task.release",
            data: { id: tt.id, version: task.version },
          });
        }
      } else if (tt.type === 0) {
        const task = this.getTaskOrThrow(tt.id);
        if (task.state === "pending") {
          actions.push({
            kind: "task.retry",
            data: { id: tt.id, version: task.version },
          });
        }
      }
    }

    // Apply actions to own state. Promise settlements are split into three
    // phases to make the tick atomic — the result must not depend on the
    // order promises appear in pTimeouts.
    //
    //   Phase 1: Settle all expired promises (state change only).
    //   Phase 2: Fulfill tasks whose own promise settled (triggerFulfilled).
    //   Phase 3: Resume suspended callbacks of settled promises (triggerCallbacks).
    //
    // Phase 2 before phase 3 ensures that a task whose own promise settled
    // is fulfilled before triggerCallbacks runs. This prevents a spurious
    // suspended → pending (version++) → fulfilled path for tasks that
    // should go directly suspended → fulfilled.

    const settledIds: string[] = [];

    // Phase 1: Settle promises
    for (const action of actions) {
      if (action.kind === "promise.settle") {
        const promise = this.getPromiseOrThrow(action.data.id);
        if (promise.state !== "pending") continue;

        changes.push(
          this.setPromise({
            ...promise,
            state: action.data.state,
            value: {},
            settledAt: promise.timeoutAt,
          }),
        );

        changes.push(this.delPTimeout(action.data.id));

        settledIds.push(action.data.id);
      }
    }

    // Phase 2: Fulfill tasks whose own promise settled
    for (const id of settledIds) {
      changes.push(...this.triggerFulfilled(id));
    }

    // Phase 3: Resume suspended callbacks and notify listeners
    for (const id of settledIds) {
      changes.push(...this.triggerCallbacks(id, now));
      changes.push(...this.triggerListeners(id));
    }

    for (const action of actions) {
      if (action.kind === "task.release") {
        const task = this.getTaskOrThrow(action.data.id);
        if (task.state === "acquired" && task.version === action.data.version) {
          changes.push(this.setTask({ ...task, state: "pending", pid: undefined, ttl: undefined }));
          changes.push(this.setTTimeout({ id: action.data.id, type: 0, timeout: now + PENDING_RETRY_TTL }));

          const promise = this.getPromiseOrThrow(action.data.id);
          const address = this.getAddressOrThrow(promise);
          changes.push(
            this.sendMessage(address, {
              kind: "execute",
              head: {},
              data: { task: { id: action.data.id, version: task.version } },
            }),
          );
        }
      } else if (action.kind === "task.retry") {
        const task = this.getTaskOrThrow(action.data.id);
        if (task.state === "pending") {
          changes.push(this.setTTimeout({ id: action.data.id, type: 0, timeout: now + PENDING_RETRY_TTL }));

          const promise = this.getPromiseOrThrow(action.data.id);
          const address = this.getAddressOrThrow(promise);
          changes.push(
            this.sendMessage(address, {
              kind: "execute",
              head: {},
              data: { task: { id: action.data.id, version: task.version } },
            }),
          );
        }
      }
    }

    // Schedule timeouts -> create promises for due schedules
    for (const st of this.sTimeouts) {
      if (now < st.timeout) continue;

      const schedule = this.getScheduleOrThrow(st.id);

      const promiseId = schedule.promiseId
        .replaceAll("{{.id}}", schedule.id)
        .replaceAll("{{.timestamp}}", String(st.timeout));

      const { changes: createChanges } = this.promiseCreate(now, {
        kind: "promise.create",
        head: { corrId: "", version: "" },
        data: {
          id: promiseId,
          timeoutAt: st.timeout + schedule.promiseTimeout,
          param: schedule.promiseParam,
          tags: { ...schedule.promiseTags, "resonate:schedule": schedule.id },
        },
      });
      changes.push(...createChanges);

      // Advance to next run
      const interval = CronExpressionParser.parse(schedule.cron, {
        currentDate: new Date(st.timeout),
      });
      const nextRunAt = interval.next().getTime();
      schedule.lastRunAt = st.timeout;
      changes.push(this.setSTimeout({ id: schedule.id, timeout: nextRunAt }));
      changes.push(this.setSchedule(schedule));
    }

    return { response: this.response("debug.tick", 200, []), changes };
  }

  private debugStop(): { response: DebugStopRes; changes: Change[] } {
    return { response: this.response("debug.stop", 200, {}), changes: [] };
  }

  // ===========================================================================
  // CONVERTERS
  // ===========================================================================

  private toPromiseRecord(p: Promise): PromiseRecord {
    const { callbacks, listeners, settledAt, ...rest } = p;
    return settledAt != null ? { ...rest, settledAt } : rest;
  }

  private toTaskRecord(t: Task): TaskRecord {
    const record: TaskRecord = { id: t.id, version: t.version, state: t.state, resumes: Array.from(t.resumes) };
    if (t.pid !== undefined) record.pid = t.pid;
    if (t.ttl !== undefined) record.ttl = t.ttl;
    return record;
  }

  private toScheduleRecord(s: Schedule): ScheduleRecord {
    const st = this.sTimeouts.find((e) => e.id === s.id);
    const record: ScheduleRecord = {
      id: s.id,
      cron: s.cron,
      promiseId: s.promiseId,
      promiseTimeout: s.promiseTimeout,
      promiseParam: s.promiseParam,
      promiseTags: s.promiseTags,
      createdAt: s.createdAt,
      nextRunAt: st!.timeout,
    };
    if (s.lastRunAt != null) {
      record.lastRunAt = s.lastRunAt;
    }
    return record;
  }

  // ===========================================================================
  // HELPERS
  // ===========================================================================

  private tryAutoTimeout(now: number, id: string): Change[] {
    const promise = this.promises.get(id);
    if (!promise || promise.state !== "pending" || now < promise.timeoutAt) {
      return [];
    }

    const changes: Change[] = [];

    const state = this.timeoutState(promise.tags);
    changes.push(this.setPromise({ ...promise, state, settledAt: promise.timeoutAt }));
    changes.push(this.delPTimeout(id));
    changes.push(...this.triggerSettlement(id, now));

    return changes;
  }

  private triggerSettlement(id: string, now: number): Change[] {
    return [...this.triggerFulfilled(id), ...this.triggerCallbacks(id, now), ...this.triggerListeners(id)];
  }

  private triggerFulfilled(promiseId: string): Change[] {
    const task = this.tasks.get(promiseId);
    if (!task) return [];
    if (task.state === "fulfilled") return [];

    const changes: Change[] = [];

    changes.push(this.setTask({ ...task, state: "fulfilled", pid: undefined, ttl: undefined, resumes: new Set() }));
    changes.push(this.delTTimeout(promiseId));

    // Remove this task from all promise callbacks (delete callbacks where awaiter_id = task_id)
    for (const [, promise] of this.promises) {
      if (promise.callbacks.delete(promiseId)) {
        changes.push(this.setPromise(promise));
      }
    }

    return changes;
  }

  private triggerCallbacks(promiseId: string, now: number): Change[] {
    const settledPromise = this.getPromiseOrThrow(promiseId);

    const changes: Change[] = [];

    // Resume or buffer for all tasks that were awaiting this promise
    for (const awaiterId of settledPromise.callbacks) {
      const task = this.getTaskOrThrow(awaiterId);

      if (task.state === "suspended") {
        changes.push(this.setTask({ ...task, state: "pending", resumes: new Set([promiseId]) }));

        // Add task timeout entry back (type=0 for pending retry)
        changes.push(
          this.setTTimeout({
            id: awaiterId,
            type: 0,
            timeout: now + PENDING_RETRY_TTL,
          }),
        );

        const awaiterPromise = this.getPromiseOrThrow(awaiterId);
        const address = this.getAddressOrThrow(awaiterPromise);
        changes.push(
          this.sendMessage(address, {
            kind: "execute",
            head: {},
            data: { task: { id: awaiterId, version: task.version } },
          }),
        );
      } else if (task.state === "pending" || task.state === "acquired" || task.state === "halted") {
        // Buffer the resume — will be checked when task suspends or continues
        task.resumes.add(promiseId);
        changes.push(this.setTask(task));
      }
    }

    // Clear callbacks after processing
    settledPromise.callbacks.clear();
    changes.push(this.setPromise(settledPromise));

    return changes;
  }

  private triggerListeners(promiseId: string): Change[] {
    const promise = this.getPromiseOrThrow(promiseId);
    if (promise.listeners.size === 0) return [];

    const changes: Change[] = [];

    for (const address of promise.listeners) {
      changes.push(
        this.sendMessage(address, {
          kind: "unblock",
          head: {},
          data: { promise: this.toPromiseRecord(promise) },
        }),
      );
    }

    promise.listeners.clear();
    changes.push(this.setPromise(promise));

    return changes;
  }

  private preload(promiseId: string): PromiseRecord[] {
    const promise = this.promises.get(promiseId);
    if (!promise) return [];

    const branch = promise.tags["resonate:branch"];
    if (!branch) return [];

    const results: PromiseRecord[] = [];
    for (const [, p] of this.promises) {
      if (p.id !== promiseId && p.tags["resonate:branch"] === branch) {
        results.push(this.toPromiseRecord(p));
      }
    }
    return results;
  }

  private timeoutState(tags: Record<string, string>): "resolved" | "rejected_timedout" {
    return tags["resonate:timer"] === "true" ? "resolved" : "rejected_timedout";
  }

  private getPromiseOrThrow(id: string): Promise {
    const promise = this.promises.get(id);
    if (!promise) {
      throw new Error(`Invariant violation: promise ${id} not found`);
    }
    return promise;
  }

  private getAddressOrThrow(promise: Promise): string {
    const address = promise.tags["resonate:target"];
    if (!address) {
      throw new Error(`Invariant violation: promise ${promise.id} has no resonate:target tag`);
    }
    return address;
  }

  private getTaskOrThrow(id: string): Task {
    const task = this.tasks.get(id);
    if (!task) {
      throw new Error(`Invariant violation: task ${id} not found`);
    }
    return task;
  }
  private getScheduleOrThrow(id: string): Schedule {
    const schedule = this.schedules.get(id);
    if (!schedule) {
      throw new Error(`Invariant violation: schedule ${id} not found`);
    }
    return schedule;
  }

  // ===========================================================================
  // ACCESSORS (change-tracking)
  // ===========================================================================

  private setPromise(p: Promise): Change {
    this.promises.set(p.id, p);
    return { kind: "promise.set", promise: this.toPromiseRecord(p) };
  }

  private setTask(t: Task): Change {
    this.tasks.set(t.id, t);
    return { kind: "task.set", task: this.toTaskRecord(t) };
  }

  private setSchedule(s: Schedule): Change {
    this.schedules.set(s.id, s);
    return { kind: "schedule.set", schedule: this.toScheduleRecord(s) };
  }

  private delSchedule(id: string): Change {
    this.schedules.delete(id);
    return { kind: "schedule.del", id };
  }

  private setPTimeout(pt: PTimeout): Change {
    const idx = this.pTimeouts.findIndex((e) => e.id === pt.id);
    if (idx !== -1) {
      this.pTimeouts[idx] = pt;
    } else {
      this.pTimeouts.push(pt);
    }
    return { kind: "ptimeout.set", timeout: { id: pt.id, timeout: pt.timeout } };
  }

  private delPTimeout(id: string): Change {
    const idx = this.pTimeouts.findIndex((e) => e.id === id);
    if (idx !== -1) {
      this.pTimeouts.splice(idx, 1);
    }
    return { kind: "ptimeout.del", id };
  }

  private setTTimeout(tt: TTimeout): Change {
    const idx = this.tTimeouts.findIndex((e) => e.id === tt.id);
    if (idx !== -1) {
      this.tTimeouts[idx] = tt;
    } else {
      this.tTimeouts.push(tt);
    }
    return { kind: "ttimeout.set", timeout: { id: tt.id, type: tt.type, timeout: tt.timeout } };
  }

  private delTTimeout(id: string): Change {
    const idx = this.tTimeouts.findIndex((e) => e.id === id);
    if (idx !== -1) {
      this.tTimeouts.splice(idx, 1);
    }
    return { kind: "ttimeout.del", id };
  }

  private setSTimeout(st: STimeout): Change {
    const idx = this.sTimeouts.findIndex((e) => e.id === st.id);
    if (idx !== -1) {
      this.sTimeouts[idx] = st;
    } else {
      this.sTimeouts.push(st);
    }
    return { kind: "stimeout.set", timeout: { id: st.id, timeout: st.timeout } };
  }

  private delSTimeout(id: string): Change {
    const idx = this.sTimeouts.findIndex((e) => e.id === id);
    if (idx !== -1) {
      this.sTimeouts.splice(idx, 1);
    }
    return { kind: "stimeout.del", id };
  }

  // FIX: Change from accumulate (push) to upsert by task ID.
  // A task can only be claimed by one worker at one version, so when a task
  // is resumed (version incremented), the previous message is obsolete.
  // Upsert keeps only the latest message per task ID, matching the HTTP's
  // outgoing table behavior (ON CONFLICT (id) DO UPDATE).
  private sendMessage(address: string, msg: Message): Change {
    if (msg.kind === "execute") {
      const taskId = msg.data.task.id;
      const idx = this.outgoing.findIndex((m) => m.message.kind === "execute" && m.message.data.task.id === taskId);
      if (idx >= 0) {
        this.outgoing[idx] = { address, message: msg };
      } else {
        this.outgoing.push({ address, message: msg });
      }
    } else {
      this.outgoing.push({ address, message: msg });
    }
    return { kind: "message.send", address, message: msg };
  }
  // OLD: accumulate (push) — causes divergence with HTTP snapshots
  // private sendMessage(address: string, msg: Message): Change {
  //   this.outgoing.push({ address, message: msg });
  //   return { kind: "message.send", address, message: msg };
  // }

  private response<K extends Response["kind"]>(kind: K, status: number, data: unknown): Extract<Response, { kind: K }> {
    return { kind, head: { status }, data } as Extract<Response, { kind: K }>;
  }
}

// =============================================================================
// LOCAL NETWORK
// =============================================================================

export class LocalNetwork implements Network {
  readonly unicast: string;
  readonly anycast: string;

  private started: boolean;
  private server: Server;
  private subscribers: Array<(msg: Message) => void> = [];
  private tickInterval?: ReturnType<typeof setInterval>;

  constructor({
    pid = crypto.randomUUID().replace(/-/g, ""),
    group = "default",
  }: {
    pid?: string;
    group?: string;
  } = {}) {
    this.started = false;
    this.server = new Server();
    this.unicast = `local://uni@${group}/${pid}`;
    this.anycast = `local://any@${group}/${pid}`;
  }

  // -- Network ---------------------------------------------------------------

  match(target: string): string {
    return `local://any@${target}`;
  }

  async init(): globalThis.Promise<void> {
    if (this.started) return;
    this.tickInterval = setInterval(() => {
      const now = Date.now();
      const result = this.server.apply(now, {
        kind: "debug.tick",
        head: { corrId: randomUUID(), version: VERSION },
        data: { time: now },
      });
      this.dispatchMessages(result);
    }, 1000);
    this.started = true;
  }

  async stop(): globalThis.Promise<void> {
    if (this.tickInterval) {
      clearInterval(this.tickInterval);
    }
    this.subscribers = [];
    this.started = false;
  }

  send = <K extends Request["kind"]>(
    req: Extract<Request, { kind: K }>,
  ): globalThis.Promise<Extract<Response, { kind: K }>> => {
    const { corrId, version } = req.head;

    const now = Date.now();
    const result = this.server.apply(now, req);
    const response = result.response;

    const res = {
      kind: response.kind,
      head: { corrId, status: response.head.status, version },
      data: response.data,
    };
    assert(isResponse(res));

    setTimeout(() => this.dispatchMessages(result), 0);

    return Promise.resolve(res as Extract<Response, { kind: K }>);
  };

  recv(callback: (msg: Message) => void): void {
    this.subscribers.push(callback);
  }

  // -- internal: message dispatch --------------------------------------------

  private dispatchMessages(result: { response: Response; changes: Change[] }): void {
    for (const change of result.changes) {
      if (change.kind !== "message.send") continue;
      for (const cb of this.subscribers) {
        cb(change.message);
      }
    }
  }
}
