import { Server } from "../../dev/server";

import type {
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
  HeartbeatTasksReq,
  HeartbeatTasksRes,
  Network,
  ReadPromiseReq,
  ReadPromiseRes,
  ReadScheduleReq,
  ReadScheduleRes,
  RecvMsg,
  RequestMsg,
  ResponseMsg,
} from "./network";

export class LocalNetwork implements Network {
  private server: Server;
  private timeoutId: ReturnType<typeof setTimeout> | undefined;

  constructor(server: Server = new Server()) {
    this.server = server;
    this.timeoutId = undefined;
  }

  private enqueueNext(): void {
    clearTimeout(this.timeoutId);
    const n = this.server.next();

    if (n !== undefined) {
      this.timeoutId = setTimeout((): void => {
        const msgs = this.server.step();
        this.enqueueNext();
        this.recv(msgs);
      }, n);
    }
  }

  send(request: RequestMsg, callback: (timeout: boolean, response: ResponseMsg) => void): void {
    const response = this.handleRequest(request);
    clearTimeout(this.timeoutId);
    this.enqueueNext();
    callback(false, response);
  }

  recv(msg: any): void {
    const msgs = msg as RecvMsg[];
    for (const m of msgs) {
      this.onMessage?.(m);
    }
  }

  public onMessage?: (msg: RecvMsg) => void;

  private handleRequest(request: RequestMsg): ResponseMsg {
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
        throw new Error("not implemented");
      case "heartbeatTasks":
        return this.heartbeatTasks(request);
      default:
        throw new Error(`Unsupported request kind: ${(request as any).kind}`);
    }
  }
  private createPromise(request: CreatePromiseReq): CreatePromiseRes {
    return {
      kind: "createPromise",
      promise: this.server.createPromise(
        request.id,
        request.timeout,
        request.param,
        request.tags,
        request.iKey,
        request.strict,
      ),
    };
  }
  private createPromiseAndTask(request: CreatePromiseAndTaskReq): CreatePromiseAndTaskRes {
    const { promise, task } = this.server.createPromiseAndTask(
      request.promise.id,
      request.promise.timeout,
      request.task.processId,
      request.task.ttl,
      request.promise.param,
      request.promise.tags,
      request.iKey,
      request.strict,
    );
    return {
      kind: "createPromiseAndTask",
      promise: promise,
      task: task,
    };
  }

  private readPromise(request: ReadPromiseReq): ReadPromiseRes {
    return {
      kind: "readPromise",
      promise: this.server.readPromise(request.id),
    };
  }

  private completePromise(request: CompletePromiseReq): CompletePromiseRes {
    return {
      kind: "completePromise",
      promise: this.server.completePromise(request.id, request.state, request.value, request.iKey, request.strict),
    };
  }

  private createSubscription(request: CreateSubscriptionReq): CreateSubscriptionRes {
    return {
      kind: "createSubscription",
      ...this.server.createSubscription(request.id, request.timeout, request.recv),
    };
  }

  private createSchedule(request: CreateScheduleReq): CreateScheduleRes {
    return {
      kind: "createSchedule",
      schedule: this.server.createSchedule(
        request.id!,
        request.cron!,
        request.promiseId!,
        request.promiseTimeout!,
        request.iKey,
        request.description,
        request.tags,
        request.promiseParam,
        request.promiseTags,
      ),
    };
  }
  private readSchedule(request: ReadScheduleReq): ReadScheduleRes {
    return { kind: "readSchedule", schedule: this.server.readSchedule(request.id) };
  }

  private deleteSchedule(request: DeleteScheduleReq): DeleteScheduleRes {
    return { kind: "deleteSchedule" };
  }
  private createCallback(request: CreateCallbackReq): CreateCallbackRes {
    return {
      kind: "createCallback",
      ...this.server.createCallback(request.id, request.rootPromiseId, request.timeout, request.recv),
    };
  }

  private claimTask(request: ClaimTaskReq): ClaimTaskRes {
    return {
      kind: "claimedtask",
      message: this.server.claimTask(request.id, request.counter, request.processId, request.ttl),
    };
  }

  private completeTask(request: CompleteTaskReq): CompleteTaskRes {
    return {
      kind: "completedtask",
      task: this.server.completeTask(request.id, request.counter),
    };
  }

  private heartbeatTasks(request: HeartbeatTasksReq): HeartbeatTasksRes {
    return {
      kind: "heartbeatTasks",
      tasksAffected: this.server.heartbeatTasks(request.processId),
    };
  }
}
