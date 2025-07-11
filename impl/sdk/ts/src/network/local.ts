import { Server } from "../server";
import type {
  ClaimTaskReq,
  ClaimTaskRes,
  CompletePromiseReq,
  CompletePromiseRes,
  CompleteTaskReq,
  CompleteTaskRes,
  CreateCallbackReq,
  CreateCallbackRes,
  CreatePromiseReq,
  CreatePromiseRes,
  CreateSubscriptionReq,
  CreateSubscriptionRes,
  HeartbeatTasksReq,
  HeartbeatTasksRes,
  Network,
  ReadPromiseReq,
  ReadPromiseRes,
  RecvMsg,
  RequestMsg,
  ResponseMsg,
} from "./network";

export class LocalNetwork implements Network {
  private server: Server;

  constructor(server: Server = new Server()) {
    this.server = server;
  }
  send(request: RequestMsg, callback: (timeout: boolean, response: ResponseMsg) => void): void {
    callback(false, this.handleRequest(request));
  }

  recv(msg: RecvMsg): void {}

  private handleRequest(request: RequestMsg): ResponseMsg {
    switch (request.kind) {
      case "createPromise":
        return this.createPromise(request);
      case "createPromiseAndTask":
        throw new Error("not implemented");
      case "readPromise":
        return this.readPromise(request);
      case "completePromise":
        return this.completePromise(request);
      case "createCallback":
        return this.createCallback(request);
      case "createSubscription":
        return this.createSubscription(request);
      case "createSchedule":
        throw new Error("not implemented");
      case "readSchedule":
        throw new Error("not implemented");
      case "deleteSchedule":
        throw new Error("not implemented");
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
      tasksAffected: this.server.hearbeatTasks(request.processId),
    };
  }
}
