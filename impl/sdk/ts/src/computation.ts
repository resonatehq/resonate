import type { Context } from "./context";
import { Coroutine, type LocalTodo, type RemoteTodo, type Suspended } from "./coroutine";
import { Handler, type Task } from "./handler";
import type { Network } from "./network/network";
import { AsyncProcessor, type Processor, type Result } from "./processor/processor";
import * as util from "./util";

interface InvocationParams {
  id: string;
  fn: (ctx: Context, ...args: any[]) => any;
  args: any[];
}

type Event = "invoke" | "return";

export class Computation {
  public handler: Handler;

  private pid: string;
  private group: string;
  private eventQueue: Event[] = [];
  private isProcessing = false;
  private network: Network;
  private processor: Processor;
  private seenTodos: Set<string>;
  private task?: Task;
  private callback?: (err: any, result: any) => void;
  private invocationParams?: InvocationParams;

  constructor(network: Network, group: string, pid: string, processor?: Processor) {
    this.handler = new Handler(network);
    this.network = network;
    this.pid = pid;
    this.group = group;
    this.processor = processor ?? new AsyncProcessor();
    this.seenTodos = new Set();
  }

  invoke(task: Task, invocationParams: InvocationParams, cb: (err: any, result: any) => void): void {
    if (this.task) {
      console.error("Trying to invoke a running computation using task:", task); // TODO (avillega): does this one holds true?
      return;
    }
    this.task = task;
    util.assert(this.eventQueue.length === 0, "The event queue must be empty on a new invocation");
    this.invocationParams = invocationParams;
    this.callback = cb;
    this.eventQueue.push("invoke");
    this.process();
  }

  // Resumes an already alive computation
  resume(task: Task): void {
    console.log("resuming", this.invocationParams?.id);
    util.assertDefined(this.invocationParams);

    this.task = task;
    this.eventQueue.push("invoke");
    this.process();
  }

  private process(): void {
    // Guard against concurrent processing
    if (this.isProcessing) {
      return;
    }

    this.isProcessing = true;

    if (this.eventQueue.length === 0) {
      this.isProcessing = false;
      return;
    }

    const event = this.eventQueue.shift();
    util.assertDefined(event);

    const { id: rootId, fn, args } = this.invocationParams!;

    Coroutine.exec(rootId, fn, args, this.handler, (result) => {
      if (result.type === "completed") {
        // TODO(avillega): Handle rejected results aswell
        this.handler.resolvePromise(rootId, result.value, (durablePromise) => {
          util.assertDefined(this.task);
          this.network.send({ kind: "completeTask", id: this.task.id, counter: this.task.counter }, () => {
            // Clear the computation
            this.task = undefined;
            this.seenTodos.clear();

            this.callback?.(null, durablePromise.value);
          });
        });
      } else {
        if (result.localTodos.length === 0) {
          this.handleRemoteTodos(rootId, result.remoteTodos);
        } else {
          this.handleLocalTodos(result.localTodos);
        }
      }
    });

    // Reset processing flag and continue if there are more events
    this.isProcessing = false;
    if (this.eventQueue.length > 0) {
      this.process();
    }
  }

  private handleRemoteTodos(rootId: string, remoteTodos: RemoteTodo[]): void {
    let createdCallbacks = 0;
    const totalCallbacks = remoteTodos.length;

    for (const remoteTodo of remoteTodos) {
      const { id } = remoteTodo;
      this.handler.createCallback(
        id,
        rootId,
        Number.MAX_SAFE_INTEGER, // TODO (avillega): use the promise timeout
        `poll://any@${this.group}/${this.pid}`,
        (result) => {
          if (result.kind === "promise") {
            this.scheduleNextProcess();
            return;
          }
          if (result.kind === "callback") {
            createdCallbacks++;
            if (createdCallbacks === totalCallbacks) {
              this.completeTask();
            }
            return;
          }
        },
      );
    }
  }

  private handleLocalTodos(localTodos: LocalTodo[]): void {
    for (const localTodo of localTodos) {
      if (this.seenTodos.has(localTodo.id)) {
        continue;
      }

      this.seenTodos.add(localTodo.id);
      const { id, fn, ctx, args } = localTodo;

      this.processor.process(
        id,
        async () => {
          return await fn(ctx, ...args);
        },
        (result) => {
          const value = result.success ? result.data : result.error;

          // TODO (avillega): Need to do a rejection instead of resolving with error
          this.handler.resolvePromise(id, value, (_) => {
            this.scheduleNextProcess();
          });
        },
      );
    }
  }

  private completeTask() {
    // TODO (avillega): Should the task always be defined?
    if (this.task) {
      util.assert(this.eventQueue.length === 0, "The event queue must be empty when completing the task"); // The queue must be empty
      this.network.send({ kind: "completeTask", id: this.task.id, counter: this.task.counter }, () => {
        // Once the task is completed reset the computation
        this.task = undefined;
      });
    }
  }

  private scheduleNextProcess(): void {
    this.eventQueue.push("return");
    if (!this.isProcessing) {
      this.process();
    }
  }
}
