import type { Context } from "./context";
import { Coroutine, type LocalTodo, type RemoteTodo } from "./coroutine";
import { Handler } from "./handler";
import type { Network } from "./network/network";
import { AsyncProcessor, type Processor } from "./processor/processor";
import type { Registry } from "./registry";
import type { ClaimedTask, CompResult, Task } from "./types";
import * as util from "./util";

interface InvocationParams {
  fn: (ctx: Context, ...args: any[]) => any;
  args: any[];
}

type Event = "invoke" | "return";

export class Computation {
  private promiseId: string;
  private pid: string;
  private group: string;
  private ttl: number;
  private eventQueue: Event[] = [];
  private isProcessing = false;
  private network: Network;
  private registry: Registry;
  private handler: Handler;
  private processor: Processor;
  private seenTodos: Set<string>;
  private task?: Task;
  private callback?: (res: CompResult) => void;
  private invocationParams?: InvocationParams;

  constructor(
    promiseId: string,
    network: Network,
    registry: Registry,
    group: string,
    pid: string,
    ttl: number,
    processor?: Processor,
  ) {
    this.promiseId = promiseId;
    this.handler = new Handler(network);
    this.network = network;
    this.registry = registry;
    this.pid = pid;
    this.group = group;
    this.ttl = ttl;
    this.processor = processor ?? new AsyncProcessor();
    this.seenTodos = new Set();
  }

  process(task: Task, cb: (res: CompResult) => void): void {
    if (task.kind === "claimed") {
      this.processClaimed(task, cb);
    } else if (task.kind === "unclaimed") {
      this.network.send(
        {
          kind: "claimTask",
          id: task.id,
          counter: task.counter,
          processId: this.pid,
          ttl: this.ttl,
        },
        (_timeout, response) => {
          if (response.kind === "claimedtask") {
            const { root, leaf } = response.message.promises;
            util.assertDefined(root);
            if (leaf) {
              this.handler.updateCache(leaf.data);
            }
            this.processClaimed({ ...task, kind: "claimed", rootPromise: root.data }, cb);
          }
        },
      );
    }
  }

  private processClaimed(task: ClaimedTask, cb: (res: CompResult) => void) {
    this.task = task;
    this.callback = cb;
    this.handler.updateCache(task.rootPromise);
    const fn = this.registry.get(task.rootPromise.param?.fn ?? "");

    if (!fn) {
      // TODO(avillega): drop the task here and call the callback with an error
      console.warn("couldn't find fn in the registry");
      return;
    }

    if (!this.invocationParams) {
      this.invocationParams = {
        fn,
        args: task.rootPromise.param?.args ?? [],
      };
    }
    this.eventQueue.push("invoke");
    this.run();
  }

  private run(): void {
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

    const { fn, args } = this.invocationParams!;

    Coroutine.exec(this.promiseId, fn, args, this.handler, (result) => {
      if (result.type === "completed") {
        this.handler.resolvePromise(this.promiseId, result.value, (durablePromise) => {
          util.assertDefined(this.task);
          this.completeTask({
            kind: "completed",
            promiseId: this.promiseId,
            result: durablePromise.value,
          });
        });
      } else {
        if (result.localTodos.length === 0) {
          this.handleRemoteTodos(this.promiseId, result.remoteTodos);
        } else {
          this.handleLocalTodos(result.localTodos);
        }
      }
    });

    // Reset processing flag and continue if there are more events
    this.isProcessing = false;
    if (this.eventQueue.length > 0) {
      this.run();
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
              this.completeTask({
                kind: "suspended",
                promiseId: this.promiseId,
              });
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

  private completeTask(result: CompResult) {
    // TODO (avillega): Should the task always be defined?
    if (this.task) {
      util.assert(this.eventQueue.length === 0, "The event queue must be empty when completing the task"); // The queue must be empty
      this.network.send({ kind: "completeTask", id: this.task.id, counter: this.task.counter }, () => {
        // Once the task is completed reset the computation
        this.task = undefined;
        this.seenTodos.clear();
        this.callback?.(result);
        this.callback = undefined;
      });
    }
  }

  private scheduleNextProcess(): void {
    this.eventQueue.push("return");
    if (!this.isProcessing) {
      this.run();
    }
  }
}
