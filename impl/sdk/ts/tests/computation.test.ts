import { LocalNetwork } from "../dev/network";
import { WallClock } from "../src/clock";
import { Computation, type Status } from "../src/computation";
import type { Context } from "../src/context";
import { JsonEncoder } from "../src/encoder";
import { Handler } from "../src/handler";
import { NoopHeartbeat } from "../src/heartbeat";
import type { DurablePromiseRecord, TaskRecord } from "../src/network/network";
import type { Processor } from "../src/processor/processor";
import { Registry } from "../src/registry";
import type { ClaimedTask } from "../src/resonate-inner";
import type { Result } from "../src/types";
import * as util from "../src/util";

async function createPromiseAndTask(
  handler: Handler,
  id: string,
  funcName: string,
  args: any[],
): Promise<{ promise: DurablePromiseRecord; task: TaskRecord }> {
  return new Promise((resolve) => {
    handler.createPromiseAndTask(
      {
        kind: "createPromiseAndTask",
        promise: {
          id: id,
          timeout: 24 * util.HOUR + Date.now(),
          param: {
            data: {
              func: funcName,
              args: args,
              version: 1,
            },
          },
          tags: { "resonate:invoke": "poll://any@default/default" },
        },
        task: {
          processId: "default",
          ttl: 5 * util.MIN,
        },
        iKey: id,
        strict: false,
      },
      (_, res) => {
        util.assertDefined(res);
        resolve({ promise: res!.promise, task: res!.task! });
      },
    );
  });
}

// A captured task waiting to be completed
interface PendingTodo {
  id: string;
  func: () => Promise<any>;
  callback: (result: Result<any>) => void;
}

// This mock allows us to control when "async" tasks complete.
class MockProcessor implements Processor {
  public pendingTodos: Map<string, PendingTodo> = new Map();
  private todoNotifier?: { expectedCount: number; resolve: () => void };

  process(id: string, name: string, func: () => Promise<any>, callback: (result: Result<any>) => void): void {
    // Instead of running the work, we just store it.
    this.pendingTodos.set(id, { id, func, callback });

    // After adding a task, check if we've met the count the test is waiting for.
    if (this.todoNotifier && this.pendingTodos.size >= this.todoNotifier.expectedCount) {
      this.todoNotifier.resolve();
      this.todoNotifier = undefined;
    }
  }

  async waitForTasks(count: number): Promise<void> {
    return new Promise((resolve) => {
      // If we already have enough tasks, resolve immediately.
      if (this.pendingTodos.size >= count) {
        return resolve();
      }
      // Otherwise, store the resolver to be called later in `process`.
      this.todoNotifier = { expectedCount: count, resolve };
    });
  }

  async completeAll() {
    const todosToComplete = [...this.pendingTodos.values()];
    this.pendingTodos.clear();

    // Fire all callbacks in the same event loop tick to simulate concurrency
    for (const todo of todosToComplete) {
      // We resolve with a simple value for the test
      todo.callback({ success: true, value: `completed-${todo.id}` });
    }
  }
}

describe("Computation Event Queue Concurrency", () => {
  let mockProcessor: MockProcessor;
  let network: LocalNetwork;
  let handler: Handler;
  let registry: Registry;
  let computation: Computation;

  // const processSpy = jest.spyOn(Computation.prototype as any, "_process");

  beforeEach(() => {
    jest.clearAllMocks();
    mockProcessor = new MockProcessor();
    network = new LocalNetwork();
    handler = new Handler(network, new JsonEncoder());
    registry = new Registry();

    computation = new Computation(
      "root-promise-1",
      "poll://uni@test-group/test-pid",
      "poll://any@test-group/test-pid",
      "poll://any@test-group",
      "test-pid",
      3600,
      new WallClock(),
      network,
      new Handler(network, new JsonEncoder()),
      registry,
      new NoopHeartbeat(),
      new Map(),
      false,
      mockProcessor,
    );
  });

  test("should process all events serially even when they complete concurrently", async () => {
    function* testCoroutine(ctx: Context) {
      const fa = yield* ctx.beginRun(() => "this-value-wont-be-used");
      const fb = yield* ctx.beginRun(() => "this-value-wont-be-used");
      const fc = yield* ctx.beginRun(() => "this-value-wont-be-used");
      const fd = yield* ctx.beginRun(() => "this-value-wont-be-used");
      const va = yield* fa;
      const vb = yield* fb;
      const vc = yield* fc;
      const vd = yield* fd;
      return { a: va, b: vb, c: vc, d: vd };
    }

    registry.add(testCoroutine, "testCoro");
    const { promise, task } = await createPromiseAndTask(handler, "root-promise-1", "testCoro", []);

    const testTask: ClaimedTask = {
      kind: "claimed",
      task: task,
      rootPromise: promise,
    };

    const computationPromise: Promise<Status> = new Promise((resolve) => {
      computation.process(testTask, (err, res) => {
        if (err || !res) {
          throw new Error("Computation processing failed");
        }

        resolve(res);
      });
    });

    await mockProcessor.waitForTasks(4);
    // At this point, doRun has been called once for the initial "invoke"
    // expect(processSpy).toHaveBeenCalledTimes(1);
    expect(mockProcessor.pendingTodos.size).toBe(4);

    // Trigger concurrent completion
    await mockProcessor.completeAll();

    const result = await computationPromise;

    // Check that `doRun` was called correctly.
    // - 1st call: Initial 'invoke'
    // - 2nd call: For the fa 'return' event.
    // - 3rd call: For the fb 'return' event.
    // - 4th call: For the fc 'return' event.
    // - 5th call: For the fd 'return' event.
    // expect(processSpy).toHaveBeenCalledTimes(5);
    expect(result).toMatchObject({
      kind: "completed",
      promise: {
        value: {
          data: {
            a: "completed-root-promise-1.0",
            b: "completed-root-promise-1.1",
            c: "completed-root-promise-1.2",
            d: "completed-root-promise-1.3",
          },
        },
      },
    });
  });
});
