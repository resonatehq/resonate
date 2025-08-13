import { LocalNetwork } from "../dev/network";
import { Computation } from "../src/computation";
import type { Context } from "../src/context";
import type { CreatePromiseAndTaskRes, DurablePromiseRecord, Network, TaskRecord } from "../src/network/network";
import type { Processor } from "../src/processor/processor";
import { Registry } from "../src/registry";
import type { ClaimedTask, CompResult, Result } from "../src/types";
import * as util from "../src/util";

async function createPromiseAndTask(
  network: Network,
  id: string,
  funcName: string,
  args: any[],
): Promise<{ promise: DurablePromiseRecord; task: TaskRecord }> {
  return new Promise((resolve, _reject) => {
    network.send(
      {
        kind: "createPromiseAndTask",
        promise: {
          id: id,
          timeout: 24 * util.HOUR + Date.now(),
          param: {
            func: funcName,
            args,
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
      (_timeout, res) => {
        const { promise, task } = res as CreatePromiseAndTaskRes;
        resolve({ promise, task: task! });
      },
    );
  });
}

// A captured task waiting to be completed
interface PendingTodo {
  id: string;
  fn: () => Promise<any>;
  callback: (result: Result<any>) => void;
}

// This mock allows us to control when "async" tasks complete.
class MockProcessor implements Processor {
  public pendingTodos: Map<string, PendingTodo> = new Map();
  private todoNotifier?: { expectedCount: number; resolve: () => void };

  process(id: string, fn: () => Promise<any>, callback: (result: Result<any>) => void): void {
    // Instead of running the work, we just store it.
    this.pendingTodos.set(id, { id, fn, callback });

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
      todo.callback({ success: true, data: `completed-${todo.id}` });
    }
  }
}

describe("Computation Event Queue Concurrency", () => {
  let mockProcessor: MockProcessor;
  let network: LocalNetwork;
  let registry: Registry;
  let computation: Computation;

  const doRunSpy = jest.spyOn(Computation.prototype as any, "doRun");

  beforeEach(() => {
    jest.clearAllMocks();
    mockProcessor = new MockProcessor();
    network = new LocalNetwork();
    registry = new Registry();

    computation = new Computation("root-promise-1", network, registry, "test-group", "test-pid", 3600, mockProcessor);
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

    registry.set("testCoro", testCoroutine);
    const { promise, task } = await createPromiseAndTask(network, "root-promise-1", "testCoro", []);

    const testTask: ClaimedTask = {
      ...task,
      kind: "claimed",
      rootPromiseId: "root-promise-1",
      rootPromise: {
        ...promise,
      },
    };

    const computationPromise: Promise<CompResult> = new Promise((resolve) => {
      computation.process(testTask, (result) => {
        resolve(result);
      });
    });

    await mockProcessor.waitForTasks(4);
    // At this point, doRun has been called once for the initial "invoke"
    expect(doRunSpy).toHaveBeenCalledTimes(1);
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
    expect(doRunSpy).toHaveBeenCalledTimes(5);
    expect(result).toMatchObject({
      kind: "completed",
      durablePromise: {
        value: {
          a: "completed-root-promise-1.0",
          b: "completed-root-promise-1.1",
          c: "completed-root-promise-1.2",
          d: "completed-root-promise-1.3",
        },
      },
    });
  });
});
