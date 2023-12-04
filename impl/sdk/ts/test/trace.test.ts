// TEST=true npm test -- test/trace.test.ts

import { jest, describe, test, expect } from "@jest/globals";
import { Resonate, Context } from "../lib/resonate";
import { TestLogger } from "../lib/loggers/test";

jest.setTimeout(100000); // 100 seconds

type DurableExecution = Func | AsyncFunc | GeneratorFunc;

interface Func {
  (c: Context, ...args: any[]): any;
}

interface AsyncFunc {
  (c: Context, ...args: any[]): Promise<any>;
}

interface GeneratorFunc {
  (c: Context, ...args: any[]): Generator<any, any, any>;
}

class TraceTestSuite {
  // Durable Executions to test.
  public durableExecutions: DurableExecution[] = [];

  // Durable Execution arguments.
  public args: any[] = [];

  // Test output from app transactions.
  public output: any[] = [];

  // Traces from loggers.
  public traces: any[] = [];

  // Flag to test timestamp ordering.
  public timestamp: boolean = false;

  // Constructor
  constructor({ timestamp }: { timestamp?: boolean }) {
    this.timestamp = timestamp ?? false;
  }

  // Add transaction to assert congruent output and traces.
  addDurableExecution(t: DurableExecution, ...args: any[]) {
    this.durableExecutions.push(t);
    this.args.push(args);
  }

  // Run app transactions and collect outputs and traces.
  async run() {
    for (let i = 0; i < this.durableExecutions.length; i++) {
      const testLogger = new TestLogger();
      const resonate = new Resonate({ logger: testLogger });

      // Get durable execution and its arguments.
      const t = this.durableExecutions[i];
      const args = this.args[i];

      // Register and run transaction.
      const r = resonate.register("test", t);
      const value = await r("id", ...args);

      // Collect output.
      this.output.push(value);

      // Collect traces.
      this.traces.push(testLogger.traces);
    }
  }

  // Validate test output and traces.
  validate(): boolean {
    // Check output values were equal.
    const first = this.output[0];
    for (let i = 1; i < this.output.length; i++) {
      if (this.output[i] !== first) {
        console.log(`error: output mismatch ${this.output[i]} ${first}`);
        return false;
      }
    }

    // Check traces were congruent.
    interface TraceInfo {
      id: string;
      parentId?: string;
      timestamp: number;
      duration: number;
      attrs?: any;
    }

    const firstTrace = this.traces[0];
    if (this.timestamp) {
      if (!isValidTraceTimestamp(firstTrace)) {
        console.log(`error: trace timestamp invalid`);
        return false;
      }
    }

    for (let i = 1; i < this.traces.length; i++) {
      const nTrace = this.traces[i];

      // Check traces are equal length.
      if (firstTrace.length !== nTrace.length) {
        console.log(`error: trace length mismatch ${firstTrace.length} ${nTrace.length}`);
        return false;
      }

      // Check individual trace info is congruent.
      for (let j = 0; j < firstTrace.length; j++) {
        const firstTraceInfo: TraceInfo = firstTrace[j];
        const nTraceInfo: TraceInfo = nTrace[j];

        if (!compareTraceIDs(firstTraceInfo.id, nTraceInfo.id)) {
          console.log(`error: trace id mismatch ${firstTraceInfo.id} ${nTraceInfo.id}`);
          return false;
        }

        if (!compareTraceIDs(firstTraceInfo.parentId, nTraceInfo.parentId)) {
          console.log(`error: trace parent id mismatch ${firstTraceInfo.parentId} ${nTraceInfo.parentId}`);
          return false;
        }
      }

      // Check timestamp ordering.
      if (this.timestamp) {
        if (!isValidTraceTimestamp(nTrace)) {
          console.log(`error: trace timestamp invalid ${nTrace}`);
          return false;
        }
      }
    }

    // Compare if trace IDs are congruent.
    function compareTraceIDs(id1: string | undefined, id2: string | undefined): boolean {
      return id1 === id2;
    }

    function isValidTraceTimestamp(traces: TraceInfo[]): boolean {
      const sorted = sortTraces(traces);
      for (let i = 0; i < sorted.length - 1; i++) {
        const trace = sorted[i];
        const nextTrace = sorted[i + 1];
        if (trace.timestamp > nextTrace.timestamp) {
          return false;
        }
      }
      return true;
    }

    function sortTraces(traces: TraceInfo[]): TraceInfo[] {
      const sorted: TraceInfo[] = [];

      const keyToTrace = new Map<string, TraceInfo>();

      for (const trace of traces) {
        keyToTrace.set(trace.id, trace);
      }

      // The trace structure forms a tree hierarchy that
      // lends itself well to a depth first search traversal.
      const visit = (trace: TraceInfo) => {
        if (sorted.includes(trace)) return;

        if (trace.parentId) {
          const parent = keyToTrace.get(trace.parentId);
          if (parent) visit(parent);
        }

        sorted.push(trace);

        for (const child of traces) {
          if (child.parentId === trace.id) {
            visit(child);
          }
        }
      };

      visit(traces.find((t) => !t.parentId)!);

      // console.log(sorted);
      return sorted;
    }

    return true;
  }
}
describe("Resonate SDK Trace Tests", () => {
  test("trace matrix: simple addition app", async () => {
    const a1 = async function (c: Context, a: number, b: number): Promise<number> {
      if (c.attempt < 1) throw new Error("a1");
      return a + b;
    };

    const g1 = function* (c: Context, a: number, b: number): Generator<Promise<number>, number, number> {
      if (c.attempt < 1) throw new Error("g1");
      return a + b;
    };

    const transactionAsyncAsync = async (c: Context, a: number, b: number): Promise<number> => {
      const x = await c.run(a1, a, b);
      const y = await c.run(a1, a, b);

      return x + y;
    };

    const transactionAsyncGenerator = async (c: Context, a: number, b: number): Promise<number> => {
      const x = await c.run(g1, a, b);
      const y = await c.run(g1, a, b);

      return x + y;
    };

    const transactionGeneratorGenerator = function* (
      c: Context,
      a: number,
      b: number,
    ): Generator<Promise<number>, number, number> {
      const x = yield c.run(g1, a, b);
      const y = yield c.run(g1, a, b);

      return x + y;
    };

    const transactionGeneratorAsync = function* (
      c: Context,
      a: number,
      b: number,
    ): Generator<Promise<Promise<number>>, number, number> {
      const x = yield c.run(a1, a, b);
      const y = yield c.run(a1, a, b);

      return x + y;
    };

    const suite = new TraceTestSuite({ timestamp: true });

    suite.addDurableExecution(transactionAsyncAsync, 1, 1);
    suite.addDurableExecution(transactionAsyncGenerator, 1, 1);
    suite.addDurableExecution(transactionGeneratorGenerator, 1, 1);
    suite.addDurableExecution(transactionGeneratorAsync, 1, 1);

    await suite.run();
    expect(suite.validate()).toBe(true);
  });

  test("trace matrix: fibonacci recursion app", async () => {
    const fib = async function (c: Context, n: number): Promise<number> {
      if (n <= 1) return n;

      const fun1 = Math.random() < 0.5 ? fib : fibGen;
      const fun2 = Math.random() < 0.5 ? fib : fibGen;

      const x = await c.run(fun1, n - 1);
      const y = await c.run(fun2, n - 2);

      return x + y;
    };

    const fibGen = function* (c: Context, n: number): Generator<Promise<any>, any, any> {
      if (n <= 1) return n;

      const fun1 = Math.random() < 0.5 ? fib : fibGen;
      const fun2 = Math.random() < 0.5 ? fib : fibGen;

      const x = yield c.run(fun1, n - 1);
      const y = yield c.run(fun2, n - 2);

      return x + y;
    };

    const transactionAsync = async (c: Context, n: number): Promise<number> => {
      return await c.run(fib, n);
    };

    const transactionGen = function* (c: Context, n: number): Generator<Promise<any>, any, any> {
      return yield c.run(fibGen, n);
    };

    const suite = new TraceTestSuite({ timestamp: true });

    suite.addDurableExecution(transactionAsync, 10);
    suite.addDurableExecution(transactionGen, 10);

    await suite.run();
    expect(suite.validate()).toBe(true);
  });
});
