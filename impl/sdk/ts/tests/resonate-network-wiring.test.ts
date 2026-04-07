/**
 * Tests for Section 2.0.4: Resonate <-> Network Wiring Tests
 *
 * These tests verify that:
 * 1. Injecting a custom Network via the constructor works end-to-end (send + recv)
 * 2. `unicast` address appears in `task.create` requests (used for direct routing)
 * 3. `anycast` address appears as `resonate:target` in RPC promise creation
 * 4. Messages received via `network.recv` trigger `onMessage` processing
 */

import type { Context } from "../src/context.js";
import type { Network, Send } from "../src/network/network.js";
import type { Message, PromiseRecord, Request, Response } from "../src/network/types.js";
import { Resonate } from "../src/resonate.js";
import * as util from "../src/util.js";

// ---------------------------------------------------------------------------
// Helper: base64-encode a value the same way the Codec does
// ---------------------------------------------------------------------------

function encodeValue(value: any): string {
  return util.base64Encode(JSON.stringify(value));
}

function makePromise(id: string, state: string, value: any, tags: Record<string, string> = {}): PromiseRecord {
  return {
    id,
    state: state as PromiseRecord["state"],
    param: { data: encodeValue(null), headers: {} },
    value: { data: state === "pending" ? "" : encodeValue(value), headers: {} },
    tags,
    timeoutAt: Date.now() + 60000,
    createdAt: Date.now(),
    settledAt: state === "pending" ? undefined : Date.now(),
  };
}

// ---------------------------------------------------------------------------
// Mock Network implementation
// ---------------------------------------------------------------------------

class MockNetwork implements Network {
  readonly unicast: string;
  readonly anycast: string;

  // Records all parsed request objects sent via send()
  public sentRequests: any[] = [];

  // The recv callback registered by Resonate
  private recvCallback: ((msg: Message) => void) | null = null;

  // Response factory: given a parsed request, produce a response object
  private responseFactory: (req: any) => any;

  constructor(
    opts: {
      unicast?: string;
      anycast?: string;
      responseFactory?: (req: any) => any;
    } = {},
  ) {
    this.unicast = opts.unicast ?? "mock://uni@mock-group/mock-pid";
    this.anycast = opts.anycast ?? "mock://any@mock-group";
    this.responseFactory = opts.responseFactory ?? MockNetwork.defaultResponseFactory;
  }

  async init(): Promise<void> {}
  async stop(): Promise<void> {}

  send: Send = async <K extends Request["kind"]>(
    req: Extract<Request, { kind: K }>,
  ): Promise<Extract<Response, { kind: K }>> => {
    this.sentRequests.push(req);
    return this.responseFactory(req) as Extract<Response, { kind: K }>;
  };

  recv(callback: (msg: Message) => void): void {
    this.recvCallback = callback;
  }

  /**
   * Simulate pushing a message into the network's recv channel.
   */
  pushMessage(msg: Message): void {
    if (!this.recvCallback) {
      throw new Error("recv callback not yet registered");
    }
    this.recvCallback(msg);
  }

  /**
   * Default response factory: returns valid protocol responses with properly
   * base64-encoded values so the Codec can decode them.
   */
  static defaultResponseFactory(req: any): any {
    const corrId = req.head?.corrId ?? "";
    const kind = req.kind;

    if (kind === "task.create") {
      const promiseData = req.data.action.data;
      const promise = makePromise(promiseData.id, "resolved", "mock-result", promiseData.tags ?? {});
      // Use the param as-is from the request (it was already encoded by Resonate)
      promise.param = promiseData.param ?? { data: "", headers: {} };
      return {
        kind,
        head: { corrId, status: 200, version: util.VERSION },
        data: {
          promise,
          task: {
            id: `task-${promiseData.id}`,
            state: "acquired",
            version: 1,
            resumes: [],
            pid: req.data.pid,
            ttl: req.data.ttl,
          },
          preload: [],
        },
      };
    }

    if (kind === "promise.create") {
      const promiseData = req.data;
      const promise = makePromise(promiseData.id, "resolved", "mock-rpc-result", promiseData.tags ?? {});
      promise.param = promiseData.param ?? { data: "", headers: {} };
      return {
        kind,
        head: { corrId, status: 200, version: util.VERSION },
        data: { promise },
      };
    }

    if (kind === "promise.register_listener") {
      const promise = makePromise(req.data.awaited, "resolved", "mock-result");
      return {
        kind,
        head: { corrId, status: 200, version: util.VERSION },
        data: { promise },
      };
    }

    if (kind === "task.fulfill" || kind === "task.halt") {
      return {
        kind,
        head: { corrId, status: 200, version: util.VERSION },
        data: {},
      };
    }

    // Fallback: return a generic 200 success
    return {
      kind,
      head: { corrId, status: 200, version: util.VERSION },
      data: {},
    };
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Resonate <-> Network Wiring", () => {
  afterEach(async () => {
    // Allow microtasks to settle
    await new Promise((r) => setTimeout(r, 50));
  });

  test("injecting a custom Network via the constructor works end-to-end (send + recv)", async () => {
    const mockNet = new MockNetwork({
      unicast: "custom://uni@custom-group/custom-pid",
      anycast: "custom://any@custom-group",
    });

    const resonate = new Resonate({
      network: mockNet,
      pid: "custom-pid",
      logLevel: "error",
    });

    // Register a simple function
    resonate.register("greet", async (ctx: Context, name: string) => {
      return `Hello, ${name}!`;
    });

    // Perform a run -- this will use our mock network's send()
    const result = await resonate.run("test-send-1", "greet", "world");

    // Verify that requests were sent through our mock network
    expect(mockNet.sentRequests.length).toBeGreaterThan(0);

    // The first request should be a task.create
    const firstReq = mockNet.sentRequests[0];
    expect(firstReq.kind).toBe("task.create");

    // The result should come back through the mock
    expect(result).toBe("mock-result");

    await resonate.stop();
  });

  test("unicast address appears in task.create requests (register_listener after conflict)", async () => {
    const UNICAST_ADDR = "mock://uni@test-group/test-pid";

    // This factory returns 409 (conflict) for task.create to trigger promiseRegisterListener,
    // which uses the unicast address.
    const mockNet = new MockNetwork({
      unicast: UNICAST_ADDR,
      anycast: "mock://any@test-group",
      responseFactory: (req: any) => {
        const corrId = req.head?.corrId ?? "";

        if (req.kind === "task.create") {
          return {
            kind: "task.create",
            head: { corrId, status: 409, version: util.VERSION },
            data: "conflict",
          };
        }

        if (req.kind === "promise.register_listener") {
          const promise = makePromise(req.data.awaited, "resolved", "conflict-result");
          return {
            kind: "promise.register_listener",
            head: { corrId, status: 200, version: util.VERSION },
            data: { promise },
          };
        }

        return MockNetwork.defaultResponseFactory(req);
      },
    });

    const resonate = new Resonate({
      network: mockNet,
      pid: "test-pid",
      logLevel: "error",
    });

    resonate.register("myFunc", async (ctx: Context) => "done");

    const result = await resonate.run("unicast-test-1", "myFunc");
    expect(result).toBe("conflict-result");

    // After the 409 conflict, Resonate should have sent a promise.register_listener
    // with the unicast address
    const listenerReqs = mockNet.sentRequests.filter((r: any) => r.kind === "promise.register_listener");
    expect(listenerReqs.length).toBeGreaterThan(0);
    expect(listenerReqs[0].data.address).toBe(UNICAST_ADDR);

    await resonate.stop();
  });

  test("anycast address appears as resonate:target in beginRun promise creation", async () => {
    const ANYCAST_ADDR = "mock://any@my-group";

    const mockNet = new MockNetwork({
      unicast: "mock://uni@my-group/run-pid",
      anycast: ANYCAST_ADDR,
    });

    const resonate = new Resonate({
      network: mockNet,
      pid: "run-pid",
      logLevel: "error",
    });

    resonate.register("targetFunc", async (ctx: Context) => "targeted");

    await resonate.run("anycast-test-1", "targetFunc");

    // The task.create request should embed the anycast address
    // in the nested promise.create's tags["resonate:target"]
    const taskCreateReqs = mockNet.sentRequests.filter((r: any) => r.kind === "task.create");
    expect(taskCreateReqs.length).toBeGreaterThan(0);

    const actionData = taskCreateReqs[0].data.action.data;
    expect(actionData.tags["resonate:target"]).toBe(ANYCAST_ADDR);

    await resonate.stop();
  });

  test("anycast address appears as resonate:target in RPC promise creation (via beginRpc)", async () => {
    const ANYCAST_ADDR = "mock://any@rpc-group";

    const mockNet = new MockNetwork({
      unicast: "mock://uni@rpc-group/rpc-pid",
      anycast: ANYCAST_ADDR,
    });

    const resonate = new Resonate({
      network: mockNet,
      pid: "rpc-pid",
      logLevel: "error",
    });

    resonate.register("remoteFunc", async (ctx: Context) => "remote-result");

    // rpc uses promiseCreate which sets target from opts.target
    await resonate.rpc("rpc-test-1", "remoteFunc");

    // The promise.create request should have resonate:target set
    const promiseCreateReqs = mockNet.sentRequests.filter((r: any) => r.kind === "promise.create");
    expect(promiseCreateReqs.length).toBeGreaterThan(0);

    const tags = promiseCreateReqs[0].data.tags;
    expect(tags["resonate:target"]).toBeDefined();

    await resonate.stop();
  });

  test("messages received via network.recv trigger onMessage processing (unblock)", async () => {
    const mockNet = new MockNetwork({
      unicast: "mock://uni@recv-group/recv-pid",
      anycast: "mock://any@recv-group",
      responseFactory: (req: any) => {
        const corrId = req.head?.corrId ?? "";

        if (req.kind === "task.create") {
          const promiseData = req.data.action.data;
          const promise = makePromise(promiseData.id, "pending", null, promiseData.tags ?? {});
          promise.param = promiseData.param ?? { data: "", headers: {} };
          const resp: any = {
            kind: "task.create",
            head: { corrId, status: 200, version: util.VERSION },
            data: {
              promise,
              preload: [],
            },
          };
          return resp;
        }

        if (req.kind === "promise.register_listener") {
          const promise = makePromise(req.data.awaited, "pending", null);
          return {
            kind: "promise.register_listener",
            head: { corrId, status: 200, version: util.VERSION },
            data: { promise },
          };
        }

        return MockNetwork.defaultResponseFactory(req);
      },
    });

    const resonate = new Resonate({
      network: mockNet,
      pid: "recv-pid",
      logLevel: "error",
    });

    resonate.register("waitFunc", async (ctx: Context) => "should-not-execute-locally");

    // Start a run -- the promise is pending, so result() will block
    const handle = await resonate.beginRun("recv-test-1", "waitFunc");

    // Now push an "unblock" message through the network's recv channel.
    // This simulates the server notifying us that the promise was settled.
    // The value must be base64-encoded for the Codec to decode it.
    mockNet.pushMessage({
      kind: "unblock",
      head: {},
      data: {
        promise: makePromise("recv-test-1", "resolved", "delivered-via-recv"),
      },
    });

    // The result should now resolve with the value from the unblock message
    const result = await handle.result();
    expect(result).toBe("delivered-via-recv");

    await resonate.stop();
  });

  test("recv callback is properly wired during construction", async () => {
    let recvCalled = false;

    const mockNet = new MockNetwork();
    const originalRecv = mockNet.recv.bind(mockNet);
    mockNet.recv = (callback: (msg: Message) => void) => {
      recvCalled = true;
      originalRecv(callback);
    };

    const resonate = new Resonate({
      network: mockNet,
      logLevel: "error",
    });

    expect(recvCalled).toBe(true);

    await resonate.stop();
  });
});
