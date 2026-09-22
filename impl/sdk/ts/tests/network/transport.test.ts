/**
 * These tests originally tested buildTransport's serialization and validation.
 * After Section 3.1, that logic moved into HttpNetwork.send() and HttpNetwork.recv().
 * The equivalent validation tests now live in http.test.ts.
 *
 * This file tests that LocalNetwork.send() also handles typed Request/Response correctly,
 * since LocalNetwork is another Network implementation.
 */

import { describe, expect, test } from "@jest/globals";
import { LocalNetwork } from "../../src/network/local.js";
import { VERSION } from "../../src/util.js";

describe("LocalNetwork typed send", () => {
  test("send accepts typed Request and returns typed Response", async () => {
    const network = new LocalNetwork();

    const req = {
      kind: "promise.create" as const,
      head: { corrId: "corr-1", version: VERSION },
      data: {
        id: "p1",
        timeoutAt: Date.now() + 60000,
        param: { headers: {}, data: "" },
        tags: {},
      },
    };

    const res = await network.send(req);
    expect(typeof res).toBe("object");
    expect(res.kind).toBe("promise.create");
    expect(res.head.corrId).toBe("corr-1");
    expect(res.head.status).toBe(200);
    const data = res.data as { promise: any };
    expect(data.promise).toBeDefined();
    expect(data.promise.id).toBe("p1");
  });

  test("send returns typed Response with correct kind", async () => {
    const network = new LocalNetwork();

    // Create a promise first
    await network.send({
      kind: "promise.create" as const,
      head: { corrId: "c1", version: VERSION },
      data: {
        id: "p2",
        timeoutAt: Date.now() + 60000,
        param: { headers: {}, data: "" },
        tags: {},
      },
    });

    // Get the promise
    const res = await network.send({
      kind: "promise.get" as const,
      head: { corrId: "c2", version: VERSION },
      data: { id: "p2" },
    });

    expect(res.kind).toBe("promise.get");
    expect(res.head.corrId).toBe("c2");
    const getData = res.data as { promise: any };
    expect(getData.promise.id).toBe("p2");
  });

  test("recv delivers typed Message objects", async () => {
    const network = new LocalNetwork();

    const received: any[] = [];
    network.recv((msg) => {
      received.push(msg);
    });

    // Create a task that will generate execute messages
    await network.send({
      kind: "task.create" as const,
      head: { corrId: "c1", version: VERSION },
      data: {
        pid: "test-pid",
        ttl: 60000,
        action: {
          kind: "promise.create" as const,
          head: { corrId: "c1", version: VERSION },
          data: {
            id: "p3",
            timeoutAt: Date.now() + 60000,
            param: { headers: {}, data: "" },
            tags: { "resonate:target": "local://any@default" },
          },
        },
      },
    });

    // Allow the setTimeout(0) in LocalNetwork to fire
    await new Promise((r) => setTimeout(r, 50));

    // Messages should be typed objects, not strings
    for (const msg of received) {
      expect(typeof msg).toBe("object");
      expect(typeof msg.kind).toBe("string");
    }
  });
});
