import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { afterEach, beforeEach, describe, expect, test } from "@jest/globals";
import { ResonateTimeoutException } from "../../src/exceptions.js";
import { HttpNetwork } from "../../src/network/http.js";
import { VERSION } from "../../src/util.js";

// Helper: create a local HTTP server that responds on
function createTestServer(
  handler: (req: IncomingMessage, res: ServerResponse) => void,
): Promise<{ server: Server; port: number }> {
  return new Promise((resolve, reject) => {
    const server = createServer(handler);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (addr && typeof addr === "object") {
        resolve({ server, port: addr.port });
      } else {
        reject(new Error("Failed to get server address"));
      }
    });
    server.once("error", reject);
  });
}

function closeServer(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.closeAllConnections();
    server.close((err) => {
      if (!err || (err as NodeJS.ErrnoException).code === "ERR_SERVER_NOT_RUNNING") {
        resolve();
      } else {
        reject(err);
      }
    });
  });
}

/** Build a minimal valid PromiseGetReq for testing. */
function makeRequest(corrId = "corr-1") {
  return {
    kind: "promise.get" as const,
    head: { corrId, version: VERSION },
    data: { id: "p1" },
  };
}

describe("HttpNetwork.send()", () => {
  let server: Server | undefined;

  afterEach(async () => {
    if (server) {
      await closeServer(server);
      server = undefined;
    }
  });

  // =========================================================================
  // Valid protocol responses — returned as-is
  // =========================================================================

  test("returns typed Response for HTTP 200", async () => {
    const responseBody = JSON.stringify({
      kind: "promise.get",
      head: { corrId: "corr-1", status: 200, version: VERSION },
      data: {
        promise: {
          id: "p1",
          state: "pending",
          param: { headers: {}, data: "" },
          value: { headers: {}, data: "" },
          tags: {},
          timeoutAt: Date.now() + 60000,
          createdAt: Date.now(),
        },
      },
    });

    const result = await createTestServer((_req, res) => {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(responseBody);
    });
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
    });

    const response = await network.send(makeRequest());
    expect(typeof response).toBe("object");
    expect(response.kind).toBe("promise.get");
    expect(response.head.corrId).toBe("corr-1");
    expect(response.head.status).toBe(200);
  });

  // Non-200 protocol statuses
  const protocolStatuses = [404, 409, 422, 501];

  for (const status of protocolStatuses) {
    test(`returns typed Response for protocol HTTP ${status}`, async () => {
      const responseBody = JSON.stringify({
        kind: "promise.get",
        head: { corrId: "corr-1", status, version: VERSION },
        data: "not found",
      });

      const result = await createTestServer((_req, res) => {
        res.writeHead(status, { "Content-Type": "application/json" });
        res.end(responseBody);
      });
      server = result.server;

      const network = new HttpNetwork({
        url: `http://127.0.0.1:${result.port}`,
        timeout: 500,
      });

      const response = await network.send(makeRequest());
      expect(typeof response).toBe("object");
      expect(response.kind).toBe("promise.get");
      expect(response.head.corrId).toBe("corr-1");
      expect(response.head.status).toBe(status);
    });
  }

  test("returns typed Response for protocol HTTP 300 (redirect)", async () => {
    const responseBody = JSON.stringify({
      kind: "promise.get",
      head: { corrId: "corr-1", status: 300, version: VERSION },
      data: "redirect",
    });

    const result = await createTestServer((_req, res) => {
      res.writeHead(300, { "Content-Type": "application/json" });
      res.end(responseBody);
    });
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
    });

    const response = await network.send(makeRequest());
    expect(response.kind).toBe("promise.get");
    expect(response.head.status).toBe(300);
  });

  // =========================================================================
  // Platform failures — throw ResonateTimeoutException
  // =========================================================================

  test("throws ResonateTimeoutException on server timeout (no response)", async () => {
    const result = await createTestServer((_req, _res) => {
      // Intentionally never respond
    });
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 100,
    });

    await expect(network.send(makeRequest())).rejects.toThrow(ResonateTimeoutException);
  });

  test("throws ResonateTimeoutException on connection refused", async () => {
    // Use a port that is very likely not in use
    const network = new HttpNetwork({
      url: "http://127.0.0.1:19999",
      timeout: 500,
    });

    await expect(network.send(makeRequest())).rejects.toThrow(ResonateTimeoutException);
  });

  test("throws ResonateTimeoutException on malformed JSON response", async () => {
    const result = await createTestServer((_req, res) => {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end("this is not json{{{");
    });
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
    });

    await expect(network.send(makeRequest())).rejects.toThrow(ResonateTimeoutException);
  });

  test("throws ResonateTimeoutException on mismatched kind", async () => {
    const result = await createTestServer((_req, res) => {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          kind: "promise.create",
          head: { corrId: "corr-1", status: 200, version: VERSION },
          data: {},
        }),
      );
    });
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
    });

    await expect(network.send(makeRequest())).rejects.toThrow(ResonateTimeoutException);
  });

  test("throws ResonateTimeoutException on mismatched corrId", async () => {
    const result = await createTestServer((_req, res) => {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          kind: "promise.get",
          head: { corrId: "wrong-corr-id", status: 200, version: VERSION },
          data: {},
        }),
      );
    });
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
    });

    await expect(network.send(makeRequest())).rejects.toThrow(ResonateTimeoutException);
  });

  test("ResonateTimeoutException message includes cause", async () => {
    const network = new HttpNetwork({
      url: "http://127.0.0.1:19999",
      timeout: 500,
    });

    try {
      await network.send(makeRequest());
      fail("expected ResonateTimeoutException");
    } catch (e) {
      expect(e).toBeInstanceOf(ResonateTimeoutException);
      expect((e as ResonateTimeoutException).message).toMatch(/^platform failure:/);
    }
  });

  // =========================================================================
  // Non-protocol HTTP status codes — platform failures
  // =========================================================================

  for (const status of [400, 401, 403, 429, 500, 503]) {
    test(`throws ResonateTimeoutException for non-protocol HTTP ${status}`, async () => {
      const result = await createTestServer((_req, res) => {
        res.writeHead(status, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "server error" }));
      });
      server = result.server;

      const network = new HttpNetwork({
        url: `http://127.0.0.1:${result.port}`,
        timeout: 500,
      });

      try {
        await network.send(makeRequest());
        fail(`expected ResonateTimeoutException for HTTP ${status}`);
      } catch (e) {
        expect(e).toBeInstanceOf(ResonateTimeoutException);
        expect((e as ResonateTimeoutException).message).toMatch(new RegExp(`HTTP ${status}`));
      }

      await closeServer(result.server);
      server = undefined;
    });
  }

  // =========================================================================
  // Classification: protocol vs platform
  // =========================================================================

  test("protocol statuses (200, 300, 404, 409, 422, 501) are returned, not thrown", async () => {
    for (const status of [200, 300, 404, 409, 422, 501]) {
      const responseBody =
        status === 200
          ? JSON.stringify({
              kind: "promise.get",
              head: { corrId: "corr-1", status: 200, version: "1.0.0" },
              data: {
                promise: {
                  id: "p1",
                  state: "pending",
                  param: { headers: {}, data: "" },
                  value: { headers: {}, data: "" },
                  tags: {},
                  timeoutAt: Date.now() + 60000,
                  createdAt: Date.now(),
                },
              },
            })
          : JSON.stringify({
              kind: "promise.get",
              head: { corrId: "corr-1", status, version: VERSION },
              data: "protocol error",
            });

      const result = await createTestServer((_req, res) => {
        res.writeHead(status, { "Content-Type": "application/json" });
        res.end(responseBody);
      });

      const network = new HttpNetwork({
        url: `http://127.0.0.1:${result.port}`,
        timeout: 500,
      });

      const response = await network.send(makeRequest());
      expect(response.head.status).toBe(status);

      await closeServer(result.server);
    }
  });

  test("send throws ResonateTimeoutException on all platform failures", async () => {
    // Connection refused
    const network = new HttpNetwork({
      url: "http://127.0.0.1:19999",
      timeout: 100,
    });

    await expect(network.send(makeRequest())).rejects.toThrow(ResonateTimeoutException);
  });
});

// =============================================================================
// Environment variable configuration
// =============================================================================

describe("HttpNetwork environment variable configuration", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
    delete process.env.RESONATE_URL;
    delete process.env.RESONATE_TIMEOUT;
    delete process.env.RESONATE_TOKEN;
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  // ---- RESONATE_URL ----

  test("RESONATE_URL used as default when url not provided", async () => {
    let capturedUrl = "";
    const result = await createTestServer((req, res) => {
      capturedUrl = `http://${req.headers.host}${req.url}`;
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          kind: "promise.get",
          head: { corrId: "corr-1", status: 200, version: "1.0.0" },
          data: {
            promise: {
              id: "p1",
              state: "pending",
              param: { headers: {}, data: "" },
              value: { headers: {}, data: "" },
              tags: {},
              timeoutAt: Date.now() + 60000,
              createdAt: Date.now(),
            },
          },
        }),
      );
    });

    const port = result.port;
    process.env.RESONATE_URL = `http://127.0.0.1:${port}`;

    const network = new HttpNetwork({ timeout: 500 });
    await network.send(makeRequest());
    await closeServer(result.server);

    expect(capturedUrl).toBe(`http://127.0.0.1:${port}/`);
  });

  test("programmatic url takes precedence over RESONATE_URL", async () => {
    let capturedUrl = "";
    const result = await createTestServer((req, res) => {
      capturedUrl = `http://${req.headers.host}${req.url}`;
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          kind: "promise.get",
          head: { corrId: "corr-1", status: 200, version: "1.0.0" },
          data: {
            promise: {
              id: "p1",
              state: "pending",
              param: { headers: {}, data: "" },
              value: { headers: {}, data: "" },
              tags: {},
              timeoutAt: Date.now() + 60000,
              createdAt: Date.now(),
            },
          },
        }),
      );
    });

    const port = result.port;
    process.env.RESONATE_URL = "http://should-not-use:9999";

    const network = new HttpNetwork({ url: `http://127.0.0.1:${port}`, timeout: 500 });
    await network.send(makeRequest());
    await closeServer(result.server);

    expect(capturedUrl).toBe(`http://127.0.0.1:${port}/`);
  });

  // ---- RESONATE_TIMEOUT ----

  test("RESONATE_TIMEOUT used as default when timeout not provided", () => {
    process.env.RESONATE_TIMEOUT = "5000";

    const network = new HttpNetwork({});
    // Access the private timeout field via any cast for testing
    expect((network as any).timeout).toBe(5000);
  });

  test("programmatic timeout takes precedence over RESONATE_TIMEOUT", () => {
    process.env.RESONATE_TIMEOUT = "5000";

    const network = new HttpNetwork({ timeout: 2000 });
    expect((network as any).timeout).toBe(2000);
  });

  test("default timeout is 10s when neither programmatic nor env var set", () => {
    const network = new HttpNetwork({});
    expect((network as any).timeout).toBe(10000);
  });

  // ---- RESONATE_TOKEN ----

  test("RESONATE_TOKEN used as default when token not provided", async () => {
    let capturedAuth = "";
    const result = await createTestServer((req, res) => {
      capturedAuth = req.headers.authorization ?? "";
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          kind: "promise.get",
          head: { corrId: "corr-1", status: 200, version: "1.0.0" },
          data: {
            promise: {
              id: "p1",
              state: "pending",
              param: { headers: {}, data: "" },
              value: { headers: {}, data: "" },
              tags: {},
              timeoutAt: Date.now() + 60000,
              createdAt: Date.now(),
            },
          },
        }),
      );
    });

    process.env.RESONATE_TOKEN = "env-token-abc";

    const network = new HttpNetwork({ url: `http://127.0.0.1:${result.port}`, timeout: 500 });
    await network.send(makeRequest());
    await closeServer(result.server);

    expect(capturedAuth).toBe("Bearer env-token-abc");
  });

  test("programmatic token takes precedence over RESONATE_TOKEN", async () => {
    let capturedAuth = "";
    const result = await createTestServer((req, res) => {
      capturedAuth = req.headers.authorization ?? "";
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          kind: "promise.get",
          head: { corrId: "corr-1", status: 200, version: "1.0.0" },
          data: {
            promise: {
              id: "p1",
              state: "pending",
              param: { headers: {}, data: "" },
              value: { headers: {}, data: "" },
              tags: {},
              timeoutAt: Date.now() + 60000,
              createdAt: Date.now(),
            },
          },
        }),
      );
    });

    process.env.RESONATE_TOKEN = "env-token-should-not-use";

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
      token: "programmatic-token",
    });
    await network.send(makeRequest());
    await closeServer(result.server);

    expect(capturedAuth).toBe("Bearer programmatic-token");
  });
});
