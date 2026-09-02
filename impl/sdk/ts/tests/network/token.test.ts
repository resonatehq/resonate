import { createServer, type Server } from "node:http";
import { afterEach, describe, expect, test } from "@jest/globals";
import { ResonateTimeoutException } from "../../src/exceptions.js";
import { HttpNetwork } from "../../src/network/http.js";
import {
  NoopTokenProvider,
  resolveTokenProvider,
  StaticTokenProvider,
  type TokenProvider,
} from "../../src/network/token.js";
import { VERSION } from "../../src/util.js";

// =============================================================================
// Helpers
// =============================================================================

function closeServer(server: Server): Promise<void> {
  return new Promise((resolve) => {
    server.closeAllConnections();
    server.close(() => resolve());
  });
}

/**
 * Starts a server that echoes the corrId from the request body. The captured
 * auth header is written to `capturedAuth` after each request.
 */
async function startCapturingServer(capturedAuth: { value: string }): Promise<{ server: Server; port: number }> {
  return new Promise((resolve, reject) => {
    const server = createServer((req, res) => {
      capturedAuth.value = req.headers.authorization ?? "<absent>";
      let body = "";
      req.on("data", (chunk: Buffer) => {
        body += chunk.toString();
      });
      req.on("end", () => {
        let corrId = "corr-1";
        try {
          const parsed = JSON.parse(body);
          if (parsed.head?.corrId) corrId = parsed.head.corrId;
        } catch {
          /* use default */
        }
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            kind: "promise.get",
            head: { corrId, status: 200, version: VERSION },
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
    });
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

function makeRequest(corrId = "corr-1") {
  return {
    kind: "promise.get" as const,
    head: { corrId, version: VERSION },
    data: { id: "p1" },
  };
}

// =============================================================================
// Unit tests: token.ts
// =============================================================================

describe("StaticTokenProvider", () => {
  test("returns the token string unchanged on every call", async () => {
    const provider = new StaticTokenProvider("my-token");
    expect(await provider.getToken()).toBe("my-token");
    expect(await provider.getToken()).toBe("my-token");
    expect(await provider.getToken()).toBe("my-token");
  });
});

describe("NoopTokenProvider", () => {
  test("returns undefined on every call", async () => {
    const provider = new NoopTokenProvider();
    expect(await provider.getToken()).toBeUndefined();
    expect(await provider.getToken()).toBeUndefined();
  });
});

describe("resolveTokenProvider", () => {
  test("tokenProvider wins over token", () => {
    const custom: TokenProvider = { getToken: async () => "custom" };
    expect(resolveTokenProvider(custom, "ignored")).toBe(custom);
  });

  test("returns StaticTokenProvider when only token is provided", async () => {
    const result = resolveTokenProvider(undefined, "abc123");
    expect(result).toBeInstanceOf(StaticTokenProvider);
    expect(await result.getToken()).toBe("abc123");
  });

  test("returns NoopTokenProvider when neither is provided", () => {
    expect(resolveTokenProvider(undefined, undefined)).toBeInstanceOf(NoopTokenProvider);
  });
});

// =============================================================================
// Integration: HttpNetwork with TokenProvider
// =============================================================================

describe("HttpNetwork with TokenProvider", () => {
  let server: Server | undefined;

  afterEach(async () => {
    if (server) {
      await closeServer(server);
      server = undefined;
    }
  });

  test("TokenProvider is called on each send() and token is sent as Bearer", async () => {
    let callCount = 0;
    const provider: TokenProvider = {
      getToken: async () => {
        callCount++;
        return `token-${callCount}`;
      },
    };

    const auth = { value: "" };
    const result = await startCapturingServer(auth);
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
      tokenProvider: provider,
    });

    await network.send(makeRequest("a1"));
    expect(auth.value).toBe("Bearer token-1");
    expect(callCount).toBe(1);

    await network.send(makeRequest("a2"));
    expect(auth.value).toBe("Bearer token-2");
    expect(callCount).toBe(2);
  });

  test("TokenProvider returning undefined sends no Authorization header", async () => {
    const provider: TokenProvider = { getToken: async () => undefined };

    const auth = { value: "" };
    const result = await startCapturingServer(auth);
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
      tokenProvider: provider,
    });

    await network.send(makeRequest());
    expect(auth.value).toBe("<absent>");
  });

  test("TokenProvider failures are wrapped as ResonateTimeoutException (platform failure)", async () => {
    const provider: TokenProvider = {
      getToken: async () => {
        throw new Error("metadata server unreachable");
      },
    };

    const auth = { value: "" };
    const result = await startCapturingServer(auth);
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
      tokenProvider: provider,
    });

    await expect(network.send(makeRequest())).rejects.toThrow(ResonateTimeoutException);
  });

  test("TokenProvider failure message includes the underlying cause", async () => {
    const provider: TokenProvider = {
      getToken: async () => {
        throw new Error("auth server is down");
      },
    };

    const auth = { value: "" };
    const result = await startCapturingServer(auth);
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
      tokenProvider: provider,
    });

    try {
      await network.send(makeRequest());
      fail("expected ResonateTimeoutException");
    } catch (e) {
      expect(e).toBeInstanceOf(ResonateTimeoutException);
      expect((e as Error).message).toContain("auth server is down");
    }
  });

  test("TokenProvider failure does not prevent subsequent successful requests", async () => {
    let shouldFail = true;
    const provider: TokenProvider = {
      getToken: async () => {
        if (shouldFail) {
          shouldFail = false;
          throw new Error("transient auth failure");
        }
        return "fresh-token";
      },
    };

    const auth = { value: "" };
    const result = await startCapturingServer(auth);
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
      tokenProvider: provider,
    });

    // First call: token fails → should throw
    await expect(network.send(makeRequest("c1"))).rejects.toThrow(ResonateTimeoutException);

    // Second call: token succeeds → should work
    await network.send(makeRequest("c2"));
    expect(auth.value).toBe("Bearer fresh-token");
  });

  test("tokenProvider takes precedence over token string", async () => {
    const provider: TokenProvider = { getToken: async () => "provider-wins" };

    const auth = { value: "" };
    const result = await startCapturingServer(auth);
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
      token: "should-not-use",
      tokenProvider: provider,
    });

    await network.send(makeRequest());
    expect(auth.value).toBe("Bearer provider-wins");
  });
});

// =============================================================================
// Backward compat: legacy `token` string still works
// =============================================================================

describe("HttpNetwork legacy token backwards compatibility", () => {
  let server: Server | undefined;

  afterEach(async () => {
    if (server) {
      await closeServer(server);
      server = undefined;
    }
  });

  test("token string is sent on every request", async () => {
    const auth = { value: "" };
    const result = await startCapturingServer(auth);
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
      token: "legacy-token",
    });

    await network.send(makeRequest("a"));
    expect(auth.value).toBe("Bearer legacy-token");

    await network.send(makeRequest("b"));
    expect(auth.value).toBe("Bearer legacy-token");
  });

  test("no token and no tokenProvider sends no auth header", async () => {
    const auth = { value: "" };
    const result = await startCapturingServer(auth);
    server = result.server;

    const network = new HttpNetwork({
      url: `http://127.0.0.1:${result.port}`,
      timeout: 500,
    });

    await network.send(makeRequest());
    expect(auth.value).toBe("<absent>");
  });
});
