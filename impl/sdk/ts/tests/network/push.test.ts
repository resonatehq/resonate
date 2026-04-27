import { HttpNetwork, PushMessageSource } from "../../src/network/http.js";
import type { Message } from "../../src/network/types.js";

describe("PushMessageSource (via HttpNetwork)", () => {
  let adapter: PushMessageSource;
  let network: HttpNetwork;

  afterEach(async () => {
    try {
      await network?.stop();
    } catch {
      // ignore errors on cleanup
    }
  });

  test("POST with valid body delivers typed Message to subscribers", async () => {
    adapter = new PushMessageSource();
    network = new HttpNetwork({ adapter });

    const received: Message[] = [];
    network.recv((msg) => received.push(msg));

    await network.init();

    // The unicast address is the HTTP URL of the push server
    const url = network.unicast;
    expect(url).toMatch(/^http:\/\//);

    const body = JSON.stringify({ kind: "execute", head: {}, data: { task: { id: "t1", version: 1 } } });
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    });

    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json).toEqual({ status: "ok" });

    // The parsed typed Message should have been delivered to the subscriber
    expect(received).toHaveLength(1);
    expect(received[0].kind).toBe("execute");
  });

  test("POST with invalid JSON is discarded", async () => {
    adapter = new PushMessageSource();
    network = new HttpNetwork({ adapter });

    const received: Message[] = [];
    network.recv((msg) => received.push(msg));

    await network.init();

    const url = network.unicast;
    await fetch(url, { method: "POST", body: "not-json{{{" });

    // Invalid JSON should be discarded, not delivered
    expect(received).toHaveLength(0);
  });

  test("POST with invalid message structure is discarded", async () => {
    adapter = new PushMessageSource();
    network = new HttpNetwork({ adapter });

    const received: Message[] = [];
    network.recv((msg) => received.push(msg));

    await network.init();

    const url = network.unicast;
    await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ notAMessage: true }),
    });

    // Invalid message structure should be discarded
    expect(received).toHaveLength(0);
  });

  test("OPTIONS request gets CORS headers (204)", async () => {
    adapter = new PushMessageSource();
    network = new HttpNetwork({ adapter });

    await network.init();

    const url = network.unicast;
    const res = await fetch(url, { method: "OPTIONS" });

    expect(res.status).toBe(204);
    expect(res.headers.get("access-control-allow-origin")).toBe("*");
    expect(res.headers.get("access-control-allow-methods")).toBe("POST, OPTIONS");
    expect(res.headers.get("access-control-allow-headers")).toBe("Content-Type");
  });

  test("non-POST request gets 405", async () => {
    adapter = new PushMessageSource();
    network = new HttpNetwork({ adapter });

    await network.init();

    const url = network.unicast;

    const getRes = await fetch(url, { method: "GET" });
    expect(getRes.status).toBe(405);
    const getBody = await getRes.json();
    expect(getBody).toEqual({ error: "Method not allowed" });

    const putRes = await fetch(url, { method: "PUT", body: "data" });
    expect(putRes.status).toBe(405);
  });

  test("init() listens on configured port, stop() closes the server", async () => {
    adapter = new PushMessageSource();
    network = new HttpNetwork({ adapter });

    await network.init();

    const url = network.unicast;
    // Server should be reachable
    const res = await fetch(url, { method: "POST", body: "ping" });
    expect(res.status).toBe(200);

    await network.stop();

    // After stop, the server should refuse connections
    await expect(fetch(url, { method: "POST", body: "ping" })).rejects.toThrow();
  });

  test("unicast and anycast addresses are set after init()", async () => {
    adapter = new PushMessageSource();
    network = new HttpNetwork({ adapter });

    await network.init();

    // Both addresses should be set to the same HTTP URL with the resolved port
    expect(network.unicast).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);
    expect(network.anycast).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);
    expect(network.unicast).toBe(network.anycast);
  });
});

/**
 * PollMessageSource tests
 *
 * PollMessageSource connects to an SSE endpoint and delivers messages to subscribers.
 * Testing it properly requires:
 * 1. A mock SSE server that emits events (using chunked transfer encoding with
 *    the text/event-stream content type)
 * 2. Handling the EventSource connection lifecycle (open, message, error events)
 * 3. Testing reconnection behavior (the current impl reconnects after 5s on error)
 *
 * Tests that would be valuable:
 * - messages received via SSE are delivered to all subscribers
 * - stop() closes the EventSource connection
 * - reconnection on error (with the current fixed 5s delay)
 * - auth headers are passed through to the SSE connection
 *
 * These tests are deferred because:
 * - PollMessageSource creates the EventSource in the constructor, making it hard
 *   to test without a running SSE server
 * - The reconnection timer (setTimeout 5000ms) makes tests slow without mocking
 * - The eventsource library requires a real HTTP connection (no easy in-process mock)
 *
 * A practical approach would be to refactor PollMessageSource to accept an
 * EventSource factory, allowing injection of a mock. This aligns with the
 * HttpAdapter refactor planned in Section 3.2.
 */
