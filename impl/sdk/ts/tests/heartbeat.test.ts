import { afterEach, describe, expect, jest, test } from "@jest/globals";
import { AsyncHeartbeat } from "../src/heartbeat.js";
import { ConsoleLogger } from "../src/logger.js";
import type { Request } from "../src/network/types.js";
import type { Send } from "../src/types.js";

function buildHeartbeat(requests: Request[]) {
  const send: Send = async (request) => {
    requests.push(request);
    return { kind: request.kind, head: { status: 200 }, data: {} } as any;
  };

  return new AsyncHeartbeat("worker", 100, send, new ConsoleLogger("error"));
}

describe("AsyncHeartbeat", () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  test("sends the currently acquired task IDs and versions", async () => {
    jest.useFakeTimers();
    const requests: Request[] = [];
    const heartbeat = buildHeartbeat(requests);

    heartbeat.start({ id: "task-a", version: 2 });
    heartbeat.start({ id: "task-b", version: 4 });
    jest.advanceTimersByTime(100);
    await Promise.resolve();

    expect(requests).toHaveLength(1);
    expect(requests[0]).toMatchObject({
      kind: "task.heartbeat",
      data: {
        pid: "worker",
        tasks: [
          { id: "task-a", version: 2 },
          { id: "task-b", version: 4 },
        ],
      },
    });

    heartbeat.stop({ id: "task-a", version: 2 });
    heartbeat.stop({ id: "task-b", version: 4 });
    jest.advanceTimersByTime(100);
    expect(requests).toHaveLength(1);
  });

  test("does not remove a newer task version with an old stop", async () => {
    jest.useFakeTimers();
    const requests: Request[] = [];
    const heartbeat = buildHeartbeat(requests);

    heartbeat.start({ id: "task-a", version: 2 });
    heartbeat.start({ id: "task-a", version: 3 });
    heartbeat.stop({ id: "task-a", version: 2 });
    jest.advanceTimersByTime(100);
    await Promise.resolve();

    expect(requests[0]).toMatchObject({
      data: { tasks: [{ id: "task-a", version: 3 }] },
    });

    heartbeat.stop();
  });
});
