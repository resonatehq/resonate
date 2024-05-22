import { describe, test, expect, jest, beforeEach } from "@jest/globals";
import { RemoteStore } from "../lib";

jest.setTimeout(10000);

describe("Auth", () => {
  // mock fetch
  // return a 500 so that an error is thrown (and ignored)
  const fetchMock = jest.fn(async () => new Response(null, { status: 500 }));
  global.fetch = fetchMock;

  const store = new RemoteStore("http://localhost:8080", {
    auth: { basic: { username: "foo", password: "bar" } },
    retries: 0,
  });

  // prettier-ignore
  const funcs = [
        // promises
        { name: "promises.get", func: () => store.promises.get("") },
        { name: "promises.create", func: () => store.promises.create("", undefined, false, undefined, undefined, 0, undefined) },
        { name: "promises.cancel", func: () => store.promises.cancel("", undefined, false, undefined, undefined) },
        { name: "promises.resolve", func: () => store.promises.resolve("", undefined, false, undefined, undefined) },
        { name: "promises.reject", func: () => store.promises.reject("", undefined, false, undefined, undefined) },
        { name: "promises.search", func: () => store.promises.search("", undefined, undefined, undefined).next() },

        // schedules
        { name: "schedules.get", func: () => store.schedules.get("") },
        { name: "schedules.create", func: () => store.schedules.create("", undefined, undefined, "", undefined, "", 0, undefined, undefined, undefined) },
        { name: "schedules.delete", func: () => store.schedules.delete("") },
        { name: "schedules.search", func: () => store.schedules.search("", undefined, undefined).next() },

        // locks
        { name: "locks.tryAcquire", func: () => store.locks.tryAcquire("", "") },
        { name: "locks.release", func: () => store.locks.release("", "") },
    ];

  beforeEach(() => {
    fetchMock.mockClear();
  });

  describe("basic auth", () => {
    for (const { name, func } of funcs) {
      test(name, async () => {
        await func().catch(() => {});

        expect(fetch).toHaveBeenCalledTimes(1);
        expect(fetch).toHaveBeenCalledWith(
          expect.any(String),
          expect.objectContaining({
            headers: expect.objectContaining({
              Authorization: `Basic Zm9vOmJhcg==`,
            }),
          }),
        );
      });
    }
  });
});
