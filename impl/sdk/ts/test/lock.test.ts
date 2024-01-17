import { jest, describe, test, expect } from "@jest/globals";
import { Resonate, Context } from "../lib/resonate";
import { LocalLock } from "../lib/core/locks/local";

// Set a larger timeout for hooks (e.g., 10 seconds)
jest.setTimeout(10000);

const sharedResource: string[] = [];

function write(context: Context, id: string, final: boolean = false) {
  return new Promise((resolve) => {
    sharedResource.push(id);

    if (final) {
      resolve(sharedResource);
    }
  });
}

describe("Lock", () => {
  const lock = new LocalLock();
  const r1 = new Resonate({ lock: lock });
  const r2 = new Resonate({ lock: lock });

  r1.register("write", write);
  r2.register("write", write);

  test("Lock guards shared resource", async () => {
    r1.run("write", "id", "a", false, { eid: "a" });
    const p2 = r2.run("write", "id", "b", true, { eid: "b" });

    while (sharedResource.length === 0) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    expect(sharedResource.length).toBe(1);

    // release lock so p2 can run
    lock.release("write/id", sharedResource[0]);

    const r = await p2;

    expect(sharedResource.length).toBe(2);
    expect(sharedResource).toContain("a");
    expect(sharedResource).toContain("b");
    expect(sharedResource).toEqual(r);
  });
});
